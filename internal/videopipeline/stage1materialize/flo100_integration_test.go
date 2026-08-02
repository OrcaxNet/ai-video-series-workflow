//go:build integration

package stage1materialize

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMaterializeFLO100PersistsReplaysAndStaysProviderFree(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	root := os.Getenv("VIDEO_TEST_FLO100_PACK_PATH")
	if dsn == "" || root == "" {
		t.Skip("disposable PostgreSQL and VIDEO_TEST_FLO100_PACK_PATH are required")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cas, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := FormalOptions{
		Root: root, ExpectedPackageHash: FormalExpectedPackageHash(),
		Approval: Approval{
			CommentID:  "5b92b347-3ce9-4e7b-831a-1f00d1454d78",
			ActorID:    "16bbc49e-750f-432d-9ba4-b33ef6812026",
			ValidUntil: time.Date(2026, time.August, 31, 15, 59, 59, 0, time.UTC),
		},
	}
	prepared, err := prepareFormal(options)
	if err != nil {
		t.Fatal(err)
	}
	initial, report, err := MaterializeFLO100(ctx, pool, cas, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 3 || report.Counts["shots"] != 30 || report.Counts["assets"] != 8 ||
		len(report.IntentIdempotencyKeys) != 30 || report.ProviderCalls != 0 ||
		report.ProviderJobs != 0 || report.BudgetReservations != 0 || report.CostLedgerEntries != 0 ||
		report.NonSubscriptionCashMicros != 0 || report.LiveExecutionAuthorized {
		t.Fatalf("unexpected formal materialization report: %+v", report)
	}
	expectedPackageHashes := []string{
		"56d21bcec47934b11290f9760c0650e5c37e244ee756b4c76056240fdeebf260",
		"5e38bac52679ec42f44c9c89380e92ba7a86926642d63457a1c44cb39e97cef4",
		"c26b39d18de621b6af47c211effaaa1ffe9966da6e58de6e3c900558cc0e7fb6",
	}
	for index := range initial {
		if initial[index].ContentHash != expectedPackageHashes[index] ||
			report.ExecutionPackages[index].ExecutionPackageHash != expectedPackageHashes[index] {
			t.Fatalf("batch %d package hash changed: package=%s report=%s", index+1,
				initial[index].ContentHash, report.ExecutionPackages[index].ExecutionPackageHash)
		}
	}
	replayed, replayReport, err := MaterializeFLO100(ctx, pool, cas, options)
	if err != nil {
		t.Fatal(err)
	}
	for index := range initial {
		if initial[index].ContentHash != replayed[index].ContentHash ||
			initial[index].ContentHash != replayReport.ExecutionPackages[index].ExecutionPackageHash {
			t.Fatalf("batch %d replay changed its sealed package", index+1)
		}
	}
	var auditsBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.audit_events
		WHERE action='flo100.execution_package.materialized'`).Scan(&auditsBefore); err != nil {
		t.Fatal(err)
	}
	driftCases := []struct {
		name         string
		mutateQuery  string
		mutateArgs   []any
		restoreQuery string
		restoreArgs  []any
	}{
		{
			name: "provider profile becomes live",
			mutateQuery: `UPDATE video_pipeline.provider_profiles
				SET enabled=true,mode='LIVE',health='READY' WHERE id=$1`,
			mutateArgs: []any{formalUUID("provider-profile:video")},
			restoreQuery: `UPDATE video_pipeline.provider_profiles
				SET enabled=false,mode='DRY_RUN',health='NOT_CONFIGURED' WHERE id=$1`,
			restoreArgs: []any{formalUUID("provider-profile:video")},
		},
		{
			name: "generation budget exceeds frozen boundary",
			mutateQuery: `UPDATE video_pipeline.generation_profiles SET budget_policy=$2
				WHERE id=$1`,
			mutateArgs: []any{formalUUID("generation-profile:" + formalBatchIDs[0]), map[string]any{
				"maximumNonSubscriptionCashMicros": int64(1_000_000),
				"monthlyAccountCapAfpMilli":        int64(999_999_999),
			}},
			restoreQuery: `UPDATE video_pipeline.generation_profiles SET budget_policy=$2
				WHERE id=$1`,
			restoreArgs: []any{formalUUID("generation-profile:" + formalBatchIDs[0]), map[string]any{
				"maximumNonSubscriptionCashMicros": int64(0),
				"monthlyAccountCapAfpMilli":        int64(135_000_000),
			}},
		},
		{
			name: "asset is approved while G1 remains returned",
			mutateQuery: `UPDATE video_pipeline.asset_versions SET status='APPROVED'
				WHERE id=$1`,
			mutateArgs: []any{mustUUID(prepared.assets.Versions[0].AssetVersionID)},
			restoreQuery: `UPDATE video_pipeline.asset_versions SET status='DRAFT'
				WHERE id=$1`,
			restoreArgs: []any{mustUUID(prepared.assets.Versions[0].AssetVersionID)},
		},
	}
	for _, testCase := range driftCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := pool.Exec(ctx, testCase.mutateQuery, testCase.mutateArgs...)
			if err != nil {
				t.Fatal(err)
			}
			if result.RowsAffected() != 1 {
				t.Fatalf("mutated %d rows, expected 1", result.RowsAffected())
			}
			restored := false
			t.Cleanup(func() {
				if restored {
					return
				}
				result, err := pool.Exec(ctx, testCase.restoreQuery, testCase.restoreArgs...)
				if err != nil {
					t.Errorf("restore drift probe: %v", err)
					return
				}
				if result.RowsAffected() != 1 {
					t.Errorf("restored %d rows in cleanup, expected 1", result.RowsAffected())
				}
			})
			packages, driftReport, err := MaterializeFLO100(ctx, pool, cas, options)
			if err == nil {
				t.Fatal("replay accepted a drifted PostgreSQL projection")
			}
			if len(packages) != 0 || len(driftReport.ExecutionPackages) != 0 || len(driftReport.Counts) != 0 {
				t.Fatalf("projection drift returned a package or report: packages=%d report=%+v", len(packages), driftReport)
			}
			var auditsAfterDrift int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.audit_events
				WHERE action='flo100.execution_package.materialized'`).Scan(&auditsAfterDrift); err != nil {
				t.Fatal(err)
			}
			if auditsAfterDrift != auditsBefore {
				t.Fatalf("projection drift changed durable audits: %d -> %d", auditsBefore, auditsAfterDrift)
			}
			result, err = pool.Exec(ctx, testCase.restoreQuery, testCase.restoreArgs...)
			if err != nil {
				t.Fatal(err)
			}
			if result.RowsAffected() != 1 {
				t.Fatalf("restored %d rows, expected 1", result.RowsAffected())
			}
			restored = true
		})
	}
	if _, _, err := MaterializeFLO100(ctx, pool, cas, options); err != nil {
		t.Fatalf("exact replay failed after restoring drift probes: %v", err)
	}
	tamperedRoot := t.TempDir()
	if err := os.CopyFS(tamperedRoot, os.DirFS(root)); err != nil {
		t.Fatal(err)
	}
	tamperedProduct := filepath.Join(tamperedRoot, "batches", "flo100-gold-a-v1", "product-input.json")
	data, err := os.ReadFile(tamperedProduct)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tamperedProduct, append(data, ' '), 0o600); err != nil {
		t.Fatal(err)
	}
	tamperedOptions := options
	tamperedOptions.Root = tamperedRoot
	if _, _, err := MaterializeFLO100(ctx, pool, cas, tamperedOptions); err == nil {
		t.Fatal("tampered formal package was accepted")
	}
	var auditsAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.audit_events
		WHERE action='flo100.execution_package.materialized'`).Scan(&auditsAfter); err != nil {
		t.Fatal(err)
	}
	if auditsAfter != auditsBefore {
		t.Fatalf("tamper rejection changed durable materialization audits: %d -> %d", auditsBefore, auditsAfter)
	}
}
