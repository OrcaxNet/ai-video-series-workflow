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
