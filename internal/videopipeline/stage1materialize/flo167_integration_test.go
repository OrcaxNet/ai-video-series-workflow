//go:build integration

package stage1materialize

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFLO167MaterializesIdenticallyAcrossFreshPostgresAndReplay(t *testing.T) {
	primaryDSN := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if primaryDSN == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is required")
	}
	packageBytes, err := os.ReadFile("../../../docs/flo-167/provider-free-execution-package.json")
	if err != nil {
		t.Fatal(err)
	}
	projectionBytes, err := os.ReadFile("../../../docs/flo-167/canonical-projection.json")
	if err != nil {
		t.Fatal(err)
	}
	var package_ stage1.FLO167SupersessionPackage
	var projection stage1.FLO167CanonicalProjection
	if json.Unmarshal(packageBytes, &package_) != nil || json.Unmarshal(projectionBytes, &projection) != nil {
		t.Fatal("decode delivered FLO-167 artifacts")
	}
	createdAt := time.Date(2026, 8, 3, 6, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	input := FLO167Materialization{LegacyActivationID: "142952f1-8dd1-5ebe-99c8-f2cb538ac702", Package: package_, Projection: projection, CreatedAt: createdAt}
	authorizationPayload, err := json.Marshal(map[string]any{
		"schemaVersion":           "flo100.batch-a-continuation-authorization.v3",
		"supersessionPackageHash": package_.ContentHash,
		"canonicalProjectionHash": projection.ContentHash,
		"pricingSnapshotDigest":   package_.Shots[0].Pricing.PricingSnapshotDigest,
		"decision": map[string]bool{"a02A10ProviderPostAuthorizedConditionally": true,
			"batchBProviderPostAuthorized": false, "batchCProviderPostAuthorized": false, "stage4Authorized": false},
	})
	if err != nil {
		t.Fatal(err)
	}
	authorization := FLO167Authorization{LegacyActivationID: input.LegacyActivationID,
		Payload: authorizationPayload, IssuedAt: createdAt.Add(time.Minute), ValidUntil: createdAt.Add(24 * time.Hour)}

	ctx := context.Background()
	dsns := make([]string, 0, 2)
	for index := range 2 {
		dsn := newFLO167FixtureDatabase(t, ctx, primaryDSN, index)
		dsns = append(dsns, dsn)
	}
	var canonical []byte
	for index, dsn := range dsns {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := MaterializeFLO167Supersession(ctx, pool, input); err != nil {
			pool.Close()
			t.Fatalf("database %d first materialization: %v", index+1, err)
		}
		if err := MaterializeFLO167Supersession(ctx, pool, input); err != nil {
			pool.Close()
			t.Fatalf("database %d exact replay: %v", index+1, err)
		}
		if err := AuthorizeFLO167Supersession(ctx, pool, authorization); err != nil {
			pool.Close()
			t.Fatalf("database %d authorization: %v", index+1, err)
		}
		if err := AuthorizeFLO167Supersession(ctx, pool, authorization); err != nil {
			pool.Close()
			t.Fatalf("database %d authorization replay: %v", index+1, err)
		}
		var stored []byte
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object(
			'package',package,'projection',canonical_projection,'packageHash',execution_package_hash,
			'projectionHash',canonical_projection_hash,'state',state,'authorizationHash',authorization_hash,
			'shots',(SELECT jsonb_agg(to_jsonb(ss) ORDER BY ordinal)
			         FROM video_pipeline.stage1_live_supersession_shots ss WHERE ss.supersession_id=s.id))
			FROM video_pipeline.stage1_live_supersessions s WHERE legacy_activation_id=$1`, input.LegacyActivationID).Scan(&stored); err != nil {
			pool.Close()
			t.Fatal(err)
		}
		pool.Close()
		var value any
		if json.Unmarshal(stored, &value) != nil {
			t.Fatal("decode stored canonical projection")
		}
		normalized, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			canonical = normalized
		} else if !reflect.DeepEqual(canonical, normalized) {
			t.Fatal("fresh PostgreSQL materializations produced different canonical bytes")
		}
	}
}

func TestFLO167RunnerRejectsPostgresPrepareBeforeProviderHTTP(t *testing.T) {
	primaryDSN := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	if primaryDSN == "" {
		t.Skip("VIDEO_TEST_POSTGRES_DSN is required")
	}
	ctx := context.Background()
	package_, projection, legacy, plan := loadFLO167RunnerFixtures(t)

	for index, test := range []struct {
		name   string
		mutate func(*testing.T, *pgxpool.Pool, *orchestration.SubscriptionQuotaSnapshot)
	}{
		{name: "stale quota", mutate: func(_ *testing.T, _ *pgxpool.Pool, quota *orchestration.SubscriptionQuotaSnapshot) {
			quota.CapturedAt = time.Now().UTC().Add(-10 * time.Minute)
		}},
		{name: "supersession hash drift", mutate: func(t *testing.T, pool *pgxpool.Pool, _ *orchestration.SubscriptionQuotaSnapshot) {
			if _, err := pool.Exec(ctx, `ALTER TABLE video_pipeline.stage1_live_supersessions DISABLE TRIGGER USER`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE video_pipeline.stage1_live_supersessions
				SET execution_package_hash=$1 WHERE legacy_activation_id=$2`, strings.Repeat("f", 64),
				"142952f1-8dd1-5ebe-99c8-f2cb538ac702"); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `ALTER TABLE video_pipeline.stage1_live_supersessions ENABLE TRIGGER USER`); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dsn := newFLO167FixtureDatabase(t, ctx, primaryDSN, index+10)
			pool, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			// PostgreSQL timestamps have microsecond precision. Freeze the input at
			// that boundary so the authorization's exact-replay assertion is
			// portable across clocks that expose nanoseconds.
			now := time.Now().UTC().Truncate(time.Microsecond)
			materialization := FLO167Materialization{
				LegacyActivationID: "142952f1-8dd1-5ebe-99c8-f2cb538ac702",
				Package:            package_, Projection: projection, CreatedAt: now,
			}
			if err := MaterializeFLO167Supersession(ctx, pool, materialization); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]any{
				"schemaVersion":           "flo100.batch-a-continuation-authorization.v3",
				"supersessionPackageHash": package_.ContentHash,
				"canonicalProjectionHash": projection.ContentHash,
				"pricingSnapshotDigest":   package_.Shots[0].Pricing.PricingSnapshotDigest,
				"decision": map[string]bool{"a02A10ProviderPostAuthorizedConditionally": true,
					"batchBProviderPostAuthorized": false, "batchCProviderPostAuthorized": false, "stage4Authorized": false},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := AuthorizeFLO167Supersession(ctx, pool, FLO167Authorization{
				LegacyActivationID: materialization.LegacyActivationID, Payload: payload,
				IssuedAt: now, ValidUntil: now.Add(time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			quota := orchestration.SubscriptionQuotaSnapshot{
				SchemaVersion: "agent-plan.quota-snapshot.v1", Source: "integration-test",
				CapturedAt: now, AccountID: "flo167-test-account", Profile: "agent-plan-large",
				Region: "cn-beijing", BillingMode: "subscription_included_only",
				FiveHourTotalAFPMilli: 135_000_000, WeeklyTotalAFPMilli: 135_000_000,
				MonthlyTotalAFPMilli: 135_000_000,
			}
			test.mutate(t, pool, &quota)

			gate, err := stage1.Open(plan, filepath.Join(t.TempDir(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := gate.BindExecutionPackage(legacy.ContentHash); err != nil {
				t.Fatal(err)
			}
			a01 := legacy.PrimaryJobs[0]
			a01Attempt := stage1.Attempt{AttemptID: a01.AttemptID, ShotID: a01.ShotID,
				IdempotencyKey: a01.IdempotencyKey, EstimatedVideoTokens: a01.EstimatedVideoTokens,
				PredictedAFPMilli:                  a01.PredictedAFPMilli,
				EstimatedNonSubscriptionCashMicros: a01.EstimatedNonSubscriptionCashMicros}
			if _, err := gate.Authorize(a01Attempt); err != nil {
				t.Fatal(err)
			}
			if err := gate.Complete(a01.IdempotencyKey, stage1.Completion{State: "TERMINAL_SUCCEEDED",
				ProviderTaskID: "legacy-a01", ActualVideoTokens: 87_300, ActualAFPMilli: 2_007_900,
				EvidenceComplete: true}); err != nil {
				t.Fatal(err)
			}

			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				http.Error(w, "provider must not be called", http.StatusInternalServerError)
			}))
			defer server.Close()
			adapter, err := stage1.NewAdapterSubmitter(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			runner, err := stage1.NewRunnerWithFLO167Supersession(gate, adapter,
				flo167ArtifactVerifier{}, repository.NewForPool(pool), legacy, package_)
			if err != nil {
				t.Fatal(err)
			}
			before := flo167PaidBoundaryCounts(t, ctx, pool)
			if _, err := runner.Submit(ctx, stage1.SubmitInput{ShotID: "GOLD-A02", QuotaSnapshot: &quota}); err == nil {
				t.Fatal("Runner accepted rejected PostgreSQL preparation")
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("provider HTTP calls=%d, want 0", got)
			}
			if after := flo167PaidBoundaryCounts(t, ctx, pool); after != before {
				t.Fatalf("paid-boundary rows changed: before=%v after=%v", before, after)
			}
			ledger, err := gate.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if ledger.Records[legacy.PrimaryJobs[1].IdempotencyKey] != nil {
				t.Fatal("rejected prepare reserved A02 in the local ledger")
			}
		})
	}
}

type flo167ArtifactVerifier struct{}

func (flo167ArtifactVerifier) Exists(string) (bool, error) { return true, nil }

func loadFLO167RunnerFixtures(t *testing.T) (stage1.FLO167SupersessionPackage, stage1.FLO167CanonicalProjection, stage1.ExecutionPackage, stage1.Plan) {
	t.Helper()
	var package_ stage1.FLO167SupersessionPackage
	var projection stage1.FLO167CanonicalProjection
	var legacy stage1.ExecutionPackage
	var plan stage1.Plan
	for path, target := range map[string]any{
		"../../../docs/flo-167/provider-free-execution-package.json": &package_,
		"../../../docs/flo-167/canonical-projection.json":            &projection,
		"testdata/flo167_legacy_execution_package.json":              &legacy,
		"testdata/flo167_legacy_readiness_plan.json":                 &plan,
	} {
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload, target); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	return package_, projection, legacy, plan
}

func flo167PaidBoundaryCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [3]int64 {
	t.Helper()
	var counts [3]int64
	for index, table := range []string{"provider_jobs", "budget_reservations", "cost_ledger"} {
		if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM video_pipeline."+table).Scan(&counts[index]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func newFLO167FixtureDatabase(t *testing.T, ctx context.Context, sourceDSN string, index int) string {
	t.Helper()
	config, err := pgx.ParseConfig(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	databaseName := fmt.Sprintf("flo167_%d_%s", index, strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminConfig := config.Copy()
	adminConfig.Database = "postgres"
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" TEMPLATE template0"); err != nil {
		admin.Close(ctx)
		t.Fatal(err)
	}
	admin.Close(ctx)
	t.Cleanup(func() {
		cleanup, connectErr := pgx.ConnectConfig(context.Background(), adminConfig)
		if connectErr != nil {
			t.Errorf("connect for fixture cleanup: %v", connectErr)
			return
		}
		defer cleanup.Close(context.Background())
		if _, dropErr := cleanup.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{databaseName}.Sanitize()+" WITH (FORCE)"); dropErr != nil {
			t.Errorf("drop fixture database: %v", dropErr)
		}
	})
	dsnURL, err := url.Parse(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	dsnURL.Path = "/" + databaseName
	dsn := dsnURL.String()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	migrationFiles, err := filepath.Glob("../../../video-pipeline/db/migrations/*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(migrationFiles)
	for _, path := range migrationFiles {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(path), err)
		}
	}
	fixture, err := os.ReadFile("testdata/flo167_legacy_a01.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(fixture)); err != nil {
		t.Fatalf("restore repository FLO-167 fixture: %v", err)
	}
	return dsn
}
