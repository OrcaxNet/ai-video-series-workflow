//go:build integration

package volcengineprovider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/jackc/pgx/v5/pgxpool"
)

const fixedSample1InputHash = "a33ac48df5d413d1467e5716821073a8baebe69f76b2125263118e8f06972e30"

func TestFixedSample1MaterializationBuildsAuthenticatedCASProviderEnvelope(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	casRoot := os.Getenv("VIDEO_TEST_ARTIFACT_ROOT")
	if dsn == "" || casRoot == "" {
		t.Skip("fixed Stage 1 PostgreSQL and CAS evidence are not configured")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var packageJSON []byte
	if err := pool.QueryRow(ctx, `
		SELECT payload->'executionPackage'
		FROM video_pipeline.audit_events
		WHERE action = 'stage1.execution_package.materialized'
		  AND payload->>'inputPackageHash' = $1
		ORDER BY occurred_at DESC
		LIMIT 1`, fixedSample1InputHash,
	).Scan(&packageJSON); err != nil {
		t.Fatal(err)
	}
	var execution stage1.ExecutionPackage
	if err := json.Unmarshal(packageJSON, &execution); err != nil {
		t.Fatal(err)
	}
	if len(execution.PrimaryJobs) != stage1.RequiredPrimaryJobs {
		t.Fatalf("fixed primary jobs = %d", len(execution.PrimaryJobs))
	}
	job := execution.PrimaryJobs[0]
	prompt, err := repository.NewForPool(pool).ResolvePromptSnapshot(ctx, job.PromptSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(prompt.Assets) != 2 {
		t.Fatalf("fixed prompt assets = %d, want visual and voice", len(prompt.Assets))
	}

	expectedTruth := orchestration.PreparedProductTruth{
		ShotSpecRevisionID:  job.ShotSpecRevisionID,
		Run:                 job.Run,
		PromptSnapshotID:    job.PromptSnapshotID,
		PromptSnapshotHash:  job.PromptSnapshotHash,
		GenerationPlanID:    job.GenerationPlanID,
		BudgetApprovalID:    job.BudgetApprovalID,
		BudgetMaximumMicros: job.BudgetMaximumMicros,
		BudgetCurrency:      job.BudgetCurrency,
		ProviderProfileID:   job.ProviderProfileID,
		Route:               job.Route,
	}
	preparation := orchestration.ExecuteProviderJobInput{
		Run: job.Run, Prompt: prompt, Route: job.Route,
		BudgetApprovalID:     job.BudgetApprovalID,
		BudgetMaximumMicros:  job.BudgetMaximumMicros,
		BudgetCurrency:       job.BudgetCurrency,
		ProviderProfileID:    job.ProviderProfileID,
		TraceID:              job.TraceID,
		PersistProductTruth:  true,
		ExpectedProductTruth: &expectedTruth,
	}
	prepared, err := repository.NewForPool(pool).PrepareProviderJob(
		ctx,
		orchestration.WorkflowStep{
			WorkflowID: job.WorkflowID, ActivityID: job.ActivityID,
			ActivityType: orchestration.ActivityExecuteProviderJob,
			TraceID:      job.TraceID,
		},
		preparation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ProductTruth != expectedTruth {
		t.Fatal("prepared Provider job differs from the sealed Stage 1 product truth")
	}
	request, err := orchestration.BuildProviderJobRequest(
		preparation,
		prepared,
	)
	if err != nil {
		t.Fatal(err)
	}

	sourceCAS, err := artifactstore.New(casRoot)
	if err != nil {
		t.Fatal(err)
	}
	adapterCAS, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range request.Request.Assets {
		reader, err := sourceCAS.Open(asset.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		copied, putErr := adapterCAS.Put(ctx, reader)
		closeErr := reader.Close()
		if putErr != nil {
			t.Fatal(putErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if copied.Digest != asset.SHA256 || copied.Size != asset.SizeBytes {
			t.Fatal("fixed asset bytes differ from PostgreSQL CAS evidence")
		}
	}

	provider := &fakeProvider{}
	adapter, err := New(testLiveConfig(), provider, adapterCAS, Options{
		DownloadClient: http.DefaultClient, Inspector: fixedInspector{},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()
	postJob(t, server.URL, request)

	submitted := provider.submittedRequest()
	if submits, _, _ := provider.counts(); submits != 1 {
		t.Fatalf("fixed fake upstream submits = %d, want 1", submits)
	}
	if len(submitted.Assets) != 1 ||
		submitted.Assets[0].Kind != providercontract.ModalityImage ||
		submitted.Assets[0].SizeBytes != 2_398_914 {
		t.Fatalf("fixed upstream visual evidence = %#v", submitted.Assets)
	}
}
