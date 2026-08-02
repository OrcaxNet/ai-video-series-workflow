//go:build integration

package volcengineprovider

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1materialize"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFixedSample1MaterializationBuildsAuthenticatedCASProviderEnvelope(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	files := stage1materialize.Files{
		Product: os.Getenv("VIDEO_TEST_STAGE1_PRODUCT_PATH"),
		Source:  os.Getenv("VIDEO_TEST_STAGE1_SOURCE_PATH"),
		Safety:  os.Getenv("VIDEO_TEST_STAGE1_SAFETY_PATH"),
		Visual:  os.Getenv("VIDEO_TEST_STAGE1_VISUAL_PATH"),
	}
	if dsn == "" || files.Product == "" || files.Source == "" ||
		files.Safety == "" || files.Visual == "" {
		t.Skip("disposable PostgreSQL and fixed Stage 1 input paths are not configured")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	planBytes, err := os.ReadFile("../../../video-pipeline/config/flo104-stage1-readiness.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan stage1.Plan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		t.Fatal(err)
	}
	casRoot := t.TempDir()
	sourceCAS, err := artifactstore.New(casRoot)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := stage1materialize.Materialize(
		ctx, pool, sourceCAS, plan, files,
		stage1materialize.Approval{
			CommentID:  "364c1c9f-90f0-401a-9431-285f2d3dc052",
			ActorID:    "16bbc49e-750f-432d-9ba4-b33ef6812026",
			ValidUntil: time.Date(2026, time.August, 8, 0, 0, 0, 0, time.UTC),
		},
	)
	if err != nil {
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
	provider := &fakeProvider{}
	if _, err := pool.Exec(ctx, `
		UPDATE video_pipeline.generation_attempts
		SET state = 'FAILED', input_hash = $2
		WHERE generation_run_id = $1 AND sequence = 1`,
		job.Run.RunID, strings.Repeat("f", 64),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.NewForPool(pool).PrepareProviderJob(
		ctx,
		orchestration.WorkflowStep{
			WorkflowID: job.WorkflowID, ActivityID: job.ActivityID,
			ActivityType: orchestration.ActivityExecuteProviderJob,
			TraceID:      job.TraceID,
		},
		preparation,
	); err == nil {
		t.Fatal("drifted fixed attempt crossed the paid Provider boundary")
	} else {
		var domain *controlplane.DomainError
		if !errors.As(err, &domain) || domain.Code != controlplane.CodeRevisionConflict {
			t.Fatalf("drifted fixed attempt error = %#v", err)
		}
	}
	var rejectedReservations, rejectedJobs, rejectedCosts int
	if err := pool.QueryRow(ctx, `
		SELECT
		  (SELECT count(*) FROM video_pipeline.budget_reservations
		   WHERE generation_run_id = $1),
		  (SELECT count(*) FROM video_pipeline.provider_jobs pj
		   JOIN video_pipeline.generation_attempts ga
		     ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1),
		  (SELECT count(*) FROM video_pipeline.cost_ledger cl
		   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
		   JOIN video_pipeline.generation_attempts ga
		     ON ga.id = pj.generation_attempt_id
		   WHERE ga.generation_run_id = $1)`, job.Run.RunID,
	).Scan(&rejectedReservations, &rejectedJobs, &rejectedCosts); err != nil {
		t.Fatal(err)
	}
	if rejectedReservations != 0 || rejectedJobs != 0 || rejectedCosts != 0 {
		t.Fatalf(
			"drifted fixed attempt side effects = reservations:%d jobs:%d costs:%d",
			rejectedReservations, rejectedJobs, rejectedCosts,
		)
	}
	if submits, polls, cancels := provider.counts(); submits != 0 || polls != 0 || cancels != 0 {
		t.Fatalf(
			"drifted fixed attempt fake upstream calls = submit:%d poll:%d cancel:%d",
			submits, polls, cancels,
		)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE video_pipeline.generation_attempts
		SET state = 'VALIDATED', input_hash = $2
		WHERE generation_run_id = $1 AND sequence = 1`,
		job.Run.RunID, job.Run.RunSpecDigest,
	); err != nil {
		t.Fatal(err)
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
