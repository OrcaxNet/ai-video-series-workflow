//go:build integration

package stage1materialize

import (
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMaterializePersistsAndRepairsImmutableGenerationPlanBindings(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	files := Files{
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
	cas, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	validUntil := time.Date(2026, time.August, 8, 0, 0, 0, 0, time.UTC)
	approval := Approval{
		CommentID:  "364c1c9f-90f0-401a-9431-285f2d3dc052",
		ActorID:    "16bbc49e-750f-432d-9ba4-b33ef6812026",
		ValidUntil: validUntil,
	}

	initial, _, err := Materialize(ctx, pool, cas, plan, files, approval)
	if err != nil {
		t.Fatal(err)
	}
	runIDs := make([]uuid.UUID, 0, len(initial.PrimaryJobs))
	for _, job := range initial.PrimaryJobs {
		runIDs = append(runIDs, uuid.MustParse(job.Run.RunID))
	}
	var bindings int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM video_pipeline.audit_events
		WHERE aggregate_type = 'GENERATION_RUN'
		  AND action = 'generation_run.created'
		  AND aggregate_id = ANY($1)
		  AND payload->>'generationPlanId' = $2`,
		runIDs, initial.PrimaryJobs[0].GenerationPlanID,
	).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != stage1.RequiredPrimaryJobs {
		t.Fatalf("initial immutable plan bindings = %d, want %d", bindings, stage1.RequiredPrimaryJobs)
	}

	legacy := initial.PrimaryJobs[0]
	legacy.Run.RunID = uuid.NewString()
	legacy.Run.Attempt = int(integrationCreativeAttempt.Add(1)) + 1
	legacy.AttemptID = "legacy-imported-attempt"
	legacy.IdempotencyKey = "provider-job-" + legacy.Run.RunID
	legacy.WorkflowID = "legacy-imported-" + legacy.Run.RunID
	legacy.ActivityID = "submit-legacy-imported"
	legacy.Run.RunSpecDigest, err = repository.GenerationRunSpecDigest(
		legacy.ShotSpecRevisionID, legacy.PromptSnapshotID,
		legacy.PromptSnapshotHash, "10400000-0000-4000-8000-00000000000a",
		legacy.GenerationPlanID, legacy.Route, legacy.Run.Attempt,
	)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.generation_runs
			(id, shot_spec_revision_id, prompt_snapshot_id, generation_profile_id,
			 temporal_workflow_id, run_spec_digest, creative_attempt, state, dry_run,
			 budget_approval_id, fallback_reason, trace_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'VALIDATED', false, $8,
		        'integration-legacy-repair', $9, 'integration-test')`,
		uuid.MustParse(legacy.Run.RunID), uuid.MustParse(legacy.ShotSpecRevisionID),
		uuid.MustParse(legacy.PromptSnapshotID),
		uuid.MustParse("10400000-0000-4000-8000-00000000000a"),
		legacy.WorkflowID, legacy.Run.RunSpecDigest, legacy.Run.Attempt,
		legacy.BudgetApprovalID, legacy.TraceID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO video_pipeline.generation_attempts
			(id, generation_run_id, sequence, attempt_kind, state, input_hash, model_snapshot)
		VALUES ($1, $2, 1, 'CREATIVE_REVISION', 'VALIDATED', $3, $4)`,
		uuid.New(), uuid.MustParse(legacy.Run.RunID), legacy.Run.RunSpecDigest, legacy.Route,
	); err != nil {
		t.Fatal(err)
	}
	if err := ensureGenerationRunPlanBindings(
		ctx, tx, stage1.ExecutionPackage{PrimaryJobs: []stage1.FrozenJob{legacy}},
		"10400000-0000-4000-8000-00000000000a", approval,
		fixedSample1ProductHash, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var repairedPlan string
	if err := pool.QueryRow(ctx, `
		SELECT payload->>'generationPlanId'
		FROM video_pipeline.audit_events
		WHERE aggregate_type = 'GENERATION_RUN'
		  AND action = 'generation_run.created'
		  AND aggregate_id = $1`,
		uuid.MustParse(legacy.Run.RunID),
	).Scan(&repairedPlan); err != nil {
		t.Fatal(err)
	}
	if repairedPlan != legacy.GenerationPlanID {
		t.Fatalf("repaired generation plan = %q, want %q", repairedPlan, legacy.GenerationPlanID)
	}
	driftCases := []struct {
		name       string
		state      string
		inputHash  string
		kind       string
		sequence   int
		driftRoute bool
	}{
		{name: "failed attempt", state: "FAILED"},
		{name: "cancelled attempt", state: "CANCELLED"},
		{name: "succeeded attempt", state: "SUCCEEDED"},
		{name: "input hash drift", inputHash: strings.Repeat("f", 64)},
		{name: "attempt kind drift", kind: "PROVIDER_REQUEST"},
		{name: "attempt sequence drift", sequence: 2},
		{name: "attempt route drift", driftRoute: true},
	}
	for _, test := range driftCases {
		t.Run(test.name, func(t *testing.T) {
			invalid := initial.PrimaryJobs[0]
			invalid.Run.RunID = uuid.NewString()
			invalid.Run.Attempt = int(integrationCreativeAttempt.Add(1)) + 1
			invalid.AttemptID = "invalid-imported-attempt-" + invalid.Run.RunID
			invalid.IdempotencyKey = "provider-job-" + invalid.Run.RunID
			invalid.WorkflowID = "invalid-imported-" + invalid.Run.RunID
			invalid.ActivityID = "submit-invalid-imported"
			invalid.Run.RunSpecDigest, err = repository.GenerationRunSpecDigest(
				invalid.ShotSpecRevisionID, invalid.PromptSnapshotID,
				invalid.PromptSnapshotHash, "10400000-0000-4000-8000-00000000000a",
				invalid.GenerationPlanID, invalid.Route, invalid.Run.Attempt,
			)
			if err != nil {
				t.Fatal(err)
			}
			attemptState := "VALIDATED"
			if test.state != "" {
				attemptState = test.state
			}
			attemptInputHash := invalid.Run.RunSpecDigest
			if test.inputHash != "" {
				attemptInputHash = test.inputHash
			}
			attemptKind := "CREATIVE_REVISION"
			if test.kind != "" {
				attemptKind = test.kind
			}
			attemptSequence := 1
			if test.sequence != 0 {
				attemptSequence = test.sequence
			}
			attemptRoute := invalid.Route
			if test.driftRoute {
				attemptRoute.ModelID += "-drifted"
			}

			driftTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := driftTx.Exec(ctx, `
				INSERT INTO video_pipeline.generation_runs
					(id, shot_spec_revision_id, prompt_snapshot_id, generation_profile_id,
					 temporal_workflow_id, run_spec_digest, creative_attempt, state, dry_run,
					 budget_approval_id, fallback_reason, trace_id, created_by)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'VALIDATED', false, $8,
				        'integration-invalid-repair', $9, 'integration-test')`,
				uuid.MustParse(invalid.Run.RunID), uuid.MustParse(invalid.ShotSpecRevisionID),
				uuid.MustParse(invalid.PromptSnapshotID),
				uuid.MustParse("10400000-0000-4000-8000-00000000000a"),
				invalid.WorkflowID, invalid.Run.RunSpecDigest, invalid.Run.Attempt,
				invalid.BudgetApprovalID, invalid.TraceID,
			); err != nil {
				driftTx.Rollback(ctx)
				t.Fatal(err)
			}
			if _, err := driftTx.Exec(ctx, `
				INSERT INTO video_pipeline.generation_attempts
					(id, generation_run_id, sequence, attempt_kind, state, input_hash, model_snapshot)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				uuid.New(), uuid.MustParse(invalid.Run.RunID), attemptSequence,
				attemptKind, attemptState, attemptInputHash, attemptRoute,
			); err != nil {
				driftTx.Rollback(ctx)
				t.Fatal(err)
			}
			if err := ensureGenerationRunPlanBindings(
				ctx, driftTx,
				stage1.ExecutionPackage{PrimaryJobs: []stage1.FrozenJob{invalid}},
				"10400000-0000-4000-8000-00000000000a", approval,
				fixedSample1ProductHash, time.Now().UTC(),
			); err == nil {
				driftTx.Rollback(ctx)
				t.Fatal("Plan binding repair accepted a drifted first attempt")
			}
			var auditCount, paidBoundaryRecords int
			if err := driftTx.QueryRow(ctx, `
				SELECT
				  (SELECT count(*) FROM video_pipeline.audit_events
				   WHERE aggregate_type = 'GENERATION_RUN'
				     AND aggregate_id = $1
				     AND action = 'generation_run.created'),
				  (SELECT count(*) FROM video_pipeline.budget_reservations
				   WHERE generation_run_id = $1) +
				  (SELECT count(*) FROM video_pipeline.provider_jobs pj
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1) +
				  (SELECT count(*) FROM video_pipeline.cost_ledger cl
				   JOIN video_pipeline.provider_jobs pj ON pj.id = cl.provider_job_id
				   JOIN video_pipeline.generation_attempts ga
				     ON ga.id = pj.generation_attempt_id
				   WHERE ga.generation_run_id = $1)`, uuid.MustParse(invalid.Run.RunID),
			).Scan(&auditCount, &paidBoundaryRecords); err != nil {
				driftTx.Rollback(ctx)
				t.Fatal(err)
			}
			if auditCount != 0 || paidBoundaryRecords != 0 {
				driftTx.Rollback(ctx)
				t.Fatalf(
					"drift rejection side effects = audits:%d paid:%d",
					auditCount, paidBoundaryRecords,
				)
			}
			if err := driftTx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}

	replayed, report, err := Materialize(ctx, pool, cas, plan, files, approval)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ContentHash != initial.ContentHash || report.ExecutionPackageHash != initial.ContentHash {
		t.Fatal("replay changed the sealed Stage 1 execution package")
	}
}

const fixedSample1ProductHash = "a33ac48df5d413d1467e5716821073a8baebe69f76b2125263118e8f06972e30"

var integrationCreativeAttempt atomic.Int32
