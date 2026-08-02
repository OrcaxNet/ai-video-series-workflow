//go:build integration

package stage1materialize

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFLO100LiveActivationQuotaAndReplayAreFailClosed(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	secondaryDSN := os.Getenv("VIDEO_TEST_POSTGRES_DSN_SECONDARY")
	root := os.Getenv("VIDEO_TEST_FLO100_PACK_PATH")
	authorizationPath := os.Getenv("VIDEO_TEST_FLO100_LIVE_AUTHORIZATION_PATH")
	if dsn == "" || secondaryDSN == "" || root == "" || authorizationPath == "" {
		t.Skip("two disposable PostgreSQL databases, FLO-100 pack, and live authorization are required")
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
	formal := FormalOptions{
		Root: root, ExpectedPackageHash: FormalExpectedPackageHash(),
		Approval: Approval{
			CommentID:  "5b92b347-3ce9-4e7b-831a-1f00d1454d78",
			ActorID:    "16bbc49e-750f-432d-9ba4-b33ef6812026",
			ValidUntil: time.Date(2026, time.August, 31, 15, 59, 59, 0, time.UTC),
		},
	}
	offline, _, err := MaterializeFLO100(ctx, pool, cas, formal)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(authorizationPath)
	if err != nil {
		t.Fatal(err)
	}
	oldHash := sha256.Sum256(data)
	candidateCommit := strings.Repeat("f", 40)
	options := LiveOptions{
		Formal: formal, AuthorizationPath: authorizationPath,
		ExpectedAuthorizationHash: hex.EncodeToString(oldHash[:]),
		SourceCodeCommit:          candidateCommit,
	}
	plan, package_, report, err := MaterializeFLO100Live(ctx, pool, cas, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	if report.SubmitAuthorized || report.ProviderJobs != 0 || report.BudgetReservations != 0 ||
		report.CostLedgerEntries != 0 || package_.LiveActivation == nil ||
		package_.LiveActivation.OfflineExecutionPackageHash != offline[0].ContentHash {
		t.Fatalf("unexpected code-drift activation report: %+v", report)
	}
	secondaryPool, err := pgxpool.New(ctx, secondaryDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer secondaryPool.Close()
	secondaryCAS, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	secondaryOffline, _, err := MaterializeFLO100(ctx, secondaryPool, secondaryCAS, formal)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPlan, secondaryPackage, secondaryReport, err := MaterializeFLO100Live(
		ctx, secondaryPool, secondaryCAS, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	primaryPlanHash, err := digest(plan)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPlanHash, err := digest(secondaryPlan)
	if err != nil {
		t.Fatal(err)
	}
	if secondaryOffline[0].ContentHash != offline[0].ContentHash ||
		secondaryPlanHash != primaryPlanHash || secondaryPackage.ContentHash != package_.ContentHash ||
		secondaryReport.LiveProjectionHash != report.LiveProjectionHash {
		t.Fatalf("fresh database hashes differ: plan %s/%s package %s/%s projection %s/%s",
			primaryPlanHash, secondaryPlanHash, package_.ContentHash, secondaryPackage.ContentHash,
			report.LiveProjectionHash, secondaryReport.LiveProjectionHash)
	}
	if secondaryReport.SubmitAuthorized {
		t.Fatal("code-drift authorization unexpectedly enabled submit in the secondary database")
	}
	var primaryCreatedAt, secondaryCreatedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT created_at FROM video_pipeline.stage1_live_activations WHERE id=$1`,
		mustUUID(package_.LiveActivation.ActivationID)).Scan(&primaryCreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := secondaryPool.QueryRow(ctx, `SELECT created_at FROM video_pipeline.stage1_live_activations WHERE id=$1`,
		mustUUID(secondaryPackage.LiveActivation.ActivationID)).Scan(&secondaryCreatedAt); err != nil {
		t.Fatal(err)
	}
	if primaryCreatedAt.Equal(secondaryCreatedAt) {
		t.Fatal("independent databases unexpectedly share the same runtime creation timestamp")
	}
	assertLiveBoundaryCounts(t, secondaryPool, 0, 0, 0, 0)
	_, replayed, replayReport, err := MaterializeFLO100Live(ctx, pool, cas, options)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ContentHash != package_.ContentHash || replayReport.LiveProjectionHash != report.LiveProjectionHash {
		t.Fatal("live activation replay changed its canonical package or projection")
	}
	var controlSeriesID, controlTitle string
	if err := pool.QueryRow(ctx, `SELECT a.control_series_id,s.title
		FROM video_pipeline.stage1_live_activations a
		JOIN video_pipeline.series s ON s.id=a.control_series_id
		WHERE a.id=$1`, mustUUID(package_.LiveActivation.ActivationID)).
		Scan(&controlSeriesID, &controlTitle); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.series SET title=title || ' drift' WHERE id=$1`, controlSeriesID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := MaterializeFLO100Live(ctx, pool, cas, options); err == nil {
		t.Fatal("live projection drift unexpectedly replayed")
	}
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.series SET title=$2 WHERE id=$1`, controlSeriesID, controlTitle); err != nil {
		t.Fatal(err)
	}

	// A synthetic fixture models the separately reissued authority expected
	// after QA fixes the candidate commit. It is test evidence only and cannot
	// authorize a deployed binary.
	var reissued map[string]any
	if err := json.Unmarshal(data, &reissued); err != nil {
		t.Fatal(err)
	}
	reissued["decision"].(map[string]any)["batchBProviderPostAuthorized"] = true
	badScopeData, err := json.Marshal(reissued)
	if err != nil {
		t.Fatal(err)
	}
	badScopePath := filepath.Join(t.TempDir(), "bad-batch-scope-authority.json")
	if err := os.WriteFile(badScopePath, badScopeData, 0o600); err != nil {
		t.Fatal(err)
	}
	badScopeHash := sha256.Sum256(badScopeData)
	badScopeOptions := options
	badScopeOptions.AuthorizationPath = badScopePath
	badScopeOptions.ExpectedAuthorizationHash = hex.EncodeToString(badScopeHash[:])
	if _, _, _, err := MaterializeFLO100Live(ctx, pool, cas, badScopeOptions); err == nil {
		t.Fatal("B-enabled authority unexpectedly activated")
	}
	reissued["decision"].(map[string]any)["batchBProviderPostAuthorized"] = false
	reissued["fixedEvidence"].(map[string]any)["mergeCommit"] = candidateCommit
	reissuedData, err := json.Marshal(reissued)
	if err != nil {
		t.Fatal(err)
	}
	reissuedPath := filepath.Join(t.TempDir(), "reissued-test-authority.json")
	if err := os.WriteFile(reissuedPath, reissuedData, 0o600); err != nil {
		t.Fatal(err)
	}
	reissuedHash := sha256.Sum256(reissuedData)
	reissuedOptions := options
	reissuedOptions.AuthorizationPath = reissuedPath
	reissuedOptions.ExpectedAuthorizationHash = hex.EncodeToString(reissuedHash[:])
	_, authorizedPackage, authorizedReport, err := MaterializeFLO100Live(ctx, pool, cas, reissuedOptions)
	if err != nil {
		t.Fatal(err)
	}
	if !authorizedReport.SubmitAuthorized || authorizedPackage.ContentHash != package_.ContentHash ||
		authorizedReport.LiveProjectionHash != report.LiveProjectionHash {
		t.Fatalf("reissued test authority did not bind the exact live projection: %+v", authorizedReport)
	}

	store, err := repository.Open(ctx, dsn, repository.PoolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	job := authorizedPackage.PrimaryJobs[0]
	prompt, err := store.ResolvePromptSnapshot(ctx, job.PromptSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	expected := orchestration.PreparedProductTruth{
		ShotSpecRevisionID: job.ShotSpecRevisionID, Run: job.Run,
		PromptSnapshotID: job.PromptSnapshotID, PromptSnapshotHash: job.PromptSnapshotHash,
		GenerationPlanID: job.GenerationPlanID, BudgetApprovalID: job.BudgetApprovalID,
		BudgetMaximumMicros: job.BudgetMaximumMicros, BudgetCurrency: job.BudgetCurrency,
		ProviderProfileID: job.ProviderProfileID, Route: job.Route,
		LiveActivationID:     authorizedPackage.LiveActivation.ActivationID,
		ExecutionPackageHash: authorizedPackage.ContentHash,
		SourceCodeCommit:     candidateCommit, EstimatedVideoTokens: job.EstimatedVideoTokens,
		PredictedAFPMilli: job.PredictedAFPMilli, BillingMode: job.BillingMode,
	}
	now := time.Now().UTC()
	quota := &orchestration.SubscriptionQuotaSnapshot{
		SchemaVersion: "ark.agent-plan-quota.v1", Source: "integration-authenticated-snapshot",
		CapturedAt: now, AccountID: "integration-agent-plan-account",
		Profile: "agent-plan_cn-beijing_personal", Region: "cn-beijing",
		BillingMode:          providercontract.BillingModeSubscriptionIncludedOnly,
		FiveHourUsedAFPMilli: 1_000_000, FiveHourTotalAFPMilli: 10_000_000,
		WeeklyUsedAFPMilli: 1_000_000, WeeklyTotalAFPMilli: 100_000_000,
		MonthlyUsedAFPMilli: 1_000_000, MonthlyTotalAFPMilli: 250_000_000,
	}
	input := orchestration.ExecuteProviderJobInput{
		Run: job.Run, Prompt: prompt, Route: job.Route,
		BudgetApprovalID: job.BudgetApprovalID, BudgetMaximumMicros: 0,
		BudgetCurrency: "CNY", ProviderProfileID: job.ProviderProfileID,
		TraceID: job.TraceID, PersistProductTruth: true, ExpectedProductTruth: &expected,
		SubscriptionQuotaSnapshot:    quota,
		ExpectedExecutionPackageHash: authorizedPackage.ContentHash,
		ExpectedLiveActivationID:     authorizedPackage.LiveActivation.ActivationID,
		ExpectedSourceCodeCommit:     candidateCommit,
		EstimatedVideoTokens:         job.EstimatedVideoTokens, PredictedAFPMilli: job.PredictedAFPMilli,
		BillingMode: providercontract.BillingModeSubscriptionIncludedOnly,
	}
	step := orchestration.WorkflowStep{WorkflowID: job.WorkflowID, ActivityID: job.ActivityID, TraceID: job.TraceID}
	stale := input
	staleSnapshot := *quota
	staleSnapshot.CapturedAt = now.Add(-301 * time.Second)
	stale.SubscriptionQuotaSnapshot = &staleSnapshot
	if _, err := store.PrepareProviderJob(ctx, step, stale); err == nil {
		t.Fatal("stale quota snapshot unexpectedly prepared a Provider job")
	}
	exceeded := input
	exceededSnapshot := *quota
	exceededSnapshot.MonthlyUsedAFPMilli = 120_000_000
	exceeded.SubscriptionQuotaSnapshot = &exceededSnapshot
	if _, err := store.PrepareProviderJob(ctx, step, exceeded); err == nil {
		t.Fatal("over-cap quota snapshot unexpectedly prepared a Provider job")
	}
	cashDrift := input
	cashDrift.BudgetMaximumMicros = 1
	if _, err := store.PrepareProviderJob(ctx, step, cashDrift); err == nil {
		t.Fatal("nonzero subscription cash unexpectedly prepared a Provider job")
	}
	packageDrift := input
	packageDrift.ExpectedExecutionPackageHash = strings.Repeat("0", 64)
	if _, err := store.PrepareProviderJob(ctx, step, packageDrift); err == nil {
		t.Fatal("execution package drift unexpectedly prepared a Provider job")
	}
	approvalDrift := input
	approvalDrift.BudgetApprovalID = "11111111-1111-4111-8111-111111111111"
	if _, err := store.PrepareProviderJob(ctx, step, approvalDrift); err == nil {
		t.Fatal("approval drift unexpectedly prepared a Provider job")
	}
	routeDrift := input
	routeDrift.Route.ModelID = "drifted-model"
	if _, err := store.PrepareProviderJob(ctx, step, routeDrift); err == nil {
		t.Fatal("route drift unexpectedly prepared a Provider job")
	}
	for _, blockedPackage := range offline[1:] {
		blocked := input
		blocked.Run = blockedPackage.PrimaryJobs[0].Run
		blocked.ExpectedLiveActivationID = ""
		if _, err := store.PrepareProviderJob(ctx, step, blocked); err == nil {
			t.Fatalf("batch %s unexpectedly prepared a Provider job", blockedPackage.BatchID)
		}
	}
	assertLiveBoundaryCounts(t, pool, 0, 0, 0, 0)
	prepared, err := store.PrepareProviderJob(ctx, step, input)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Budget.MaxCostMicros != 0 || prepared.Budget.ReservedAFPMilli != job.PredictedAFPMilli ||
		prepared.Budget.BillingMode != providercontract.BillingModeSubscriptionIncludedOnly {
		t.Fatalf("unexpected subscription allocation: %+v", prepared.Budget)
	}
	if _, err := store.PrepareProviderJob(ctx, step, input); err != nil {
		t.Fatalf("exact prepare replay failed: %v", err)
	}
	assertLiveBoundaryCounts(t, pool, 1, 1, 1, 1)

	// A retry package cannot create its new Run until the primary has complete,
	// settled terminal-failure evidence in PostgreSQL.
	unboundRetry := testFLO100LiveControlledRetry(t, pool, plan, authorizedPackage, job, uuid.New())
	if err := BindFLO100LiveControlledRetry(ctx, pool, plan, authorizedPackage, unboundRetry); err == nil {
		t.Fatal("controlled retry bound before the primary terminal failure")
	}
	assertLiveBoundaryCounts(t, pool, 1, 1, 1, 1)

	primaryRunID := mustUUID(job.Run.RunID)
	primaryProviderJobID := uuid.NewSHA1(primaryRunID, []byte("provider-job"))
	failureSnapshot := map[string]any{
		"code": "TRANSIENT",
		"providerCost": map[string]any{
			"estimatedMicros": 0, "currency": "CNY",
			"pricingVersion": "agent-plan-subscription-v1", "verified": false,
		},
		"providerUsage":          map[string]any{"videoTokens": 0, "unit": "video_tokens"},
		"actualTrustedForBudget": false,
	}
	terminalAt := time.Now().UTC()
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.generation_runs
		SET state='FAILED',failure_class='TRANSIENT',failure_code='TRANSIENT',finished_at=$2 WHERE id=$1`,
		primaryRunID, terminalAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.generation_attempts
		SET state='FAILED',failure_code='TRANSIENT',finished_at=$2 WHERE generation_run_id=$1`,
		primaryRunID, terminalAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.provider_jobs
		SET state='FAILED',upstream_task_id='task-controlled-retry-primary',
		    upstream_request_id='request-controlled-retry-primary',error_code='TRANSIENT',
		    error_snapshot=$2,terminal_at=$3,updated_at=$3 WHERE id=$1`,
		primaryProviderJobID, failureSnapshot, terminalAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE video_pipeline.budget_reservations
		SET status='SETTLED' WHERE generation_run_id=$1`, primaryRunID); err != nil {
		t.Fatal(err)
	}
	terminalEvidence := controlledRetryTerminalEvidence{
		GenerationRunState: "FAILED", RunFailureClass: "TRANSIENT", AttemptState: "FAILED",
		ProviderJobID: primaryProviderJobID.String(), ProviderJobState: "FAILED",
		UpstreamTaskID: "task-controlled-retry-primary", UpstreamRequestID: "request-controlled-retry-primary",
		ErrorCode: "TRANSIENT", TerminalAt: terminalAt, ReservationStatus: "SETTLED", CostEvidenceCount: 1,
	}
	if err := pool.QueryRow(ctx, `SELECT error_snapshot,terminal_at
		FROM video_pipeline.provider_jobs WHERE id=$1`, primaryProviderJobID).
		Scan(&terminalEvidence.ErrorSnapshot, &terminalEvidence.TerminalAt); err != nil {
		t.Fatal(err)
	}
	evidenceHash, err := digest(terminalEvidence)
	if err != nil {
		t.Fatal(err)
	}
	duplicateEvidenceID := uuid.NewSHA1(primaryProviderJobID, []byte("duplicate-task-evidence:"+evidenceHash))
	retryPackage := testFLO100LiveControlledRetry(t, pool, plan, authorizedPackage, job, duplicateEvidenceID)
	if err := BindFLO100LiveControlledRetry(ctx, pool, plan, authorizedPackage, retryPackage); err != nil {
		t.Fatalf("bind controlled retry: %v", err)
	}
	if err := BindFLO100LiveControlledRetry(ctx, pool, plan, authorizedPackage, retryPackage); err != nil {
		t.Fatalf("exact controlled retry binding replay: %v", err)
	}
	competing := retryPackage
	competing.Approval.ApprovalID = uuid.New().String()
	competing, err = stage1.SealControlledRetryPackage(competing)
	if err != nil {
		t.Fatal(err)
	}
	if err := BindFLO100LiveControlledRetry(ctx, pool, plan, authorizedPackage, competing); err == nil {
		t.Fatal("competing or second controlled retry package unexpectedly bound")
	}
	if err := BindFLO100LiveControlledRetry(ctx, pool, plan, offline[1], retryPackage); err == nil {
		t.Fatal("Batch B controlled retry unexpectedly bound")
	}
	assertLiveBoundaryCounts(t, pool, 1, 1, 1, 1)

	retryJob := retryPackage.Job
	retryPrompt, err := store.ResolvePromptSnapshot(ctx, retryJob.PromptSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	retryExpected := orchestration.PreparedProductTruth{
		ShotSpecRevisionID: retryJob.ShotSpecRevisionID, Run: retryJob.Run,
		PromptSnapshotID: retryJob.PromptSnapshotID, PromptSnapshotHash: retryJob.PromptSnapshotHash,
		GenerationPlanID: retryJob.GenerationPlanID, BudgetApprovalID: retryJob.BudgetApprovalID,
		BudgetMaximumMicros: retryJob.BudgetMaximumMicros, BudgetCurrency: retryJob.BudgetCurrency,
		ProviderProfileID: retryJob.ProviderProfileID, Route: retryJob.Route,
		LiveActivationID:     authorizedPackage.LiveActivation.ActivationID,
		ExecutionPackageHash: authorizedPackage.ContentHash, ControlledRetryPackageHash: retryPackage.ContentHash,
		SourceCodeCommit: candidateCommit, EstimatedVideoTokens: retryJob.EstimatedVideoTokens,
		PredictedAFPMilli: retryJob.PredictedAFPMilli, BillingMode: retryJob.BillingMode,
	}
	retryInput := orchestration.ExecuteProviderJobInput{
		Run: retryJob.Run, Prompt: retryPrompt, Route: retryJob.Route,
		BudgetApprovalID: retryJob.BudgetApprovalID, BudgetMaximumMicros: 0,
		BudgetCurrency: "CNY", ProviderProfileID: retryJob.ProviderProfileID,
		TraceID: retryJob.TraceID, PersistProductTruth: true, ExpectedProductTruth: &retryExpected,
		SubscriptionQuotaSnapshot: quota, ExpectedExecutionPackageHash: authorizedPackage.ContentHash,
		ExpectedControlledRetryPackageHash: retryPackage.ContentHash,
		ExpectedLiveActivationID:           authorizedPackage.LiveActivation.ActivationID,
		ExpectedSourceCodeCommit:           candidateCommit, EstimatedVideoTokens: retryJob.EstimatedVideoTokens,
		PredictedAFPMilli: retryJob.PredictedAFPMilli, BillingMode: retryJob.BillingMode,
	}
	retryStep := orchestration.WorkflowStep{
		WorkflowID: retryJob.WorkflowID, ActivityID: retryJob.ActivityID, TraceID: retryJob.TraceID,
	}
	for name, mutate := range map[string]func(*orchestration.ExecuteProviderJobInput){
		"package": func(in *orchestration.ExecuteProviderJobInput) {
			in.ExpectedControlledRetryPackageHash = strings.Repeat("0", 64)
		},
		"approval": func(in *orchestration.ExecuteProviderJobInput) { in.BudgetApprovalID = uuid.New().String() },
		"run":      func(in *orchestration.ExecuteProviderJobInput) { in.Run.RunSpecDigest = strings.Repeat("0", 64) },
		"route":    func(in *orchestration.ExecuteProviderJobInput) { in.Route.ModelID = "drifted-model" },
		"hash":     func(in *orchestration.ExecuteProviderJobInput) { in.Prompt.Digest = strings.Repeat("0", 64) },
	} {
		drifted := retryInput
		mutate(&drifted)
		if _, err := store.PrepareProviderJob(ctx, retryStep, drifted); err == nil {
			t.Fatalf("controlled retry %s drift unexpectedly prepared", name)
		}
	}
	assertLiveBoundaryCounts(t, pool, 1, 1, 1, 1)
	retryPrepared, err := store.PrepareProviderJob(ctx, retryStep, retryInput)
	if err != nil {
		t.Fatalf("prepare controlled retry: %v", err)
	}
	if retryPrepared.Budget.MaxCostMicros != 0 ||
		retryPrepared.ProductTruth.ControlledRetryPackageHash != retryPackage.ContentHash {
		t.Fatalf("unexpected controlled retry product truth: %+v", retryPrepared)
	}
	if _, err := store.PrepareProviderJob(ctx, retryStep, retryInput); err != nil {
		t.Fatalf("exact controlled retry prepare replay: %v", err)
	}
	assertLiveBoundaryCounts(t, pool, 2, 2, 2, 1)

	offlineReplay, _, err := MaterializeFLO100(ctx, pool, cas, formal)
	if err != nil {
		t.Fatal(err)
	}
	if offlineReplay[0].ContentHash != offline[0].ContentHash {
		t.Fatal("live boundary records changed the immutable offline package")
	}
	_, liveReplay, postBoundaryReport, err := MaterializeFLO100Live(ctx, pool, cas, reissuedOptions)
	if err != nil {
		t.Fatal(err)
	}
	if liveReplay.ContentHash != authorizedPackage.ContentHash || postBoundaryReport.ProviderJobs != 1 ||
		postBoundaryReport.BudgetReservations != 1 || postBoundaryReport.CostLedgerEntries != 1 {
		t.Fatalf("post-boundary live replay drifted: %+v", postBoundaryReport)
	}
}

func testFLO100LiveControlledRetry(
	t *testing.T,
	pool *pgxpool.Pool,
	plan stage1.Plan,
	primary stage1.ExecutionPackage,
	original stage1.FrozenJob,
	duplicateEvidenceID uuid.UUID,
) stage1.ControlledRetryPackage {
	t.Helper()
	retry := original
	retryRunID := uuid.NewSHA1(mustUUID(primary.LiveActivation.ActivationID), []byte("controlled-retry:"+original.Run.RunID))
	var generationProfileID string
	if err := pool.QueryRow(t.Context(), `SELECT generation_profile_id
		FROM video_pipeline.generation_runs WHERE id=$1`, mustUUID(original.Run.RunID)).
		Scan(&generationProfileID); err != nil {
		t.Fatal(err)
	}
	runDigest, err := repository.GenerationRunSpecDigest(
		original.ShotSpecRevisionID, original.PromptSnapshotID, original.PromptSnapshotHash,
		generationProfileID, original.GenerationPlanID, original.Route, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	retry.AttemptID = uuid.NewSHA1(retryRunID, []byte("controlled-retry-identity")).String()
	retry.IdempotencyKey = "provider-job-" + retryRunID.String()
	retry.Run = orchestration.GenerationRunRef{RunID: retryRunID.String(), RunSpecDigest: runDigest, Attempt: 2}
	retry.WorkflowID = "flo100-live-controlled-retry-" + retryRunID.String()
	retry.ActivityID = "submit-controlled-retry-" + retry.ShotID
	retry.TraceID += "-controlled-retry"
	post := primary.PostProduction
	post.RunIDs = append([]string(nil), post.RunIDs...)
	for index, runID := range post.RunIDs {
		if runID == original.Run.RunID {
			post.RunIDs[index] = retryRunID.String()
		}
	}
	package_, err := stage1.SealControlledRetryPackage(stage1.ControlledRetryPackage{
		SchemaVersion: stage1.ControlledRetryPackageSchemaVersion, BatchID: primary.BatchID,
		ParentExecutionPackageHash: primary.ContentHash, Job: retry,
		Approval: stage1.RetryApproval{
			ApprovalID:        uuid.NewSHA1(retryRunID, []byte("manual-approval")).String(),
			OriginalAttemptID: original.AttemptID, FailureClass: "TRANSIENT",
			DuplicateTaskEvidenceID: duplicateEvidenceID.String(),
		},
		PostProduction: post,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := package_.Validate(plan, primary); err != nil {
		t.Fatal(err)
	}
	return package_
}

func assertLiveBoundaryCounts(
	t *testing.T,
	pool *pgxpool.Pool,
	jobs, cashReservations, costs, afpReservations int64,
) {
	t.Helper()
	var gotJobs, gotCash, gotCosts, gotAFP int64
	if err := pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM video_pipeline.provider_jobs),
		(SELECT count(*) FROM video_pipeline.budget_reservations),
		(SELECT count(*) FROM video_pipeline.cost_ledger),
		(SELECT count(*) FROM video_pipeline.stage1_agent_plan_afp_reservations)`).
		Scan(&gotJobs, &gotCash, &gotCosts, &gotAFP); err != nil {
		t.Fatal(err)
	}
	if gotJobs != jobs || gotCash != cashReservations || gotCosts != costs || gotAFP != afpReservations {
		t.Fatalf("paid boundary counts = jobs:%d cash:%d costs:%d afp:%d, want %d/%d/%d/%d",
			gotJobs, gotCash, gotCosts, gotAFP, jobs, cashReservations, costs, afpReservations)
	}
}
