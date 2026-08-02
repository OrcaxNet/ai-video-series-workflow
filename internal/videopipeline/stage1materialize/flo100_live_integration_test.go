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
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFLO100LiveActivationQuotaAndReplayAreFailClosed(t *testing.T) {
	dsn := os.Getenv("VIDEO_TEST_POSTGRES_DSN")
	root := os.Getenv("VIDEO_TEST_FLO100_PACK_PATH")
	authorizationPath := os.Getenv("VIDEO_TEST_FLO100_LIVE_AUTHORIZATION_PATH")
	if dsn == "" || root == "" || authorizationPath == "" {
		t.Skip("disposable PostgreSQL, FLO-100 pack, and live authorization are required")
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
