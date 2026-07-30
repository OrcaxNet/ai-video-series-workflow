package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestEpisodeProductionWorkflow_LocksAfterG3(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	registerHappyPathActivities(env)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(Gate3DecisionSignal, Gate3Decision{
			DecisionID: "decision-g3-1",
			Approved:   true,
			ActorID:    "reviewer-1",
		})
	}, time.Second)

	input := EpisodeProductionInput{
		SchemaVersion:        "v1",
		SeriesID:             "series-1",
		EpisodeRevisionID:    "episode-revision-1",
		ShotSpecRevisionIDs:  []string{"shot-revision-1", "shot-revision-2"},
		GenerationProfileRef: "profile-revision-1",
		Gate2DecisionID:      "decision-g2-1",
		ProviderRoute:        testProviderRoute(),
		BudgetApprovalID:     "budget-approval-1",
		BudgetMaximumMicros:  500,
		BudgetCurrency:       "CNY",
		TraceID:              "trace-1",
	}
	env.ExecuteWorkflow(EpisodeProductionWorkflow, input)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result EpisodeProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	if result.State != "LOCKED" || len(result.LockedRunIDs) != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestEpisodeProductionWorkflow_RejectsDuplicateShot(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(EpisodeProductionWorkflow, EpisodeProductionInput{
		SchemaVersion:        "v1",
		SeriesID:             "series-1",
		EpisodeRevisionID:    "episode-1",
		ShotSpecRevisionIDs:  []string{"shot-1", "shot-1"},
		GenerationProfileRef: "profile-1",
		Gate2DecisionID:      "decision-1",
		ProviderRoute:        testProviderRoute(),
		BudgetApprovalID:     "budget-1",
		BudgetMaximumMicros:  500,
		BudgetCurrency:       "CNY",
	})
	if env.GetWorkflowError() == nil {
		t.Fatal("workflow error = nil, want validation error")
	}
}

func registerHappyPathActivities(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(context.Context, EpisodeProductionInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityValidateBatch})
	env.RegisterActivityWithOptions(func(_ context.Context, input CompilePromptInput) (PromptSnapshotRef, error) {
		sum := sha256.Sum256([]byte(input.ShotSpecRevisionID))
		return PromptSnapshotRef{ID: "prompt-" + input.ShotSpecRevisionID, Digest: hex.EncodeToString(sum[:])}, nil
	}, activity.RegisterOptions{Name: ActivityCompilePrompt})
	env.RegisterActivityWithOptions(func(_ context.Context, input CreateRunInput) (GenerationRunRef, error) {
		sum := sha256.Sum256([]byte(input.ShotSpecRevisionID))
		return GenerationRunRef{
			RunID:         "run-" + input.ShotSpecRevisionID,
			RunSpecDigest: hex.EncodeToString(sum[:]),
			Attempt:       input.CreativeAttempt,
		}, nil
	}, activity.RegisterOptions{Name: ActivityCreateRun})
	env.RegisterActivityWithOptions(func(_ context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
		sum := sha256.Sum256([]byte(input.Run.RunID))
		digest := hex.EncodeToString(sum[:])
		return ProviderResult{
			UpstreamTaskID: "upstream-task-1",
			RequestID:      "request-1",
			ArtifactDigest: digest,
			ArtifactURI:    "cas://sha256/" + digest,
			Model:          input.Route,
		}, nil
	}, activity.RegisterOptions{Name: ActivityExecuteProviderJob})
	env.RegisterActivityWithOptions(func(context.Context, RunQCInput) (QCResult, error) {
		return QCResult{Passed: true}, nil
	}, activity.RegisterOptions{Name: ActivityRunAutomaticQC})
	env.RegisterActivityWithOptions(func(context.Context, CreateReviewInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityCreateShotReview})
	env.RegisterActivityWithOptions(func(context.Context, EscalateShotInput) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityEscalateShot})
	env.RegisterActivityWithOptions(func(context.Context, CreateGate3Input) error {
		return nil
	}, activity.RegisterOptions{Name: ActivityCreateGate3})
}

func testProviderRoute() providercontract.ModelSnapshot {
	sum := sha256.Sum256([]byte("video-capability"))
	return providercontract.ModelSnapshot{
		CapabilityAlias: "video.primary",
		Provider:        "mock",
		ModelID:         "fixture-video-v1",
		RouteVersion:    "mock-routes-v1",
		CapabilityHash:  hex.EncodeToString(sum[:]),
		Verification:    "mock_only",
	}
}
