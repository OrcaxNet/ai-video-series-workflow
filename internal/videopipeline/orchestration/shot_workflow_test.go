package orchestration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestShotProductionWorkflowPauseResumeAndQ1(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var finalized FinalizeShotRunInput
	registerShotActivities(env, &finalized, nil)
	var pausedStatus WorkflowStatus
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ControlSignal, WorkflowControl{
			CommandID: "pause-1", Action: "PAUSE", ActorID: "operator-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(StatusQuery)
		if err != nil {
			t.Fatal(err)
		}
		if err := value.Get(&pausedStatus); err != nil {
			t.Fatal(err)
		}
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ControlSignal, WorkflowControl{
			CommandID: "resume-1", Action: "RESUME", ActorID: "operator-1",
		})
	}, 3*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID: "q1-1", ShotSpecRevisionID: "shot-revision-1",
			RunID: "run-1", Approved: true, ActorID: "reviewer-1",
		})
	}, 4*time.Second)

	env.ExecuteWorkflow(ShotProductionWorkflow, shotWorkflowTestInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result ShotProductionResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "SUCCEEDED" || finalized.State != "SUCCEEDED" {
		t.Fatalf("result=%#v finalized=%#v", result, finalized)
	}
	if !pausedStatus.Paused || pausedStatus.State != "PAUSED" {
		t.Fatalf("paused status = %#v", pausedStatus)
	}
}

func TestShotProductionWorkflowCancellationRunsProviderCompensation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var finalized FinalizeShotRunInput
	cancelCalls := 0
	registerShotActivities(env, &finalized, &cancelCalls)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, time.Second)

	env.ExecuteWorkflow(ShotProductionWorkflow, shotWorkflowTestInput())

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("workflow error = nil, want cancellation")
	}
	if cancelCalls != 1 {
		t.Fatalf("cancel compensation calls = %d, want 1", cancelCalls)
	}
}

func TestShotProductionWorkflowCancellationWinsProviderNetworkFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var finalized FinalizeShotRunInput
	cancelCalls := 0
	registerShotActivities(env, &finalized, &cancelCalls)
	env.OnActivity(
		ActivityExecuteProviderJob,
		mock.Anything,
		mock.Anything,
	).Return(func(ctx context.Context, _ ExecuteProviderJobInput) (ProviderResult, error) {
		<-ctx.Done()
		return ProviderResult{}, errors.New("lookup mock-provider: no such host")
	})
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Second)

	env.ExecuteWorkflow(ShotProductionWorkflow, shotWorkflowTestInput())

	workflowErr := env.GetWorkflowError()
	if workflowErr == nil || !temporal.IsCanceledError(workflowErr) {
		t.Fatalf("workflow error = %v, want cancellation", workflowErr)
	}
	if cancelCalls != 1 {
		t.Fatalf("cancel compensation calls = %d, want 1", cancelCalls)
	}
	if finalized.State != "" {
		t.Fatalf("ordinary failure finalization ran after cancellation: %#v", finalized)
	}
}

func TestShotProductionWorkflowPauseWinningProviderCompletionReplaysBeforeQC(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var finalized FinalizeShotRunInput
	registerShotActivities(env, &finalized, nil)
	digest := strings.Repeat("a", 64)
	providerResult := ProviderResult{
		UpstreamTaskID: "task-1", RequestID: "request-1",
		ArtifactDigest: digest, ArtifactURI: "cas://sha256/" + digest,
		Usage: providercontract.Usage{GeneratedMillis: 5000},
		Cost: providercontract.Cost{
			EstimatedMicros: 100, Currency: "CNY", PricingVersion: "test",
		},
	}
	env.OnActivity(
		ActivityExecuteProviderJob,
		mock.Anything,
		mock.Anything,
	).After(time.Second).Return(providerResult, nil).Twice()
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ControlSignal, WorkflowControl{
			CommandID: "pause-race", Action: "PAUSE", ActorID: "operator-1",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ControlSignal, WorkflowControl{
			CommandID: "resume-race", Action: "RESUME", ActorID: "operator-1",
		})
	}, 2*time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(ShotDecisionSignal, ShotDecision{
			DecisionID: "q1-race", ShotSpecRevisionID: "shot-revision-1",
			RunID: "run-1", Approved: true, ActorID: "reviewer-1",
		})
	}, 4*time.Second)

	env.ExecuteWorkflow(ShotProductionWorkflow, shotWorkflowTestInput())

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	if finalized.State != "SUCCEEDED" {
		t.Fatalf("finalized = %#v", finalized)
	}
	env.AssertExpectations(t)
}

func TestShotReconciliationWorkflowRetriesUntilProviderTruthConverges(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	cancelActivity := func(context.Context, CancelProviderJobInput) (CancelProviderResult, error) {
		return CancelProviderResult{}, nil
	}
	env.RegisterActivityWithOptions(
		cancelActivity,
		activity.RegisterOptions{Name: ActivityCancelProviderJob},
	)
	first := env.OnActivity(
		ActivityCancelProviderJob,
		mock.Anything,
		mock.Anything,
	).Return(
		CancelProviderResult{},
		temporal.NewApplicationError("provider unavailable", "CANCEL_NOT_CONFIRMED"),
	).Once()
	env.OnActivity(
		ActivityCancelProviderJob,
		mock.Anything,
		mock.Anything,
	).Return(
		CancelProviderResult{State: "CANCELLED"},
		nil,
	).Once().NotBefore(first)

	env.ExecuteWorkflow(ShotReconciliationWorkflow, ShotReconciliationInput{
		OperationID: "recovery-operation-1",
		Dispatch: ExecuteProviderJobInput{
			Run: GenerationRunRef{
				RunID: "run-1", RunSpecDigest: strings.Repeat("b", 64),
			},
			PersistProductTruth: true,
		},
		TraceID: "trace-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v", err)
	}
	var result CancelProviderResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "CANCELLED" {
		t.Fatalf("result = %#v", result)
	}
	env.AssertExpectations(t)
}

func registerShotActivities(
	env *testsuite.TestWorkflowEnvironment,
	finalized *FinalizeShotRunInput,
	cancelCalls *int,
) {
	env.RegisterActivityWithOptions(func(_ context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
		digest := strings.Repeat("a", 64)
		return ProviderResult{
			UpstreamTaskID: "task-1", RequestID: "request-1",
			ArtifactDigest: digest, ArtifactURI: "cas://sha256/" + digest,
			Model: input.Route,
			Usage: providercontract.Usage{GeneratedMillis: 5000},
			Cost: providercontract.Cost{
				EstimatedMicros: 100, Currency: "CNY", PricingVersion: "test",
			},
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
	env.RegisterActivityWithOptions(func(_ context.Context, input FinalizeShotRunInput) error {
		*finalized = input
		return nil
	}, activity.RegisterOptions{Name: ActivityFinalizeShotRun})
	env.RegisterActivityWithOptions(func(context.Context, CancelProviderJobInput) (CancelProviderResult, error) {
		if cancelCalls != nil {
			(*cancelCalls)++
		}
		return CancelProviderResult{State: "CANCELLED"}, nil
	}, activity.RegisterOptions{Name: ActivityCancelProviderJob})
}

func shotWorkflowTestInput() ShotProductionInput {
	return ShotProductionInput{
		OperationID: "operation-1", ShotSpecRevisionID: "shot-revision-1",
		Run:    GenerationRunRef{RunID: "run-1", RunSpecDigest: strings.Repeat("b", 64), Attempt: 1},
		Prompt: PromptSnapshotRef{ID: "prompt-1", Digest: strings.Repeat("c", 64)},
		Route: providercontract.ModelSnapshot{
			CapabilityAlias: "video.primary", Provider: "MOCK", ModelID: "fixture-video-v1",
			RouteVersion: "route-v1", CapabilityHash: strings.Repeat("d", 64),
			Verification: "test",
		},
		ProviderProfileID: "provider-profile-1", BudgetApprovalID: "budget-1",
		BudgetMaximumMicros: 1000, BudgetCurrency: "CNY", TraceID: "trace-1",
		RequireShotApproval: true, PersistProductTruth: true,
	}
}
