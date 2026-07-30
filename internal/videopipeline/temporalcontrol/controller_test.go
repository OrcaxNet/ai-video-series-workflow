package temporalcontrol

import (
	"context"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"go.temporal.io/sdk/client"
)

type clientFixture struct {
	client.Client
	executions []executionCall
	signals    []signalCall
	cancels    []string
}

type executionCall struct {
	options  client.StartWorkflowOptions
	workflow any
	input    any
}

type signalCall struct {
	workflowID string
	signalName string
	payload    any
}

func (f *clientFixture) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (client.WorkflowRun, error) {
	f.executions = append(f.executions, executionCall{options: options, workflow: workflow, input: args[0]})
	return workflowRunFixture{id: options.ID, runID: "temporal-run-1"}, nil
}

func (f *clientFixture) SignalWorkflow(
	_ context.Context,
	workflowID, _ string,
	signalName string,
	arg any,
) error {
	f.signals = append(f.signals, signalCall{workflowID: workflowID, signalName: signalName, payload: arg})
	return nil
}

func (f *clientFixture) CancelWorkflow(_ context.Context, workflowID, _ string) error {
	f.cancels = append(f.cancels, workflowID)
	return nil
}

type workflowRunFixture struct {
	client.WorkflowRun
	id    string
	runID string
}

func (f workflowRunFixture) GetID() string    { return f.id }
func (f workflowRunFixture) GetRunID() string { return f.runID }

type storeFixture struct {
	controlplane.Store
	shot controlplane.ShotWorkflowRecord
}

func (f *storeFixture) GetShotWorkflowRecord(context.Context, string) (controlplane.ShotWorkflowRecord, error) {
	return f.shot, nil
}

func TestControllerStartShotUsesStablePersistedWorkflowAndDispatch(t *testing.T) {
	t.Parallel()
	temporalClient := &clientFixture{}
	store := &storeFixture{shot: controlplane.ShotWorkflowRecord{
		Run: controlplane.GenerationRun{
			RunID: "run-1", ShotSpecRevisionID: "shot-revision-1",
			RunSpecDigest: "digest-1", CreativeAttempt: 2, TraceID: "trace-1",
		},
		PromptSnapshotID: "prompt-1", PromptHash: "prompt-digest-1",
		RouteSnapshot: controlplane.ModelRouteSnapshot{
			CapabilityAlias: "video.primary", ProviderProfileID: "provider-profile-1",
			Provider: "MOCK", ModelID: "fixture-video-v1", RouteVersion: "route-v1",
			CapabilityHash: "capability-hash-1",
		},
		BudgetApprovalID: "budget-approval-1",
		BudgetLimit:      controlplane.BudgetLimit{AmountMicros: 1000, Currency: "CNY"},
	}}
	controller, err := New(temporalClient, "queue-1", store)
	if err != nil {
		t.Fatal(err)
	}
	start, err := controller.StartShot(t.Context(), controlplane.Operation{
		OperationID: "operation-1", AggregateID: "run-1",
		TemporalWorkflowID: "shot-generation-run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if start.WorkflowID != "shot-generation-run-1" || start.RunID != "temporal-run-1" {
		t.Fatalf("start = %#v", start)
	}
	if len(temporalClient.executions) != 1 {
		t.Fatalf("executions = %d", len(temporalClient.executions))
	}
	call := temporalClient.executions[0]
	if call.options.ID != "shot-generation-run-1" || call.workflow != orchestration.ShotWorkflowName {
		t.Fatalf("execution = %#v", call)
	}
	input, ok := call.input.(orchestration.ShotProductionInput)
	if !ok {
		t.Fatalf("input type = %T", call.input)
	}
	if input.Run.RunID != "run-1" || input.Run.Attempt != 2 ||
		input.Prompt.Digest != "prompt-digest-1" ||
		input.ProviderProfileID != "provider-profile-1" ||
		input.BudgetApprovalID != "budget-approval-1" ||
		!input.PersistProductTruth {
		t.Fatalf("shot input = %#v", input)
	}
}

func TestControllerSignalsPauseResumeAndCancel(t *testing.T) {
	t.Parallel()
	temporalClient := &clientFixture{}
	controller, err := New(temporalClient, "queue-1", &storeFixture{})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Pause(t.Context(), "workflow-1", "pause-operation-1", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Resume(t.Context(), "workflow-1", "resume-operation-1", "RESUME_PAUSED"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Cancel(t.Context(), "workflow-1", "operator"); err != nil {
		t.Fatal(err)
	}
	if len(temporalClient.signals) != 2 || len(temporalClient.cancels) != 1 {
		t.Fatalf("signals=%#v cancels=%#v", temporalClient.signals, temporalClient.cancels)
	}
	pause := temporalClient.signals[0].payload.(orchestration.WorkflowControl)
	resume := temporalClient.signals[1].payload.(orchestration.WorkflowControl)
	if pause.Action != "PAUSE" || pause.CommandID != "pause-operation-1" ||
		resume.Action != "RESUME" || resume.CommandID != "resume-operation-1" {
		t.Fatalf("pause=%#v resume=%#v", pause, resume)
	}
}
