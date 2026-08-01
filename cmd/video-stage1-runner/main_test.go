package main

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
)

func TestStage1FinalizationStartOptionsRecoverOnlyFailedWorkflow(t *testing.T) {
	t.Parallel()

	options := stage1FinalizationStartOptions("stage1-finalization-batch-hash", "video-production-v1")
	if options.ID != "stage1-finalization-batch-hash" {
		t.Fatalf("workflow ID = %q", options.ID)
	}
	if options.TaskQueue != "video-production-v1" {
		t.Fatalf("task queue = %q", options.TaskQueue)
	}
	if options.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		t.Fatalf("workflow conflict policy = %s", options.WorkflowIDConflictPolicy)
	}
	if options.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY {
		t.Fatalf("workflow reuse policy = %s", options.WorkflowIDReusePolicy)
	}
}
