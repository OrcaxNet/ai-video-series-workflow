package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
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

func TestStage1FinalizationWorkflowIDIncludesPostProductionAlgorithmRevision(t *testing.T) {
	t.Parallel()
	package_ := stage1.ExecutionPackage{
		BatchID:     "flo104-sample-1",
		ContentHash: strings.Repeat("a", 64),
	}
	want := "stage1-finalization-flo104-sample-1-aaaaaaaaaaaaaaaa-" + postproduction.AlgorithmRevision
	if got := stage1FinalizationWorkflowID(package_); got != want {
		t.Fatalf("workflow ID = %q, want %q", got, want)
	}
}

func TestValidateImmutableExecutionPackageClassifiesMalformedRevisionParent(t *testing.T) {
	t.Parallel()
	package_ := stage1.ExecutionPackage{
		PostProduction: orchestration.FinalizeEpisodeInput{
			Config: orchestration.PostProductionConfig{
				SpeechIdentityVersion: postproduction.SpeechIdentityV2,
			},
		},
	}
	assertForbiddenNonRetryable(t, validateImmutableExecutionPackage(stage1.Plan{}, package_))
}

func TestLoadRevisionParentClassifiesArtifactFailures(t *testing.T) {
	t.Parallel()
	child := stage1.ExecutionPackage{ParentExecutionPackageHash: strings.Repeat("a", 64)}
	tests := []struct {
		name   string
		lookup func(string) (string, bool)
	}{
		{
			name: "missing path",
			lookup: func(string) (string, bool) {
				return "", false
			},
		},
		{
			name: "unreadable path",
			lookup: func(string) (string, bool) {
				return filepath.Join(t.TempDir(), "missing-parent.json"), true
			},
		},
		{
			name:   "malformed JSON",
			lookup: revisionParentFixtureLookup(t, "{"),
		},
		{
			name:   "invalid decoded artifact",
			lookup: revisionParentFixtureLookup(t, "{}"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadRevisionParent(stage1.Plan{}, child, test.lookup)
			assertForbiddenNonRetryable(t, err)
		})
	}
}

func revisionParentFixtureLookup(t *testing.T, content string) func(string) (string, bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "parent.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return func(name string) (string, bool) {
		if name != "VIDEO_STAGE1_PARENT_EXECUTION_PACKAGE_PATH" {
			return "", false
		}
		return path, true
	}
}

func assertForbiddenNonRetryable(t *testing.T, err error) {
	t.Helper()
	if got := providercontract.ErrorCodeOf(err); got != providercontract.CodeForbidden {
		t.Fatalf("error = %v (code %q), want forbidden", err, got)
	}
	var contractError *providercontract.Error
	if !errors.As(err, &contractError) {
		t.Fatalf("error type = %T, want *providercontract.Error", err)
	}
	if contractError.Retryable {
		t.Fatalf("error unexpectedly retryable: %#v", contractError)
	}
}
