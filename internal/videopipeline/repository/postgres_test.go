package repository

import "testing"

func TestEventTypeForAction(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"series.created":                    "video.series.created.v1",
		"source_revision.created":           "video.revision.created.v1",
		"generation_plan.created":           "video.generation-plan.created.v1",
		"episode.production.requested":      "video.production.requested.v1",
		"generation_run.created":            "video.run.state-changed.v1",
		"generation_run.cancel_requested":   "video.run.state-changed.v1",
		"generation_run.recovery_requested": "video.run.state-changed.v1",
		"approval.decided":                  "video.approval.decided.v1",
		"manifest.locked":                   "video.manifest.locked.v1",
		"dependency.stale":                  "video.dependency.stale.v1",
		"workflow_step.completed":           "video.workflow-step.completed.v1",
	}
	for action, expected := range tests {
		action, expected := action, expected
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			actual, err := eventTypeForAction(action)
			if err != nil {
				t.Fatalf("eventTypeForAction() error = %v", err)
			}
			if actual != expected {
				t.Fatalf("eventTypeForAction() = %q, want %q", actual, expected)
			}
		})
	}
	if _, err := eventTypeForAction("unregistered"); err == nil {
		t.Fatal("eventTypeForAction(unregistered) error = nil")
	}
}
