package repository

import (
	"errors"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
)

func TestEventTypeForAction(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"series.created":                       "video.series.created.v1",
		"source_revision.created":              "video.revision.created.v1",
		"generation_plan.created":              "video.generation-plan.created.v1",
		"episode.production.requested":         "video.production.requested.v1",
		"generation_run.created":               "video.run.state-changed.v1",
		"generation_run.cancel_requested":      "video.run.state-changed.v1",
		"generation_run.recovery_requested":    "video.run.state-changed.v1",
		"generation_run.pause_requested":       "video.run.state-changed.v1",
		"generation_run.resumed":               "video.run.state-changed.v1",
		"generation_run.workflow_finalized":    "video.run.state-changed.v1",
		"provider_job.cancellation_reconciled": "video.provider-job.state-changed.v1",
		"approval.decided":                     "video.approval.decided.v1",
		"manifest.locked":                      "video.manifest.locked.v1",
		"dependency.stale":                     "video.dependency.stale.v1",
		"workflow_step.completed":              "video.workflow-step.completed.v1",
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

func TestRequireExecutionPolicyFailsClosed(t *testing.T) {
	t.Parallel()
	validLimits := map[string]any{
		"allowedTerritories":          []any{"CN"},
		"productForms":                []any{"INTERNAL_PREVIEW"},
		"contentSafetyPolicyVersions": []any{"safety-v1"},
		"remainingCalls":              float64(2),
	}
	validPolicy := controlplane.ExecutionPolicy{
		TargetTerritory: "CN", ProductForm: "INTERNAL_PREVIEW",
		ContentSafetyPolicyVersion: "safety-v1", ContentSafetyDecisionID: "00000000-0000-4000-8000-000000000001",
	}
	tests := []struct {
		name   string
		limits map[string]any
		policy controlplane.ExecutionPolicy
		calls  int
		code   controlplane.ErrorCode
	}{
		{name: "valid", limits: validLimits, policy: validPolicy, calls: 2},
		{name: "missing limits", limits: map[string]any{}, policy: validPolicy, calls: 1, code: controlplane.CodeRegionUnavailable},
		{name: "territory", limits: validLimits, policy: func() controlplane.ExecutionPolicy {
			p := validPolicy
			p.TargetTerritory = "US"
			return p
		}(), calls: 1, code: controlplane.CodeRegionUnavailable},
		{name: "product form", limits: validLimits, policy: func() controlplane.ExecutionPolicy {
			p := validPolicy
			p.ProductForm = "COMMERCIAL_RELEASE"
			return p
		}(), calls: 1, code: controlplane.CodeCapability},
		{name: "safety", limits: validLimits, policy: func() controlplane.ExecutionPolicy {
			p := validPolicy
			p.ContentSafetyDecisionID = ""
			return p
		}(), calls: 1, code: controlplane.CodeContentBlocked},
		{name: "quota", limits: validLimits, policy: validPolicy, calls: 3, code: controlplane.CodeQuotaExceeded},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := requireExecutionPolicy(test.limits, test.policy, test.calls)
			if test.code == "" {
				if err != nil {
					t.Fatalf("requireExecutionPolicy() error = %v", err)
				}
				return
			}
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != test.code {
				t.Fatalf("error = %#v, want code %s", err, test.code)
			}
		})
	}
}
