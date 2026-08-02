package repository

import (
	"errors"
	"math"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/google/uuid"
)

func TestProviderUsageUnits(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		usage   providercontract.Usage
		want    int64
		wantErr bool
	}{
		{name: "zero"},
		{name: "sum", usage: providercontract.Usage{InputUnits: 10, OutputUnits: 20}, want: 30},
		{name: "negative input", usage: providercontract.Usage{InputUnits: -1}, wantErr: true},
		{name: "negative output", usage: providercontract.Usage{OutputUnits: -1}, wantErr: true},
		{name: "overflow", usage: providercontract.Usage{InputUnits: math.MaxInt64, OutputUnits: 1}, wantErr: true},
		{name: "maximum", usage: providercontract.Usage{InputUnits: math.MaxInt64}, want: math.MaxInt64},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := providerUsageUnits(test.usage)
			if (err != nil) != test.wantErr {
				t.Fatalf("providerUsageUnits() error = %v, wantErr %t", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("providerUsageUnits() = %d, want %d", got, test.want)
			}
		})
	}
}

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
		"episode.postproduction.completed":     "video.episode.postproduction-completed.v1",
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

func TestCollectVoiceAssetBindingsFailsClosed(t *testing.T) {
	t.Parallel()
	voiceID := uuid.New()
	tests := []struct {
		name       string
		cues       []postproduction.Cue
		assets     []uuid.UUID
		require    bool
		wantCode   controlplane.ErrorCode
		wantAssets int
	}{
		{
			name:   "approved exact binding",
			cues:   []postproduction.Cue{{ID: "cue", Text: "line", VoiceRef: voiceID.String(), EndMillis: 1}},
			assets: []uuid.UUID{voiceID}, require: true, wantAssets: 1,
		},
		{
			name: "mock default voice may omit binding",
			cues: []postproduction.Cue{{ID: "cue", Text: "line", EndMillis: 1}},
		},
		{
			name:    "live voice cannot be implicit",
			cues:    []postproduction.Cue{{ID: "cue", Text: "line", EndMillis: 1}},
			require: true, wantCode: controlplane.CodeConsentRequired,
		},
		{
			name:   "voice must be an immutable UUID",
			cues:   []postproduction.Cue{{ID: "cue", Text: "line", VoiceRef: "voice-alias", EndMillis: 1}},
			assets: []uuid.UUID{voiceID}, wantCode: controlplane.CodeLicenseBlocked,
		},
		{
			name:     "voice must be part of shot assets",
			cues:     []postproduction.Cue{{ID: "cue", Text: "line", VoiceRef: voiceID.String(), EndMillis: 1}},
			wantCode: controlplane.CodeLicenseBlocked,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := make(map[uuid.UUID]struct{})
			err := collectVoiceAssetBindings(test.cues, test.assets, test.require, got)
			if test.wantCode == "" {
				if err != nil {
					t.Fatalf("collectVoiceAssetBindings() error = %v", err)
				}
				if len(got) != test.wantAssets {
					t.Fatalf("collected assets = %d, want %d", len(got), test.wantAssets)
				}
				return
			}
			var domain *controlplane.DomainError
			if !errors.As(err, &domain) || domain.Code != test.wantCode {
				t.Fatalf("error = %#v, want code %s", err, test.wantCode)
			}
		})
	}
}

func TestSummarizeSpeechCostPreservesUnknownActual(t *testing.T) {
	t.Parallel()
	actual := int64(7)
	summary, err := summarizeSpeechCost([]postproduction.ProviderAttempt{
		{Cost: providercontract.Cost{
			EstimatedMicros: 10, ActualMicros: &actual,
			Currency: "CNY", PricingVersion: "v1", Verified: true,
		}},
		{Cost: providercontract.Cost{
			EstimatedMicros: 20, ActualMicros: nil,
			Currency: "CNY", PricingVersion: "v1", Verified: false,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.EstimatedMicros != 30 || summary.ActualMicros != nil || summary.Verified {
		t.Fatalf("speech cost summary = %#v", summary)
	}
}

func TestValidatePostProductionEvidenceModePreventsPromotion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		evidence string
		mode     string
		health   string
		wantErr  bool
	}{
		{name: "mock", evidence: "mock_only", mode: "MOCK", health: "READY"},
		{name: "live", evidence: "live_provider_call", mode: "LIVE", health: "READY"},
		{name: "pending dry run", evidence: "pending_key", mode: "DRY_RUN", health: "NOT_CONFIGURED"},
		{name: "pending live", evidence: "pending_key", mode: "LIVE", health: "NOT_CONFIGURED"},
		{name: "mock cannot claim live", evidence: "live_provider_call", mode: "MOCK", health: "READY", wantErr: true},
		{name: "live must be ready", evidence: "live_provider_call", mode: "LIVE", health: "DEGRADED", wantErr: true},
		{name: "mock cannot claim pending key", evidence: "pending_key", mode: "MOCK", health: "READY", wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validatePostProductionEvidenceMode(test.evidence, test.mode, test.health)
			if (err != nil) != test.wantErr {
				t.Fatalf("validatePostProductionEvidenceMode() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}
