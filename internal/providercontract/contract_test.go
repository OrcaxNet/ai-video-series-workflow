package providercontract

import (
	"strings"
	"testing"
)

func TestGenerationRequest_Validate(t *testing.T) {
	t.Parallel()
	valid := testGenerationRequest()
	tests := []struct {
		name   string
		mutate func(*GenerationRequest)
		want   string
	}{
		{name: "valid"},
		{
			name:   "request id required",
			mutate: func(r *GenerationRequest) { r.RequestID = "" },
			want:   "request_id",
		},
		{
			name:   "idempotency key required",
			mutate: func(r *GenerationRequest) { r.IdempotencyKey = "" },
			want:   "idempotency_key",
		},
		{
			name:   "known modality required",
			mutate: func(r *GenerationRequest) { r.Modality = "hologram" },
			want:   "unsupported modality",
		},
		{
			name:   "prompt required",
			mutate: func(r *GenerationRequest) { r.Prompt = " " },
			want:   "prompt is required",
		},
		{
			name: "asset lineage required",
			mutate: func(r *GenerationRequest) {
				r.Assets[0].LicenseReference = ""
			},
			want: "license_reference",
		},
		{
			name: "positive cost ceiling required",
			mutate: func(r *GenerationRequest) {
				r.Budget.MaxCostMicros = 0
			},
			want: "max_cost_micros",
		},
		{
			name: "estimate cannot exceed ceiling",
			mutate: func(r *GenerationRequest) {
				r.Budget.EstimatedCostMicros = r.Budget.MaxCostMicros + 1
			},
			want: "estimated_cost_micros",
		},
		{
			name: "positive attempt ceiling required",
			mutate: func(r *GenerationRequest) {
				r.Budget.MaxAttempts = 0
			},
			want: "max_attempts",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			request.Assets = append([]AssetRef(nil), valid.Assets...)
			if tt.mutate != nil {
				tt.mutate(&request)
			}
			err := request.Validate()
			if tt.want == "" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestMapHTTPError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       int
		providerCode string
		wantCode     ErrorCode
		retryable    bool
	}{
		{name: "401", status: 401, wantCode: CodeUnauthenticated},
		{name: "403", status: 403, wantCode: CodeForbidden},
		{name: "429 rate", status: 429, providerCode: "RateLimitExceeded", wantCode: CodeRateLimited, retryable: true},
		{name: "429 quota", status: 429, providerCode: "QuotaExceeded", wantCode: CodeQuotaExceeded},
		{name: "content policy", status: 400, providerCode: "InputTextSensitiveContentDetected", wantCode: CodeContentBlocked},
		{name: "model missing", status: 404, providerCode: "ModelNotFound", wantCode: CodeModelUnavailable},
		{name: "server", status: 503, providerCode: "InternalError", wantCode: CodeUnavailable, retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rawSecret := "sensitive upstream body " + "Bearer " + strings.Repeat("x", 20)
			got := MapHTTPError(tt.status, tt.providerCode, "request-1", rawSecret)
			if got.Code != tt.wantCode || got.Retryable != tt.retryable {
				t.Fatalf("MapHTTPError() = %#v, want code=%s retryable=%t", got, tt.wantCode, tt.retryable)
			}
			if strings.Contains(got.Error(), rawSecret) || strings.Contains(got.Error(), strings.Repeat("x", 20)) {
				t.Fatalf("safe error leaked raw provider body: %q", got.Error())
			}
		})
	}
}

func testGenerationRequest() GenerationRequest {
	return GenerationRequest{
		RequestID:        "request-1",
		IdempotencyKey:   "series-1/episode-1/shot-1/attempt-1",
		Modality:         ModalityVideo,
		Prompt:           "A licensed fictional character walks through a reusable set.",
		PromptSnapshotID: "prompt-snapshot-1",
		Context: ContextRefs{
			SeriesSnapshotID:  "context-series-1",
			EpisodeSnapshotID: "context-episode-1",
			SceneSnapshotID:   "context-scene-1",
			ShotSnapshotID:    "context-shot-1",
		},
		Assets: []AssetRef{{
			ID:               "asset-character-1",
			Revision:         "rev-3",
			Kind:             ModalityImage,
			Role:             AssetRoleReferenceImage,
			URI:              "https://example.invalid/character.png",
			SHA256:           "aed3be0288c35d8180fc6de26885fbd87a984a75255f7f8d9527663930266643",
			LicenseReference: "license-fixture-1",
		}},
		Output: OutputSpec{
			Width:          1280,
			Height:         720,
			AspectRatio:    "16:9",
			FPS:            24,
			DurationMillis: 5_000,
			Format:         "mp4",
		},
		Budget: BudgetEnvelope{
			EstimatedCostMicros: 14_000_000,
			MaxCostMicros:       25_000_000,
			MaxAttempts:         2,
		},
	}
}
