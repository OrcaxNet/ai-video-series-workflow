package providercontract

import (
	"strings"
	"testing"
)

func TestJobRequest_Validate(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("0", 64)
	valid := JobRequest{
		SchemaVersion: "v1",
		JobID:         "job-1",
		RunID:         "run-1",
		Capability:    CapabilityVideo,
		InputHash:     digest,
		Model: ModelSnapshot{
			CapabilityAlias: string(CapabilityVideo),
			Provider:        "fake",
			ModelID:         "fake-video-v1",
			RouteVersion:    "route-v1",
			CapabilityHash:  digest,
			Verification:    "mock_only",
		},
		Request: GenerationRequest{
			RequestID:        "request-1",
			IdempotencyKey:   "job-1",
			Modality:         ModalityVideo,
			Prompt:           "fixture",
			PromptSnapshotID: "prompt-1",
			Budget: BudgetEnvelope{
				EstimatedCostMicros: 10,
				MaxCostMicros:       100,
				MaxAttempts:         1,
			},
		},
		BudgetReservation: BudgetReservation{
			ReservationID:  "reservation-1",
			Currency:       "CNY",
			AmountMicros:   100,
			PricingVersion: "mock-v1",
			ConfirmedBy:    "reviewer-1",
		},
		TraceID: "trace-1",
	}

	tests := []struct {
		name    string
		mutate  func(*JobRequest)
		wantErr bool
	}{
		{name: "valid"},
		{name: "modality mismatch", mutate: func(r *JobRequest) { r.Request.Modality = ModalityImage }, wantErr: true},
		{name: "bad digest", mutate: func(r *JobRequest) { r.InputHash = "nope" }, wantErr: true},
		{name: "unverified route", mutate: func(r *JobRequest) { r.Model.Verification = "" }, wantErr: true},
		{name: "reservation exceeds maximum", mutate: func(r *JobRequest) { r.BudgetReservation.AmountMicros++ }, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			request := valid
			if tt.mutate != nil {
				tt.mutate(&request)
			}
			err := request.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}
