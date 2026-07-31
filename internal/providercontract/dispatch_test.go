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
	var err error
	valid.BudgetReservation, err = BindBudgetReservation(valid.BudgetReservation, BudgetBindingInput{
		RunID:     valid.RunID,
		InputHash: valid.InputHash,
		Model:     valid.Model,
		Budget:    valid.Request.Budget,
	})
	if err != nil {
		t.Fatal(err)
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
		{name: "reservation below estimate", mutate: func(r *JobRequest) { r.BudgetReservation.AmountMicros = 9 }, wantErr: true},
		{name: "reservation exceeds maximum", mutate: func(r *JobRequest) { r.BudgetReservation.AmountMicros++ }, wantErr: true},
		{name: "reservation bound to another run", mutate: func(r *JobRequest) { r.RunID = "run-2" }, wantErr: true},
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

func TestSmokeBudgetReservationBindingFixture(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("0", 64)
	model := ModelSnapshot{
		CapabilityAlias: string(CapabilityVideo),
		Provider:        "fake",
		ModelID:         "fixture-video-v1",
		RouteVersion:    "mock-routes-v1",
		CapabilityHash:  digest,
		Verification:    "mock_only",
	}
	budget := BudgetEnvelope{
		EstimatedCostMicros: 100,
		MaxCostMicros:       150,
		MaxAttempts:         1,
	}
	reservation, err := BindBudgetReservation(BudgetReservation{
		ReservationID:  "smoke-budget",
		Currency:       "CNY",
		AmountMicros:   150,
		PricingVersion: "mock-pricing-v1",
		ConfirmedBy:    "smoke-reviewer",
	}, BudgetBindingInput{
		RunID:     "smoke-run",
		InputHash: digest,
		Model:     model,
		Budget:    budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	const smokeBindingHash = "eca6cdd0d058692fdec0593cd73b96b6c89151836c394de4b5a93ceeaa189510"
	if reservation.BindingHash != smokeBindingHash {
		t.Fatalf("smoke binding hash = %s, update the pinned smoke fixture", reservation.BindingHash)
	}
}
