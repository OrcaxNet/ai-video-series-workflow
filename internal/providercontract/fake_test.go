package providercontract

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestFakeProvider_FixtureScenarios(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/scenarios.json")
	if err != nil {
		t.Fatal(err)
	}
	var scenarios []struct {
		Name       string       `json:"name"`
		Scenario   FakeScenario `json:"scenario"`
		ErrorCode  ErrorCode    `json:"error_code"`
		HTTPStatus int          `json:"http_status"`
	}
	if err := json.Unmarshal(data, &scenarios); err != nil {
		t.Fatal(err)
	}
	for _, tt := range scenarios {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			provider := NewFakeProvider(tt.Scenario)
			job, err := provider.Submit(t.Context(), testGenerationRequest())
			if ErrorCodeOf(err) != tt.ErrorCode {
				t.Fatalf("Submit() error code = %q, want %q; err=%v", ErrorCodeOf(err), tt.ErrorCode, err)
			}
			if tt.ErrorCode == "" && job.Status != StatusQueued {
				t.Fatalf("Submit() status = %q, want queued", job.Status)
			}
			if providerErr, ok := err.(*Error); ok && providerErr.HTTPStatus != tt.HTTPStatus {
				t.Fatalf("Submit() http status = %d, want %d", providerErr.HTTPStatus, tt.HTTPStatus)
			}
		})
	}
}

func TestFakeProvider_SuccessAndIdempotency(t *testing.T) {
	t.Parallel()
	provider := NewFakeProvider(FakeSuccess)
	request := testGenerationRequest()

	first, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent submit created two jobs: %q and %q", first.ID, second.ID)
	}

	running, err := provider.Poll(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if running.Status != StatusRunning {
		t.Fatalf("first poll status = %q, want running", running.Status)
	}
	succeeded, err := provider.Poll(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if succeeded.Status != StatusSucceeded || succeeded.Output == nil ||
		len(succeeded.Output.Assets) != 1 {
		t.Fatalf("second poll = %#v, want succeeded output", succeeded)
	}
}

func TestFakeProvider_RecoveryUsesOriginalJob(t *testing.T) {
	t.Parallel()
	provider := NewFakeProvider(FakeRecovery)
	request := testGenerationRequest()

	if _, err := provider.Submit(t.Context(), request); ErrorCodeOf(err) != CodeUnavailable {
		t.Fatalf("first Submit() error = %v, want unavailable", err)
	}
	recovered, err := provider.Submit(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != "fake-job-001" {
		t.Fatalf("recovered job ID = %q, want fake-job-001", recovered.ID)
	}
}

func TestFakeProvider_DuplicateCallback(t *testing.T) {
	t.Parallel()
	provider := NewFakeProvider(FakeDuplicateCallback)
	submitted, err := provider.Submit(t.Context(), testGenerationRequest())
	if err != nil {
		t.Fatal(err)
	}
	callback := Callback{
		EventID:   "event-1",
		JobID:     submitted.ID,
		Status:    StatusRunning,
		CreatedAt: time.Unix(1_800_000_010, 0).UTC(),
	}
	applied, _, err := provider.ApplyCallback(callback)
	if err != nil || !applied {
		t.Fatalf("first ApplyCallback() applied=%t err=%v", applied, err)
	}
	applied, job, err := provider.ApplyCallback(callback)
	if err != nil || applied || job.Status != StatusRunning {
		t.Fatalf("duplicate ApplyCallback() applied=%t job=%#v err=%v", applied, job, err)
	}
}

func TestFakeProvider_CancelAndTerminalRace(t *testing.T) {
	t.Parallel()
	t.Run("queued cancellation is idempotent", func(t *testing.T) {
		t.Parallel()
		provider := NewFakeProvider(FakeSuccess)
		job, err := provider.Submit(t.Context(), testGenerationRequest())
		if err != nil {
			t.Fatal(err)
		}
		cancelled, err := provider.Cancel(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		again, err := provider.Cancel(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if cancelled.Status != StatusCancelled || again.Status != StatusCancelled {
			t.Fatalf("Cancel() statuses = %q, %q", cancelled.Status, again.Status)
		}
	})
	t.Run("success wins cancel race", func(t *testing.T) {
		t.Parallel()
		provider := NewFakeProvider(FakeCancelRace)
		job, err := provider.Submit(t.Context(), testGenerationRequest())
		if err != nil {
			t.Fatal(err)
		}
		succeeded, err := provider.Poll(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		afterCancel, err := provider.Cancel(t.Context(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if succeeded.Status != StatusSucceeded || afterCancel.Status != StatusSucceeded {
			t.Fatalf("terminal state regressed: before=%q after=%q", succeeded.Status, afterCancel.Status)
		}
	})
}

func TestFakeProvider_ContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	provider := NewFakeProvider(FakeSuccess)
	if _, err := provider.Submit(ctx, testGenerationRequest()); ErrorCodeOf(err) != CodeConflict {
		t.Fatalf("Submit() error = %v, want conflict", err)
	}
}
