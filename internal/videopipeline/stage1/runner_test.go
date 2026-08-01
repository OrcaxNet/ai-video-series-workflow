package stage1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

type runnerArtifactFixture struct {
	digests map[string]bool
}

func (s runnerArtifactFixture) Exists(digest string) (bool, error) {
	return s.digests[digest], nil
}

type runnerAdapterFixture struct {
	mu        sync.Mutex
	created   bool
	ambiguous bool
	posts     int
	gets      int
	job       providercontract.JobResponse
}

func (s *runnerAdapterFixture) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		s.gets++
		if !s.created || !strings.HasSuffix(r.URL.Path, "/"+s.job.JobID) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": &providercontract.Error{
				Code: providercontract.CodeNotFound, SafeMessage: "job not found",
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(s.job)
	case http.MethodPost:
		s.posts++
		if s.ambiguous {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": &providercontract.Error{
				Code: providercontract.CodeUnavailable, SafeMessage: "ambiguous fixture", Retryable: true,
			}})
			return
		}
		s.created = true
		queued := s.job
		queued.State = providercontract.StatusQueued
		queued.Artifacts = nil
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(queued)
	default:
		http.Error(w, "unexpected", http.StatusMethodNotAllowed)
	}
}

func (s *runnerAdapterFixture) counts() (gets, posts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.posts
}

func TestRunnerIsTheGatedSubmitAndImmutableCompletionPath(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	runner := newTestRunner(t, path, server, digest)
	input := runnerSubmitInput(t, "shot-01", "attempt-01")

	result, err := runner.Submit(t.Context(), input)
	if err != nil || result.ProviderTaskID != "provider-task-1" {
		t.Fatalf("Submit() result=%#v err=%v", result, err)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider submits = %d, want 1", posts)
	}

	// A restarted runner must recover the adapter job and never POST again.
	restarted := newTestRunner(t, path, server, digest)
	if _, err := restarted.Submit(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider submits after restart = %d, want 1", posts)
	}

	completionInput := CompleteInput{
		IdempotencyKey: input.Attempt.IdempotencyKey,
		ActualAFPMilli: 2_504_700, EvidenceComplete: true,
	}
	for range 3 {
		completed, err := restarted.Complete(t.Context(), completionInput)
		if err != nil {
			t.Fatal(err)
		}
		if completed.State != "TERMINAL_SUCCEEDED" || completed.ActualVideoTokens != 50_000 ||
			!completed.EvidenceComplete {
			t.Fatalf("completion = %#v", completed)
		}
	}
	ledger := readTestLedger(t, path)
	if ledger.ReservedVideoTokens != 50_000 || ledger.NextTerminalSequence != 1 {
		t.Fatalf("runner replay ledger = %#v", ledger)
	}
	conflict := completionInput
	conflict.ActualAFPMilli++
	if _, err := restarted.Complete(t.Context(), conflict); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("conflicting runner completion error = %v", err)
	}
}

func TestRunnerRejectsBeforeSubmitAndFreezesIncompleteEvidence(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	runner := newTestRunner(t, path, server, digest)
	rejected := runnerSubmitInput(t, "shot-01", "attempt-01")
	rejected.Attempt.LicenseCurrent = false
	if _, err := runner.Submit(t.Context(), rejected); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("rejected submit error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 0 {
		t.Fatalf("rejected provider submits = %d, want 0", posts)
	}

	approved := runnerSubmitInput(t, "shot-01", "attempt-01")
	if _, err := runner.Submit(t.Context(), approved); err != nil {
		t.Fatal(err)
	}
	_, err := runner.Complete(t.Context(), CompleteInput{
		IdempotencyKey: approved.Attempt.IdempotencyKey,
		ActualAFPMilli: 2_504_700, EvidenceComplete: false,
	})
	if providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("incomplete evidence error = %v", err)
	}
	if _, err := runner.Submit(t.Context(), runnerSubmitInput(t, "shot-02", "attempt-02")); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("next submit after incomplete evidence error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider submits after incomplete evidence = %d, want 1", posts)
	}
}

func TestRunnerAmbiguousSubmitIsRecoverOnlyAcrossRestart(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{ambiguous: true, job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	input := runnerSubmitInput(t, "shot-01", "attempt-01")
	if _, err := newTestRunner(t, path, server, digest).Submit(t.Context(), input); err == nil {
		t.Fatal("ambiguous submit error = nil")
	}
	if _, err := newTestRunner(t, path, server, digest).Submit(t.Context(), input); providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
		t.Fatalf("recover-only error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("ambiguous provider submits = %d, want 1", posts)
	}
}

func newTestRunner(t *testing.T, path string, server *httptest.Server, digest string) *Runner {
	t.Helper()
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner(gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func runnerSubmitInput(t *testing.T, shotID, attemptID string) SubmitInput {
	t.Helper()
	attempt := testAttempt(shotID, attemptID)
	hash := strings.Repeat("b", 64)
	model := providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilityVideo), Provider: "volcengine_ark",
		ModelID: FormalVideoModel, RouteVersion: "agent-plan-large-v1",
		CapabilityHash: strings.Repeat("c", 64), Verification: providercontract.PendingKey,
	}
	budget := providercontract.BudgetEnvelope{EstimatedCostMicros: 0, MaxCostMicros: 1, MaxAttempts: 1}
	job := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: attempt.IdempotencyKey, RunID: "run-" + attemptID,
		Capability: providercontract.CapabilityVideo, InputHash: hash, Model: model,
		Request: providercontract.GenerationRequest{
			RequestID: attempt.IdempotencyKey, IdempotencyKey: attempt.IdempotencyKey,
			Modality: providercontract.ModalityVideo, Prompt: "fixture prompt",
			PromptSnapshotID: "prompt-" + attemptID,
			Output: providercontract.OutputSpec{
				Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
				FPS: 24, DurationMillis: 5_000, Format: "mp4",
			},
			ModelHint: FormalVideoModel, Budget: budget,
		},
		TraceID: "trace-" + attemptID,
	}
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID: "budget-" + attemptID, Currency: "CNY", AmountMicros: 1,
		PricingVersion: "agent-plan-large-included-v1", ConfirmedBy: "stage1-approval",
	}, providercontract.BudgetBindingInput{
		RunID: job.RunID, InputHash: job.InputHash, Model: model, Budget: budget,
	})
	if err != nil {
		t.Fatal(err)
	}
	job.BudgetReservation = reservation
	return SubmitInput{Attempt: attempt, JobRequest: job}
}

func successfulRunnerJob(digest string) providercontract.JobResponse {
	zero := int64(0)
	return providercontract.JobResponse{
		JobID: "idempotency-attempt-01", RunID: "run-attempt-01",
		UpstreamTaskID: "provider-task-1", RequestID: "provider-request-1",
		State: providercontract.StatusSucceeded, Progress: 100,
		Model: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilityVideo), Provider: "volcengine_ark",
			ModelID: FormalVideoModel, RouteVersion: "agent-plan-large-v1",
			CapabilityHash: strings.Repeat("c", 64), Verification: providercontract.PendingKey,
		},
		Artifacts: []providercontract.AssetRef{{
			ID: "video-1", Revision: digest, Kind: providercontract.ModalityVideo,
			Role: providercontract.AssetRoleOutput, URI: "cas://sha256/" + digest,
			SHA256: digest, LicenseReference: "license-1", MediaType: "video/mp4", SizeBytes: 10,
		}},
		Usage: providercontract.Usage{VideoTokens: 50_000},
		Cost: providercontract.Cost{
			ActualMicros: &zero, Currency: "CNY", PricingVersion: "agent-plan-large-included-v1",
			BillingMode: "subscription", Verified: true,
		},
	}
}
