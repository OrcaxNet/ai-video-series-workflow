package stage1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/google/uuid"
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

type runnerTruthFixture struct {
	mu                sync.Mutex
	executionPackage  ExecutionPackage
	controlledRetry   *ControlledRetryPackage
	resolveErr        error
	prepareErr        error
	completeErr       error
	productTruthDrift bool
	shotTruthDrift    bool
	resolves          int
	prepares          int
	committedPrepares int
	completes         int
}

func (s *runnerTruthFixture) ResolvePromptSnapshot(
	_ context.Context,
	promptID string,
) (orchestration.PromptSnapshotRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolves++
	if s.resolveErr != nil {
		return orchestration.PromptSnapshotRef{}, s.resolveErr
	}
	for _, job := range s.executionPackage.PrimaryJobs {
		if job.PromptSnapshotID == promptID {
			return testPrompt(job), nil
		}
	}
	if s.controlledRetry != nil && s.controlledRetry.Job.PromptSnapshotID == promptID {
		return testPrompt(s.controlledRetry.Job), nil
	}
	return orchestration.PromptSnapshotRef{}, errors.New("prompt not found")
}

func (s *runnerTruthFixture) PrepareProviderJob(
	_ context.Context,
	_ orchestration.WorkflowStep,
	input orchestration.ExecuteProviderJobInput,
) (orchestration.PreparedProviderJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepares++
	if s.prepareErr != nil {
		return orchestration.PreparedProviderJob{}, s.prepareErr
	}
	var frozen FrozenJob
	found := false
	for _, job := range s.executionPackage.PrimaryJobs {
		if job.Run.RunID == input.Run.RunID {
			frozen, found = job, true
			break
		}
	}
	if !found && s.controlledRetry != nil && s.controlledRetry.Job.Run.RunID == input.Run.RunID {
		frozen, found = s.controlledRetry.Job, true
	}
	if !found {
		return orchestration.PreparedProviderJob{}, errors.New("run not found")
	}
	budget := providercontract.BudgetEnvelope{
		EstimatedCostMicros: 1, MaxCostMicros: 1, MaxAttempts: 1,
	}
	reservation, err := providercontract.BindBudgetReservation(
		providercontract.BudgetReservation{
			ReservationID: uuid.NewSHA1(uuid.MustParse(input.Run.RunID), []byte("budget-reservation")).String(),
			Currency:      input.BudgetCurrency, AmountMicros: 1,
			PricingVersion: "agent-plan-large-included-v1", ConfirmedBy: input.BudgetApprovalID,
		},
		providercontract.BudgetBindingInput{
			RunID: input.Run.RunID, InputHash: input.Run.RunSpecDigest,
			Model: input.Route, Budget: budget,
		},
	)
	if err != nil {
		return orchestration.PreparedProviderJob{}, err
	}
	truth := orchestration.PreparedProductTruth{
		ShotSpecRevisionID: frozen.ShotSpecRevisionID,
		Run:                frozen.Run, PromptSnapshotID: frozen.PromptSnapshotID,
		PromptSnapshotHash:  frozen.PromptSnapshotHash,
		GenerationPlanID:    frozen.GenerationPlanID,
		BudgetApprovalID:    frozen.BudgetApprovalID,
		BudgetMaximumMicros: frozen.BudgetMaximumMicros,
		BudgetCurrency:      frozen.BudgetCurrency,
		ProviderProfileID:   frozen.ProviderProfileID, Route: frozen.Route,
	}
	if s.productTruthDrift {
		truth.GenerationPlanID = uuid.NewString()
	}
	if s.shotTruthDrift {
		truth.ShotSpecRevisionID = uuid.NewString()
	}
	if input.ExpectedProductTruth != nil && *input.ExpectedProductTruth != truth {
		return orchestration.PreparedProviderJob{}, providerError(
			providercontract.CodeConflict,
			"stage 1 frozen product truth differs from the locked PostgreSQL run",
		)
	}
	s.committedPrepares++
	return orchestration.PreparedProviderJob{
		Budget: budget, BudgetReservation: reservation, ProductTruth: truth,
	}, nil
}

func (s *runnerTruthFixture) committedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committedPrepares
}

func (s *runnerTruthFixture) CompletePreparedProviderJob(
	_ context.Context,
	_ orchestration.WorkflowStep,
	_ string,
	_ orchestration.ProviderResult,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completes++
	return s.completeErr
}

func (s *runnerTruthFixture) counts() (resolves, prepares, completes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolves, s.prepares, s.completes
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
		var request providercontract.JobRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid fixture request", http.StatusBadRequest)
			return
		}
		s.created = true
		queued := s.job
		if request.JobID != s.job.JobID {
			queued.JobID = request.JobID
			queued.RunID = request.RunID
			queued.UpstreamTaskID = "provider-task-" + request.JobID
			queued.RequestID = "provider-request-" + request.JobID
			s.job = queued
		}
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

func (s *runnerAdapterFixture) publish(job providercontract.JobResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = true
	s.job = job
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
		IdempotencyKey: testFrozenJob(1).IdempotencyKey,
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

func TestRunnerPollIsReadOnlyAndRequiresPreparedFrozenAttempt(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	runner := newTestRunner(t, path, server, digest)
	key := testFrozenJob(1).IdempotencyKey

	if _, err := runner.Poll(t.Context(), PollInput{IdempotencyKey: key}); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("poll before prepare error = %v", err)
	}
	if gets, posts := fixture.counts(); gets != 0 || posts != 0 {
		t.Fatalf("adapter calls before prepare = GET %d POST %d", gets, posts)
	}
	if _, err := runner.Submit(t.Context(), SubmitInput{ShotID: "shot-01"}); err != nil {
		t.Fatal(err)
	}
	response, err := runner.Poll(t.Context(), PollInput{IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if response.State != providercontract.StatusSucceeded || response.JobID != key {
		t.Fatalf("poll response = %#v", response)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider POSTs after repeated GET = %d, want 1", posts)
	}
	ledger := readTestLedger(t, path)
	if record := ledger.Records[key]; record == nil || record.State != "PREPARED" ||
		ledger.NextTerminalSequence != 0 {
		t.Fatalf("poll mutated ledger = %#v", ledger)
	}
	if _, err := runner.Poll(t.Context(), PollInput{IdempotencyKey: "provider-job-outside"}); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("outside poll error = %v", err)
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
	rejected := runnerSubmitInput(t, "outside", "attempt-01")
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
		IdempotencyKey: testFrozenJob(1).IdempotencyKey,
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

func TestRunnerFailsClosedOnEveryPostgreSQLProductTruthDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "license"},
		{name: "consent"},
		{name: "G2 approval"},
		{name: "SAFETY approval"},
		{name: "generation plan"},
		{name: "budget scope"},
		{name: "budget amount"},
		{name: "budget currency"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			digest := strings.Repeat("a", 64)
			fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
			server := httptest.NewServer(http.HandlerFunc(fixture.handler))
			defer server.Close()
			gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := NewAdapterSubmitter(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			executionPackage := testExecutionPackage(t)
			truth := &runnerTruthFixture{
				executionPackage: executionPackage,
				prepareErr:       fmt.Errorf("%s product truth drift", test.name),
			}
			runner, err := NewRunner(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, executionPackage,
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.Submit(t.Context(), SubmitInput{ShotID: "shot-01"}); err == nil {
				t.Fatal("product truth drift was accepted")
			}
			if _, posts := fixture.counts(); posts != 0 {
				t.Fatalf("Provider POST after %s drift = %d, want 0", test.name, posts)
			}
			resolves, prepares, completes := truth.counts()
			if resolves != 1 || prepares != 1 || completes != 0 {
				t.Fatalf("product truth calls = resolve:%d prepare:%d complete:%d", resolves, prepares, completes)
			}
		})
	}
}

func TestRunnerRejectsPreparedProductTruthThatDiffersFromFrozenPackage(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*runnerTruthFixture)
	}{
		{name: "shot spec revision", mutate: func(truth *runnerTruthFixture) { truth.shotTruthDrift = true }},
		{name: "generation plan", mutate: func(truth *runnerTruthFixture) { truth.productTruthDrift = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			digest := strings.Repeat("a", 64)
			fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
			server := httptest.NewServer(http.HandlerFunc(fixture.handler))
			defer server.Close()
			gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := NewAdapterSubmitter(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			executionPackage := testExecutionPackage(t)
			truth := &runnerTruthFixture{executionPackage: executionPackage}
			test.mutate(truth)
			runner, err := NewRunner(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, executionPackage,
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.Submit(t.Context(), SubmitInput{ShotID: "shot-01"}); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
				t.Fatalf("prepared product truth drift error = %v", err)
			}
			if _, posts := fixture.counts(); posts != 0 {
				t.Fatalf("Provider POST after prepared product truth drift = %d, want 0", posts)
			}
			if truth.committedCount() != 0 {
				t.Fatalf("PostgreSQL prepare commits after frozen truth drift = %d, want 0", truth.committedCount())
			}
			ledger := readTestLedger(t, gate.path)
			if len(ledger.Records) != 0 {
				t.Fatalf("Stage 1 ledger after frozen truth drift = %#v, want zero records", ledger)
			}
		})
	}
}

func TestSubmitInputRejectsCallerReportedAuthorizationAndProviderEnvelope(t *testing.T) {
	t.Parallel()
	decoder := json.NewDecoder(strings.NewReader(`{
		"shotId":"shot-01",
		"licenseCurrent":true,
		"consentCurrent":true,
		"gateApproved":true,
		"budgetCurrent":true,
		"jobRequest":{}
	}`))
	decoder.DisallowUnknownFields()
	var input SubmitInput
	if err := decoder.Decode(&input); err == nil {
		t.Fatal("caller-reported authorization fields were accepted")
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

func TestRunnerFreezesTasklessContentSafetyRejection(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{ambiguous: true, job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	input := runnerSubmitInput(t, "shot-01", "attempt-01")
	runner := newTestRunner(t, path, server, digest)
	if _, err := runner.Submit(t.Context(), input); err == nil {
		t.Fatal("ambiguous submit error = nil")
	}

	zero := int64(0)
	rejection := successfulRunnerJob(digest)
	rejection.UpstreamTaskID = ""
	rejection.RequestID = "provider-request-safety-1"
	rejection.State = providercontract.StatusRequiresAction
	rejection.Progress = 0
	rejection.Artifacts = nil
	rejection.Usage = providercontract.Usage{}
	rejection.Cost = providercontract.Cost{
		ActualMicros: &zero, Currency: "CNY", PricingVersion: "agent-plan-large-included-v1",
		BillingMode: "subscription", Verified: true,
	}
	rejection.Error = &providercontract.Error{
		Code: providercontract.CodeContentBlocked, SafeMessage: "provider rejected content before task creation",
	}
	fixture.publish(rejection)

	completionInput := CompleteInput{
		IdempotencyKey: testFrozenJob(1).IdempotencyKey,
		ActualAFPMilli: 0, EvidenceComplete: true,
	}
	for range 2 {
		result, err := runner.Complete(t.Context(), completionInput)
		if providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
			t.Fatalf("taskless safety completion error = %v", err)
		}
		if result.ProviderTaskID != "" || result.State != "TERMINAL_FAILED" || result.EvidenceComplete ||
			!result.ContentSafetyFailed {
			t.Fatalf("taskless safety completion = %#v", result)
		}
	}
	ledger := readTestLedger(t, path)
	record := ledger.Records[testFrozenJob(1).IdempotencyKey]
	if record == nil || record.State != "TERMINAL_FAILED" || record.ProviderTaskID != "" ||
		record.EvidenceComplete || !record.ContentSafetyFailed || record.TerminalSequence != 1 ||
		ledger.ConsecutiveSafetyFailures != 1 || ledger.NextTerminalSequence != 1 {
		t.Fatalf("taskless safety record = %#v, ledger=%#v", record, ledger)
	}
	if _, err := Open(testPlan(), path); err != nil {
		t.Fatalf("Open() after taskless safety completion = %v", err)
	}
	restarted := newTestRunner(t, path, server, digest)
	replayed, err := restarted.Submit(t.Context(), input)
	if providercontract.ErrorCodeOf(err) != providercontract.CodeContentBlocked {
		t.Fatalf("taskless terminal submit replay result=%#v error=%v", replayed, err)
	}
	if replayed.ProviderTaskID != "" {
		t.Fatalf("taskless terminal submit replay returned task %q", replayed.ProviderTaskID)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider submits after taskless terminal replay = %d, want 1", posts)
	}
	if _, err := restarted.Submit(t.Context(), runnerSubmitInput(t, "shot-02", "attempt-02")); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("next submit after taskless safety rejection error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 1 {
		t.Fatalf("provider submits after taskless safety rejection = %d, want 1", posts)
	}
}

func TestRunnerControlledRetryIsAuditedOnceAndFinalizationUsesSuccessfulRun(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	primaryPackage := testExecutionPackage(t)
	retryPackage := testControlledRetryPackage(t, primaryPackage)
	truth := &runnerTruthFixture{
		executionPackage: primaryPackage, controlledRetry: &retryPackage,
	}
	newRunner := func(retry *ControlledRetryPackage) *Runner {
		t.Helper()
		gate, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := NewAdapterSubmitter(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		var runner *Runner
		if retry == nil {
			runner, err = NewRunner(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, primaryPackage,
			)
		} else {
			runner, err = NewRunnerWithControlledRetry(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, primaryPackage, *retry,
			)
		}
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	runner := newRunner(nil)
	prematureGate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	prematureAdapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRunnerWithControlledRetry(
		prematureGate, prematureAdapter,
		runnerArtifactFixture{digests: map[string]bool{digest: true}},
		truth, primaryPackage, retryPackage,
	); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("controlled retry package before terminal failure error = %v", err)
	}
	if bound := readTestLedger(t, path).ControlledRetryPackageHash; bound != "" {
		t.Fatalf("premature controlled retry package hash = %q", bound)
	}
	if _, err := runner.Submit(t.Context(), SubmitInput{ShotID: "shot-01"}); err != nil {
		t.Fatal(err)
	}
	failed := successfulRunnerJob(digest)
	failed.State = providercontract.StatusFailed
	failed.Artifacts = nil
	failed.Error = &providercontract.Error{
		Code: providercontract.CodeUnavailable, SafeMessage: "terminal Provider failure",
	}
	fixture.publish(failed)
	failedCompletion, err := runner.Complete(t.Context(), CompleteInput{
		IdempotencyKey: primaryPackage.PrimaryJobs[0].IdempotencyKey,
		ActualAFPMilli: 2_504_700, EvidenceComplete: true,
	})
	if err != nil || failedCompletion.FailureClass != string(providercontract.CodeUnavailable) {
		t.Fatalf("primary failure completion=%#v err=%v", failedCompletion, err)
	}

	// Freeze the remaining successful primary evidence without exercising nine
	// additional HTTP calls in this no-cost production-path unit regression.
	for _, frozen := range primaryPackage.PrimaryJobs[1:] {
		attempt := Attempt{
			AttemptID: frozen.AttemptID, ShotID: frozen.ShotID,
			IdempotencyKey:                     frozen.IdempotencyKey,
			EstimatedVideoTokens:               frozen.EstimatedVideoTokens,
			PredictedAFPMilli:                  frozen.PredictedAFPMilli,
			EstimatedNonSubscriptionCashMicros: frozen.EstimatedNonSubscriptionCashMicros,
		}
		if _, err := runner.gate.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		if err := runner.gate.Complete(attempt.IdempotencyKey, Completion{
			ProviderTaskID: "provider-task-" + frozen.AttemptID,
			State:          "TERMINAL_SUCCEEDED", ActualVideoTokens: 50_000,
			ActualAFPMilli: 2_504_700, EvidenceComplete: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runner.FinalizationInput(); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
		t.Fatalf("finalization before controlled retry error = %v", err)
	}

	restarted := newRunner(&retryPackage)
	driftedRetry := retryPackage
	driftedRetry.Approval.ApprovalID = uuid.NewString()
	driftedRetry, err = SealControlledRetryPackage(driftedRetry)
	if err != nil {
		t.Fatal(err)
	}
	driftGate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	driftAdapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRunnerWithControlledRetry(
		driftGate, driftAdapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
		truth, primaryPackage, driftedRetry,
	); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("replacement controlled retry package error = %v", err)
	}
	retryResult, err := restarted.SubmitControlledRetry(t.Context(), SubmitInput{ShotID: "shot-01"})
	if err != nil || retryResult.ProviderTaskID == "" {
		t.Fatalf("controlled retry result=%#v err=%v", retryResult, err)
	}
	if _, posts := fixture.counts(); posts != 2 {
		t.Fatalf("primary + controlled retry Provider POSTs = %d, want 2", posts)
	}
	retrySuccess := successfulRunnerJob(digest)
	retrySuccess.JobID = retryPackage.Job.IdempotencyKey
	retrySuccess.RunID = retryPackage.Job.Run.RunID
	retrySuccess.UpstreamTaskID = retryResult.ProviderTaskID
	retrySuccess.RequestID = "provider-request-retry"
	fixture.publish(retrySuccess)
	if _, err := restarted.Complete(t.Context(), CompleteInput{
		IdempotencyKey: retryPackage.Job.IdempotencyKey,
		ActualAFPMilli: 2_504_700, EvidenceComplete: true,
	}); err != nil {
		t.Fatal(err)
	}
	finalization, err := restarted.FinalizationInput()
	if err != nil {
		t.Fatal(err)
	}
	if finalization.RunIDs[0] != retryPackage.Job.Run.RunID ||
		finalization.RunIDs[0] == primaryPackage.PrimaryJobs[0].Run.RunID {
		t.Fatalf("controlled retry finalization Run IDs = %#v", finalization.RunIDs)
	}
	if _, err := restarted.SubmitControlledRetry(t.Context(), SubmitInput{ShotID: "shot-01"}); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("controlled retry replay error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 2 {
		t.Fatalf("controlled retry replay Provider POSTs = %d, want 2", posts)
	}
	ledger := readTestLedger(t, path)
	if len(ledger.Records) != MaximumNewProviderJobs ||
		ledger.ControlledRetryPackageHash != retryPackage.ContentHash ||
		ledger.ReservedVideoTokens != 550_000 {
		t.Fatalf("controlled retry ledger = %#v", ledger)
	}
}

func TestRunnerControlledRetryAmbiguousSubmitRemainsRecoverOnly(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("a", 64)
	fixture := &runnerAdapterFixture{job: successfulRunnerJob(digest)}
	server := httptest.NewServer(http.HandlerFunc(fixture.handler))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "ledger.json")
	primary := testExecutionPackage(t)
	retry := testControlledRetryPackage(t, primary)
	truth := &runnerTruthFixture{executionPackage: primary, controlledRetry: &retry}
	openRunner := func(withRetry bool) *Runner {
		t.Helper()
		gate, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		adapter, err := NewAdapterSubmitter(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		var runner *Runner
		if withRetry {
			runner, err = NewRunnerWithControlledRetry(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, primary, retry,
			)
		} else {
			runner, err = NewRunner(
				gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
				truth, primary,
			)
		}
		if err != nil {
			t.Fatal(err)
		}
		return runner
	}
	runner := openRunner(false)
	if _, err := runner.Submit(t.Context(), SubmitInput{ShotID: "shot-01"}); err != nil {
		t.Fatal(err)
	}
	failed := successfulRunnerJob(digest)
	failed.State = providercontract.StatusFailed
	failed.Artifacts = nil
	failed.Error = &providercontract.Error{
		Code: providercontract.CodeUnavailable, SafeMessage: "terminal Provider failure",
	}
	fixture.publish(failed)
	if _, err := runner.Complete(t.Context(), CompleteInput{
		IdempotencyKey: primary.PrimaryJobs[0].IdempotencyKey,
		ActualAFPMilli: 2_504_700, EvidenceComplete: true,
	}); err != nil {
		t.Fatal(err)
	}
	runner = openRunner(true)
	fixture.mu.Lock()
	fixture.ambiguous = true
	fixture.created = false
	fixture.mu.Unlock()
	if _, err := runner.SubmitControlledRetry(t.Context(), SubmitInput{ShotID: "shot-01"}); err == nil {
		t.Fatal("ambiguous controlled retry submit error = nil")
	}
	if _, err := openRunner(true).SubmitControlledRetry(t.Context(), SubmitInput{ShotID: "shot-01"}); providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
		t.Fatalf("controlled retry recover-only error = %v", err)
	}
	if _, posts := fixture.counts(); posts != 2 {
		t.Fatalf("ambiguous controlled retry Provider POSTs = %d, want 2 total", posts)
	}
}

func TestRunnerUsesCompletedPrimaryLedgerForSpeechV2PackageRevision(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	parent := testExecutionPackage(t)
	revised := testSpeechV2ExecutionPackage(t, parent)
	gate := completePrimaryPackage(t, path, parent, RequiredPrimaryJobs, "TERMINAL_SUCCEEDED", true)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	adapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRunner(
		gate,
		adapter,
		runnerArtifactFixture{digests: map[string]bool{}},
		&runnerTruthFixture{executionPackage: revised},
		revised,
	); err == nil {
		t.Fatal("speech-v2 runner accepted only a parent hash without the parent artifact")
	} else {
		assertForbiddenRevisionParent(t, err)
	}
	runner, err := NewRunnerWithExecutionPackageRevision(
		gate,
		adapter,
		runnerArtifactFixture{digests: map[string]bool{}},
		&runnerTruthFixture{executionPackage: revised},
		parent,
		revised,
	)
	if err != nil {
		t.Fatal(err)
	}
	input, err := runner.FinalizationInput()
	if err != nil {
		t.Fatal(err)
	}
	if input.TraceID != revised.PostProduction.TraceID ||
		input.Config.SpeechIdentityVersion != postproduction.SpeechIdentityV2 {
		t.Fatalf("speech-v2 finalization input = %#v", input)
	}
}

func TestRunnerRejectsControlledRetryBeforePromotingSpeechV2Package(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	parent := testExecutionPackage(t)
	revised := testSpeechV2ExecutionPackage(t, parent)
	retry := testControlledRetryPackage(t, revised)
	gate := completePrimaryPackage(t, path, parent, RequiredPrimaryJobs, "TERMINAL_SUCCEEDED", true)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	adapter, err := NewAdapterSubmitter(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRunnerWithControlledRetry(
		gate,
		adapter,
		runnerArtifactFixture{digests: map[string]bool{}},
		&runnerTruthFixture{executionPackage: revised, controlledRetry: &retry},
		revised,
		retry,
	); err == nil {
		t.Fatal("speech-v2 package revision unexpectedly accepted a controlled retry")
	}
	ledger := readTestLedger(t, path)
	if ledger.ExecutionPackageHash != parent.ContentHash ||
		ledger.SupersededExecutionPackageHash != "" {
		t.Fatalf("rejected runner changed package binding: %#v", ledger)
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
	executionPackage := testExecutionPackage(t)
	truth := &runnerTruthFixture{executionPackage: executionPackage}
	runner, err := NewRunner(
		gate, adapter, runnerArtifactFixture{digests: map[string]bool{digest: true}},
		truth, executionPackage,
	)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func runnerSubmitInput(t *testing.T, shotID, attemptID string) SubmitInput {
	t.Helper()
	_ = attemptID
	return SubmitInput{ShotID: shotID}
}

func successfulRunnerJob(digest string) providercontract.JobResponse {
	job := testFrozenJob(1)
	zero := int64(0)
	return providercontract.JobResponse{
		JobID: job.IdempotencyKey, RunID: job.Run.RunID,
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

func testExecutionPackage(t *testing.T) ExecutionPackage {
	t.Helper()
	jobs := make([]FrozenJob, RequiredPrimaryJobs)
	runIDs := make([]string, RequiredPrimaryJobs)
	for index := range jobs {
		jobs[index] = testFrozenJob(index + 1)
		runIDs[index] = jobs[index].Run.RunID
	}
	package_ := ExecutionPackage{
		SchemaVersion: ExecutionPackageSchemaVersion,
		BatchID:       testPlan().BatchID,
		PrimaryJobs:   jobs,
		PostProduction: orchestration.FinalizeEpisodeInput{
			EpisodeRevisionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-episode")).String(),
			RunIDs:            runIDs,
			GenerationPlanID:  jobs[0].GenerationPlanID,
			Config: orchestration.PostProductionConfig{
				Enabled: true, Evidence: postproduction.EvidenceLive,
				SpeechRoute: providercontract.ModelSnapshot{
					CapabilityAlias: string(providercontract.CapabilitySpeech),
					Provider:        "volcengine_ark", ModelID: "doubao-seed-tts-2.0",
					RouteVersion: "agent-plan-large-v1", CapabilityHash: strings.Repeat("d", 64),
					Verification: providercontract.PendingKey,
				},
				SpeechProviderProfileID:   uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-profile")).String(),
				SpeechBudgetApprovalID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-budget")).String(),
				SpeechBudgetMaximumMicros: 1_000, SpeechBudgetCurrency: "CNY",
				SubtitleLanguage: "zh-CN", BurnSubtitles: true, EnforcePoCDuration: true,
			},
			TraceID: "flo104-stage1-finalize", PersistProductTruth: true,
		},
	}
	sealed, err := SealExecutionPackage(package_)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.Validate(testPlan()); err != nil {
		t.Fatal(err)
	}
	return sealed
}

func testSpeechV2ExecutionPackage(t *testing.T, parent ExecutionPackage) ExecutionPackage {
	t.Helper()
	revised := parent
	revised.PrimaryJobs = append([]FrozenJob(nil), parent.PrimaryJobs...)
	revised.PostProduction.RunIDs = append([]string(nil), parent.PostProduction.RunIDs...)
	revised.ParentExecutionPackageHash = parent.ContentHash
	revised.PostProduction.Config.SpeechIdentityVersion = postproduction.SpeechIdentityV2
	parentVoiceVersionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-v1-version")).String()
	if parent.PostProduction.Config.SpeechVoice != nil {
		parentVoiceVersionID = parent.PostProduction.Config.SpeechVoice.AssetVersionID
	}
	revised.PostProduction.Config.SpeechVoice = &postproduction.SpeechVoiceBinding{
		AssetID:              uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-v2-asset")).String(),
		ParentAssetVersionID: parentVoiceVersionID,
		AssetVersionID:       uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-v2-version:"+parent.ContentHash)).String(),
		AssetVersionHash:     strings.Repeat("e", 64),
		LicenseSnapshotID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-speech-v2-license:"+parent.ContentHash)).String(),
		LicenseSnapshotHash:  strings.Repeat("f", 64),
		Provider:             "volcengine_ark",
		ModelID:              revised.PostProduction.Config.SpeechRoute.ModelID,
		ResourceID:           "seed-tts-2.0",
		Speaker:              "zh_female_vv_uranus_bigtts",
	}
	revised.PostProduction.Config.SpeechAuthorizedCueID = "cue-001"
	revised.PostProduction.Config.SpeechMaximumAFPMilli = 2_228
	revised.PostProduction.Config.SpeechMaximumCashMicros = 0
	revised.PostProduction.Config.SpeechMaxAttempts = 1
	revised.PostProduction.TraceID += "-speech-v2"
	sealed, err := SealExecutionPackage(revised)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.Validate(testPlan()); err != nil {
		t.Fatal(err)
	}
	return sealed
}

func testControlledRetryPackage(t *testing.T, primary ExecutionPackage) ControlledRetryPackage {
	t.Helper()
	original := primary.PrimaryJobs[0]
	retry := original
	retry.AttemptID = "stage1-controlled-retry-01"
	retry.Run = orchestration.GenerationRunRef{
		RunID:         uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-retry-run:"+retry.ShotID)).String(),
		RunSpecDigest: strings.Repeat("f", 64), Attempt: original.Run.Attempt + 1,
	}
	retry.IdempotencyKey = "provider-job-" + retry.Run.RunID
	retry.PromptSnapshotID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-retry-prompt:"+retry.ShotID)).String()
	retry.PromptSnapshotHash = strings.Repeat("b", 64)
	retry.ActivityID = "retry-" + retry.ShotID
	retry.TraceID = "flo104-stage1-retry-" + retry.ShotID
	postProduction := primary.PostProduction
	postProduction.RunIDs = append([]string(nil), primary.PostProduction.RunIDs...)
	postProduction.RunIDs[0] = retry.Run.RunID
	package_ := ControlledRetryPackage{
		SchemaVersion: ControlledRetryPackageSchemaVersion,
		BatchID:       testPlan().BatchID, ParentExecutionPackageHash: primary.ContentHash,
		Job: retry,
		Approval: RetryApproval{
			ApprovalID:              uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-retry-approval")).String(),
			OriginalAttemptID:       original.AttemptID,
			FailureClass:            string(providercontract.CodeUnavailable),
			DuplicateTaskEvidenceID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-retry-dedup-evidence")).String(),
		},
		PostProduction: postProduction,
	}
	sealed, err := SealControlledRetryPackage(package_)
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.Validate(testPlan(), primary); err != nil {
		t.Fatal(err)
	}
	return sealed
}

func testFrozenJob(index int) FrozenJob {
	shotID := fmt.Sprintf("shot-%02d", index)
	runID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-run:"+shotID)).String()
	return FrozenJob{
		ShotID:             shotID,
		ShotSpecRevisionID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-shot:"+shotID)).String(),
		AttemptID:          fmt.Sprintf("stage1-attempt-%02d", index),
		IdempotencyKey:     "provider-job-" + runID,
		Run: orchestration.GenerationRunRef{
			RunID: runID, RunSpecDigest: fmt.Sprintf("%064x", index), Attempt: 1,
		},
		PromptSnapshotID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-prompt:"+shotID)).String(),
		PromptSnapshotHash:  fmt.Sprintf("%064x", index+100),
		GenerationPlanID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-plan")).String(),
		BudgetApprovalID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-video-budget")).String(),
		BudgetMaximumMicros: 1_000, BudgetCurrency: "CNY",
		ProviderProfileID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-video-profile")).String(),
		Route: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilityVideo), Provider: "volcengine_ark",
			ModelID: FormalVideoModel, RouteVersion: "agent-plan-large-v1",
			CapabilityHash: strings.Repeat("c", 64), Verification: providercontract.PendingKey,
		},
		EstimatedVideoTokens: 100_000, PredictedAFPMilli: 2_504_700,
		WorkflowID: "flo104-stage1", ActivityID: "submit-" + shotID,
		TraceID: "flo104-stage1-" + shotID,
	}
}

func testPrompt(job FrozenJob) orchestration.PromptSnapshotRef {
	return orchestration.PromptSnapshotRef{
		ID: job.PromptSnapshotID, Digest: job.PromptSnapshotHash,
		PositivePrompt: "fixture prompt for " + job.ShotID,
		Context: providercontract.ContextRefs{
			SeriesSnapshotID: "series-context", EpisodeSnapshotID: "episode-context",
			SceneSnapshotID: "scene-context", ShotSnapshotID: "shot-context",
		},
		Output: providercontract.OutputSpec{
			Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
			FPS: 24, DurationMillis: 5_000, Format: "mp4",
		},
		InputRevisionHashes: map[string]string{"shot_spec": strings.Repeat("e", 64)},
	}
}
