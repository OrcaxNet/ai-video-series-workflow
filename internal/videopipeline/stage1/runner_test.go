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
	resolveErr        error
	prepareErr        error
	completeErr       error
	productTruthDrift bool
	resolves          int
	prepares          int
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
	return orchestration.PreparedProviderJob{
		Budget: budget, BudgetReservation: reservation, ProductTruth: truth,
	}, nil
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
		executionPackage: executionPackage, productTruthDrift: true,
	}
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
