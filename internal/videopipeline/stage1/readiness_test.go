package stage1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

func TestCommittedReadinessPlan(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../../video-pipeline/config/flo104-stage1-readiness.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan Plan
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPlanValidatePinsApprovedStage1Boundary(t *testing.T) {
	t.Parallel()
	valid := testPlan()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Plan)
	}{
		{name: "mini is not formal acceptance", mutate: func(plan *Plan) { plan.VideoModel = "doubao-seedance-2.0-mini" }},
		{name: "eleventh primary", mutate: func(plan *Plan) { plan.PrimaryShotIDs = append(plan.PrimaryShotIDs, "shot-11") }},
		{name: "extra retry", mutate: func(plan *Plan) { plan.MaximumControlledRetries = 2 }},
		{name: "token drift", mutate: func(plan *Plan) { plan.MaximumVideoTokens++ }},
		{name: "AFP drift", mutate: func(plan *Plan) { plan.MonthlyMaximumAFPMilli++ }},
		{name: "cash drift", mutate: func(plan *Plan) { plan.MaximumCashMicros++ }},
		{name: "dialogue drift", mutate: func(plan *Plan) { plan.MaximumDialogueCharacters++ }},
		{name: "TTS AFP drift", mutate: func(plan *Plan) { plan.MaximumTTSAFPMilli++ }},
		{name: "missing evidence", mutate: func(plan *Plan) { plan.RequiredEvidence = plan.RequiredEvidence[1:] }},
		{name: "TTS resource drift", mutate: func(plan *Plan) { plan.TTSPreflight.ResourceID = "seed-tts-1.0" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := valid
			plan.PrimaryShotIDs = append([]string(nil), valid.PrimaryShotIDs...)
			plan.RequiredEvidence = append([]string(nil), valid.RequiredEvidence...)
			tt.mutate(&plan)
			if err := plan.Validate(); err == nil {
				t.Fatal("mutated stage 1 plan unexpectedly passed")
			}
		})
	}
}

func TestValidateDialoguePins600CharactersAnd81AFP(t *testing.T) {
	t.Parallel()
	characters, afpMilli, err := ValidateDialogue([]string{strings.Repeat("字", 300), strings.Repeat("文", 300)})
	if err != nil {
		t.Fatal(err)
	}
	if characters != 600 || afpMilli != 81_000 {
		t.Fatalf("dialogue = %d chars, %d milli-AFP", characters, afpMilli)
	}
	if _, _, err := ValidateDialogue([]string{strings.Repeat("字", 601)}); providercontract.ErrorCodeOf(err) != providercontract.CodeBudgetExceeded {
		t.Fatalf("over-limit error = %v", err)
	}
}

func TestGateFailsClosedBeforeSubmitAcrossBudgetAndEvidenceBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		mutatePlan func(*Plan)
		mutate     func(*Attempt)
		want       providercontract.ErrorCode
	}{
		{name: "unapproved shot", mutate: func(attempt *Attempt) { attempt.ShotID = "outside" }, want: providercontract.CodeForbidden},
		{name: "license revoked", mutate: func(attempt *Attempt) { attempt.LicenseCurrent = false }, want: providercontract.CodeForbidden},
		{name: "consent revoked", mutate: func(attempt *Attempt) { attempt.ConsentCurrent = false }, want: providercontract.CodeForbidden},
		{name: "gate stale", mutate: func(attempt *Attempt) { attempt.GateApproved = false }, want: providercontract.CodeForbidden},
		{name: "budget stale", mutate: func(attempt *Attempt) { attempt.BudgetCurrent = false }, want: providercontract.CodeForbidden},
		{name: "safety stale", mutate: func(attempt *Attempt) { attempt.ContentSafetyApproved = false }, want: providercontract.CodeForbidden},
		{name: "evidence incomplete", mutate: func(attempt *Attempt) { attempt.PriorEvidenceComplete = false }, want: providercontract.CodeForbidden},
		{name: "token cap", mutate: func(attempt *Attempt) { attempt.EstimatedVideoTokens = MaximumVideoTokens + 1 }, want: providercontract.CodeBudgetExceeded},
		{name: "AFP drift", mutate: func(attempt *Attempt) { attempt.PredictedAFPMilli = 2_755_171 }, want: providercontract.CodeBudgetExceeded},
		{name: "monthly AFP cap", mutatePlan: func(plan *Plan) { plan.MonthlyBaselineAFPMilli = 37_000_000 }, want: providercontract.CodeBudgetExceeded},
		{name: "cash cap", mutate: func(attempt *Attempt) { attempt.EstimatedNonSubscriptionCashMicros = MaximumCashMicros + 1 }, want: providercontract.CodeBudgetExceeded},
		{name: "unpriced non-subscription call", mutate: func(attempt *Attempt) {
			attempt.EstimatedNonSubscriptionCashMicros = 1
			attempt.NonSubscriptionPricingVerified = false
		}, want: providercontract.CodeBudgetExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan := testPlan()
			if tt.mutatePlan != nil {
				tt.mutatePlan(&plan)
			}
			gate, err := Open(plan, filepath.Join(t.TempDir(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			attempt := testAttempt("shot-01", "attempt-01")
			if tt.mutate != nil {
				tt.mutate(&attempt)
			}
			_, err = gate.Authorize(attempt)
			if providercontract.ErrorCodeOf(err) != tt.want {
				t.Fatalf("Authorize() error = %v (%s), want %s", err, providercontract.ErrorCodeOf(err), tt.want)
			}
		})
	}
}

func TestGateAllowsExactlyTenPrimaryJobsAndOneControlledRetry(t *testing.T) {
	t.Parallel()
	gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= RequiredPrimaryJobs; index++ {
		shotID := fmt.Sprintf("shot-%02d", index)
		attempt := testAttempt(shotID, fmt.Sprintf("attempt-%02d", index))
		decision, err := gate.Authorize(attempt)
		if err != nil || decision != DecisionSubmit {
			t.Fatalf("primary %d decision=%s err=%v", index, decision, err)
		}
		completion := Completion{
			ProviderTaskID: fmt.Sprintf("task-%02d", index), State: "TERMINAL_SUCCEEDED",
			ActualVideoTokens: 100_000, ActualAFPMilli: 2_504_700,
			EvidenceComplete: true,
		}
		if index == 1 {
			completion.State = "TERMINAL_FAILED"
		}
		if err := gate.Complete(attempt.IdempotencyKey, completion); err != nil {
			t.Fatal(err)
		}
	}
	retry := testAttempt("shot-01", "attempt-retry")
	retry.Retry = &RetryApproval{
		ApprovalID: "retry-approval-1", OriginalAttemptID: "attempt-01",
		OriginalTerminal: true, FailureClass: "PROVIDER_TERMINAL_FAILURE", DuplicateTaskRuledOut: true,
	}
	if decision, err := gate.Authorize(retry); err != nil || decision != DecisionSubmit {
		t.Fatalf("controlled retry decision=%s err=%v", decision, err)
	}
	secondRetry := testAttempt("shot-01", "attempt-retry-2")
	secondRetry.Retry = retry.Retry
	if _, err := gate.Authorize(secondRetry); providercontract.ErrorCodeOf(err) != providercontract.CodeQuotaExceeded {
		t.Fatalf("second retry error = %v", err)
	}
}

func TestGateBlocksAfterTwoConsecutiveContentSafetyFailures(t *testing.T) {
	t.Parallel()
	gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 2; index++ {
		attempt := testAttempt(fmt.Sprintf("shot-%02d", index), fmt.Sprintf("attempt-%02d", index))
		if _, err := gate.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		if err := gate.Complete(attempt.IdempotencyKey, Completion{
			ProviderTaskID: fmt.Sprintf("task-%d", index), State: "TERMINAL_FAILED",
			ActualVideoTokens: 10, ActualAFPMilli: 2_504_700,
			EvidenceComplete: true, ContentSafetyFailed: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := gate.Authorize(testAttempt("shot-03", "attempt-03")); providercontract.ErrorCodeOf(err) != providercontract.CodeContentBlocked {
		t.Fatalf("third authorization error = %v", err)
	}
}

func TestGateBlocksNextSubmitOnIncompleteEvidenceOrActualAFPDrift(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		completion Completion
		want       providercontract.ErrorCode
	}{
		{
			name: "evidence incomplete",
			completion: Completion{
				ProviderTaskID: "task-1", State: "TERMINAL_SUCCEEDED",
				ActualVideoTokens: 100, ActualAFPMilli: 2_504_700,
			},
			want: providercontract.CodeForbidden,
		},
		{
			name: "actual AFP drift",
			completion: Completion{
				ProviderTaskID: "task-1", State: "TERMINAL_SUCCEEDED",
				ActualVideoTokens: 100, ActualAFPMilli: 3_000_000, EvidenceComplete: true,
			},
			want: providercontract.CodeBudgetExceeded,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
			if err != nil {
				t.Fatal(err)
			}
			first := testAttempt("shot-01", "attempt-01")
			if _, err := gate.Authorize(first); err != nil {
				t.Fatal(err)
			}
			if err := gate.Complete(first.IdempotencyKey, test.completion); err != nil {
				t.Fatal(err)
			}
			if _, err := gate.Authorize(testAttempt("shot-02", "attempt-02")); providercontract.ErrorCodeOf(err) != test.want {
				t.Fatalf("next submit error = %v", err)
			}
		})
	}
}

type fakeSubmitter struct {
	mu        sync.Mutex
	submits   int
	tasks     map[string]string
	submitErr error
}

func (s *fakeSubmitter) Recover(_ context.Context, key string) (RecoveryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[key]
	return RecoveryResult{Found: task != "", ProviderTaskID: task}, nil
}

func (s *fakeSubmitter) Submit(_ context.Context, attempt Attempt) (SubmitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.submits++
	if s.submitErr != nil {
		return SubmitResult{}, s.submitErr
	}
	task := "provider-task-" + attempt.AttemptID
	s.tasks[attempt.IdempotencyKey] = task
	return SubmitResult{ProviderTaskID: task}, nil
}

func (s *fakeSubmitter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submits
}

func TestExecutorRecoversIdempotentlyAndNeverAutoResubmitsAmbiguousSubmit(t *testing.T) {
	t.Parallel()
	t.Run("concurrent replay submits once", func(t *testing.T) {
		gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		submitter := &fakeSubmitter{tasks: make(map[string]string)}
		executor, err := NewExecutor(gate, submitter)
		if err != nil {
			t.Fatal(err)
		}
		attempt := testAttempt("shot-01", "attempt-01")
		var wait sync.WaitGroup
		errorsSeen := make(chan error, 2)
		for range 2 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				_, callErr := executor.Execute(context.Background(), attempt)
				errorsSeen <- callErr
			}()
		}
		wait.Wait()
		close(errorsSeen)
		for callErr := range errorsSeen {
			if callErr != nil {
				t.Fatalf("Execute() error = %v", callErr)
			}
		}
		if submitter.count() != 1 {
			t.Fatalf("provider submits = %d, want 1", submitter.count())
		}
	})

	t.Run("ambiguous result is recovery only", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ledger.json")
		gate, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		submitter := &fakeSubmitter{tasks: make(map[string]string), submitErr: errors.New("connection closed after submit")}
		executor, err := NewExecutor(gate, submitter)
		if err != nil {
			t.Fatal(err)
		}
		attempt := testAttempt("shot-01", "attempt-01")
		if _, err := executor.Execute(t.Context(), attempt); err == nil {
			t.Fatal("ambiguous submit error = nil")
		}
		// Reopen the ledger to model a worker restart. The same input must only
		// run recovery and must not issue a second submit.
		restartedGate, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := NewExecutor(restartedGate, submitter)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restarted.Execute(t.Context(), attempt); providercontract.ErrorCodeOf(err) != providercontract.CodeUnavailable {
			t.Fatalf("recovery-only error = %v", err)
		}
		if submitter.count() != 1 {
			t.Fatalf("ambiguous provider submits = %d, want 1", submitter.count())
		}
	})
}

func TestSeparateGateInstancesShareTheDurableSubmissionLock(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	gateA, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	gateB, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	submitter := &fakeSubmitter{tasks: make(map[string]string)}
	executorA, _ := NewExecutor(gateA, submitter)
	executorB, _ := NewExecutor(gateB, submitter)
	attempt := testAttempt("shot-01", "attempt-01")
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, executor := range []*Executor{executorA, executorB} {
		wait.Add(1)
		go func(executor *Executor) {
			defer wait.Done()
			_, callErr := executor.Execute(context.Background(), attempt)
			results <- callErr
		}(executor)
	}
	wait.Wait()
	close(results)
	for callErr := range results {
		if callErr != nil && providercontract.ErrorCodeOf(callErr) != providercontract.CodeUnavailable {
			t.Fatalf("unexpected concurrent error = %v", callErr)
		}
	}
	if submitter.count() != 1 {
		t.Fatalf("cross-instance provider submits = %d, want 1", submitter.count())
	}
}

func testPlan() Plan {
	shots := make([]string, RequiredPrimaryJobs)
	for index := range shots {
		shots[index] = fmt.Sprintf("shot-%02d", index+1)
	}
	return Plan{
		SchemaVersion: SchemaVersion, BatchID: "flo104-sample-1", VideoModel: FormalVideoModel,
		PrimaryShotIDs: shots, MaximumNewJobs: MaximumNewProviderJobs,
		MaximumControlledRetries: MaximumControlledRetries,
		MaximumVideoTokens:       MaximumVideoTokens,
		MonthlyBaselineAFPMilli:  7_514_100, MonthlyMaximumAFPMilli: MaximumMonthlyAFPMilli,
		ReferenceJobAFPMilli: 2_504_700, MaximumAFPDriftBPS: MaximumAFPDriftBPS,
		MaximumCashMicros: MaximumCashMicros, MaximumDialogueCharacters: MaximumDialogueChars,
		MaximumTTSAFPMilli: 81_000,
		RequiredEvidence:   append([]string(nil), requiredEvidence...),
		TTSPreflight: TTSPreflight{
			CompletedNoCost: true, Provider: "volcengine_ark", Model: "doubao-seed-tts-2.0",
			Region: "cn-beijing", ResourceID: "seed-tts-2.0", CredentialReference: "ARK_API_KEY",
			CredentialAvailable: true, Pricing: "1350_afp_per_10000_chars",
			UsageAttribution: "provider_usage_tokens_per_request",
		},
	}
}

func testAttempt(shotID, attemptID string) Attempt {
	return Attempt{
		AttemptID: attemptID, ShotID: shotID, IdempotencyKey: "idempotency-" + attemptID,
		EstimatedVideoTokens: 100_000, PredictedAFPMilli: 2_504_700,
		LicenseCurrent: true, ConsentCurrent: true, GateApproved: true,
		BudgetCurrent: true, ContentSafetyApproved: true, PriorEvidenceComplete: true,
		NonSubscriptionPricingVerified: true, PerRequestCostAttributionReady: true,
	}
}
