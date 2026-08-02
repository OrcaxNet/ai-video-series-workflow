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

func assertForbiddenRevisionParent(t *testing.T, err error) {
	t.Helper()
	if got := providercontract.ErrorCodeOf(err); got != providercontract.CodeForbidden {
		t.Fatalf("revision parent error = %v (code %q), want forbidden", err, got)
	}
	var contractError *providercontract.Error
	if !errors.As(err, &contractError) {
		t.Fatalf("revision parent error type = %T, want *providercontract.Error", err)
	}
	if contractError.Retryable {
		t.Fatalf("revision parent error unexpectedly retryable: %#v", contractError)
	}
}

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

func TestOpenRejectsLegacyLedgerInsteadOfGuessingTerminalOrder(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	legacy := Ledger{SchemaVersion: SchemaVersion, BatchID: testPlan().BatchID, Records: map[string]*Record{}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(testPlan(), path); err == nil {
		t.Fatal("legacy ledger unexpectedly opened")
	}
}

func TestGateBindsLedgerToExactExecutionPackage(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BindExecutionPackage(hashA); err != nil {
		t.Fatal(err)
	}
	// Binding itself is a durable operation. A restart before the first
	// PREPARED record must still reject any replacement package.
	restartedEmpty, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedEmpty.BindExecutionPackage(hashB); err == nil {
		t.Fatal("different execution package replaced an empty bound ledger")
	}
	if err := restartedEmpty.BindExecutionPackage(hashA); err != nil {
		t.Fatalf("same package failed before the first record: %v", err)
	}
	if _, err := gate.Authorize(testAttempt("shot-01", "attempt-01")); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.BindExecutionPackage(hashA); err != nil {
		t.Fatalf("same execution package failed after restart: %v", err)
	}
	if err := restarted.BindExecutionPackage(hashB); err == nil {
		t.Fatal("different execution package unexpectedly opened the existing ledger")
	}
	ledger := readTestLedger(t, path)
	if ledger.SchemaVersion != LedgerSchemaVersion || ledger.ExecutionPackageHash != hashA {
		t.Fatalf("bound ledger = %#v", ledger)
	}
}

func TestExecutionPackageBindingSurvivesRejectedPrepareAndRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BindExecutionPackage(hashA); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("PostgreSQL rejected frozen truth")
	if _, err := gate.AuthorizePrepared(
		testAttempt("shot-01", "attempt-01"),
		func() error { return wantErr },
	); !errors.Is(err, wantErr) {
		t.Fatalf("AuthorizePrepared() error = %v, want %v", err, wantErr)
	}
	restarted, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.BindExecutionPackage(hashB); err == nil {
		t.Fatal("different execution package replaced binding after rejected prepare")
	}
	ledger := readTestLedger(t, path)
	if ledger.ExecutionPackageHash != hashA || len(ledger.Records) != 0 {
		t.Fatalf("rejected prepare ledger = %#v", ledger)
	}
}

func TestGatePromotesOneSpeechV2PackageAfterAllPrimaryVideoSuccess(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	parent := testExecutionPackage(t)
	revised := testSpeechV2ExecutionPackage(t, parent)
	gate := completePrimaryPackage(t, path, parent, RequiredPrimaryJobs, "TERMINAL_SUCCEEDED", true)

	if err := gate.BindExecutionPackageRevision(revised, parent); err != nil {
		t.Fatal(err)
	}
	if err := gate.BindExecutionPackageRevision(revised, parent); err != nil {
		t.Fatalf("idempotent speech-v2 package revision replay: %v", err)
	}
	ledger := readTestLedger(t, path)
	if ledger.ExecutionPackageHash != revised.ContentHash ||
		ledger.SupersededExecutionPackageHash != parent.ContentHash ||
		!equalOrderedStrings(ledger.SupersededExecutionPackageHashes, []string{parent.ContentHash}) ||
		len(ledger.Records) != RequiredPrimaryJobs {
		t.Fatalf("revised stage 1 ledger = %#v", ledger)
	}
	for _, record := range ledger.Records {
		if record.State != "TERMINAL_SUCCEEDED" || !record.EvidenceComplete {
			t.Fatalf("package revision changed primary evidence: %#v", record)
		}
	}

	restarted, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.BindExecutionPackageRevision(revised, parent); err != nil {
		t.Fatalf("speech-v2 package revision failed after restart: %v", err)
	}
	if err := restarted.BindExecutionPackage(parent.ContentHash); err == nil {
		t.Fatal("superseded parent execution package unexpectedly rebound")
	}
	second := testSpeechV2ExecutionPackage(t, revised)
	if err := restarted.BindExecutionPackageRevision(second, revised); err != nil {
		t.Fatalf("linear second package revision: %v", err)
	}
	if err := restarted.BindExecutionPackageRevision(second, revised); err != nil {
		t.Fatalf("idempotent linear second package revision: %v", err)
	}
	ledger = readTestLedger(t, path)
	if ledger.ExecutionPackageHash != second.ContentHash ||
		ledger.SupersededExecutionPackageHash != revised.ContentHash ||
		!equalOrderedStrings(
			ledger.SupersededExecutionPackageHashes,
			[]string{parent.ContentHash, revised.ContentHash},
		) {
		t.Fatalf("linearly revised stage 1 ledger = %#v", ledger)
	}
}

func TestGateAllowsOnlyOneCompetingSpeechV2Child(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	parent := testExecutionPackage(t)
	first := testSpeechV2ExecutionPackage(t, parent)
	second := testSpeechV2ExecutionPackage(t, parent)
	second.PostProduction.Config.SpeechAuthorizedCueID = "cue-002"
	var err error
	second, err = SealExecutionPackage(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.ValidateSpeechV2Revision(testPlan(), parent); err != nil {
		t.Fatal(err)
	}
	completePrimaryPackage(t, path, parent, RequiredPrimaryJobs, "TERMINAL_SUCCEEDED", true)

	children := []ExecutionPackage{first, second}
	errorsByChild := make([]error, len(children))
	var wait sync.WaitGroup
	for index, child := range children {
		wait.Add(1)
		go func(index int, child ExecutionPackage) {
			defer wait.Done()
			gate, openErr := Open(testPlan(), path)
			if openErr != nil {
				errorsByChild[index] = openErr
				return
			}
			errorsByChild[index] = gate.BindExecutionPackageRevision(child, parent)
		}(index, child)
	}
	wait.Wait()

	var succeeded, conflicted int
	for _, bindErr := range errorsByChild {
		switch providercontract.ErrorCodeOf(bindErr) {
		case "":
			if bindErr == nil {
				succeeded++
			} else {
				t.Fatalf("unexpected package revision error: %v", bindErr)
			}
		case providercontract.CodeConflict:
			conflicted++
		default:
			t.Fatalf("unexpected package revision error: %v", bindErr)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("competing revisions: succeeded=%d conflicted=%d", succeeded, conflicted)
	}
}

func TestGateRejectsSpeechV2RevisionWithoutCompleteParentArtifact(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	parent := testExecutionPackage(t)
	child := testSpeechV2ExecutionPackage(t, parent)
	gate := completePrimaryPackage(t, path, parent, RequiredPrimaryJobs, "TERMINAL_SUCCEEDED", true)

	err := gate.BindExecutionPackageRevision(child)
	assertForbiddenRevisionParent(t, err)
	ledger := readTestLedger(t, path)
	if ledger.ExecutionPackageHash != parent.ContentHash || ledger.SupersededExecutionPackageHash != "" {
		t.Fatalf("missing parent artifact changed ledger: %#v", ledger)
	}
}

func TestGateRejectsSpeechV2PackageRevisionWithoutExactSuccessfulPrimaryEvidence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		completed      int
		lastState      string
		lastEvidence   bool
		mutateRevision func(*ExecutionPackage)
	}{
		{name: "missing primary", completed: RequiredPrimaryJobs - 1, lastState: "TERMINAL_SUCCEEDED", lastEvidence: true},
		{name: "failed primary", completed: RequiredPrimaryJobs, lastState: "TERMINAL_FAILED", lastEvidence: true},
		{name: "incomplete evidence", completed: RequiredPrimaryJobs, lastState: "TERMINAL_SUCCEEDED", lastEvidence: false},
		{name: "attempt drift", completed: RequiredPrimaryJobs, lastState: "TERMINAL_SUCCEEDED", lastEvidence: true, mutateRevision: func(p *ExecutionPackage) {
			p.PrimaryJobs[0].EstimatedVideoTokens++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.json")
			parent := testExecutionPackage(t)
			gate := completePrimaryPackage(t, path, parent, test.completed, test.lastState, test.lastEvidence)
			revised := testSpeechV2ExecutionPackage(t, parent)
			if test.mutateRevision != nil {
				test.mutateRevision(&revised)
				var err error
				revised, err = SealExecutionPackage(revised)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := gate.BindExecutionPackageRevision(revised, parent); providercontract.ErrorCodeOf(err) != providercontract.CodeForbidden {
				t.Fatalf("package revision error = %v", err)
			}
			ledger := readTestLedger(t, path)
			if ledger.ExecutionPackageHash != parent.ContentHash || ledger.SupersededExecutionPackageHash != "" {
				t.Fatalf("rejected revision changed ledger binding: %#v", ledger)
			}
		})
	}
}

func completePrimaryPackage(
	t *testing.T,
	path string,
	package_ ExecutionPackage,
	completed int,
	lastState string,
	lastEvidence bool,
) *Gate {
	t.Helper()
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BindExecutionPackage(package_.ContentHash); err != nil {
		t.Fatal(err)
	}
	for index, frozen := range package_.PrimaryJobs[:completed] {
		attempt := attemptFromFrozen(frozen, nil)
		if _, err := gate.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		state := "TERMINAL_SUCCEEDED"
		evidence := true
		if index == completed-1 {
			state = lastState
			evidence = lastEvidence
		}
		completion := Completion{
			ProviderTaskID:    "provider-task-" + frozen.AttemptID,
			State:             state,
			ActualVideoTokens: frozen.EstimatedVideoTokens,
			ActualAFPMilli:    frozen.PredictedAFPMilli,
			EvidenceComplete:  evidence,
		}
		if state == "TERMINAL_FAILED" {
			completion.FailureClass = string(providercontract.CodeUnavailable)
		}
		if err := gate.Complete(frozen.IdempotencyKey, completion); err != nil {
			t.Fatal(err)
		}
	}
	return gate
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

func TestPlanValidatePinsFLO154NativeZeroTTSBoundary(t *testing.T) {
	t.Parallel()
	valid := nativeTestPlan()
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Plan)
	}{
		{name: "FLO-104 batch reuse", mutate: func(plan *Plan) { plan.BatchID = "flo104-sample-1" }},
		{name: "TTS AFP reservation", mutate: func(plan *Plan) { plan.MaximumTTSAFPMilli = 1 }},
		{name: "TTS preflight", mutate: func(plan *Plan) { plan.TTSPreflight.CompletedNoCost = true }},
		{name: "speech submit", mutate: func(plan *Plan) { plan.NativeAudio.MaximumSpeechSubmits = 1 }},
		{name: "TTS strategy", mutate: func(plan *Plan) { plan.NativeAudio.AudioStrategy = providercontract.AudioStrategyTTSRequired }},
		{name: "audio disabled", mutate: func(plan *Plan) { plan.NativeAudio.GenerateAudio = false }},
		{name: "fake stems", mutate: func(plan *Plan) { plan.NativeAudio.AudioDelivery = providercontract.NativeAudioStems }},
		{name: "analyzer hash drift", mutate: func(plan *Plan) { plan.NativeAudio.AnalyzerSealSHA256 = "missing" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := valid
			plan.PrimaryShotIDs = append([]string(nil), valid.PrimaryShotIDs...)
			plan.RequiredEvidence = append([]string(nil), valid.RequiredEvidence...)
			native := *valid.NativeAudio
			plan.NativeAudio = &native
			test.mutate(&plan)
			if err := plan.Validate(); err == nil {
				t.Fatal("mutated FLO-154 native plan unexpectedly passed")
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
		{name: "token cap", mutate: func(attempt *Attempt) { attempt.EstimatedVideoTokens = MaximumVideoTokens + 1 }, want: providercontract.CodeBudgetExceeded},
		{name: "AFP drift", mutate: func(attempt *Attempt) { attempt.PredictedAFPMilli = 2_755_171 }, want: providercontract.CodeBudgetExceeded},
		{name: "monthly AFP cap", mutatePlan: func(plan *Plan) { plan.MonthlyBaselineAFPMilli = 37_000_000 }, want: providercontract.CodeBudgetExceeded},
		{name: "cash cap", mutate: func(attempt *Attempt) { attempt.EstimatedNonSubscriptionCashMicros = MaximumCashMicros + 1 }, want: providercontract.CodeBudgetExceeded},
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
		FailureClass: "PROVIDER_TERMINAL_FAILURE", DuplicateTaskEvidenceID: "recovery-evidence-1",
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

func TestGateTerminalCompletionReplayIsStrictlyIdempotent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	attempt := testAttempt("shot-01", "attempt-01")
	if _, err := gate.Authorize(attempt); err != nil {
		t.Fatal(err)
	}
	completion := Completion{
		ProviderTaskID: "task-1", State: "TERMINAL_FAILED",
		ActualVideoTokens: 50_000, ActualAFPMilli: 2_504_700,
		EvidenceComplete: true, ContentSafetyFailed: true,
	}
	for range 3 {
		if err := gate.Complete(attempt.IdempotencyKey, completion); err != nil {
			t.Fatal(err)
		}
	}
	ledger := readTestLedger(t, path)
	if ledger.ReservedVideoTokens != 50_000 || ledger.ReservedAFPMilli != 2_504_700 ||
		ledger.ConsecutiveSafetyFailures != 1 || ledger.NextTerminalSequence != 1 {
		t.Fatalf("replayed ledger = %#v", ledger)
	}
	record := ledger.Records[attempt.IdempotencyKey]
	if record == nil || record.TerminalSequence != 1 {
		t.Fatalf("terminal record = %#v", record)
	}

	conflict := completion
	conflict.ActualVideoTokens++
	if err := gate.Complete(attempt.IdempotencyKey, conflict); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("conflicting completion error = %v", err)
	}
	if after := readTestLedger(t, path); after.ReservedVideoTokens != 50_000 ||
		after.ConsecutiveSafetyFailures != 1 || after.NextTerminalSequence != 1 {
		t.Fatalf("conflicting completion mutated ledger = %#v", after)
	}
}

func TestGateAllowsMissingProviderTaskOnlyForIncompleteSafetyRejection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		completion Completion
		wantOK     bool
	}{
		{
			name: "incomplete content safety rejection",
			completion: Completion{
				State: "TERMINAL_FAILED", ContentSafetyFailed: true, EvidenceComplete: false,
			},
			wantOK: true,
		},
		{
			name: "ordinary failure",
			completion: Completion{
				State: "TERMINAL_FAILED", EvidenceComplete: false,
			},
		},
		{
			name: "claimed complete evidence",
			completion: Completion{
				State: "TERMINAL_FAILED", ContentSafetyFailed: true, EvidenceComplete: true,
			},
		},
		{
			name: "success",
			completion: Completion{
				State: "TERMINAL_SUCCEEDED", EvidenceComplete: false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "ledger.json")
			gate, err := Open(testPlan(), path)
			if err != nil {
				t.Fatal(err)
			}
			attempt := testAttempt("shot-01", "attempt-01")
			if _, err := gate.Authorize(attempt); err != nil {
				t.Fatal(err)
			}
			err = gate.Complete(attempt.IdempotencyKey, test.completion)
			if test.wantOK && err != nil {
				t.Fatalf("Complete() error = %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatal("Complete() error = nil")
			}
			ledger := readTestLedger(t, path)
			record := ledger.Records[attempt.IdempotencyKey]
			if test.wantOK {
				if record.State != "TERMINAL_FAILED" || record.TerminalSequence != 1 ||
					!record.ContentSafetyFailed || record.EvidenceComplete {
					t.Fatalf("taskless terminal record = %#v", record)
				}
				if _, err := Open(testPlan(), path); err != nil {
					t.Fatalf("Open() after taskless completion = %v", err)
				}
				return
			}
			if record.State != "PREPARED" || record.TerminalSequence != 0 {
				t.Fatalf("rejected taskless completion mutated record = %#v", record)
			}
		})
	}
}

func TestGateOldSuccessReplayCannotResetSafetyCircuit(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "ledger.json")
	gate, err := Open(testPlan(), path)
	if err != nil {
		t.Fatal(err)
	}
	completions := make(map[string]Completion)
	for index := 1; index <= 3; index++ {
		attempt := testAttempt(fmt.Sprintf("shot-%02d", index), fmt.Sprintf("attempt-%02d", index))
		if _, err := gate.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		completion := Completion{
			ProviderTaskID: fmt.Sprintf("task-%02d", index), State: "TERMINAL_SUCCEEDED",
			ActualVideoTokens: 100_000, ActualAFPMilli: 2_504_700, EvidenceComplete: true,
		}
		if index > 1 {
			completion.State = "TERMINAL_FAILED"
			completion.ContentSafetyFailed = true
		}
		if err := gate.Complete(attempt.IdempotencyKey, completion); err != nil {
			t.Fatal(err)
		}
		completions[attempt.IdempotencyKey] = completion
	}
	if _, err := gate.Authorize(testAttempt("shot-04", "attempt-04")); providercontract.ErrorCodeOf(err) != providercontract.CodeContentBlocked {
		t.Fatalf("circuit before replay error = %v", err)
	}
	if err := gate.Complete("idempotency-attempt-01", completions["idempotency-attempt-01"]); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Authorize(testAttempt("shot-04", "attempt-04")); providercontract.ErrorCodeOf(err) != providercontract.CodeContentBlocked {
		t.Fatalf("old success replay reopened circuit: %v", err)
	}
	ledger := readTestLedger(t, path)
	if ledger.ConsecutiveSafetyFailures != 2 || ledger.NextTerminalSequence != 3 {
		t.Fatalf("safety sequence after replay = %#v", ledger)
	}
}

func TestSeparateGateInstancesReplayCompletionWithoutUsageDrift(t *testing.T) {
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
	attempt := testAttempt("shot-01", "attempt-01")
	if _, err := gateA.Authorize(attempt); err != nil {
		t.Fatal(err)
	}
	completion := Completion{
		ProviderTaskID: "task-1", State: "TERMINAL_SUCCEEDED",
		ActualVideoTokens: 50_000, ActualAFPMilli: 2_504_700,
		ActualCashMicros: 125, EvidenceComplete: true,
	}
	errorsSeen := make(chan error, 2)
	var wait sync.WaitGroup
	for _, gate := range []*Gate{gateA, gateB} {
		wait.Add(1)
		go func(gate *Gate) {
			defer wait.Done()
			errorsSeen <- gate.Complete(attempt.IdempotencyKey, completion)
		}(gate)
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	ledger := readTestLedger(t, path)
	if ledger.ReservedVideoTokens != 50_000 || ledger.ReservedAFPMilli != 2_504_700 ||
		ledger.ReservedCashMicros != 125 || len(ledger.Records) != 1 || ledger.NextTerminalSequence != 1 {
		t.Fatalf("cross-process replay ledger = %#v", ledger)
	}
}

func TestGateTerminalCompletionCannotBeDowngradedToAmbiguous(t *testing.T) {
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
	attempt := testAttempt("shot-01", "attempt-01")
	if _, err := gateA.Authorize(attempt); err != nil {
		t.Fatal(err)
	}
	completion := Completion{
		ProviderTaskID: "task-1", State: "TERMINAL_SUCCEEDED",
		ActualVideoTokens: 50_000, ActualAFPMilli: 2_504_700, EvidenceComplete: true,
	}
	if err := gateA.Complete(attempt.IdempotencyKey, completion); err != nil {
		t.Fatal(err)
	}
	if err := gateB.MarkAmbiguous(attempt.IdempotencyKey); providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
		t.Fatalf("late MarkAmbiguous() error = %v", err)
	}
	restarted, err := Open(testPlan(), path)
	if err != nil {
		t.Fatalf("Open() after late ambiguous transition = %v", err)
	}
	if decision, err := restarted.Authorize(attempt); err != nil || decision != DecisionReplay {
		t.Fatalf("terminal replay decision=%s err=%v", decision, err)
	}
	ledger := readTestLedger(t, path)
	record := ledger.Records[attempt.IdempotencyKey]
	if record == nil || record.State != "TERMINAL_SUCCEEDED" || record.TerminalSequence != 1 ||
		ledger.NextTerminalSequence != 1 {
		t.Fatalf("terminal record after late ambiguous callback = %#v, ledger=%#v", record, ledger)
	}
}

func TestSeparateGateInstancesSerializeAmbiguousCompletionRace(t *testing.T) {
	t.Parallel()
	for iteration := range 20 {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("ledger-%02d.json", iteration))
		gateA, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		gateB, err := Open(testPlan(), path)
		if err != nil {
			t.Fatal(err)
		}
		attempt := testAttempt("shot-01", fmt.Sprintf("attempt-%02d", iteration))
		if _, err := gateA.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		completion := Completion{
			ProviderTaskID: fmt.Sprintf("task-%02d", iteration), State: "TERMINAL_SUCCEEDED",
			ActualVideoTokens: 50_000, ActualAFPMilli: 2_504_700, EvidenceComplete: true,
		}
		start := make(chan struct{})
		errorsSeen := make(chan error, 2)
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			errorsSeen <- gateA.Complete(attempt.IdempotencyKey, completion)
		}()
		go func() {
			defer wait.Done()
			<-start
			errorsSeen <- gateB.MarkAmbiguous(attempt.IdempotencyKey)
		}()
		close(start)
		wait.Wait()
		close(errorsSeen)
		for err := range errorsSeen {
			if err != nil && providercontract.ErrorCodeOf(err) != providercontract.CodeConflict {
				t.Fatalf("race error = %v", err)
			}
		}
		if _, err := Open(testPlan(), path); err != nil {
			t.Fatalf("Open() after race = %v", err)
		}
		ledger := readTestLedger(t, path)
		record := ledger.Records[attempt.IdempotencyKey]
		if record == nil || record.State != "TERMINAL_SUCCEEDED" || record.TerminalSequence != 1 {
			t.Fatalf("race terminal record = %#v", record)
		}
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
	mu             sync.Mutex
	submits        int
	tasks          map[string]string
	submitErr      error
	forcedRecovery *RecoveryResult
}

func (s *fakeSubmitter) Recover(_ context.Context, key string) (RecoveryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forcedRecovery != nil {
		return *s.forcedRecovery, nil
	}
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

	t.Run("found recovery without task fails closed", func(t *testing.T) {
		gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		submitter := &fakeSubmitter{
			tasks: make(map[string]string), forcedRecovery: &RecoveryResult{Found: true},
		}
		executor, err := NewExecutor(gate, submitter)
		if err != nil {
			t.Fatal(err)
		}
		result, err := executor.Execute(context.Background(), testAttempt("shot-01", "attempt-01"))
		if providercontract.ErrorCodeOf(err) != providercontract.CodeConflict || result.ProviderTaskID != "" {
			t.Fatalf("empty recovery result=%#v error=%v", result, err)
		}
		if submitter.count() != 0 {
			t.Fatalf("provider submits = %d, want 0", submitter.count())
		}
	})

	t.Run("terminal ledger wins over recovered task", func(t *testing.T) {
		gate, err := Open(testPlan(), filepath.Join(t.TempDir(), "ledger.json"))
		if err != nil {
			t.Fatal(err)
		}
		attempt := testAttempt("shot-01", "attempt-01")
		if _, err := gate.Authorize(attempt); err != nil {
			t.Fatal(err)
		}
		if err := gate.Complete(attempt.IdempotencyKey, Completion{
			ProviderTaskID: "provider-task-attempt-01", State: "TERMINAL_SUCCEEDED",
			ActualVideoTokens: 50_000, ActualAFPMilli: 2_504_700, EvidenceComplete: true,
		}); err != nil {
			t.Fatal(err)
		}
		submitter := &fakeSubmitter{tasks: map[string]string{attempt.IdempotencyKey: "provider-task-attempt-01"}}
		executor, err := NewExecutor(gate, submitter)
		if err != nil {
			t.Fatal(err)
		}
		result, err := executor.Execute(context.Background(), attempt)
		if providercontract.ErrorCodeOf(err) != providercontract.CodeConflict || result.ProviderTaskID != "" {
			t.Fatalf("terminal replay result=%#v error=%v", result, err)
		}
		if submitter.count() != 0 {
			t.Fatalf("provider submits = %d, want 0", submitter.count())
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

func TestPreparedProductTruthAndLedgerReservationShareCrossProcessLock(t *testing.T) {
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
	var prepareMu sync.Mutex
	prepares := 0
	prepare := func(context.Context) (providercontract.JobRequest, error) {
		prepareMu.Lock()
		prepares++
		prepareMu.Unlock()
		return providercontract.JobRequest{}, nil
	}
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, executor := range []*Executor{executorA, executorB} {
		wait.Add(1)
		go func(executor *Executor) {
			defer wait.Done()
			_, callErr := executor.ExecutePrepared(context.Background(), attempt, prepare)
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
	prepareMu.Lock()
	preparedCount := prepares
	prepareMu.Unlock()
	if preparedCount != 1 || submitter.count() != 1 {
		t.Fatalf("product-truth prepares=%d provider submits=%d, want 1/1", preparedCount, submitter.count())
	}

	rejectedGate, err := Open(testPlan(), filepath.Join(t.TempDir(), "rejected-ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	rejectedSubmitter := &fakeSubmitter{tasks: make(map[string]string)}
	rejectedExecutor, _ := NewExecutor(rejectedGate, rejectedSubmitter)
	prepareErr := errors.New("PostgreSQL product truth rejected")
	if _, err := rejectedExecutor.ExecutePrepared(
		context.Background(), attempt,
		func(context.Context) (providercontract.JobRequest, error) {
			return providercontract.JobRequest{}, prepareErr
		},
	); !errors.Is(err, prepareErr) {
		t.Fatalf("prepare error = %v, want %v", err, prepareErr)
	}
	if decision, err := rejectedGate.Inspect(attempt); err != nil || decision != DecisionSubmit {
		t.Fatalf("rejected product truth left reservation: decision=%s err=%v", decision, err)
	}
	if rejectedSubmitter.count() != 0 {
		t.Fatalf("rejected product truth Provider submits=%d, want 0", rejectedSubmitter.count())
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

func nativeTestPlan() Plan {
	plan := testPlan()
	plan.SchemaVersion = NativeSchemaVersion
	plan.BatchID = "flo154-native-sample-1"
	for index := range plan.PrimaryShotIDs {
		plan.PrimaryShotIDs[index] = fmt.Sprintf("flo154-shot-%02d", index+1)
	}
	plan.MonthlyBaselineAFPMilli = 0
	plan.MaximumTTSAFPMilli = 0
	plan.RequiredEvidence = append([]string(nil), nativeRequiredEvidence...)
	plan.TTSPreflight = TTSPreflight{}
	plan.NativeAudio = &NativeAudioPreflight{
		ProductSchema: NativeProductSchemaVersion,
		AudioStrategy: providercontract.AudioStrategyNativePreferred,
		GenerateAudio: true, AudioDelivery: providercontract.NativeAudioMix,
		MaximumSpeechSubmits: 0, AnalyzerSealSHA256: strings.Repeat("a", 64),
	}
	return plan
}

func testAttempt(shotID, attemptID string) Attempt {
	return Attempt{
		AttemptID: attemptID, ShotID: shotID, IdempotencyKey: "idempotency-" + attemptID,
		EstimatedVideoTokens: 100_000, PredictedAFPMilli: 2_504_700,
	}
}

func readTestLedger(t *testing.T, path string) Ledger {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ledger Ledger
	if err := json.Unmarshal(data, &ledger); err != nil {
		t.Fatal(err)
	}
	return ledger
}
