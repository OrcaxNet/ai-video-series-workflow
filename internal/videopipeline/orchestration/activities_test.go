package orchestration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"go.temporal.io/sdk/testsuite"
)

type activityJournalFixture struct {
	replay          json.RawMessage
	replayCompleted bool
	beginStep       WorkflowStep
	inputHash       string
	completedStep   WorkflowStep
	output          json.RawMessage
}

type cancellationLedgerFixture struct {
	ProductionLedger
	prepared      bool
	preparedCalls int
	recorded      []CancelProviderResult
}

func (l *cancellationLedgerFixture) ProviderJobPrepared(context.Context, string) (bool, error) {
	l.preparedCalls++
	return l.prepared, nil
}

func (l *cancellationLedgerFixture) RecordProviderCancellation(
	_ context.Context,
	_ WorkflowStep,
	_ CancelProviderJobInput,
	result CancelProviderResult,
) error {
	l.recorded = append(l.recorded, result)
	return nil
}

func (j *activityJournalFixture) BeginWorkflowStep(
	_ context.Context,
	step WorkflowStep,
	inputHash string,
) (json.RawMessage, bool, error) {
	j.beginStep = step
	j.inputHash = inputHash
	return j.replay, j.replayCompleted, nil
}

func (j *activityJournalFixture) CompleteWorkflowStep(
	_ context.Context,
	step WorkflowStep,
	_ string,
	output json.RawMessage,
) error {
	j.completedStep = step
	j.output = append(json.RawMessage(nil), output...)
	return nil
}

func TestActivities_CompilePromptCommitsDurableJournalResult(t *testing.T) {
	journal := &activityJournalFixture{}
	activities := NewActivitiesWithJournal("http://provider.invalid", journal)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CompilePrompt)

	encoded, err := env.ExecuteActivity(activities.CompilePrompt, CompilePromptInput{
		ShotSpecRevisionID:   "shot-1",
		GenerationProfileRef: "profile-1",
		TraceID:              "trace-1",
	})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result PromptSnapshotRef
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.ID == "" || result.Digest == "" {
		t.Fatalf("result = %#v", result)
	}
	if journal.beginStep.WorkflowID == "" || journal.beginStep.ActivityID == "" ||
		journal.beginStep.ActivityType == "" || journal.inputHash == "" {
		t.Fatalf("journal begin = %#v, inputHash=%q", journal.beginStep, journal.inputHash)
	}
	if journal.completedStep != journal.beginStep || len(journal.output) == 0 {
		t.Fatalf("journal completion = %#v, output=%s", journal.completedStep, journal.output)
	}
}

func TestActivities_CompilePromptReplaysWithoutRevalidation(t *testing.T) {
	expected := PromptSnapshotRef{ID: "prompt-replayed", Digest: "replayed-digest"}
	replay, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	journal := &activityJournalFixture{replay: replay, replayCompleted: true}
	activities := NewActivitiesWithJournal("http://provider.invalid", journal)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CompilePrompt)

	encoded, err := env.ExecuteActivity(activities.CompilePrompt, CompilePromptInput{TraceID: "trace-2"})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result PromptSnapshotRef
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result != expected {
		t.Fatalf("result = %#v, want %#v", result, expected)
	}
	if len(journal.output) != 0 {
		t.Fatalf("replay wrote a second completion: %s", journal.output)
	}
}

func TestActivities_CancelProviderJobConvergesWithoutCallingUnpreparedProvider(t *testing.T) {
	t.Parallel()
	ledger := &cancellationLedgerFixture{}
	activities := NewProductionActivities(
		"http://provider.invalid",
		nil,
		ledger,
		nil,
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	input := CancelProviderJobInput{
		OperationID: "cancel-operation",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-never-prepared"},
			PersistProductTruth: true,
		},
		ReasonCode: "USER_CANCELLED",
		TraceID:    "cancel-before-submit",
	}

	encoded, err := env.ExecuteActivity(activities.CancelProviderJob, input)
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result CancelProviderResult
	if err := encoded.Get(&result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.State != "CANCELLED" || result.ErrorCode != "" {
		t.Fatalf("result = %#v", result)
	}
	if ledger.preparedCalls != 1 || len(ledger.recorded) != 1 ||
		ledger.recorded[0].State != "CANCELLED" {
		t.Fatalf(
			"ledger calls = prepared:%d recorded:%#v",
			ledger.preparedCalls,
			ledger.recorded,
		)
	}
}

func TestActivities_CancelProviderJobReplaysAfterResponseLossAndWorkerRestart(t *testing.T) {
	t.Parallel()
	providerCalls := 0
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls++
		if r.Method != http.MethodPost ||
			r.URL.Path != "/v1/jobs/provider-job-run-prepared/cancel" {
			t.Fatalf("provider request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(providercontract.JobResponse{
			JobID:          "provider-job-run-prepared",
			RunID:          "run-prepared",
			UpstreamTaskID: "upstream-1",
			State:          providercontract.StatusCancelled,
		}); err != nil {
			t.Fatalf("encode provider response: %v", err)
		}
	}))
	defer provider.Close()

	journal := &activityJournalFixture{}
	ledger := &cancellationLedgerFixture{prepared: true}
	input := CancelProviderJobInput{
		OperationID: "cancel-operation",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-prepared"},
			PersistProductTruth: true,
		},
		ReasonCode: "USER_CANCELLED",
		TraceID:    "cancel-response-loss",
	}
	execute := func(activities *Activities) CancelProviderResult {
		t.Helper()
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(activities.CancelProviderJob)
		encoded, err := env.ExecuteActivity(activities.CancelProviderJob, input)
		if err != nil {
			t.Fatalf("ExecuteActivity() error = %v", err)
		}
		var result CancelProviderResult
		if err := encoded.Get(&result); err != nil {
			t.Fatalf("decode result: %v", err)
		}
		return result
	}

	firstWorker := NewProductionActivities(provider.URL, journal, ledger, nil)
	first := execute(firstWorker)
	if first.State != "CANCELLED" || providerCalls != 1 ||
		ledger.preparedCalls != 1 || len(ledger.recorded) != 1 ||
		len(journal.output) == 0 {
		t.Fatalf(
			"first execution = result:%#v provider:%d prepared:%d recorded:%d journal:%s",
			first,
			providerCalls,
			ledger.preparedCalls,
			len(ledger.recorded),
			journal.output,
		)
	}

	// Model a completed Activity response being lost after the journal commit,
	// followed by a retry on a newly constructed worker.
	journal.replay = append(json.RawMessage(nil), journal.output...)
	journal.replayCompleted = true
	secondWorker := NewProductionActivities(provider.URL, journal, ledger, nil)
	second := execute(secondWorker)
	if second != first {
		t.Fatalf("replayed result = %#v, want %#v", second, first)
	}
	if providerCalls != 1 || ledger.preparedCalls != 1 || len(ledger.recorded) != 1 {
		t.Fatalf(
			"replay repeated side effects = provider:%d prepared:%d recorded:%d",
			providerCalls,
			ledger.preparedCalls,
			len(ledger.recorded),
		)
	}
}
