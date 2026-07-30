package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
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

type postProductionLedgerFixture struct {
	request        postproduction.Request
	authorizeErr   error
	prepareCalls   int
	authorizeCalls int
	commitCalls    int
}

func (l *postProductionLedgerFixture) PrepareEpisodePostProduction(
	context.Context,
	WorkflowStep,
	FinalizeEpisodeInput,
) (postproduction.Request, error) {
	l.prepareCalls++
	return l.request, nil
}

func (l *postProductionLedgerFixture) AuthorizeEpisodePostProduction(
	context.Context,
	WorkflowStep,
	FinalizeEpisodeInput,
) error {
	l.authorizeCalls++
	return l.authorizeErr
}

func (l *postProductionLedgerFixture) CommitEpisodePostProduction(
	context.Context,
	WorkflowStep,
	FinalizeEpisodeInput,
	postproduction.Result,
) error {
	l.commitCalls++
	return nil
}

type postProductionExecutorFixture struct {
	result postproduction.Result
	err    error
	calls  int
}

func (e *postProductionExecutorFixture) Finalize(
	context.Context,
	postproduction.Request,
) (postproduction.Result, error) {
	e.calls++
	return e.result, e.err
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

func TestValidPostProductionManifestHash(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "valid", value: strings.Repeat("a", 64), want: true},
		{name: "zero", value: strings.Repeat("0", 64)},
		{name: "uppercase", value: strings.Repeat("A", 64)},
		{name: "non hex", value: strings.Repeat("z", 64)},
		{name: "short", value: strings.Repeat("a", 63)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := validPostProductionManifestHash(test.value); got != test.want {
				t.Fatalf("validPostProductionManifestHash() = %t, want %t", got, test.want)
			}
		})
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

func TestActivities_CancelProviderJobKeepsUnknownRetryable(t *testing.T) {
	t.Parallel()
	ledger := &cancellationLedgerFixture{prepared: true}
	activities := NewProductionActivities(
		"http://127.0.0.1:1",
		nil,
		ledger,
		nil,
	)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	_, err := env.ExecuteActivity(activities.CancelProviderJob, CancelProviderJobInput{
		OperationID: "reconcile-operation",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-provider-offline"},
			PersistProductTruth: true,
		},
		ReasonCode: "RECONCILE_HISTORY",
		TraceID:    "provider-offline",
	})
	if err == nil {
		t.Fatal("ExecuteActivity() error = nil, want retryable cancellation uncertainty")
	}
	if len(ledger.recorded) != 1 ||
		ledger.recorded[0].State != "UNKNOWN" ||
		ledger.recorded[0].ErrorCode != "CANCEL_NOT_CONFIRMED" {
		t.Fatalf("recorded cancellation = %#v", ledger.recorded)
	}
}

func TestActivities_CancelProviderJobTreatsAuthoritativeAbsenceAsCancelled(t *testing.T) {
	t.Parallel()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": providercontract.Error{
				Code:        providercontract.CodeNotFound,
				SafeMessage: "provider job was not found",
			},
		})
	}))
	defer provider.Close()
	ledger := &cancellationLedgerFixture{prepared: true}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	encoded, err := env.ExecuteActivity(activities.CancelProviderJob, CancelProviderJobInput{
		OperationID: "reconcile-operation",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-provider-absent"},
			PersistProductTruth: true,
		},
		ReasonCode: "RECONCILE_HISTORY",
		TraceID:    "provider-restored",
	})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result CancelProviderResult
	if err := encoded.Get(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "CANCELLED" || len(ledger.recorded) != 1 ||
		ledger.recorded[0].State != "CANCELLED" {
		t.Fatalf("result=%#v recorded=%#v", result, ledger.recorded)
	}
}

func TestActivities_FinalizeEpisodeCommitsBeforeReturning(t *testing.T) {
	t.Parallel()
	manifestHash := strings.Repeat("a", 64)
	ledger := &postProductionLedgerFixture{}
	executor := &postProductionExecutorFixture{result: postproduction.Result{
		SchemaVersion:     postproduction.SchemaVersion,
		Evidence:          postproduction.EvidenceMockOnly,
		EpisodeRevisionID: "episode-revision-1",
		ManifestHash:      manifestHash,
	}}
	activities := NewActivities("http://provider.invalid")
	activities.ConfigurePostProduction(executor, ledger)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.FinalizeEpisode)
	encoded, err := env.ExecuteActivity(activities.FinalizeEpisode, FinalizeEpisodeInput{
		EpisodeRevisionID: "episode-revision-1",
		RunIDs:            []string{"run-1"},
		Config: PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidenceMockOnly,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN",
		},
		TraceID: "trace-postproduction",
	})
	if err != nil {
		t.Fatalf("ExecuteActivity() error = %v", err)
	}
	var result postproduction.Result
	if err := encoded.Get(&result); err != nil {
		t.Fatal(err)
	}
	if result.ManifestHash != manifestHash || executor.calls != 1 ||
		ledger.prepareCalls != 1 || ledger.authorizeCalls != 1 || ledger.commitCalls != 1 {
		t.Fatalf(
			"result=%#v executor=%d prepare=%d authorize=%d commit=%d",
			result, executor.calls, ledger.prepareCalls, ledger.authorizeCalls, ledger.commitCalls,
		)
	}
}

func TestActivities_FinalizeEpisodeBlocksBeforeExecutorWhenRightsChangeAfterPrepare(t *testing.T) {
	t.Parallel()
	revoked := errors.New("consent revoked after prepare")
	ledger := &postProductionLedgerFixture{authorizeErr: revoked}
	executor := &postProductionExecutorFixture{}
	activities := NewActivities("http://provider.invalid")
	activities.ConfigurePostProduction(executor, ledger)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.FinalizeEpisode)
	_, err := env.ExecuteActivity(activities.FinalizeEpisode, FinalizeEpisodeInput{
		EpisodeRevisionID: "episode-revision-1",
		RunIDs:            []string{"run-1"},
		Config: PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidenceMockOnly,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN",
		},
		TraceID: "trace-postproduction-revoked",
	})
	if err == nil || !strings.Contains(err.Error(), revoked.Error()) {
		t.Fatalf("ExecuteActivity() error = %v, want revoked authorization", err)
	}
	if ledger.prepareCalls != 1 || ledger.authorizeCalls != 1 ||
		executor.calls != 0 || ledger.commitCalls != 0 {
		t.Fatalf(
			"side effects = prepare:%d authorize:%d executor:%d commit:%d",
			ledger.prepareCalls, ledger.authorizeCalls, executor.calls, ledger.commitCalls,
		)
	}
}

func TestActivities_FinalizeEpisodeKeepsPendingKeyNonRetryableAndUncommitted(t *testing.T) {
	t.Parallel()
	ledger := &postProductionLedgerFixture{}
	executor := &postProductionExecutorFixture{err: postproduction.ErrPendingKey}
	activities := NewActivities("http://provider.invalid")
	activities.ConfigurePostProduction(executor, ledger)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.FinalizeEpisode)
	_, err := env.ExecuteActivity(activities.FinalizeEpisode, FinalizeEpisodeInput{
		EpisodeRevisionID: "episode-revision-1",
		RunIDs:            []string{"run-1"},
		Config: PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidencePendingKey,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN",
		},
		TraceID: "trace-pending-key",
	})
	if err == nil {
		t.Fatal("ExecuteActivity() error = nil, want pending_key failure")
	}
	if executor.calls != 1 || ledger.prepareCalls != 1 ||
		ledger.authorizeCalls != 1 || ledger.commitCalls != 0 {
		t.Fatalf(
			"side effects = executor:%d prepare:%d authorize:%d commit:%d",
			executor.calls, ledger.prepareCalls, ledger.authorizeCalls, ledger.commitCalls,
		)
	}
}
