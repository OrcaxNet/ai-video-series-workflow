package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type activityLiveProvider struct {
	outputURL string
	submits   atomic.Int32
}

func (p *activityLiveProvider) Discover(context.Context) ([]providercontract.Capability, error) {
	return nil, nil
}

func (p *activityLiveProvider) Submit(_ context.Context, request providercontract.GenerationRequest) (providercontract.Job, error) {
	p.submits.Add(1)
	return providercontract.Job{
		ID: "cgt-activity-live-1", Status: providercontract.StatusQueued,
		Provider: "volcengine_ark", ProviderModel: request.ModelHint,
		ProviderRegion: "cn-beijing", ProviderRequestID: "activity-submit-request-1",
		CreatedAt: time.Unix(1_800_000_000, 0), UpdatedAt: time.Unix(1_800_000_000, 0),
	}, nil
}

func (p *activityLiveProvider) Poll(context.Context, string) (providercontract.Job, error) {
	return providercontract.Job{
		ID: "cgt-activity-live-1", Status: providercontract.StatusSucceeded,
		Provider: "volcengine_ark", ProviderModel: "doubao-seedance-2.0",
		ProviderRegion: "cn-beijing", ProviderRequestID: "activity-poll-request-1",
		CreatedAt: time.Unix(1_800_000_000, 0), UpdatedAt: time.Unix(1_800_000_030, 0),
		Output: &providercontract.Output{
			Actual: providercontract.OutputSpec{
				Resolution: "720p", AspectRatio: "16:9", FPS: 24,
				DurationMillis: 5_000, Format: "mp4",
			},
			Usage: providercontract.Usage{VideoTokens: 250_000, GeneratedMillis: 5_000},
			Assets: []providercontract.AssetRef{{
				ID: "activity-video", Revision: "provider-result",
				Kind: providercontract.ModalityVideo, Role: providercontract.AssetRoleOutput,
				URI: p.outputURL, SHA256: "pending_download",
				LicenseReference: "request-license-manifest",
			}},
		},
	}, nil
}

func (p *activityLiveProvider) Cancel(context.Context, string) (providercontract.Job, error) {
	return providercontract.Job{ID: "cgt-activity-live-1", Status: providercontract.StatusCancelled}, nil
}

type activityLiveInspector struct{}

func (activityLiveInspector) Inspect(context.Context, string) (volcengineprovider.MediaSpec, error) {
	return volcengineprovider.MediaSpec{
		Width: 1280, Height: 720, FPS: 24, DurationMillis: 5_062, Format: "mp4",
	}, nil
}

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
	recordErr     error
}

type providerPreparationLedgerFixture struct {
	ProductionLedger
	resolvedPrompt PromptSnapshotRef
	resolveErr     error
	resolveCalls   int
	prepareErr     error
	prepareCalls   int
	prepared       PreparedProviderJob
	reconcileOnly  bool
	observations   []ProviderJobObservation
}

func (l *providerPreparationLedgerFixture) ResolvePromptSnapshot(
	context.Context,
	string,
) (PromptSnapshotRef, error) {
	l.resolveCalls++
	return l.resolvedPrompt, l.resolveErr
}

func (l *providerPreparationLedgerFixture) PrepareProviderJob(
	context.Context,
	WorkflowStep,
	ExecuteProviderJobInput,
) (PreparedProviderJob, error) {
	l.prepareCalls++
	prepared := l.prepared
	prepared.ReconcileOnly = l.reconcileOnly
	return prepared, l.prepareErr
}

func (l *providerPreparationLedgerFixture) RecordProviderJobObservation(
	_ context.Context,
	_ WorkflowStep,
	_ ExecuteProviderJobInput,
	observation ProviderJobObservation,
) error {
	l.observations = append(l.observations, observation)
	if observation.State == "UNKNOWN" || observation.State == "RUNNING" ||
		observation.UpstreamTaskID != "" {
		l.reconcileOnly = true
	}
	return nil
}

type postProductionLedgerFixture struct {
	request        postproduction.Request
	prepareErr     error
	authorizeErr   error
	authorizeErrAt int
	commitErr      error
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
	return l.request, l.prepareErr
}

func (l *postProductionLedgerFixture) AuthorizeEpisodePostProduction(
	context.Context,
	WorkflowStep,
	FinalizeEpisodeInput,
) error {
	l.authorizeCalls++
	if l.authorizeErrAt > 0 && l.authorizeCalls != l.authorizeErrAt {
		return nil
	}
	return l.authorizeErr
}

func (l *postProductionLedgerFixture) CommitEpisodePostProduction(
	context.Context,
	WorkflowStep,
	FinalizeEpisodeInput,
	postproduction.Result,
) error {
	l.commitCalls++
	return l.commitErr
}

type postProductionExecutorFixture struct {
	result                  postproduction.Result
	err                     error
	invokePaidAuthorization bool
	calls                   int
}

func (e *postProductionExecutorFixture) Finalize(
	ctx context.Context,
	request postproduction.Request,
) (postproduction.Result, error) {
	e.calls++
	if e.invokePaidAuthorization {
		if request.AuthorizePaidSubmit == nil {
			return postproduction.Result{}, errors.New("paid submission authorizer is missing")
		}
		if err := request.AuthorizePaidSubmit(ctx, postproduction.Cue{ID: "cue"}); err != nil {
			return postproduction.Result{}, err
		}
	}
	return e.result, e.err
}

type gate3LedgerFixture struct {
	ProductionLedger
	buildErr    error
	commitErr   error
	buildCalls  int
	commitCalls int
}

func (l *gate3LedgerFixture) BuildEpisodeManifest(
	context.Context,
	WorkflowStep,
	CreateGate3Input,
) ([]byte, error) {
	l.buildCalls++
	if l.buildErr != nil {
		return nil, l.buildErr
	}
	return []byte(`{"schemaVersion":"v1"}`), nil
}

func (l *gate3LedgerFixture) CommitEpisodeManifest(
	context.Context,
	WorkflowStep,
	CreateGate3Input,
	[]byte,
	artifactstore.Artifact,
) error {
	l.commitCalls++
	return l.commitErr
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
	return l.recordErr
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
	if !reflect.DeepEqual(result, expected) {
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
	if result.State != "CANCELLED" || !result.NoRemoteTask || result.ErrorCode != "" {
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
	actualMicros := int64(20)
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
			RequestID:      "request-1",
			State:          providercontract.StatusCancelled,
			Usage: providercontract.Usage{
				InputUnits: 2, OutputUnits: 3, Unit: "mock-units",
			},
			Cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: &actualMicros,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
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
	if first.State != "CANCELLED" || first.RequestID != "request-1" ||
		first.Cost.ActualMicros == nil || *first.Cost.ActualMicros != 20 ||
		first.Usage.InputUnits != 2 || providerCalls != 1 ||
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
	if !reflect.DeepEqual(second, first) {
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

func TestActivities_CancelProviderJobConvergesOnFailedTerminalCost(t *testing.T) {
	t.Parallel()
	actualMicros := int64(12)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
			JobID: "provider-job-run-provider-failed", RunID: "run-provider-failed",
			State: providercontract.StatusFailed,
			Usage: providercontract.Usage{
				InputUnits: 1, OutputUnits: 2, Unit: "mock-units",
			},
			Cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: &actualMicros,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
			Error: &providercontract.Error{Code: providercontract.CodeModelUnavailable},
		})
	}))
	defer provider.Close()
	ledger := &cancellationLedgerFixture{prepared: true}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	encoded, err := env.ExecuteActivity(activities.CancelProviderJob, CancelProviderJobInput{
		OperationID: "reconcile-failed-operation",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-provider-failed"},
			PersistProductTruth: true,
		},
		ReasonCode: "RECONCILE_HISTORY", TraceID: "provider-failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	var result CancelProviderResult
	if err := encoded.Get(&result); err != nil {
		t.Fatal(err)
	}
	if result.State != "FAILED" ||
		result.ErrorCode != string(providercontract.CodeModelUnavailable) ||
		result.Cost.ActualMicros == nil || *result.Cost.ActualMicros != 12 ||
		len(ledger.recorded) != 1 || ledger.recorded[0].State != "FAILED" {
		t.Fatalf("failed cancellation reconciliation = result:%#v recorded:%#v", result, ledger.recorded)
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
	if result.State != "CANCELLED" || !result.NoRemoteTask || len(ledger.recorded) != 1 ||
		ledger.recorded[0].State != "CANCELLED" || !ledger.recorded[0].NoRemoteTask {
		t.Fatalf("result=%#v recorded=%#v", result, ledger.recorded)
	}
}

func TestActivities_CancelProviderJobRetriesWhenDurableIdentityDisprovesAbsence(t *testing.T) {
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

	ledger := &cancellationLedgerFixture{
		prepared:  true,
		recordErr: errors.New("durable upstream task still requires reconciliation"),
	}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	_, err := env.ExecuteActivity(activities.CancelProviderJob, CancelProviderJobInput{
		OperationID: "reconcile-known-upstream",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-known-upstream"},
			PersistProductTruth: true,
		},
		ReasonCode: "RECONCILE_HISTORY",
		TraceID:    "provider-registry-missing",
	})
	if err == nil {
		t.Fatal("ExecuteActivity() error = nil, want retryable durable reconciliation")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.NonRetryable() {
		t.Fatalf("ExecuteActivity() error = %#v, want retryable ApplicationError", err)
	}
	if len(ledger.recorded) != 1 || ledger.recorded[0].State != "CANCELLED" ||
		!ledger.recorded[0].NoRemoteTask {
		t.Fatalf("recorded cancellation = %#v", ledger.recorded)
	}
}

func TestActivities_CancelProviderJobRejectsTerminalIdentityConflict(t *testing.T) {
	t.Parallel()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		actualMicros := int64(12)
		_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
			JobID: "provider-job-run-terminal-conflict", RunID: "run-terminal-conflict",
			State:          providercontract.StatusFailed,
			UpstreamTaskID: "unexpected-task",
			RequestID:      "unexpected-request",
			Cost: providercontract.Cost{
				EstimatedMicros: 50, ActualMicros: &actualMicros,
				Currency: "CNY", PricingVersion: "pricing-v1", Verified: true,
			},
		})
	}))
	defer provider.Close()

	ledger := &cancellationLedgerFixture{
		prepared: true,
		recordErr: controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"provider terminal identity differs from the durable record",
		),
	}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.CancelProviderJob)
	_, err := env.ExecuteActivity(activities.CancelProviderJob, CancelProviderJobInput{
		OperationID: "terminal-identity-conflict",
		Dispatch: ExecuteProviderJobInput{
			Run:                 GenerationRunRef{RunID: "run-terminal-conflict"},
			PersistProductTruth: true,
		},
		ReasonCode: "RECONCILE_HISTORY",
		TraceID:    "terminal-identity-conflict",
	})
	if err == nil {
		t.Fatal("ExecuteActivity() error = nil, want non-retryable identity conflict")
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || !applicationErr.NonRetryable() ||
		applicationErr.Type() != string(controlplane.CodeRevisionConflict) {
		t.Fatalf("ExecuteActivity() error = %#v, want non-retryable revision conflict", err)
	}
	if len(ledger.recorded) != 1 || ledger.recorded[0].State != "FAILED" {
		t.Fatalf("recorded cancellation = %#v", ledger.recorded)
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

func TestActivities_FinalizeEpisodePreservesRightsErrorContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		boundary string
		code     controlplane.ErrorCode
	}{
		{name: "prepare consent", boundary: "prepare", code: controlplane.CodeConsentRequired},
		{name: "prepare license", boundary: "prepare", code: controlplane.CodeLicenseBlocked},
		{name: "authorize consent", boundary: "authorize", code: controlplane.CodeConsentRequired},
		{name: "authorize license", boundary: "authorize", code: controlplane.CodeLicenseBlocked},
		{name: "paid submit consent", boundary: "paid_submit", code: controlplane.CodeConsentRequired},
		{name: "paid submit license", boundary: "paid_submit", code: controlplane.CodeLicenseBlocked},
		{name: "commit consent", boundary: "commit", code: controlplane.CodeConsentRequired},
		{name: "commit license", boundary: "commit", code: controlplane.CodeLicenseBlocked},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policyErr := controlplane.NewPolicyError(
				test.code,
				"fixture rights failure",
				"renew rights",
			)
			ledger := &postProductionLedgerFixture{}
			executor := &postProductionExecutorFixture{}
			switch test.boundary {
			case "prepare":
				ledger.prepareErr = policyErr
			case "authorize":
				ledger.authorizeErr = policyErr
				ledger.authorizeErrAt = 1
			case "paid_submit":
				ledger.authorizeErr = policyErr
				ledger.authorizeErrAt = 2
				executor.invokePaidAuthorization = true
			case "commit":
				ledger.commitErr = policyErr
			default:
				t.Fatalf("unknown boundary %q", test.boundary)
			}
			activities := NewActivities("http://provider.invalid")
			activities.ConfigurePostProduction(executor, ledger)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(activities.FinalizeEpisode)
			_, err := env.ExecuteActivity(
				activities.FinalizeEpisode,
				testFinalizeEpisodeInput("trace-"+test.name),
			)
			assertNonRetryableApplicationError(t, err, string(test.code))
		})
	}
}

func TestActivities_CreateGate3PreservesRightsErrorContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		boundary string
		code     controlplane.ErrorCode
	}{
		{name: "build consent", boundary: "build", code: controlplane.CodeConsentRequired},
		{name: "build license", boundary: "build", code: controlplane.CodeLicenseBlocked},
		{name: "commit consent", boundary: "commit", code: controlplane.CodeConsentRequired},
		{name: "commit license", boundary: "commit", code: controlplane.CodeLicenseBlocked},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policyErr := controlplane.NewPolicyError(
				test.code,
				"fixture rights failure",
				"renew rights",
			)
			ledger := &gate3LedgerFixture{}
			switch test.boundary {
			case "build":
				ledger.buildErr = policyErr
			case "commit":
				ledger.commitErr = policyErr
			default:
				t.Fatalf("unknown boundary %q", test.boundary)
			}
			store, err := artifactstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			activities := NewProductionActivities(
				"http://provider.invalid",
				nil,
				ledger,
				store,
			)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(activities.CreateGate3)
			_, err = env.ExecuteActivity(activities.CreateGate3, CreateGate3Input{
				EpisodeRevisionID:   "episode-revision-1",
				RunIDs:              []string{"run-1"},
				TraceID:             "trace-" + test.name,
				PersistProductTruth: true,
			})
			assertNonRetryableApplicationError(t, err, string(test.code))
			if ledger.buildCalls != 1 {
				t.Fatalf("build calls = %d, want 1", ledger.buildCalls)
			}
			wantCommitCalls := 0
			if test.boundary == "commit" {
				wantCommitCalls = 1
			}
			if ledger.commitCalls != wantCommitCalls {
				t.Fatalf("commit calls = %d, want %d", ledger.commitCalls, wantCommitCalls)
			}
		})
	}
}

func TestActivities_ExecuteProviderJobUsesLiveAdapterAndReturnsMeasuredCASArtifact(t *testing.T) {
	t.Parallel()
	const serviceAuthSecret = "test-service-auth-secret-32-bytes-long"
	download := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte("synthetic-live-activity-video"))
	}))
	defer download.Close()
	provider := &activityLiveProvider{outputURL: download.URL + "/result.mp4?signature=transient"}
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := volcengineprovider.New(runtimeconfig.VolcengineProvider{
		ProviderID: "volcengine-agent-plan-large", Region: "cn-beijing",
		VideoModel: "doubao-seedance-2.0", PlanName: "agent-plan-large",
		PricingVersion: "agent-plan-large-included-v1", Currency: "CNY",
		MaxDownloadBytes: 1 << 20, DownloadTimeout: 5 * time.Second,
		ServiceAuthSecret: serviceAuthSecret,
	}, provider, store, volcengineprovider.Options{
		DownloadClient: download.Client(), Inspector: activityLiveInspector{},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(adapter.Handler())
	defer server.Close()
	capability := sha256.Sum256([]byte("volcengine-agent-plan-large\x00doubao-seedance-2.0"))
	inputDigest := sha256.Sum256([]byte("immutable activity live input"))
	activities := NewActivities(server.URL)
	activities.HTTPClient, err = volcengineprovider.AuthenticatedHTTPClient(activities.HTTPClient, serviceAuthSecret)
	if err != nil {
		t.Fatal(err)
	}
	input := ExecuteProviderJobInput{
		Run: GenerationRunRef{
			RunID: "live-activity-run", RunSpecDigest: hex.EncodeToString(inputDigest[:]), Attempt: 1,
		},
		Prompt: PromptSnapshotRef{
			ID: "live-activity-prompt", Digest: hex.EncodeToString(inputDigest[:]),
			PositivePrompt: "original abstract glass fluid, fixed camera, no people or text",
			Context: providercontract.ContextRefs{
				SeriesSnapshotID: "series-live", EpisodeSnapshotID: "episode-live",
				SceneSnapshotID: "scene-live", ShotSnapshotID: "shot-live",
			},
			Output: providercontract.OutputSpec{
				Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
				FPS: 24, DurationMillis: 5_000, Format: "mp4",
			},
		},
		Route: providercontract.ModelSnapshot{
			CapabilityAlias: string(providercontract.CapabilityVideo),
			Provider:        "volcengine_ark", ModelID: "doubao-seedance-2.0",
			RouteVersion: "agent-plan-large-v1", CapabilityHash: hex.EncodeToString(capability[:]),
			Verification: providercontract.PendingKey,
		},
		BudgetApprovalID: "activity-live-budget", BudgetMaximumMicros: 1_000_000,
		BudgetCurrency: "CNY", TraceID: "trace-activity-live",
	}
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestActivityEnvironment()
	environment.RegisterActivity(activities.ExecuteProviderJob)
	encodedResult, err := environment.ExecuteActivity(activities.ExecuteProviderJob, input)
	if err != nil {
		t.Fatal(err)
	}
	var result ProviderResult
	if err := encodedResult.Get(&result); err != nil {
		t.Fatal(err)
	}
	if provider.submits.Load() != 1 || result.UpstreamTaskID != "cgt-activity-live-1" ||
		result.RequestID != "activity-submit-request-1" || result.Width != 1280 ||
		result.Height != 720 || result.DurationMillis != 5_062 ||
		result.ArtifactURI != "cas://sha256/"+result.ArtifactDigest ||
		result.Usage.VideoTokens != 250_000 || result.Cost.ActualMicros == nil ||
		*result.Cost.ActualMicros != 0 || result.Cost.BillingMode != "subscription_included" {
		t.Fatalf("live Activity result = %#v, submits = %d", result, provider.submits.Load())
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "signature=") || strings.Contains(string(encoded), download.URL) {
		t.Fatalf("Activity result leaked signed transport URL: %s", encoded)
	}
}

func TestActivities_ExecuteProviderJobNeverResubmitsAfterAmbiguousPaidBoundary(t *testing.T) {
	t.Parallel()
	runID := "b5787943-28a8-43c8-a4b8-401b85ad7a9d"
	approvalID := "39d1c01b-bf34-413b-893a-9eb7f8e48a69"
	inputHash := strings.Repeat("a", 64)
	capabilityHash := strings.Repeat("b", 64)
	route := providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilityVideo),
		Provider:        "MOCK", ModelID: "fixture-video-v1",
		RouteVersion: "route-v1", CapabilityHash: capabilityHash,
		Verification: "test",
	}
	prompt := PromptSnapshotRef{
		ID: "prompt-ambiguous", Digest: strings.Repeat("c", 64),
		PositivePrompt: "immutable ambiguous submission fixture",
		Context: providercontract.ContextRefs{
			SeriesSnapshotID: "series", EpisodeSnapshotID: "episode",
			SceneSnapshotID: "scene", ShotSnapshotID: "shot",
		},
		Output: providercontract.OutputSpec{
			Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
			FPS: 24, DurationMillis: 5_000, Format: "mp4",
		},
	}
	budget := providercontract.BudgetEnvelope{
		EstimatedCostMicros: 50, MaxCostMicros: 50, MaxAttempts: 1,
	}
	reservation, err := providercontract.BindBudgetReservation(
		providercontract.BudgetReservation{
			ReservationID: "993d4ba9-c37a-4536-a985-0859584274f3",
			Currency:      "CNY", AmountMicros: 50, PricingVersion: "pricing-v1",
			ConfirmedBy: approvalID,
		},
		providercontract.BudgetBindingInput{
			RunID: runID, InputHash: inputHash, Model: route, Budget: budget,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	input := ExecuteProviderJobInput{
		Run:    GenerationRunRef{RunID: runID, RunSpecDigest: inputHash, Attempt: 1},
		Prompt: prompt, Route: route, BudgetApprovalID: strings.ToUpper(approvalID),
		BudgetMaximumMicros: 50, BudgetCurrency: "CNY",
		TraceID: "trace-ambiguous-submit", PersistProductTruth: true,
	}
	var submits atomic.Int32
	var gets atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			submits.Add(1)
			// Simulate a paid task accepted upstream while the adapter loses the
			// response. Retrying this endpoint would create a second paid attempt.
			http.Error(w, "accepted response lost", http.StatusServiceUnavailable)
		case http.MethodGet:
			call := gets.Add(1)
			if call%2 == 0 {
				http.Error(w, "poll unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(providercontract.JobResponse{
				JobID: "provider-job-" + runID, RunID: runID,
				UpstreamTaskID: "upstream-ambiguous-1", RequestID: "request-ambiguous-1",
				State: providercontract.StatusRunning, Model: route,
			})
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer provider.Close()
	ledger := &providerPreparationLedgerFixture{
		resolvedPrompt: prompt,
		prepared: PreparedProviderJob{
			Budget: budget, BudgetReservation: reservation,
		},
	}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	for attempt := 0; attempt < 4; attempt++ {
		env := suite.NewTestActivityEnvironment()
		env.RegisterActivity(activities.ExecuteProviderJob)
		if _, err := env.ExecuteActivity(activities.ExecuteProviderJob, input); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	if submits.Load() != 1 {
		t.Fatalf("paid POST calls = %d, want 1", submits.Load())
	}
	if gets.Load() != 6 {
		t.Fatalf("stable JobID GET calls = %d, want 6", gets.Load())
	}
	if len(ledger.observations) < 7 ||
		ledger.observations[0].State != "UNKNOWN" ||
		ledger.observations[0].ErrorCode != "PROVIDER_SUBMISSION_PENDING" ||
		!ledger.reconcileOnly {
		t.Fatalf("durable reconciliation observations = %#v", ledger.observations)
	}
}

func TestActivities_ExecuteProviderJobPreservesVideoBudgetErrorBeforeProvider(
	t *testing.T,
) {
	t.Parallel()
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		_ *http.Request,
	) {
		providerCalls.Add(1)
		http.Error(response, "provider must not be called", http.StatusInternalServerError)
	}))
	defer provider.Close()

	ledger := &providerPreparationLedgerFixture{
		prepareErr: controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"fixture VIDEO budget changed after run creation",
			"approve the exact frozen plan envelope",
		),
	}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.ExecuteProviderJob)
	_, err := env.ExecuteActivity(activities.ExecuteProviderJob, ExecuteProviderJobInput{
		Run:                 GenerationRunRef{RunID: "run-budget-boundary"},
		BudgetApprovalID:    "approval-budget-boundary",
		BudgetMaximumMicros: 100,
		BudgetCurrency:      "CNY",
		TraceID:             "trace-video-budget-boundary",
		PersistProductTruth: true,
	})
	assertNonRetryableApplicationError(
		t, err, string(controlplane.CodeBudgetExceeded),
	)
	if ledger.prepareCalls != 1 || providerCalls.Load() != 0 {
		t.Fatalf(
			"VIDEO budget boundary side effects = prepare:%d provider:%d, want 1/0",
			ledger.prepareCalls,
			providerCalls.Load(),
		)
	}
}

func TestActivities_ExecuteProviderJobPreservesAttemptContractErrorsBeforeProvider(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name string
		code controlplane.ErrorCode
	}{
		{name: "terminal attempt", code: controlplane.CodeConflict},
		{name: "immutable attempt drift", code: controlplane.CodeRevisionConflict},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var providerCalls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(
				response http.ResponseWriter,
				_ *http.Request,
			) {
				providerCalls.Add(1)
				http.Error(response, "provider must not be called", http.StatusInternalServerError)
			}))
			defer provider.Close()

			ledger := &providerPreparationLedgerFixture{
				prepareErr: controlplane.NewConflictError(
					test.code,
					"fixture generation attempt is terminal or drifted",
				),
			}
			activities := NewProductionActivities(provider.URL, nil, ledger, nil)
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestActivityEnvironment()
			env.RegisterActivity(activities.ExecuteProviderJob)
			_, err := env.ExecuteActivity(
				activities.ExecuteProviderJob,
				ExecuteProviderJobInput{
					Run:     GenerationRunRef{RunID: "run-attempt-contract"},
					TraceID: "trace-attempt-contract", PersistProductTruth: true,
				},
			)
			assertNonRetryableApplicationError(t, err, string(test.code))
			if ledger.prepareCalls != 1 || providerCalls.Load() != 0 {
				t.Fatalf(
					"attempt contract boundary side effects = prepare:%d provider:%d, want 1/0",
					ledger.prepareCalls, providerCalls.Load(),
				)
			}
		})
	}
}

func TestActivities_ExecuteProviderJobRejectsTamperedPersistedPromptBeforeProvider(
	t *testing.T,
) {
	t.Parallel()
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		_ *http.Request,
	) {
		providerCalls.Add(1)
		http.Error(response, "provider must not be called", http.StatusInternalServerError)
	}))
	defer provider.Close()

	exact := PromptSnapshotRef{
		ID:             "prompt-immutable",
		Digest:         strings.Repeat("a", 64),
		PositivePrompt: "approved immutable prompt",
		Context: providercontract.ContextRefs{
			SeriesSnapshotID: "series-context", EpisodeSnapshotID: "episode-context",
			SceneSnapshotID: "scene-context", ShotSnapshotID: "shot-context",
		},
		Output: providercontract.OutputSpec{
			Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
			FPS: 24, DurationMillis: 5_000, Format: "mp4",
		},
		InputRevisionHashes: map[string]string{"shot_spec": strings.Repeat("b", 64)},
	}
	tampered := exact
	tampered.PositivePrompt = "unapproved tampered prompt"
	ledger := &providerPreparationLedgerFixture{resolvedPrompt: exact}
	activities := NewProductionActivities(provider.URL, nil, ledger, nil)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.ExecuteProviderJob)
	_, err := env.ExecuteActivity(activities.ExecuteProviderJob, ExecuteProviderJobInput{
		Run:                 GenerationRunRef{RunID: "run-prompt-tamper"},
		Prompt:              tampered,
		BudgetApprovalID:    "approval-prompt-tamper",
		BudgetMaximumMicros: 100,
		BudgetCurrency:      "CNY",
		TraceID:             "trace-prompt-tamper",
		PersistProductTruth: true,
	})
	assertNonRetryableApplicationError(
		t,
		err,
		string(controlplane.CodeRevisionConflict),
	)
	if ledger.resolveCalls != 1 || ledger.prepareCalls != 0 || providerCalls.Load() != 0 {
		t.Fatalf(
			"tampered Prompt side effects = resolve:%d prepare:%d provider:%d, want 1/0/0",
			ledger.resolveCalls,
			ledger.prepareCalls,
			providerCalls.Load(),
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
	assertNonRetryableApplicationError(t, err, "PENDING_KEY")
	if executor.calls != 1 || ledger.prepareCalls != 1 ||
		ledger.authorizeCalls != 1 || ledger.commitCalls != 0 {
		t.Fatalf(
			"side effects = executor:%d prepare:%d authorize:%d commit:%d",
			executor.calls, ledger.prepareCalls, ledger.authorizeCalls, ledger.commitCalls,
		)
	}
}

func TestActivities_FinalizeEpisodePreservesSpeechBudgetErrorContract(t *testing.T) {
	t.Parallel()
	ledger := &postProductionLedgerFixture{
		authorizeErr: controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"fixture speech budget mismatch",
			"approve the current speech envelope",
		),
		authorizeErrAt: 1,
	}
	executor := &postProductionExecutorFixture{}
	activities := NewActivities("http://provider.invalid")
	activities.ConfigurePostProduction(executor, ledger)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(activities.FinalizeEpisode)
	_, err := env.ExecuteActivity(
		activities.FinalizeEpisode,
		testFinalizeEpisodeInput("trace-speech-budget"),
	)
	assertNonRetryableApplicationError(
		t, err, string(controlplane.CodeBudgetExceeded),
	)
	if executor.calls != 0 {
		t.Fatalf("post-production executor calls = %d, want 0", executor.calls)
	}
}

func testFinalizeEpisodeInput(traceID string) FinalizeEpisodeInput {
	return FinalizeEpisodeInput{
		EpisodeRevisionID: "episode-revision-1",
		RunIDs:            []string{"run-1"},
		Config: PostProductionConfig{
			Enabled: true, Evidence: postproduction.EvidenceMockOnly,
			SpeechRoute: testSpeechRoute(), SpeechProviderProfileID: "speech-profile",
			SpeechBudgetApprovalID: "speech-budget", SpeechBudgetMaximumMicros: 100,
			SpeechBudgetCurrency: "CNY", SubtitleLanguage: "zh-CN",
		},
		TraceID: traceID,
	}
}

func assertNonRetryableApplicationError(t *testing.T, err error, errorType string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", errorType)
	}
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		t.Fatalf("error = %T %v, want Temporal ApplicationError", err, err)
	}
	if applicationErr.Type() != errorType {
		t.Fatalf("ApplicationError.Type() = %q, want %q", applicationErr.Type(), errorType)
	}
	if !applicationErr.NonRetryable() {
		t.Fatal("ApplicationError.NonRetryable() = false, want true")
	}
}
