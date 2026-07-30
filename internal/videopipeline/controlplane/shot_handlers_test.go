package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/google/uuid"
)

type shotHandlerStore struct {
	Store
	createRun      func(context.Context, string, int, CreateGenerationRunCommand, Idempotency, string) (Stored[Operation], error)
	requestPause   func(context.Context, string, int, Actor, string, Idempotency, string) (Stored[Operation], error)
	requestCancel  func(context.Context, string, int, Actor, string, Idempotency, string) (Stored[Operation], error)
	operation      Operation
	cancelRequests int
	startedCalls   int
	succeededCalls int
}

func (s *shotHandlerStore) Ping(context.Context) error { return nil }

func (s *shotHandlerStore) CreateGenerationRun(
	ctx context.Context,
	shotID string,
	expected int,
	command CreateGenerationRunCommand,
	idempotency Idempotency,
	traceID string,
) (Stored[Operation], error) {
	return s.createRun(ctx, shotID, expected, command, idempotency, traceID)
}

func (s *shotHandlerStore) RequestRunPause(
	ctx context.Context,
	runID string,
	expected int,
	actor Actor,
	reason string,
	idempotency Idempotency,
	traceID string,
) (Stored[Operation], error) {
	return s.requestPause(ctx, runID, expected, actor, reason, idempotency, traceID)
}

func (s *shotHandlerStore) RequestRunCancellation(
	ctx context.Context,
	runID string,
	expected int,
	actor Actor,
	reason string,
	idempotency Idempotency,
	traceID string,
) (Stored[Operation], error) {
	s.cancelRequests++
	return s.requestCancel(ctx, runID, expected, actor, reason, idempotency, traceID)
}

func (s *shotHandlerStore) MarkOperationStarted(context.Context, string, string, string) error {
	s.startedCalls++
	return nil
}

func (s *shotHandlerStore) MarkOperationSucceeded(context.Context, string) error {
	s.succeededCalls++
	s.operation.State = "SUCCEEDED"
	return nil
}

func (s *shotHandlerStore) GetOperation(context.Context, string) (Operation, error) {
	return s.operation, nil
}

type shotWorkflowFixture struct {
	WorkflowController
	startCalls  int
	pauseCalls  int
	cancelCalls int
}

func (f *shotWorkflowFixture) StartShot(_ context.Context, operation Operation) (WorkflowStart, error) {
	f.startCalls++
	return WorkflowStart{WorkflowID: operation.TemporalWorkflowID, RunID: "temporal-run-1"}, nil
}

func (f *shotWorkflowFixture) Pause(context.Context, string, string, string) error {
	f.pauseCalls++
	return nil
}

func (f *shotWorkflowFixture) Cancel(context.Context, string, string) error {
	f.cancelCalls++
	return nil
}

func TestServerCreateGenerationRunStartsStableTemporalWorkflow(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	runID := uuid.NewString()
	operationID := uuid.NewString()
	store := &shotHandlerStore{}
	store.createRun = func(
		_ context.Context,
		_ string,
		expected int,
		_ CreateGenerationRunCommand,
		_ Idempotency,
		traceID string,
	) (Stored[Operation], error) {
		if expected != 1 {
			t.Fatalf("expected revision = %d", expected)
		}
		return Stored[Operation]{Value: Operation{
			OperationID: operationID, OperationType: "CREATE_GENERATION_RUN",
			AggregateType: "GENERATION_RUN", AggregateID: runID, State: "ACCEPTED",
			TemporalWorkflowID: "shot-generation-" + runID,
			TraceID:            traceID, CreatedAt: now, UpdatedAt: now,
		}}, nil
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	request := httptest.NewRequest(
		http.MethodPost,
		APIBase+"/shots/"+uuid.NewString()+"/runs",
		strings.NewReader(validShotRunBody()),
	)
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("If-Match", `"1"`)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if workflows.startCalls != 1 || store.startedCalls != 1 {
		t.Fatalf("workflow starts=%d operation starts=%d", workflows.startCalls, store.startedCalls)
	}
	if !strings.Contains(recorder.Body.String(), `"state":"RUNNING"`) ||
		!strings.Contains(recorder.Body.String(), `"temporalRunId":"temporal-run-1"`) {
		t.Fatalf("response = %s", recorder.Body.String())
	}
}

func TestServerCreateGenerationRunPolicyBlockMakesZeroWorkflowCalls(t *testing.T) {
	t.Parallel()
	store := &shotHandlerStore{}
	store.createRun = func(
		context.Context, string, int, CreateGenerationRunCommand, Idempotency, string,
	) (Stored[Operation], error) {
		return Stored[Operation]{}, NewPolicyError(
			CodeQuotaExceeded, "insufficient frozen quota", "reduce the batch",
		)
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	request := httptest.NewRequest(
		http.MethodPost,
		APIBase+"/shots/"+uuid.NewString()+"/runs",
		strings.NewReader(validShotRunBody()),
	)
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("If-Match", `"1"`)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if workflows.startCalls != 0 {
		t.Fatalf("workflow/provider boundary calls = %d, want 0", workflows.startCalls)
	}
}

func TestServerCreateGenerationRunMissingSafetyDecisionMakesZeroCalls(t *testing.T) {
	t.Parallel()
	store := &shotHandlerStore{}
	store.createRun = func(
		context.Context, string, int, CreateGenerationRunCommand, Idempotency, string,
	) (Stored[Operation], error) {
		t.Fatal("store called for an invalid safety decision")
		return Stored[Operation]{}, nil
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	body := validShotRunBody()
	body = strings.Replace(
		body,
		`"contentSafetyDecisionId":"`,
		`"contentSafetyDecisionId":"not-a-uuid-`,
		1,
	)
	request := httptest.NewRequest(
		http.MethodPost,
		APIBase+"/shots/"+uuid.NewString()+"/runs",
		strings.NewReader(body),
	)
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("If-Match", `"1"`)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if workflows.startCalls != 0 {
		t.Fatalf("workflow/provider boundary calls = %d, want 0", workflows.startCalls)
	}
}

func TestServerDuplicateCancelReplaysTheSameOperation(t *testing.T) {
	t.Parallel()
	runID := uuid.NewString()
	operationID := uuid.NewString()
	now := time.Now().UTC()
	store := &shotHandlerStore{
		operation: Operation{
			OperationID: operationID, OperationType: "CANCEL_GENERATION_RUN",
			AggregateType: "GENERATION_RUN", AggregateID: runID, State: "ACCEPTED",
			TemporalWorkflowID: "shot-generation-" + runID,
			TraceID:            "cancel-duplicate", CreatedAt: now, UpdatedAt: now,
		},
	}
	store.requestCancel = func(
		context.Context, string, int, Actor, string, Idempotency, string,
	) (Stored[Operation], error) {
		return Stored[Operation]{Value: store.operation, Replayed: store.cancelRequests > 1}, nil
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	idempotencyKey := uuid.NewString()
	body := `{"actor":{"actorId":"operator-1","role":"OPERATOR"},"reasonCode":"USER_CANCELLED"}`

	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(
			http.MethodPost,
			APIBase+"/runs/"+runID+"/cancel",
			strings.NewReader(body),
		)
		request.Header.Set("Idempotency-Key", idempotencyKey)
		request.Header.Set("If-Match", `"1"`)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusAccepted ||
			!strings.Contains(recorder.Body.String(), operationID) {
			t.Fatalf("attempt %d status=%d body=%s", attempt+1, recorder.Code, recorder.Body.String())
		}
	}
	if store.cancelRequests != 2 || workflows.cancelCalls != 2 {
		t.Fatalf(
			"duplicate cancellation calls = store:%d temporal:%d",
			store.cancelRequests,
			workflows.cancelCalls,
		)
	}
}

func TestServerPauseRunPersistsSignalsAndClosesOperation(t *testing.T) {
	t.Parallel()
	runID := uuid.NewString()
	operationID := uuid.NewString()
	now := time.Now().UTC()
	store := &shotHandlerStore{}
	store.operation = Operation{
		OperationID: operationID, OperationType: "PAUSE_GENERATION_RUN",
		AggregateType: "GENERATION_RUN", AggregateID: runID, State: "ACCEPTED",
		TemporalWorkflowID: "shot-generation-" + runID, TraceID: "trace-1",
		CreatedAt: now, UpdatedAt: now,
	}
	store.requestPause = func(
		context.Context, string, int, Actor, string, Idempotency, string,
	) (Stored[Operation], error) {
		return Stored[Operation]{Value: store.operation}, nil
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	request := httptest.NewRequest(
		http.MethodPost,
		APIBase+"/runs/"+runID+"/pause",
		strings.NewReader(`{"actor":{"actorId":"operator-1","role":"OPERATOR"},"reasonCode":"OPERATOR_PAUSE"}`),
	)
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("If-Match", `"1"`)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if workflows.pauseCalls != 1 || store.succeededCalls != 1 ||
		!strings.Contains(recorder.Body.String(), `"state":"SUCCEEDED"`) {
		t.Fatalf("pauseCalls=%d succeededCalls=%d response=%s",
			workflows.pauseCalls, store.succeededCalls, recorder.Body.String())
	}
}

func validShotRunBody() string {
	return `{
		"schemaVersion":"v1",
		"shotSpecRevisionId":"` + uuid.NewString() + `",
		"promptSnapshotId":"` + uuid.NewString() + `",
		"generationProfileRevisionId":"` + uuid.NewString() + `",
		"generationPlanId":"` + uuid.NewString() + `",
		"routeSnapshot":{
			"capabilityAlias":"video.primary",
			"providerProfileId":"` + uuid.NewString() + `",
			"provider":"MOCK",
			"modelId":"fixture-video-v1",
			"routeVersion":"route-v1",
			"capabilityHash":"` + strings.Repeat("a", 64) + `"
		},
		"budgetApprovalId":"` + uuid.NewString() + `",
		"executionPolicy":{
			"targetTerritory":"CN",
			"productForm":"INTERNAL_PREVIEW",
			"contentSafetyPolicyVersion":"safety-v1",
			"contentSafetyDecisionId":"` + uuid.NewString() + `"
		},
		"creativeAttempt":1,
		"actor":{"actorId":"operator-1","role":"OPERATOR"}
	}`
}
