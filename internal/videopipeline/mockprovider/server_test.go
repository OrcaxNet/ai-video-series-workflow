package mockprovider

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
)

func TestServer_IdempotentReplayAndConflict(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	request := validRequest("success")

	first := submit(t, server, request, http.StatusCreated)
	second := submit(t, server, request, http.StatusOK)
	if first.UpstreamTaskID != second.UpstreamTaskID {
		t.Fatalf("replay upstream task = %q, want %q", second.UpstreamTaskID, first.UpstreamTaskID)
	}

	request.Request.Prompt = "changed"
	recorder := submitRecorder(t, server, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflicting replay status = %d, want %d; body=%s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
}

func TestServer_DiscoversFourCapabilitiesAndEstimates(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)

	capabilitiesRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(capabilitiesRecorder, httptest.NewRequest(http.MethodGet, "/v1/capabilities", nil))
	if capabilitiesRecorder.Code != http.StatusOK {
		t.Fatalf("capabilities status = %d", capabilitiesRecorder.Code)
	}
	var capabilities struct {
		Capabilities []providercontract.CapabilitySnapshot `json:"capabilities"`
	}
	if err := json.NewDecoder(capabilitiesRecorder.Body).Decode(&capabilities); err != nil {
		t.Fatal(err)
	}
	if len(capabilities.Capabilities) != 4 {
		t.Fatalf("capability count = %d, want 4", len(capabilities.Capabilities))
	}

	estimateBody, err := json.Marshal(providercontract.EstimateRequest{
		Capability: providercontract.CapabilityVideo,
		Model:      validRequest("success").Model,
		Parameters: map[string]any{"durationSeconds": 5},
		Candidates: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	estimateRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(estimateRecorder, httptest.NewRequest(http.MethodPost, "/v1/estimates", bytes.NewReader(estimateBody)))
	if estimateRecorder.Code != http.StatusOK {
		t.Fatalf("estimate status = %d; body=%s", estimateRecorder.Code, estimateRecorder.Body.String())
	}
	var estimate providercontract.EstimateResponse
	if err := json.NewDecoder(estimateRecorder.Body).Decode(&estimate); err != nil {
		t.Fatal(err)
	}
	if estimate.AmountMaximum == nil || *estimate.AmountMaximum != 300 || estimate.PricingVersion == "" {
		t.Fatalf("estimate = %#v", estimate)
	}
}

func TestServer_FailureMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scenario string
		status   int
		code     providercontract.ErrorCode
		retry    bool
		action   bool
	}{
		{scenario: "unauthorized", status: http.StatusUnauthorized, code: providercontract.CodeUnauthenticated, action: true},
		{scenario: "forbidden", status: http.StatusForbidden, code: providercontract.CodeForbidden, action: true},
		{scenario: "rate_limited", status: http.StatusTooManyRequests, code: providercontract.CodeRateLimited, retry: true},
		{scenario: "provider_unavailable", status: http.StatusServiceUnavailable, code: providercontract.CodeUnavailable, retry: true},
		{scenario: "quota_exhausted", status: http.StatusPaymentRequired, code: providercontract.CodeQuotaExceeded, action: true},
		{scenario: "budget_exceeded", status: http.StatusUnprocessableEntity, code: providercontract.CodeBudgetExceeded, action: true},
		{scenario: "content_blocked", status: http.StatusUnprocessableEntity, code: providercontract.CodeContentBlocked, action: true},
		{scenario: "region_unavailable", status: http.StatusUnprocessableEntity, code: providercontract.CodeRegionUnavailable, action: true},
		{scenario: "model_unavailable", status: http.StatusUnprocessableEntity, code: providercontract.CodeModelUnavailable, action: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.scenario, func(t *testing.T) {
			t.Parallel()
			server := newTestServer(t)
			recorder := submitRecorder(t, server, validRequest(tt.scenario))
			if recorder.Code != tt.status {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, tt.status, recorder.Body.String())
			}
			var body struct {
				Error providercontract.Error `json:"error"`
			}
			if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tt.code || body.Error.Retryable != tt.retry || body.Error.RequiresAction != tt.action {
				t.Fatalf("error = %#v", body.Error)
			}
		})
	}
}

func TestServer_UnknownRecoveryAndImmutableArtifact(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	request := validRequest("recovery")
	created := submit(t, server, request, http.StatusCreated)
	if created.State != providercontract.StatusQueued || created.UpstreamTaskID == "" {
		t.Fatalf("created = %#v", created)
	}
	running := get(t, server, request.JobID)
	if running.State != providercontract.StatusRunning {
		t.Fatalf("first poll state = %s, want RUNNING", running.State)
	}
	completed := get(t, server, request.JobID)
	if completed.State != providercontract.StatusSucceeded || len(completed.Artifacts) != 1 {
		t.Fatalf("completed = %#v", completed)
	}
	if completed.Artifacts[0].URI != "cas://sha256/"+completed.Artifacts[0].SHA256 {
		t.Fatalf("artifact is not content addressed: %#v", completed.Artifacts[0])
	}

	timeout := validRequest("timeout")
	timeout.JobID = "job-timeout"
	timeout.Request.IdempotencyKey = timeout.JobID
	unknown := submit(t, server, timeout, http.StatusCreated)
	if unknown.State != providercontract.StatusUnknown || unknown.UpstreamTaskID == "" {
		t.Fatalf("unknown = %#v", unknown)
	}
	if polled := get(t, server, timeout.JobID); polled.State != providercontract.StatusUnknown {
		t.Fatalf("unknown poll regressed to %s", polled.State)
	}
}

func TestServer_DeduplicatesAndOrdersCallbacks(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	request := validRequest("duplicate_callback")
	submit(t, server, request, http.StatusCreated)

	callback := callbackRequest{CallbackID: "callback-1", Sequence: 2, State: providercontract.StatusRunning}
	first := callbackRecorder(t, server, request.JobID, callback)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first callback status = %d", first.Code)
	}
	duplicate := callbackRecorder(t, server, request.JobID, callback)
	if duplicate.Code != http.StatusOK || !bytes.Contains(duplicate.Body.Bytes(), []byte(`"duplicate":true`)) {
		t.Fatalf("duplicate callback = %d %s", duplicate.Code, duplicate.Body.String())
	}
	stale := callbackRecorder(t, server, request.JobID, callbackRequest{
		CallbackID: "callback-old",
		Sequence:   1,
		State:      providercontract.StatusQueued,
	})
	if stale.Code != http.StatusOK || !bytes.Contains(stale.Body.Bytes(), []byte(`"stale":true`)) {
		t.Fatalf("stale callback = %d %s", stale.Code, stale.Body.String())
	}
}

func TestServer_RejectsInvalidCallbackState(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	request := validRequest("duplicate_callback")
	submit(t, server, request, http.StatusCreated)

	recorder := callbackRecorder(t, server, request.JobID, callbackRequest{
		CallbackID: "callback-invalid",
		Sequence:   1,
		State:      providercontract.JobStatus("PROVIDER_PRIVATE_STATE"),
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid callback status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestServer_CancellationRacePreservesSuccess(t *testing.T) {
	t.Parallel()
	server := newTestServer(t)
	request := validRequest("cancel_race")
	submit(t, server, request, http.StatusCreated)
	recorder := httptest.NewRecorder()
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+request.JobID+"/cancel", nil)
	server.Handler().ServeHTTP(recorder, httpRequest)
	if recorder.Code != http.StatusOK || recorder.Header().Get("X-Cancel-Result") != "already-terminal" {
		t.Fatalf("cancel race = %d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	var response providercontract.JobResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.State != providercontract.StatusSucceeded {
		t.Fatalf("cancel race state = %s", response.State)
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("artifactstore.New() error = %v", err)
	}
	return New(runtimeconfig.MockProvider{
		ProviderID:   "test-mock",
		Capabilities: []string{"text.primary", "image.primary", "video.primary", "speech.primary"},
	}, store)
}

func validRequest(simulation string) providercontract.JobRequest {
	input := sha256.Sum256([]byte("input"))
	capability := sha256.Sum256([]byte("capability"))
	return providercontract.JobRequest{
		SchemaVersion: "v1",
		JobID:         "job-1",
		RunID:         "run-1",
		Capability:    providercontract.CapabilityVideo,
		InputHash:     hex.EncodeToString(input[:]),
		Model: providercontract.ModelSnapshot{
			CapabilityAlias: "video.primary",
			Provider:        "mock",
			ModelID:         "fixture-video-v1",
			RouteVersion:    "mock-routes-v1",
			CapabilityHash:  hex.EncodeToString(capability[:]),
			Verification:    "mock_only",
		},
		Request: providercontract.GenerationRequest{
			RequestID:        "request-1",
			IdempotencyKey:   "job-1",
			Modality:         providercontract.ModalityVideo,
			Prompt:           "fixture",
			PromptSnapshotID: "prompt-fixture-v1",
			Output: providercontract.OutputSpec{
				Width:          1280,
				Height:         720,
				AspectRatio:    "16:9",
				FPS:            24,
				DurationMillis: 5_000,
				Format:         "mp4",
			},
			Budget: providercontract.BudgetEnvelope{
				EstimatedCostMicros: 100,
				MaxCostMicros:       150,
				MaxAttempts:         1,
			},
		},
		BudgetReservation: providercontract.BudgetReservation{
			ReservationID:  "reservation-1",
			Currency:       "CNY",
			AmountMicros:   150,
			PricingVersion: "mock-pricing-v1",
			ConfirmedBy:    "reviewer-1",
		},
		TraceID:    "trace-1",
		Simulation: simulation,
	}
}

func submit(t *testing.T, server *Server, request providercontract.JobRequest, wantStatus int) providercontract.JobResponse {
	t.Helper()
	recorder := submitRecorder(t, server, request)
	if recorder.Code != wantStatus {
		t.Fatalf("submit status = %d, want %d; body=%s", recorder.Code, wantStatus, recorder.Body.String())
	}
	var response providercontract.JobResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func submitRecorder(t *testing.T, server *Server, request providercontract.JobRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader(body))
	httpRequest.Header.Set("Idempotency-Key", request.JobID)
	server.Handler().ServeHTTP(recorder, httpRequest)
	return recorder
}

func get(t *testing.T, server *Server, jobID string) providercontract.JobResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
	var response providercontract.JobResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func callbackRecorder(t *testing.T, server *Server, jobID string, callback callbackRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(callback)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/jobs/"+jobID+"/callbacks", bytes.NewReader(body)))
	return recorder
}
