package controlplane

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/google/uuid"
)

type handlerStore struct {
	Store
	createSeries func(context.Context, CreateSeriesCommand, Idempotency, string) (Stored[Operation], error)
}

type approvalHandlerStore struct {
	Store
	createCalls int
}

func (s *approvalHandlerStore) Ping(context.Context) error { return nil }

func (s *approvalHandlerStore) CreateApprovalDecision(
	_ context.Context,
	command CreateApprovalDecisionCommand,
	_ Idempotency,
	traceID string,
) (Stored[ApprovalDecision], error) {
	s.createCalls++
	return Stored[ApprovalDecision]{
		Value: ApprovalDecision{
			CreateApprovalDecisionCommand: command,
			DecisionID:                    uuid.NewString(),
			DecidedAt:                     time.Now().UTC(),
			TraceID:                       traceID,
		},
		Replayed: true,
	}, nil
}

type approvalWorkflowFixture struct {
	WorkflowController
	deliveries int
}

func (f *approvalWorkflowFixture) RecordApproval(context.Context, ApprovalDecision) error {
	f.deliveries++
	return nil
}

func (s *handlerStore) Ping(context.Context) error { return nil }

func (s *handlerStore) CreateSeries(
	ctx context.Context,
	command CreateSeriesCommand,
	idempotency Idempotency,
	traceID string,
) (Stored[Operation], error) {
	return s.createSeries(ctx, command, idempotency, traceID)
}

func TestServer_CreateSeriesValidatesAndForwardsIdempotency(t *testing.T) {
	t.Parallel()
	profileID := uuid.NewString()
	idempotencyKey := uuid.NewString()
	evidenceHash := strings.Repeat("a", 64)
	var gotIdempotency Idempotency
	store := &handlerStore{
		createSeries: func(_ context.Context, _ CreateSeriesCommand, idempotency Idempotency, traceID string) (Stored[Operation], error) {
			gotIdempotency = idempotency
			now := time.Now().UTC()
			return Stored[Operation]{Value: Operation{
				OperationID: uuid.NewString(), OperationType: "CREATE_SERIES",
				AggregateType: "SERIES", AggregateID: uuid.NewString(), State: "SUCCEEDED",
				TraceID: traceID, CreatedAt: now, UpdatedAt: now,
			}}, nil
		},
	}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, nil, nil)
	body := `{
		"schemaVersion":"v1",
		"title":"Immutable series",
		"generationProfileRevisionId":"` + profileID + `",
		"rightsDeclaration":{"basis":"licensed adaptation","evidenceArtifactHash":"` + evidenceHash + `"},
		"actor":{"actorId":"creator-1","role":"CREATOR"}
	}`
	request := httptest.NewRequest(http.MethodPost, APIBase+"/series", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if gotIdempotency.Key != idempotencyKey || gotIdempotency.Scope != "series:create:creator-1" ||
		len(gotIdempotency.RequestHash) != 64 {
		t.Fatalf("idempotency = %#v", gotIdempotency)
	}
	if location := recorder.Header().Get("Location"); !strings.HasPrefix(location, APIBase+"/operations/") {
		t.Fatalf("Location = %q", location)
	}
}

func TestServer_CreateSeriesRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	store := &handlerStore{
		createSeries: func(context.Context, CreateSeriesCommand, Idempotency, string) (Stored[Operation], error) {
			t.Fatal("store must not be called")
			return Stored[Operation]{}, nil
		},
	}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, nil, nil)
	request := httptest.NewRequest(http.MethodPost, APIBase+"/series", strings.NewReader(`{"unexpected":true}`))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/problem+json" {
		t.Fatalf("Content-Type = %q", contentType)
	}
	var response problem
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.ErrorCode != CodeValidation || response.TraceID == "" {
		t.Fatalf("problem = %#v", response)
	}
}

func TestValidateStartProductionPostProductionBoundary(t *testing.T) {
	t.Parallel()
	valid := func() StartProductionCommand {
		return StartProductionCommand{
			SchemaVersion:               "v1",
			EpisodeRevisionID:           uuid.NewString(),
			ShotSpecRevisionIDs:         []string{uuid.NewString()},
			GenerationProfileRevisionID: uuid.NewString(),
			Gate2DecisionID:             uuid.NewString(),
			GenerationPlanID:            uuid.NewString(),
			RouteSnapshot: ModelRouteSnapshot{
				CapabilityAlias: "video.primary", ProviderProfileID: uuid.NewString(),
				Provider: "mock", ModelID: "video-v1", RouteVersion: "route-v1",
				CapabilityHash: strings.Repeat("a", 64),
			},
			BudgetApprovalID: uuid.NewString(),
			ExecutionPolicy: ExecutionPolicy{
				TargetTerritory: "CN", ProductForm: "INTERNAL_PREVIEW",
				ContentSafetyPolicyVersion: "safety-v1",
				ContentSafetyDecisionID:    uuid.NewString(),
			},
			PostProduction: &PostProductionCommand{
				Enabled: true, Evidence: "mock_only",
				SpeechRouteSnapshot: ModelRouteSnapshot{
					CapabilityAlias: "speech.primary", ProviderProfileID: uuid.NewString(),
					Provider: "mock", ModelID: "speech-v1", RouteVersion: "route-v1",
					CapabilityHash: strings.Repeat("b", 64),
				},
				SpeechBudgetApprovalID: uuid.NewString(),
				SpeechBudgetLimit:      BudgetLimit{AmountMicros: 100, Currency: "CNY"},
				SubtitleLanguage:       "zh-CN",
			},
			Actor: Actor{ActorID: "operator", Role: "OPERATOR"},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*StartProductionCommand)
		wantErr bool
	}{
		{name: "valid mock boundary"},
		{
			name: "valid pending key boundary",
			mutate: func(command *StartProductionCommand) {
				command.PostProduction.Evidence = "pending_key"
			},
		},
		{
			name: "rejects evidence promotion typo",
			mutate: func(command *StartProductionCommand) {
				command.PostProduction.Evidence = "live"
			},
			wantErr: true,
		},
		{
			name: "rejects non-speech route",
			mutate: func(command *StartProductionCommand) {
				command.PostProduction.SpeechRouteSnapshot.CapabilityAlias = "video.primary"
			},
			wantErr: true,
		},
		{
			name: "rejects zero paid budget",
			mutate: func(command *StartProductionCommand) {
				command.PostProduction.SpeechBudgetLimit.AmountMicros = 0
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := valid()
			if test.mutate != nil {
				test.mutate(&command)
			}
			err := validateStartProduction(command)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateStartProduction() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestValidateCreatePlanSpeechBudgetBoundary(t *testing.T) {
	t.Parallel()
	valid := func() CreateGenerationPlanCommand {
		return CreateGenerationPlanCommand{
			SchemaVersion:       "v1",
			SeriesID:            uuid.NewString(),
			EpisodeRevisionID:   uuid.NewString(),
			ShotSpecRevisionIDs: []string{uuid.NewString()},
			CandidatesPerShot:   1,
			RouteSnapshot: ModelRouteSnapshot{
				CapabilityAlias:   "video.primary",
				ProviderProfileID: uuid.NewString(),
				Provider:          "mock", ModelID: "video-v1", RouteVersion: "route-v1",
				CapabilityHash: strings.Repeat("a", 64),
			},
			BudgetLimit:       BudgetLimit{AmountMicros: 1_000, Currency: "CNY"},
			SpeechBudgetLimit: &BudgetLimit{AmountMicros: 500, Currency: "CNY"},
			ExecutionPolicy: ExecutionPolicy{
				TargetTerritory: "CN", ProductForm: "INTERNAL_PREVIEW",
				ContentSafetyPolicyVersion: "safety-v1",
				ContentSafetyDecisionID:    uuid.NewString(),
			},
			Actor: Actor{ActorID: "producer", Role: "PRODUCER"},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*CreateGenerationPlanCommand)
		wantErr bool
	}{
		{name: "valid speech envelope"},
		{
			name: "valid plan without post-production",
			mutate: func(command *CreateGenerationPlanCommand) {
				command.SpeechBudgetLimit = nil
			},
		},
		{
			name: "rejects zero speech envelope",
			mutate: func(command *CreateGenerationPlanCommand) {
				command.SpeechBudgetLimit.AmountMicros = 0
			},
			wantErr: true,
		},
		{
			name: "rejects non ISO speech currency",
			mutate: func(command *CreateGenerationPlanCommand) {
				command.SpeechBudgetLimit.Currency = "yuan"
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command := valid()
			if test.mutate != nil {
				test.mutate(&command)
			}
			err := validateCreatePlan(command)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateCreatePlan() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestAuthorizeApprovalDoesNotLetAdminBypassGateRole(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		action  Action
		role    string
		wantErr bool
	}{
		{name: "director approves G3", action: ActionApproveG3, role: "DIRECTOR"},
		{name: "reviewer approves Q1", action: ActionApproveQ1, role: "REVIEWER"},
		{name: "safety reviewer approves safety", action: ActionApproveSafety, role: "SAFETY_REVIEWER"},
		{name: "admin cannot bypass G3", action: ActionApproveG3, role: "ADMIN", wantErr: true},
		{name: "producer cannot approve safety", action: ActionApproveSafety, role: "PRODUCER", wantErr: true},
		{name: "creator cannot cancel", action: ActionCancelRun, role: "CREATOR", wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := authorize(test.action, Actor{ActorID: "actor-1", Role: test.role})
			if (err != nil) != test.wantErr {
				t.Fatalf("authorize() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestServer_BusinessRouteWithoutStoreIsUnavailable(t *testing.T) {
	t.Parallel()
	server := NewWithDependencies(runtimeconfig.ControlPlane{}, nil)
	request := httptest.NewRequest(http.MethodGet, APIBase+"/operations/"+uuid.NewString(), nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestServer_ReplayedApprovalRedeliversTemporalSignal(t *testing.T) {
	t.Parallel()
	store := &approvalHandlerStore{}
	workflows := &approvalWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{}, nil, store, workflows, nil)
	body := `{
		"schemaVersion":"v1",
		"seriesId":"` + uuid.NewString() + `",
		"episodeId":"` + uuid.NewString() + `",
		"gate":"Q1",
		"decision":"APPROVED",
		"reasonCode":"QUALITY_ACCEPTED",
		"bindings":[
			{"objectType":"SHOT_SPEC_REVISION","revisionId":"` + uuid.NewString() + `","contentHash":"` + strings.Repeat("a", 64) + `"},
			{"objectType":"GENERATION_RUN","revisionId":"` + uuid.NewString() + `","contentHash":"` + strings.Repeat("b", 64) + `"}
		],
		"actor":{"actorId":"reviewer-1","role":"REVIEWER"}
	}`
	request := httptest.NewRequest(http.MethodPost, APIBase+"/approvals", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if workflows.deliveries != 1 {
		t.Fatalf("Temporal approval deliveries = %d, want 1", workflows.deliveries)
	}
}

func TestServer_ProductionAuthenticationBindsSignedActor(t *testing.T) {
	t.Parallel()
	secret := strings.Repeat("x", 32)
	calls := 0
	store := &handlerStore{
		createSeries: func(_ context.Context, _ CreateSeriesCommand, _ Idempotency, traceID string) (Stored[Operation], error) {
			calls++
			now := time.Now().UTC()
			return Stored[Operation]{Value: Operation{
				OperationID: uuid.NewString(), OperationType: "CREATE_SERIES",
				AggregateType: "SERIES", AggregateID: uuid.NewString(), State: "SUCCEEDED",
				TraceID: traceID, CreatedAt: now, UpdatedAt: now,
			}}, nil
		},
	}
	server := NewWithRuntime(runtimeconfig.ControlPlane{
		Environment: "production", AuthHMACSecret: secret, AuthAudience: "video-control-plane",
	}, nil, store, nil, nil)
	body := `{
		"schemaVersion":"v1",
		"title":"Authenticated series",
		"generationProfileRevisionId":"` + uuid.NewString() + `",
		"rightsDeclaration":{"basis":"licensed","evidenceArtifactHash":"` + strings.Repeat("a", 64) + `"},
		"actor":{"actorId":"creator-1","role":"CREATOR"}
	}`

	request := httptest.NewRequest(http.MethodPost, APIBase+"/series", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("missing auth status=%d calls=%d body=%s", recorder.Code, calls, recorder.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, APIBase+"/series", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("Authorization", "Bearer "+signedTestToken(t, secret, "other-creator", "CREATOR"))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || calls != 0 {
		t.Fatalf("actor mismatch status=%d calls=%d body=%s", recorder.Code, calls, recorder.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost, APIBase+"/series", strings.NewReader(body))
	request.Header.Set("Idempotency-Key", uuid.NewString())
	request.Header.Set("Authorization", "Bearer "+signedTestToken(t, secret, "creator-1", "CREATOR"))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted || calls != 1 {
		t.Fatalf("valid auth status=%d calls=%d body=%s", recorder.Code, calls, recorder.Body.String())
	}
}

func signedTestToken(t *testing.T, secret, subject, role string) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := json.Marshal(map[string]any{
		"sub": subject, "role": role, "aud": "video-control-plane", "exp": time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	head := base64.RawURLEncoding.EncodeToString(header)
	body := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := head + "." + body
	signature := hmac.New(sha256.New, []byte(secret))
	_, _ = signature.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature.Sum(nil))
}
