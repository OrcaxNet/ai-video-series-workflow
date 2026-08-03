package controlplane

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/google/uuid"
)

type creatorStoreFixture struct {
	shotHandlerStore
	createCalls  int
	confirmCalls int
	plan         CreatorLiveShotPlan
	run          CreatorLiveShotRun
	artifact     CreatorArtifactRecord
	manifest     CreatorLiveShotManifest
}

func (f *creatorStoreFixture) CreateCreatorLiveShotPlan(_ context.Context, command CreatorLiveShotPlanCommand, _ Idempotency, _ string) (Stored[CreatorLiveShotPlan], error) {
	f.createCalls++
	if command.Actor.ActorID != "local-studio" || command.Route.Alias != providercontract.CapabilityVideo || command.SourceArtifactHash == "" {
		return Stored[CreatorLiveShotPlan]{}, validationError("fixture received incomplete server-derived plan")
	}
	return Stored[CreatorLiveShotPlan]{Value: f.plan}, nil
}

func (f *creatorStoreFixture) ConfirmCreatorLiveShotPlan(_ context.Context, _ string, command ConfirmCreatorLiveShotCommand, _ Idempotency, _ string) (Stored[CreatorLiveShotRun], error) {
	f.confirmCalls++
	if !command.LiveCallsEnabled {
		return Stored[CreatorLiveShotRun]{}, NewPolicyError(CodeLiveCallsDisabled, "live Provider calls are not armed", "arm live calls")
	}
	if !command.Confirmed || command.PlanHash != f.plan.PlanHash || command.Actor.ActorID != "local-studio" {
		return Stored[CreatorLiveShotRun]{}, validationError("fixture received incomplete exact confirmation")
	}
	return Stored[CreatorLiveShotRun]{Value: f.run}, nil
}

func (f *creatorStoreFixture) ListCreatorLiveShots(context.Context, string, Actor) (CreatorLiveShotProject, error) {
	return CreatorLiveShotProject{SchemaVersion: "v1", SeriesID: f.run.SeriesID, Plan: f.plan, Runs: []CreatorLiveShotRun{f.run}}, nil
}
func (f *creatorStoreFixture) GetCreatorLiveShotRun(context.Context, string, Actor) (CreatorLiveShotRun, error) {
	return f.run, nil
}
func (f *creatorStoreFixture) GetCreatorLiveShotArtifact(context.Context, string, Actor) (CreatorArtifactRecord, error) {
	return f.artifact, nil
}
func (f *creatorStoreFixture) GetCreatorLiveShotManifest(context.Context, string, Actor) (CreatorLiveShotManifest, error) {
	return f.manifest, nil
}

func creatorCapabilityServer(t *testing.T, calls *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/capabilities" {
			t.Fatalf("capability request = %s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, http.StatusOK, adapterCapabilitiesResponse{SchemaVersion: "v1", ProviderID: "agent-plan", Capabilities: []providercontract.CapabilitySnapshot{{
			Alias: providercontract.CapabilityVideo,
			Capability: providercontract.Capability{Provider: "volcengine_ark", ModelFamily: "doubao-seedance-2.0", OutputModality: providercontract.ModalityVideo,
				Resolutions: []string{"720p"}, AspectRatios: []string{"16:9", "9:16"}, MinDurationMillis: 4000, MaxDurationMillis: 15000, NativeFPS: []int{24}},
			Configured: true, Enabled: true, Mode: "live", RouteVersion: "agent-plan-large-v1",
			SnapshotHash: strings.Repeat("a", 64), EffectiveAt: time.Now(),
			Limits:          map[string]any{"billingMode": "subscription", "maximumConcurrency": float64(1)},
			SupportedInputs: []string{"text"},
		}}})
	}))
}

func TestCreatorLiveShotPlanIsZeroSubmitAndConfirmIsArmed(t *testing.T) {
	capabilityCalls := 0
	adapter := creatorCapabilityServer(t, &capabilityCalls)
	defer adapter.Close()
	artifacts, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	planID, seriesID, runID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	planHash := strings.Repeat("b", 64)
	now := time.Now().UTC()
	fixture := &creatorStoreFixture{
		plan: CreatorLiveShotPlan{PlanID: planID, SeriesID: seriesID, State: "READY", PlanHash: planHash, ExpiresAt: now.Add(15 * time.Minute), CreatedAt: now},
		run:  CreatorLiveShotRun{RunID: runID, PlanID: planID, SeriesID: seriesID, State: "QUEUED", PlanHash: planHash, CreatedAt: now, UpdatedAt: now},
	}
	workflows := &shotWorkflowFixture{}
	server := NewWithRuntime(runtimeconfig.ControlPlane{Environment: "test", ProviderAdapterURL: adapter.URL}, nil, fixture, workflows, artifacts)

	planRequest := httptest.NewRequest(http.MethodPost, APIBase+"/creator/live-shot-plans", strings.NewReader(`{"schemaVersion":"v1","title":"第一镜","sceneText":"雨夜里，主角推开旧车站的门。","aspectRatio":"16:9","rightsAccepted":true}`))
	planRequest.Header.Set("Content-Type", "application/json")
	planRequest.Header.Set("Idempotency-Key", uuid.NewString())
	planResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(planResponse, planRequest)
	if planResponse.Code != http.StatusCreated || fixture.createCalls != 1 || fixture.confirmCalls != 0 || workflows.startCalls != 0 || capabilityCalls != 1 {
		t.Fatalf("plan status=%d create=%d confirm=%d workflow=%d capability=%d body=%s", planResponse.Code, fixture.createCalls, fixture.confirmCalls, workflows.startCalls, capabilityCalls, planResponse.Body.String())
	}

	confirmBody := `{"schemaVersion":"v1","confirmed":true,"planHash":"` + planHash + `"}`
	confirmRequest := httptest.NewRequest(http.MethodPost, APIBase+"/creator/live-shot-plans/"+planID+"/confirm", strings.NewReader(confirmBody))
	confirmRequest.Header.Set("Idempotency-Key", uuid.NewString())
	confirmRequest.Header.Set("If-Match", `"`+planHash+`"`)
	unarmedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(unarmedResponse, confirmRequest)
	if unarmedResponse.Code != http.StatusUnprocessableEntity || fixture.confirmCalls != 1 || workflows.startCalls != 0 {
		t.Fatalf("unarmed status=%d confirm=%d workflow=%d body=%s", unarmedResponse.Code, fixture.confirmCalls, workflows.startCalls, unarmedResponse.Body.String())
	}

	server.config.LiveCallsEnabled = true
	armedRequest := httptest.NewRequest(http.MethodPost, APIBase+"/creator/live-shot-plans/"+planID+"/confirm", strings.NewReader(confirmBody))
	armedRequest.Header.Set("Idempotency-Key", uuid.NewString())
	armedRequest.Header.Set("If-Match", `"`+planHash+`"`)
	armedResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(armedResponse, armedRequest)
	if armedResponse.Code != http.StatusAccepted || fixture.confirmCalls != 2 || workflows.startCalls != 1 || capabilityCalls != 2 {
		t.Fatalf("armed status=%d confirm=%d workflow=%d capability=%d body=%s", armedResponse.Code, fixture.confirmCalls, workflows.startCalls, capabilityCalls, armedResponse.Body.String())
	}
}

func TestCreatorArtifactSupportsRangeAndManifestHasNullCash(t *testing.T) {
	artifacts, err := artifactstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	object, err := artifacts.Put(t.Context(), strings.NewReader("0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	fixture := &creatorStoreFixture{
		artifact: CreatorArtifactRecord{Digest: object.Digest, MediaType: "video/mp4", SizeBytes: object.Size},
		manifest: CreatorLiveShotManifest{SchemaVersion: "creator-live-shot-manifest.v1", Evidence: "live_provider_call", RunID: runID, CashCost: CreatorCashCost{AmountMicros: nil, Verified: false, BillingMode: "subscription"}},
	}
	server := NewWithRuntime(runtimeconfig.ControlPlane{Environment: "test"}, nil, fixture, nil, artifacts)
	request := httptest.NewRequest(http.MethodGet, APIBase+"/creator/live-shot-runs/"+runID+"/artifact", nil)
	request.Header.Set("Range", "bytes=2-5")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusPartialContent || response.Body.String() != "2345" || !strings.Contains(response.Header().Get("Content-Disposition"), runID) {
		t.Fatalf("range status=%d body=%q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	manifestResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(manifestResponse, httptest.NewRequest(http.MethodGet, APIBase+"/creator/live-shot-runs/"+runID+"/manifest", nil))
	var manifest map[string]any
	if manifestResponse.Code != http.StatusOK || json.Unmarshal(manifestResponse.Body.Bytes(), &manifest) != nil {
		t.Fatalf("manifest status=%d body=%s", manifestResponse.Code, manifestResponse.Body.String())
	}
	cash := manifest["cost"].(map[string]any)
	if cash["amountMicros"] != nil || cash["verified"] != false || cash["billingMode"] != "subscription" {
		t.Fatalf("cash evidence=%#v", cash)
	}
}

func TestProviderStatusUsesAuthenticatedAdapterSnapshotAndArmsOnlyVideo(t *testing.T) {
	calls := 0
	adapter := creatorCapabilityServer(t, &calls)
	defer adapter.Close()
	server := NewWithDependencies(runtimeconfig.ControlPlane{Environment: "test", ProviderAdapterURL: adapter.URL, LiveCallsEnabled: true}, nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, APIBase+"/providers/status", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Mode         string                     `json:"mode"`
		Capabilities []providerCapabilityStatus `json:"capabilities"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Mode != "live" || len(payload.Capabilities) != 4 || calls != 1 {
		t.Fatalf("payload=%#v calls=%d", payload, calls)
	}
	for _, capability := range payload.Capabilities {
		if capability.Alias == "video.primary" {
			if !capability.LiveConfigured || !capability.LiveCallsEnabled || capability.CapabilityHash != strings.Repeat("a", 64) || capability.BillingMode != "subscription" {
				t.Fatalf("video=%#v", capability)
			}
		} else if capability.LiveConfigured || capability.LiveCallsEnabled {
			t.Fatalf("non-video armed=%#v", capability)
		}
	}
}
