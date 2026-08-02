package controlplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/google/uuid"
)

type creatorPlanRequest struct {
	SchemaVersion  string `json:"schemaVersion"`
	Title          string `json:"title"`
	SceneText      string `json:"sceneText"`
	AspectRatio    string `json:"aspectRatio"`
	RightsAccepted bool   `json:"rightsAccepted"`
}

type creatorConfirmRequest struct {
	SchemaVersion string `json:"schemaVersion"`
	Confirmed     bool   `json:"confirmed"`
	PlanHash      string `json:"planHash"`
}

type adapterCapabilitiesResponse struct {
	SchemaVersion string                                `json:"schemaVersion"`
	ProviderID    string                                `json:"providerId"`
	Capabilities  []providercontract.CapabilitySnapshot `json:"capabilities"`
}

type creatorArtifactStore interface {
	ArtifactVerifier
	Put(context.Context, io.Reader) (artifactstore.Artifact, error)
	OpenSeeker(string) (interface {
		io.Reader
		io.Seeker
		io.Closer
	}, error)
}

func (s *Server) creatorDependencies(w http.ResponseWriter, r *http.Request) (CreatorStore, creatorArtifactStore, bool) {
	store, ok := s.store.(CreatorStore)
	if !ok {
		writeProblem(w, traceID(r), domainError(CodeDependency, http.StatusServiceUnavailable,
			"creator live-shot PostgreSQL store is not configured", "apply migration 000008 and restore PostgreSQL", nil))
		return nil, nil, false
	}
	artifacts, ok := s.artifacts.(creatorArtifactStore)
	if !ok {
		writeProblem(w, traceID(r), domainError(CodeDependency, http.StatusServiceUnavailable,
			"creator live-shot artifact store is not configured", "restore CAS and retry", nil))
		return nil, nil, false
	}
	return store, artifacts, true
}

func creatorActor(r *http.Request) Actor {
	if actor, ok := authenticatedActor(r.Context()); ok {
		return actor
	}
	return Actor{ActorID: "local-studio", Role: "CREATOR"}
}

func authorizeCreatorLiveShot(actor Actor) error {
	switch actor.Role {
	case "CREATOR", "PRODUCER", "ADMIN":
		return nil
	default:
		return forbiddenError("authenticated role cannot create or confirm a Studio live shot")
	}
}

func (s *Server) createCreatorLiveShotPlan(w http.ResponseWriter, r *http.Request) {
	store, artifacts, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	var request creatorPlanRequest
	if !decodeCommand(w, r, &request) {
		return
	}
	trace := traceID(r)
	if request.SchemaVersion != "v1" {
		writeProblem(w, trace, validationError("schemaVersion must be v1"))
		return
	}
	request.Title = strings.TrimSpace(request.Title)
	request.SceneText = strings.TrimSpace(request.SceneText)
	if count := utf8.RuneCountInString(request.Title); count < 1 || count > 80 {
		writeProblem(w, trace, validationError("title must contain 1 to 80 Unicode characters"))
		return
	}
	if count := utf8.RuneCountInString(request.SceneText); count < 1 || count > 800 {
		writeProblem(w, trace, validationError("sceneText must contain 1 to 800 Unicode characters"))
		return
	}
	if request.AspectRatio != "16:9" && request.AspectRatio != "9:16" {
		writeProblem(w, trace, validationError("aspectRatio must be 16:9 or 9:16"))
		return
	}
	if !request.RightsAccepted {
		writeProblem(w, trace, NewPolicyError(CodeLicenseBlocked, "rightsAccepted must be true", "accept the source rights declaration before planning"))
		return
	}
	actor := creatorActor(r)
	if err := authorizeCreatorLiveShot(actor); err != nil {
		writeProblem(w, trace, err)
		return
	}
	idempotency, err := commandIdempotency(r, "creator:live-shot-plan:"+actor.ActorID, struct {
		Request creatorPlanRequest `json:"request"`
		ActorID string             `json:"actorId"`
	}{request, actor.ActorID})
	if err != nil {
		writeProblem(w, trace, err)
		return
	}
	capability, err := s.liveVideoCapability(r.Context())
	if err != nil {
		writeProblem(w, trace, err)
		return
	}
	artifact, err := artifacts.Put(r.Context(), bytes.NewReader([]byte(request.SceneText)))
	if err != nil {
		writeProblem(w, trace, domainError(CodeDependency, http.StatusServiceUnavailable, "source text could not be committed to CAS", "restore CAS and retry with the same idempotency key", err))
		return
	}
	command := CreatorLiveShotPlanCommand{
		Title: request.Title, SceneText: request.SceneText, AspectRatio: request.AspectRatio,
		RightsAccepted: true, SourceArtifactHash: artifact.Digest, SourceArtifactURI: artifact.URI,
		Route: capability, Actor: actor,
	}
	stored, err := store.CreateCreatorLiveShotPlan(r.Context(), command, idempotency, trace)
	if err != nil {
		writeProblem(w, trace, err)
		return
	}
	w.Header().Set("Location", APIBase+"/creator/live-shot-plans/"+stored.Value.PlanID)
	w.Header().Set("ETag", `"`+stored.Value.PlanHash+`"`)
	writeJSON(w, http.StatusCreated, stored.Value)
}

func (s *Server) confirmCreatorLiveShotPlan(w http.ResponseWriter, r *http.Request) {
	store, _, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	var request creatorConfirmRequest
	if !decodeCommand(w, r, &request) {
		return
	}
	trace := traceID(r)
	if request.SchemaVersion != "v1" {
		writeProblem(w, trace, validationError("schemaVersion must be v1"))
		return
	}
	planID := r.PathValue("planId")
	if _, err := uuid.Parse(planID); err != nil {
		writeProblem(w, trace, NewNotFoundError("creator live-shot plan", planID))
		return
	}
	if !sha256Pattern.MatchString(request.PlanHash) {
		writeProblem(w, trace, validationError("planHash must be a lowercase SHA-256 digest"))
		return
	}
	if strings.TrimSpace(r.Header.Get("If-Match")) != `"`+request.PlanHash+`"` {
		writeProblem(w, trace, NewConflictError(CodePlanHashMismatch, "If-Match must quote the exact planHash from the request body"))
		return
	}
	actor := creatorActor(r)
	if err := authorizeCreatorLiveShot(actor); err != nil {
		writeProblem(w, trace, err)
		return
	}
	idempotency, err := commandIdempotency(r, "creator:live-shot-plan:"+planID+":confirm:"+actor.ActorID, struct {
		Request creatorConfirmRequest `json:"request"`
		ActorID string                `json:"actorId"`
	}{request, actor.ActorID})
	if err != nil {
		writeProblem(w, trace, err)
		return
	}
	var capability providercontract.CapabilitySnapshot
	if s.config.LiveCallsEnabled {
		capability, err = s.liveVideoCapability(r.Context())
		if err != nil {
			writeProblem(w, trace, err)
			return
		}
	}
	stored, err := store.ConfirmCreatorLiveShotPlan(r.Context(), planID, ConfirmCreatorLiveShotCommand{
		Confirmed: request.Confirmed, PlanHash: request.PlanHash, LiveCallsEnabled: s.config.LiveCallsEnabled,
		Route: capability, Actor: actor,
	}, idempotency, trace)
	if err != nil {
		writeProblem(w, trace, err)
		return
	}
	stored.Value.Replayed = stored.Replayed
	if stored.Replayed && !s.config.LiveCallsEnabled {
		// Return the durable identity, but never (re)start execution while the
		// operational kill switch is down.
		w.Header().Set("Location", APIBase+"/creator/live-shot-runs/"+stored.Value.RunID)
		w.Header().Set("ETag", creatorRunETag(stored.Value))
		writeJSON(w, http.StatusAccepted, stored.Value)
		return
	}
	if s.workflows == nil {
		writeProblem(w, trace, domainError(CodeTemporal, http.StatusServiceUnavailable,
			"Temporal did not acknowledge the persisted creator provider intent",
			"restore Temporal and retry with the same idempotency key", nil))
		return
	}
	operationID := uuid.NewSHA1(uuid.MustParse(planID), []byte("confirm-operation")).String()
	operation := Operation{
		OperationID: operationID, OperationType: "CONFIRM_CREATOR_LIVE_SHOT",
		AggregateType: "CREATOR_LIVE_SHOT_RUN", AggregateID: stored.Value.RunID,
		State: "ACCEPTED", TemporalWorkflowID: "creator-live-shot-" + stored.Value.RunID,
		TraceID: trace, CreatedAt: stored.Value.CreatedAt,
	}
	if _, err := s.workflows.StartShot(r.Context(), operation); err != nil {
		writeProblem(w, trace, &DomainError{Code: CodeTemporal, Status: http.StatusServiceUnavailable,
			Retryable: true, Detail: "Temporal did not acknowledge the persisted creator provider intent",
			SuggestedAction: "retry with the same idempotency key; the stable workflow and Provider job IDs prevent duplicate billing", Cause: err})
		return
	}
	w.Header().Set("Location", APIBase+"/creator/live-shot-runs/"+stored.Value.RunID)
	w.Header().Set("ETag", creatorRunETag(stored.Value))
	writeJSON(w, http.StatusAccepted, stored.Value)
}

func (s *Server) listCreatorLiveShots(w http.ResponseWriter, r *http.Request) {
	store, _, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	project, err := store.ListCreatorLiveShots(r.Context(), r.PathValue("seriesId"), creatorActor(r))
	if err != nil {
		writeProblem(w, traceID(r), err)
		return
	}
	writeJSON(w, http.StatusOK, project)
}

func (s *Server) getCreatorLiveShotRun(w http.ResponseWriter, r *http.Request) {
	store, _, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	run, err := store.GetCreatorLiveShotRun(r.Context(), r.PathValue("runId"), creatorActor(r))
	if err != nil {
		writeProblem(w, traceID(r), err)
		return
	}
	etag := creatorRunETag(run)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", etag)
	writeJSON(w, http.StatusOK, run)
}

func creatorRunETag(run CreatorLiveShotRun) string {
	sum := sha256.Sum256([]byte(run.RunID + "\x00" + run.State + "\x00" + run.ManifestHash + "\x00" + run.UpdatedAt.UTC().Format(time.RFC3339Nano)))
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (s *Server) getCreatorLiveShotArtifact(w http.ResponseWriter, r *http.Request) {
	store, artifacts, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	runID := r.PathValue("runId")
	record, err := store.GetCreatorLiveShotArtifact(r.Context(), runID, creatorActor(r))
	if err != nil {
		writeProblem(w, traceID(r), err)
		return
	}
	reader, err := artifacts.OpenSeeker(record.Digest)
	if err != nil {
		writeProblem(w, traceID(r), domainError(CodeArtifactCommitFailed, http.StatusServiceUnavailable, "the committed artifact is unavailable", "restore the CAS object and retry", err))
		return
	}
	defer reader.Close()
	mediaType := record.MediaType
	if mediaType == "" {
		mediaType = "video/mp4"
	}
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="live-shot-%s.mp4"`, runID))
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "live-shot-"+runID+".mp4", time.Time{}, reader)
}

func (s *Server) getCreatorLiveShotManifest(w http.ResponseWriter, r *http.Request) {
	store, _, ok := s.creatorDependencies(w, r)
	if !ok {
		return
	}
	manifest, err := store.GetCreatorLiveShotManifest(r.Context(), r.PathValue("runId"), creatorActor(r))
	if err != nil {
		writeProblem(w, traceID(r), err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) liveVideoCapability(ctx context.Context) (providercontract.CapabilitySnapshot, error) {
	client := s.providerHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.config.ProviderAdapterURL, "/")+"/v1/capabilities", nil)
	if err != nil {
		return providercontract.CapabilitySnapshot{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return providercontract.CapabilitySnapshot{}, domainError(CodeDependency, http.StatusServiceUnavailable, "the Provider Adapter capability snapshot is unavailable", "restore the authenticated Adapter and retry", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return providercontract.CapabilitySnapshot{}, domainError(CodeDependency, http.StatusServiceUnavailable, "the Provider Adapter rejected capability discovery", "verify service authentication and Adapter readiness", nil)
	}
	var payload adapterCapabilitiesResponse
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return providercontract.CapabilitySnapshot{}, domainError(CodeDependency, http.StatusServiceUnavailable, "the Provider Adapter capability response is invalid", "upgrade or restore the Adapter", err)
	}
	for _, capability := range payload.Capabilities {
		if capability.Alias == providercontract.CapabilityVideo {
			billing, _ := capability.Limits["billingMode"].(string)
			if !capability.Configured || !capability.Enabled || capability.Mode != "live" || billing != "subscription" {
				return providercontract.CapabilitySnapshot{}, NewPolicyError(CodeSubscriptionRouteRequired, "video.primary is not an enabled subscription capability", "configure the verified Agent Plan route")
			}
			for _, key := range []string{"cashAmountMaximum", "unitPriceMicros"} {
				if value, exists := capability.Limits[key]; exists && value != nil && !creatorCashValueIsZero(value) {
					return providercontract.CapabilitySnapshot{}, NewPolicyError(CodeCashChargeNotAllowed, "video.primary carries a positive or unverifiable cash estimate", "configure a subscription route with null or exact-zero cash maximum")
				}
			}
			if !creatorCapabilitySupportsFixedOutput(capability) {
				return providercontract.CapabilitySnapshot{}, NewPolicyError(CodeCapability, "video.primary does not support the fixed Studio output contract", "configure 5s 720p 24fps support for 16:9 and 9:16")
			}
			return capability, nil
		}
	}
	return providercontract.CapabilitySnapshot{}, NewPolicyError(CodeCapability, "the Adapter exposes no video.primary capability", "configure and verify the video Adapter")
}

func creatorCashValueIsZero(value any) bool {
	switch typed := value.(type) {
	case float64:
		return typed == 0
	case int:
		return typed == 0
	case int64:
		return typed == 0
	case json.Number:
		parsed, err := typed.Int64()
		return err == nil && parsed == 0
	default:
		return false
	}
}

func creatorCapabilitySupportsFixedOutput(snapshot providercontract.CapabilitySnapshot) bool {
	capability := snapshot.Capability
	if capability.OutputModality != providercontract.ModalityVideo || capability.MinDurationMillis > 5_000 || capability.MaxDurationMillis < 5_000 {
		return false
	}
	stringsFound := map[string]bool{}
	for _, value := range capability.Resolutions {
		stringsFound[value] = true
	}
	for _, value := range capability.AspectRatios {
		stringsFound[value] = true
	}
	fps24 := false
	for _, value := range capability.NativeFPS {
		if value == 24 {
			fps24 = true
		}
	}
	textInput := false
	for _, value := range snapshot.SupportedInputs {
		if value == "text" {
			textInput = true
		}
	}
	return stringsFound["720p"] && stringsFound["16:9"] && stringsFound["9:16"] && fps24 && textInput
}
