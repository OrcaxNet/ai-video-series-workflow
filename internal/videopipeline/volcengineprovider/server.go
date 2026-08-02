// Package volcengineprovider implements the provider-neutral video adapter for
// Volcengine Ark Agent Plan. Credentials and signed download URLs terminate at
// this process; only immutable CAS references leave the adapter boundary.
package volcengineprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/runtimeconfig"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
)

const (
	maxResolvedProviderAssetBytes = 20 << 20
	maxSpeechVoiceDescriptorBytes = 64 << 10
	speechDurationToleranceMillis = int64(250)
)

var errProviderJobIntentExists = errors.New("provider job intent already exists")

type jobRecord struct {
	RequestHash      string                       `json:"request_hash"`
	Expected         providercontract.OutputSpec  `json:"expected_output"`
	Response         providercontract.JobResponse `json:"response"`
	SpeechInspection *speechInspectionCheckpoint  `json:"speech_inspection,omitempty"`
	SpeechDuration   *speechDurationQC            `json:"speech_duration_qc,omitempty"`
	Reconciliations  []speechReconciliation       `json:"speech_reconciliations,omitempty"`
}

type speechDurationQC struct {
	RequestedMillis int64  `json:"requested_millis"`
	MeasuredMillis  int64  `json:"measured_millis"`
	DeltaMillis     int64  `json:"delta_millis"`
	ToleranceMillis int64  `json:"tolerance_millis"`
	State           string `json:"state"`
}

type speechReconciliation struct {
	Attempt                int                          `json:"attempt"`
	StartedAt              string                       `json:"started_at"`
	AuthorizedRecordSHA256 string                       `json:"authorized_record_sha256"`
	PreviousResponse       providercontract.JobResponse `json:"previous_response"`
}

type speechRetryClaim struct {
	SchemaVersion          string `json:"schema_version"`
	JobID                  string `json:"job_id"`
	AuthorizedRecordSHA256 string `json:"authorized_record_sha256"`
	ClaimedAt              string `json:"claimed_at"`
}

type speechVoiceDescriptor struct {
	SchemaVersion        string `json:"schemaVersion"`
	Provider             string `json:"provider"`
	ModelID              string `json:"modelId"`
	ResourceID           string `json:"resourceId"`
	Speaker              string `json:"speaker"`
	RouteVersion         string `json:"routeVersion"`
	VoiceClone           *bool  `json:"voiceClone"`
	InternalMVPOnly      *bool  `json:"internalMvpOnly"`
	ParentAssetVersionID string `json:"parentAssetVersionId"`
	AssetVersionID       string `json:"assetVersionId"`
	LicenseSnapshotID    string `json:"licenseSnapshotId"`
	RevisionInputHash    string `json:"revisionInputHash"`
}

type Server struct {
	config         runtimeconfig.VolcengineProvider
	provider       providercontract.Provider
	speech         SpeechSynthesizer
	store          *artifactstore.Store
	downloadClient *http.Client
	inspector      MediaInspector
	authenticator  *serviceAuthenticator
	now            func() time.Time
	stateDir       string

	mu sync.Mutex
}

type Options struct {
	DownloadClient *http.Client
	Inspector      MediaInspector
	Speech         SpeechSynthesizer
	Now            func() time.Time
	AuthNow        func() time.Time
}

func New(
	config runtimeconfig.VolcengineProvider,
	provider providercontract.Provider,
	store *artifactstore.Store,
	options Options,
) (*Server, error) {
	if provider == nil || store == nil {
		return nil, errors.New("live provider and artifact store are required")
	}
	if (options.Speech != nil || config.SpeechSpeaker != "") &&
		config.SpeechSpeaker != AgentPlanTTSSpeakerID {
		return nil, errors.New("live Adapter requires the approved TTS 2.0 speaker")
	}
	client := options.DownloadClient
	if client == nil {
		client = &http.Client{Timeout: config.DownloadTimeout}
	}
	inspector := options.Inspector
	if inspector == nil {
		inspector = FFprobeInspector{}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	authNow := options.AuthNow
	if authNow == nil {
		authNow = time.Now
	}
	stateDir := filepath.Join(store.Root(), "provider-jobs")
	if err := os.MkdirAll(stateDir, 0o750); err != nil {
		return nil, fmt.Errorf("create live provider job registry: %w", err)
	}
	authenticator, err := newServiceAuthenticator(config.ServiceAuthSecret, authNow)
	if err != nil {
		return nil, err
	}
	return &Server{
		config: config, provider: provider, store: store,
		speech:         options.Speech,
		downloadClient: client, inspector: inspector, authenticator: authenticator, now: now,
		stateDir: stateDir,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.health)
	mux.HandleFunc("GET /health/ready", s.health)
	mux.Handle("GET /v1/capabilities", s.authenticator.middleware(http.HandlerFunc(s.capabilities)))
	mux.Handle("POST /v1/estimates", s.authenticator.middleware(http.HandlerFunc(s.estimate)))
	mux.Handle("POST /v1/jobs", s.authenticator.middleware(http.HandlerFunc(s.createJob)))
	mux.Handle("GET /v1/jobs/{jobID}", s.authenticator.middleware(http.HandlerFunc(s.getJob)))
	mux.Handle("POST /v1/jobs/{jobID}/cancel", s.authenticator.middleware(http.HandlerFunc(s.cancelJob)))
	return http.MaxBytesHandler(mux, 1<<20)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ready",
		"providerId":  s.config.ProviderID,
		"mode":        "live",
		"configured":  true,
		"requiresKey": true,
		"plan":        s.config.PlanName,
		"model":       s.config.VideoModel,
		"speechModel": s.config.SpeechModel,
		"region":      s.config.Region,
	})
}

func (s *Server) capabilities(w http.ResponseWriter, _ *http.Request) {
	material := strings.Join([]string{
		s.config.ProviderID, s.config.Region, s.config.VideoModel,
		s.config.PlanName, s.config.PricingVersion,
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	capabilities := []providercontract.CapabilitySnapshot{{
		Alias: providercontract.CapabilityVideo,
		Capability: providercontract.Capability{
			Provider: "volcengine_ark", ModelFamily: s.config.VideoModel,
			OutputModality: providercontract.ModalityVideo,
			Async:          true, SupportsPolling: true, SupportsCancel: true,
			SupportsReferenceImage: true, SupportsLastFrame: true,
			Resolutions:       []string{"480p", "720p", "1080p"},
			AspectRatios:      []string{"16:9", "9:16", "4:3", "3:4", "21:9"},
			MinDurationMillis: 4_000, MaxDurationMillis: 15_000,
			NativeFPS: []int{24}, Verification: providercontract.PendingKey,
		},
		Configured: true, Enabled: true, Mode: "live",
		RouteVersion: "agent-plan-large-v1",
		SnapshotHash: hex.EncodeToString(sum[:]),
		EffectiveAt:  s.now().UTC(),
		Limits: map[string]any{
			"maximumConcurrency": 1,
			"billingMode":        "subscription",
		},
		SupportedInputs: []string{"text", "image-reference", "tail-frame"},
	}}
	if s.speech != nil {
		capabilities = append(capabilities, providercontract.CapabilitySnapshot{
			Alias: providercontract.CapabilitySpeech,
			Capability: providercontract.Capability{
				Provider: "volcengine_ark", ModelFamily: s.config.SpeechModel,
				InputModalities: []providercontract.Modality{providercontract.ModalityText},
				OutputModality:  providercontract.ModalityAudio,
				Verification:    providercontract.PendingKey,
			},
			Configured: true, Enabled: true, Mode: "live",
			RouteVersion: AgentPlanTTSRouteVersion,
			SnapshotHash: AgentPlanTTSCapabilityHash(s.config),
			EffectiveAt:  s.now().UTC(),
			Limits: map[string]any{
				"resourceId":           AgentPlanTTSResourceID,
				"defaultSpeaker":       s.config.SpeechSpeaker,
				"maximumCharacters":    AgentPlanTTSMaxChars,
				"billingMode":          "subscription",
				"afpMilliPerCharacter": ttsAFPMilliPerChar,
			},
			SupportedInputs: []string{"text", "speaker"},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemaVersion": "v1",
		"providerId":    s.config.ProviderID,
		"capabilities":  capabilities,
	})
}

func (s *Server) estimate(w http.ResponseWriter, r *http.Request) {
	var request providercontract.EstimateRequest
	if err := decodeJSON(r, &request); err != nil || request.Candidates < 1 ||
		(request.Capability != providercontract.CapabilityVideo &&
			request.Capability != providercontract.CapabilitySpeech) {
		writeError(w, http.StatusUnprocessableEntity, safeError(
			providercontract.CodeInvalidRequest, "a video or speech capability and positive candidate count are required", false,
		))
		return
	}
	if err := request.Model.Validate(request.Capability); err != nil {
		writeError(w, http.StatusUnprocessableEntity, safeError(
			providercontract.CodeInvalidRequest, "a valid frozen provider model route is required", false,
		))
		return
	}
	expectedModel := s.config.VideoModel
	if request.Capability == providercontract.CapabilitySpeech {
		expectedModel = s.config.SpeechModel
		if s.speech == nil {
			writeError(w, http.StatusServiceUnavailable, safeError(
				providercontract.CodeModelUnavailable, "Agent Plan TTS is not configured", false,
			))
			return
		}
	}
	if !acceptedProviderIdentity(request.Model.Provider) || request.Model.ModelID != expectedModel {
		writeError(w, http.StatusUnprocessableEntity, safeError(
			providercontract.CodeModelUnavailable, "the frozen model route is not configured by this adapter", false,
		))
		return
	}
	minimum := int64(request.Candidates)
	maximum := int64(request.Candidates) * 1_000_000
	unit := "video_tokens"
	if request.Capability == providercontract.CapabilitySpeech {
		minimum *= ttsAFPMilliPerChar
		maximum = int64(request.Candidates*AgentPlanTTSMaxChars) * ttsAFPMilliPerChar
		unit = "milli_afp"
	}
	writeJSON(w, http.StatusOK, providercontract.EstimateResponse{
		EstimateID:   "agent-plan-" + request.Model.CapabilityHash[:16],
		UnitsMinimum: minimum, UnitsMaximum: maximum, Unit: unit,
		PricingVersion: s.config.PricingVersion,
		ValidUntil:     s.now().UTC().Add(15 * time.Minute).Format(time.RFC3339),
	})
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var request providercontract.JobRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, safeError(providercontract.CodeInvalidRequest, "invalid job envelope", false))
		return
	}
	if err := s.validateJob(request, r.Header.Get("Idempotency-Key")); err != nil {
		writeProviderError(w, err)
		return
	}
	encoded, _ := json.Marshal(request)
	sum := sha256.Sum256(encoded)
	requestHash := hex.EncodeToString(sum[:])

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok, err := s.loadRecord(request.JobID)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	var intent *jobRecord
	authorizedRetry := false
	if ok {
		if existing.RequestHash != requestHash {
			writeError(w, http.StatusConflict, safeError(providercontract.CodeConflict, "jobId was already used for different input", false))
			return
		}
		if request.Capability == providercontract.CapabilitySpeech {
			recovered, handled, recoverErr := s.reconcileSpeechInspection(
				r.Context(), request.JobID, requestHash, existing,
			)
			if recoverErr != nil {
				writeProviderError(w, recoverErr)
				return
			}
			if handled {
				w.Header().Set("Idempotent-Replayed", "true")
				writeJSON(w, http.StatusOK, recovered)
				return
			}
		}
		if !s.authorizedSpeechRetry(request, existing) {
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, http.StatusOK, existing.Response)
			return
		}
		authorizedRetry = true
	}
	if !ok && request.Capability == providercontract.CapabilitySpeech {
		if err := s.validateSpeechBatchSequence(request); err != nil {
			writeProviderError(w, err)
			return
		}
	}
	upstreamRequest, err := s.resolveProviderAssets(request.Request)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	if authorizedRetry {
		intent, authorizedRetry, err = s.prepareSpeechRetry(request, requestHash)
		if err != nil {
			writeProviderError(w, err)
			return
		}
		if !authorizedRetry {
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, http.StatusOK, intent.Response)
			return
		}
	}

	if intent == nil {
		intent = &jobRecord{
			RequestHash: requestHash,
			Expected:    request.Request.Output,
			Response:    s.pendingResponse(request),
		}
		if err := s.createRecord(request.JobID, intent); err != nil {
			if errors.Is(err, errProviderJobIntentExists) {
				replayed, ok, loadErr := s.loadRecordFromDisk(request.JobID)
				if loadErr != nil {
					writeProviderError(w, loadErr)
					return
				}
				if !ok {
					writeProviderError(w, safeError(
						providercontract.CodeUnavailable,
						"live provider job intent is not yet durable",
						true,
					))
					return
				}
				if replayed.RequestHash != requestHash {
					writeError(w, http.StatusConflict, safeError(
						providercontract.CodeConflict,
						"jobId was already used for different input",
						false,
					))
					return
				}
				if request.Capability == providercontract.CapabilitySpeech {
					recovered, handled, recoverErr := s.reconcileSpeechInspection(
						r.Context(), request.JobID, requestHash, replayed,
					)
					if recoverErr != nil {
						writeProviderError(w, recoverErr)
						return
					}
					if handled {
						w.Header().Set("Idempotent-Replayed", "true")
						writeJSON(w, http.StatusOK, recovered)
						return
					}
				}
				w.Header().Set("Idempotent-Replayed", "true")
				writeJSON(w, http.StatusOK, replayed.Response)
				return
			}
			writeProviderError(w, err)
			return
		}
	}
	if request.Capability == providercontract.CapabilitySpeech {
		// A receipt can outlive a missing mutable registry file. Reconcile it
		// after publishing the replacement intent and before any Provider call.
		if recovered, handled, recoverErr := s.reconcileSpeechInspection(
			r.Context(), request.JobID, requestHash, intent,
		); recoverErr != nil {
			writeProviderError(w, recoverErr)
			return
		} else if handled {
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, http.StatusOK, recovered)
			return
		}
		response, inspection, err := s.synthesizeSpeech(r.Context(), request, requestHash)
		if inspection != nil {
			intent.Response = response
			intent.SpeechInspection = inspection
			if persistErr := s.updateRecord(request.JobID, intent); persistErr != nil {
				writeProviderError(w, persistErr)
				return
			}
		}
		if err != nil {
			response.Error = providerErrorOrGeneric(err)
			if response.Error.Retryable {
				response.State = providercontract.StatusUnknown
			} else {
				response.State = providercontract.StatusRequiresAction
			}
			if inspection == nil {
				intent.Response = response
				if persistErr := s.updateRecord(request.JobID, intent); persistErr != nil {
					writeProviderError(w, persistErr)
					return
				}
			}
			writeProviderError(w, err)
			return
		}
		recovered, handled, recoverErr := s.reconcileSpeechInspection(
			r.Context(), request.JobID, requestHash, intent,
		)
		if recoverErr != nil {
			writeProviderError(w, recoverErr)
			return
		}
		if !handled {
			writeProviderError(w, safeError(
				providercontract.CodeUnavailable,
				"speech inspection receipt is not durable",
				false,
			))
			return
		}
		if recovered.State != providercontract.StatusSucceeded {
			writeProviderError(w, recovered.Error)
			return
		}
		writeJSON(w, http.StatusCreated, recovered)
		return
	}
	upstream, err := s.provider.Submit(r.Context(), upstreamRequest)
	if err != nil {
		intent.Response.Error = providerErrorOrGeneric(err)
		if intent.Response.Error.Retryable {
			intent.Response.State = providercontract.StatusUnknown
		} else {
			intent.Response.State = providercontract.StatusRequiresAction
		}
		_ = s.updateRecord(request.JobID, intent)
		writeProviderError(w, err)
		return
	}
	response := providercontract.JobResponse{
		JobID: request.JobID, RunID: request.RunID,
		UpstreamTaskID: upstream.ID, RequestID: upstream.ProviderRequestID,
		ProviderRegion: s.config.Region,
		State:          upstream.Status, Model: request.Model,
		Cost: s.subscriptionCost(request),
	}
	if response.State == "" {
		response.State = providercontract.StatusUnknown
	}
	intent.Response = response
	if err := s.updateRecord(request.JobID, intent); err != nil {
		intent.Response = providercontract.JobResponse{
			JobID: request.JobID, RunID: request.RunID,
			State: providercontract.StatusRequiresAction, Model: request.Model,
			Cost:  s.subscriptionCost(request),
			Error: safeError(providercontract.CodeUnavailable, "provider submit result requires operator reconciliation", false),
		}
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *Server) authorizedSpeechRetry(request providercontract.JobRequest, record *jobRecord) bool {
	if request.Capability != providercontract.CapabilitySpeech || request.Request.Budget.MaxAttempts < 2 ||
		s.config.SpeechRetryJobID != request.JobID || !validSpeechReconciliationHistory(record) ||
		record.Response.State != providercontract.StatusRequiresAction || record.Response.Error == nil ||
		record.Response.UpstreamTaskID != "" || record.Response.RequestID != "" ||
		record.Response.ConnectID != "" || record.Response.LogID != "" ||
		len(record.Response.Artifacts) != 0 || record.Response.Usage != (providercontract.Usage{}) {
		return false
	}
	digest, err := persistedRecordSHA256(record)
	return err == nil && digest == s.config.SpeechRetryRecord
}

// prepareSpeechRetry turns a configured record hash into a durable, one-shot
// authorization. The claim is immutable and intentionally never removed: its
// existence means that authorization was consumed, including after a crash or
// an ambiguous persistence failure. The record is read from disk both before
// and after claiming so a process-local cache can never authorize a Provider
// call from stale state.
func (s *Server) prepareSpeechRetry(
	request providercontract.JobRequest,
	requestHash string,
) (*jobRecord, bool, error) {
	record, ok, err := s.loadRecordFromDisk(request.JobID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, safeError(providercontract.CodeUnavailable, "live provider job registry is unavailable", true)
	}
	if record.RequestHash != requestHash {
		return nil, false, safeError(providercontract.CodeConflict, "jobId was already used for different input", false)
	}
	if !s.authorizedSpeechRetry(request, record) {
		return record, false, nil
	}

	claimed, err := s.createSpeechRetryClaim(request.JobID, s.config.SpeechRetryRecord)
	if err != nil {
		return nil, false, err
	}
	if !claimed {
		return s.reloadSpeechRetryRecord(request.JobID)
	}

	// Re-read after the exclusive claim. This is the compare-and-set check:
	// only the claimant may persist the next attempt, and only if the claimed
	// SHA still describes the current durable record.
	record, ok, err = s.loadRecordFromDisk(request.JobID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, safeError(providercontract.CodeUnavailable, "live provider job registry is unavailable", true)
	}
	if record.RequestHash != requestHash {
		return nil, false, safeError(providercontract.CodeConflict, "jobId was already used for different input", false)
	}
	if !s.authorizedSpeechRetry(request, record) {
		return record, false, nil
	}
	record, err = s.beginSpeechRetry(request, record)
	if err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (s *Server) reloadSpeechRetryRecord(jobID string) (*jobRecord, bool, error) {
	record, ok, err := s.loadRecordFromDisk(jobID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, safeError(providercontract.CodeUnavailable, "live provider job registry is unavailable", true)
	}
	return record, false, nil
}

func validSpeechReconciliationHistory(record *jobRecord) bool {
	if len(record.Reconciliations) >= 2 {
		return false
	}
	for index, reconciliation := range record.Reconciliations {
		digest, err := hex.DecodeString(reconciliation.AuthorizedRecordSHA256)
		if reconciliation.Attempt != index+1 || err != nil || len(digest) != sha256.Size ||
			strings.ToLower(reconciliation.AuthorizedRecordSHA256) != reconciliation.AuthorizedRecordSHA256 ||
			reconciliation.StartedAt == "" ||
			reconciliation.PreviousResponse.JobID != record.Response.JobID ||
			reconciliation.PreviousResponse.RunID != record.Response.RunID ||
			reconciliation.PreviousResponse.State != providercontract.StatusRequiresAction ||
			reconciliation.PreviousResponse.Error == nil {
			return false
		}
		if _, err := time.Parse(time.RFC3339Nano, reconciliation.StartedAt); err != nil {
			return false
		}
	}
	return true
}

func (s *Server) beginSpeechRetry(
	request providercontract.JobRequest,
	record *jobRecord,
) (*jobRecord, error) {
	previous := record.Response
	previousReconciliationCount := len(record.Reconciliations)
	record.Reconciliations = append(record.Reconciliations, speechReconciliation{
		Attempt:                len(record.Reconciliations) + 1,
		StartedAt:              s.now().UTC().Format(time.RFC3339Nano),
		AuthorizedRecordSHA256: s.config.SpeechRetryRecord,
		PreviousResponse:       previous,
	})
	record.Response = s.pendingResponse(request)
	if err := s.updateRecord(request.JobID, record); err != nil {
		record.Response = previous
		record.Reconciliations = record.Reconciliations[:previousReconciliationCount]
		return nil, err
	}
	return record, nil
}

func (s *Server) pendingResponse(request providercontract.JobRequest) providercontract.JobResponse {
	return providercontract.JobResponse{
		JobID: request.JobID, RunID: request.RunID,
		ProviderRegion: s.config.Region,
		State:          providercontract.StatusUnknown, Model: request.Model,
		Cost: s.subscriptionCost(request),
	}
}

func persistedRecordSHA256(record *jobRecord) (string, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append(data, '\n'))
	return hex.EncodeToString(digest[:]), nil
}

// resolveProviderAssets keeps durable product truth on immutable CAS URIs but
// converts provider-facing visual inputs to transient data URLs. Private CAS
// paths are not reachable from Ark, while temporary signed URLs must never be
// persisted in PromptSnapshots, job records, logs, or evidence packages.
// Voice profile descriptors contribute to rights/consent and TTS lineage, but
// are metadata rather than audio accepted by the video model and therefore do
// not cross the video Provider boundary.
func (s *Server) resolveProviderAssets(
	request providercontract.GenerationRequest,
) (providercontract.GenerationRequest, error) {
	resolved := request
	resolved.Assets = make([]providercontract.AssetRef, 0, len(request.Assets))
	for _, asset := range request.Assets {
		mediaType, _, err := mime.ParseMediaType(asset.MediaType)
		if err != nil {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeInvalidRequest, SafeMessage: "provider input asset has an invalid media type",
			}
		}
		if mediaType == "audio/x-voice-profile+json" {
			if asset.Kind != providercontract.ModalityAudio || asset.Role != providercontract.AssetRoleReferenceAudio {
				return providercontract.GenerationRequest{}, &providercontract.Error{
					Code: providercontract.CodeInvalidRequest, SafeMessage: "provider voice descriptor has an invalid kind or role",
				}
			}
			continue
		}
		if asset.URI != "cas://sha256/"+asset.SHA256 {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeConflict, SafeMessage: "provider input CAS identity does not match its immutable digest",
			}
		}
		if !providerInputMediaType(mediaType, asset.Kind, asset.Role) {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeInvalidRequest, SafeMessage: "provider input CAS object has an unsupported media type",
			}
		}
		object, err := s.store.Open(asset.SHA256)
		if err != nil {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeUnavailable, SafeMessage: "provider input CAS object is unavailable", Retryable: true,
			}
		}
		content, readErr := io.ReadAll(io.LimitReader(object, maxResolvedProviderAssetBytes+1))
		closeErr := object.Close()
		if readErr != nil || closeErr != nil {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeUnavailable, SafeMessage: "provider input CAS object could not be read", Retryable: true,
			}
		}
		if len(content) == 0 || len(content) > maxResolvedProviderAssetBytes {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeInvalidRequest, SafeMessage: "provider input CAS object exceeds the supported size",
			}
		}
		if asset.SizeBytes <= 0 || asset.SizeBytes != int64(len(content)) {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeConflict, SafeMessage: "provider input CAS object size differs from its frozen metadata",
			}
		}
		digest := sha256.Sum256(content)
		if hex.EncodeToString(digest[:]) != asset.SHA256 {
			return providercontract.GenerationRequest{}, &providercontract.Error{
				Code: providercontract.CodeConflict, SafeMessage: "provider input CAS object failed its immutable digest check",
			}
		}
		if err := validateProviderInputMedia(content, mediaType); err != nil {
			return providercontract.GenerationRequest{}, err
		}
		asset.URI = "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(content)
		resolved.Assets = append(resolved.Assets, asset)
	}
	return resolved, nil
}

func providerInputMediaType(
	mediaType string,
	kind providercontract.Modality,
	role providercontract.AssetRole,
) bool {
	if kind != providercontract.ModalityImage {
		return false
	}
	if role != providercontract.AssetRoleReferenceImage &&
		role != providercontract.AssetRoleFirstFrame &&
		role != providercontract.AssetRoleLastFrame {
		return false
	}
	return mediaType == "image/png" || mediaType == "image/jpeg"
}

func validateProviderInputMedia(content []byte, declaredMediaType string) error {
	_, format, err := image.DecodeConfig(bytes.NewReader(content))
	if err != nil {
		return &providercontract.Error{
			Code: providercontract.CodeInvalidRequest, SafeMessage: "provider input CAS object is not a decodable supported image",
		}
	}
	actualMediaType := ""
	switch format {
	case "png":
		actualMediaType = "image/png"
	case "jpeg":
		actualMediaType = "image/jpeg"
	}
	if actualMediaType == "" || actualMediaType != declaredMediaType {
		return &providercontract.Error{
			Code: providercontract.CodeConflict, SafeMessage: "provider input CAS object media bytes differ from its frozen metadata",
		}
	}
	return nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID := r.PathValue("jobID")
	record, ok, err := s.loadRecord(jobID)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, safeError(providercontract.CodeNotFound, "job not found", false))
		return
	}
	if recovered, handled, recoverErr := s.reconcileSpeechInspection(
		r.Context(), jobID, record.RequestHash, record,
	); recoverErr != nil {
		writeProviderError(w, recoverErr)
		return
	} else if handled {
		writeJSON(w, http.StatusOK, recovered)
		return
	}
	if providercontract.Terminal(record.Response.State) &&
		(record.Response.State != providercontract.StatusSucceeded || len(record.Response.Artifacts) > 0) {
		writeJSON(w, http.StatusOK, record.Response)
		return
	}
	if record.Response.UpstreamTaskID == "" {
		record.Response.State = providercontract.StatusRequiresAction
		record.Response.Error = safeError(
			providercontract.CodeUnavailable,
			"provider submit outcome is ambiguous and requires operator reconciliation",
			false,
		)
		_ = s.updateRecord(jobID, record)
		writeJSON(w, http.StatusOK, record.Response)
		return
	}
	previous := record.Response
	upstream, err := s.provider.Poll(r.Context(), record.Response.UpstreamTaskID)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	if upstream.ProviderModel != "" && upstream.ProviderModel != record.Response.Model.ModelID {
		writeError(w, http.StatusUnprocessableEntity, safeError(
			providercontract.CodeModelUnavailable, "provider completed with a different model than the frozen route", false,
		))
		return
	}
	record.Response.State = upstream.Status
	record.Response.Progress = progressFor(upstream.Status)
	record.Response.Error = upstream.Error
	if upstream.Output != nil {
		record.Response.Usage = upstream.Output.Usage
	}
	if upstream.Status == providercontract.StatusSucceeded {
		artifacts, err := s.commitOutputs(r.Context(), upstream, record.Expected)
		if err != nil {
			writeProviderError(w, err)
			return
		}
		record.Response.Artifacts = artifacts
	}
	if err := s.updateRecord(jobID, record); err != nil {
		record.Response = previous
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record.Response)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID := r.PathValue("jobID")
	record, ok, err := s.loadRecord(jobID)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, safeError(providercontract.CodeNotFound, "job not found", false))
		return
	}
	if providercontract.Terminal(record.Response.State) {
		writeJSON(w, http.StatusOK, record.Response)
		return
	}
	if record.Response.UpstreamTaskID == "" {
		writeJSON(w, http.StatusOK, record.Response)
		return
	}
	previous := record.Response
	upstream, err := s.provider.Cancel(r.Context(), record.Response.UpstreamTaskID)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	record.Response.State = upstream.Status
	record.Response.Error = upstream.Error
	if upstream.Output != nil {
		record.Response.Usage = upstream.Output.Usage
	}
	if err := s.updateRecord(jobID, record); err != nil {
		record.Response = previous
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, record.Response)
}

func (s *Server) loadRecord(jobID string) (*jobRecord, bool, error) {
	record, ok, err := s.loadRecordFromDisk(jobID)
	if err != nil || !ok {
		return record, ok, err
	}
	result, resultOK, err := s.loadSpeechInspectionResult(jobID)
	if err != nil {
		return nil, false, err
	}
	if resultOK {
		receipt, receiptOK, receiptErr := s.loadSpeechInspectionReceipt(jobID)
		if receiptErr != nil {
			return nil, false, receiptErr
		}
		if result.RequestHash != record.RequestHash ||
			result.Duration.RequestedMillis != int64(record.Expected.DurationMillis) ||
			!receiptOK || validateSpeechInspectionResultBinding(result, receipt) != nil {
			return nil, false, safeError(
				providercontract.CodeUnavailable,
				"speech inspection result does not match the durable job intent",
				false,
			)
		}
		applySpeechInspectionResult(record, result)
	}
	return record, true, nil
}

func (s *Server) loadRecordFromDisk(jobID string) (*jobRecord, bool, error) {
	data, err := os.ReadFile(s.recordPath(jobID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, safeError(providercontract.CodeUnavailable, "live provider job registry is unavailable", true)
	}
	var record jobRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || record.Response.JobID != jobID || record.RequestHash == "" {
		return nil, false, safeError(providercontract.CodeUnavailable, "live provider job registry is invalid", false)
	}
	return &record, true, nil
}

func (s *Server) createSpeechRetryClaim(jobID, authorizedRecordSHA256 string) (bool, error) {
	claim := speechRetryClaim{
		SchemaVersion:          "v1",
		JobID:                  jobID,
		AuthorizedRecordSHA256: authorizedRecordSHA256,
		ClaimedAt:              s.now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(claim)
	if err != nil {
		return false, safeError(providercontract.CodeUnavailable, "speech reconciliation claim could not be encoded", false)
	}
	file, err := os.OpenFile(
		s.speechRetryClaimPath(jobID, authorizedRecordSHA256),
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, safeError(providercontract.CodeUnavailable, "speech reconciliation claim could not be committed", true)
	}
	// Never remove a partially written claim. Once O_EXCL succeeds the
	// authorization is consumed, so every later process must fail closed.
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, "speech reconciliation claim could not be committed", true)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, safeError(providercontract.CodeUnavailable, "speech reconciliation claim could not be committed", true)
	}
	if err := file.Close(); err != nil {
		return false, safeError(providercontract.CodeUnavailable, "speech reconciliation claim could not be committed", true)
	}
	if err := s.syncStateDirectory(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) createRecord(jobID string, record *jobRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be encoded", true)
	}
	file, err := os.CreateTemp(s.stateDir, ".provider-job-intent-*.tmp")
	if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	if err := file.Close(); err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	// Linking a completely written inode publishes the intent atomically while
	// preserving create-if-absent semantics across Adapter processes.
	if err := os.Link(temporaryPath, s.recordPath(jobID)); errors.Is(err, os.ErrExist) {
		return errProviderJobIntentExists
	} else if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job intent could not be committed", true)
	}
	return s.syncStateDirectory()
}

func (s *Server) updateRecord(jobID string, record *jobRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be encoded", true)
	}
	file, err := os.CreateTemp(s.stateDir, ".provider-job-*.tmp")
	if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	if err := file.Close(); err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	if err := os.Rename(tempPath, s.recordPath(jobID)); err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job state could not be committed", true)
	}
	return s.syncStateDirectory()
}

func (s *Server) recordPath(jobID string) string {
	sum := sha256.Sum256([]byte(jobID))
	return filepath.Join(s.stateDir, hex.EncodeToString(sum[:])+".json")
}

func (s *Server) speechRetryClaimPath(jobID, authorizedRecordSHA256 string) string {
	sum := sha256.Sum256([]byte(jobID))
	return filepath.Join(
		s.stateDir,
		hex.EncodeToString(sum[:])+"."+authorizedRecordSHA256+".speech-retry.claim",
	)
}

func (s *Server) syncStateDirectory() error {
	directory, err := os.Open(s.stateDir)
	if err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job registry could not be committed", true)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return safeError(providercontract.CodeUnavailable, "live provider job registry could not be committed", true)
	}
	if err := directory.Close(); err != nil {
		return safeError(providercontract.CodeUnavailable, "live provider job registry could not be committed", true)
	}
	return nil
}

func (s *Server) validateJob(request providercontract.JobRequest, idempotencyKey string) error {
	if err := request.Validate(); err != nil {
		return safeError(providercontract.CodeInvalidRequest, err.Error(), false)
	}
	if strings.TrimSpace(idempotencyKey) != request.JobID {
		return safeError(providercontract.CodeInvalidRequest, "Idempotency-Key must equal jobId", false)
	}
	if request.Simulation != "" {
		return safeError(providercontract.CodeInvalidRequest, "simulation is not accepted by a live adapter", false)
	}
	if request.Capability != providercontract.CapabilityVideo &&
		request.Capability != providercontract.CapabilitySpeech {
		return safeError(providercontract.CodeInvalidRequest, "the live adapter accepts only video.primary or speech.primary", false)
	}
	expectedModel := s.config.VideoModel
	if request.Capability == providercontract.CapabilitySpeech {
		expectedModel = s.config.SpeechModel
		if s.speech == nil {
			return safeError(providercontract.CodeModelUnavailable, "Agent Plan TTS is not configured", false)
		}
		chars := len([]rune(strings.TrimSpace(request.Request.Prompt)))
		if chars < 1 || chars > AgentPlanTTSMaxChars || strings.TrimSpace(request.Request.ModelHint) == "" {
			return safeError(providercontract.CodeInvalidRequest, "speech.primary requires a speaker and at most 600 Unicode characters", false)
		}
		if request.Request.ModelHint != s.config.SpeechSpeaker ||
			request.Model.RouteVersion != AgentPlanTTSRouteVersion ||
			request.Model.CapabilityHash != AgentPlanTTSCapabilityHash(s.config) {
			return safeError(providercontract.CodeModelUnavailable, "the frozen Agent Plan TTS voice route is not configured by this adapter", false)
		}
		if err := s.validateSpeechCanary(request); err != nil {
			return err
		}
	} else if request.Request.ModelHint != s.config.VideoModel {
		return safeError(providercontract.CodeModelUnavailable, "the frozen model route is not configured by this adapter", false)
	}
	if !acceptedProviderIdentity(request.Model.Provider) || request.Model.ModelID != expectedModel {
		return safeError(providercontract.CodeModelUnavailable, "the frozen model route is not configured by this adapter", false)
	}
	return nil
}

func (s *Server) validateSpeechCanary(request providercontract.JobRequest) error {
	if s.config.SpeechBatchAuthorization != nil {
		return s.validateSpeechBatch(request)
	}
	if !strings.HasPrefix(request.JobID, "speech-v2-") && !speechCanaryConfigured(s.config) {
		return nil
	}
	if !completeSpeechCanaryConfig(s.config) {
		return speechCanaryViolation()
	}
	if request.JobID != s.config.SpeechCanaryJobID ||
		request.InputHash != s.config.SpeechCanaryInputHash ||
		request.Request.RequestID != s.config.SpeechCanaryJobID ||
		request.Request.IdempotencyKey != s.config.SpeechCanaryJobID ||
		request.Request.PromptSnapshotID == "" ||
		!strings.HasSuffix(request.Request.PromptSnapshotID, ":"+s.config.SpeechCanaryCueID) ||
		request.Request.Budget.MaxAttempts != 1 ||
		request.Request.Output.Format != "mp3" ||
		s.config.SpeechCanaryMaximumCashMicros != 0 {
		return speechCanaryViolation()
	}
	predictedAFPMilli := int64(len([]rune(strings.TrimSpace(request.Request.Prompt)))) * ttsAFPMilliPerChar
	if predictedAFPMilli <= 0 || predictedAFPMilli > s.config.SpeechCanaryMaximumAFPMilli {
		return speechCanaryViolation()
	}
	if len(request.Request.Assets) != 1 {
		return speechCanaryViolation()
	}
	voice := request.Request.Assets[0]
	if voice.ID != s.config.SpeechCanaryVoiceAssetID ||
		voice.Revision != s.config.SpeechCanaryVoiceVersion ||
		voice.SHA256 != s.config.SpeechCanaryVoiceHash ||
		voice.URI != "cas://sha256/"+s.config.SpeechCanaryVoiceHash ||
		voice.Kind != providercontract.ModalityAudio ||
		voice.Role != providercontract.AssetRoleReferenceAudio ||
		voice.MediaType != "audio/x-voice-profile+json" ||
		voice.LicenseReference != s.config.SpeechCanaryLicenseSnapshotID+":"+
			s.config.SpeechCanaryLicenseHash {
		return speechCanaryViolation()
	}
	if err := s.validateSpeechCanaryVoiceDescriptor(voice); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateSpeechBatch(request providercontract.JobRequest) error {
	batch := s.config.SpeechBatchAuthorization
	if batch == nil || batch.Validate() != nil {
		return speechBatchViolation()
	}
	validUntil, err := time.Parse(time.RFC3339, batch.ValidUntil)
	if err != nil || !s.now().UTC().Before(validUntil) {
		return safeError(providercontract.CodeForbidden, "speech batch authorization expired", false)
	}
	var authorization *speechcontract.CueAuthorization
	for index := range batch.Cues {
		if batch.Cues[index].JobID == request.JobID {
			authorization = &batch.Cues[index]
			break
		}
	}
	if authorization == nil || request.InputHash != authorization.InputHash ||
		request.Request.RequestID != authorization.JobID ||
		request.Request.IdempotencyKey != authorization.JobID ||
		request.Request.PromptSnapshotID == "" ||
		!strings.HasSuffix(request.Request.PromptSnapshotID, ":"+authorization.CueID) ||
		request.Request.Budget.MaxAttempts != authorization.MaxAttempts ||
		request.Request.Output.Format != "mp3" ||
		batch.MaximumNonSubscriptionCashMicros != 0 {
		return speechBatchViolation()
	}
	characters := len([]rune(strings.TrimSpace(request.Request.Prompt)))
	predictedAFPMilli := int64(characters) * ttsAFPMilliPerChar
	if characters != authorization.UnicodeCharacters ||
		predictedAFPMilli != authorization.EstimatedAFPMilli ||
		predictedAFPMilli > authorization.MaximumAFPMilli {
		return speechBatchViolation()
	}
	if len(request.Request.Assets) != 1 {
		return speechBatchViolation()
	}
	voice := request.Request.Assets[0]
	if voice.ID != batch.VoiceAssetID || voice.Revision != batch.VoiceAssetVersionID ||
		voice.SHA256 != batch.VoiceAssetVersionHash ||
		voice.URI != "cas://sha256/"+batch.VoiceAssetVersionHash ||
		voice.Kind != providercontract.ModalityAudio ||
		voice.Role != providercontract.AssetRoleReferenceAudio ||
		voice.MediaType != "audio/x-voice-profile+json" ||
		voice.LicenseReference != batch.LicenseSnapshotID+":"+batch.LicenseSnapshotHash {
		return speechBatchViolation()
	}
	return s.validateSpeechBatchVoiceDescriptor(voice, *batch)
}

func (s *Server) validateSpeechBatchVoiceDescriptor(
	voice providercontract.AssetRef,
	batch speechcontract.BatchAuthorization,
) error {
	object, err := s.store.Open(voice.SHA256)
	if err != nil {
		return speechBatchViolation()
	}
	content, readErr := io.ReadAll(io.LimitReader(object, maxSpeechVoiceDescriptorBytes+1))
	closeErr := object.Close()
	if readErr != nil || closeErr != nil || len(content) == 0 || len(content) > maxSpeechVoiceDescriptorBytes {
		return speechBatchViolation()
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != voice.SHA256 ||
		(voice.SizeBytes > 0 && voice.SizeBytes != int64(len(content))) {
		return speechBatchViolation()
	}
	var descriptor speechVoiceDescriptor
	if err := json.Unmarshal(content, &descriptor); err != nil ||
		descriptor.SchemaVersion != "v2" || descriptor.Provider != batch.Provider ||
		descriptor.ModelID != batch.ModelID || descriptor.ResourceID != batch.ResourceID ||
		descriptor.Speaker != batch.Speaker || descriptor.RouteVersion != batch.RouteVersion ||
		descriptor.VoiceClone == nil || *descriptor.VoiceClone ||
		descriptor.InternalMVPOnly == nil || !*descriptor.InternalMVPOnly ||
		descriptor.ParentAssetVersionID != batch.ParentVoiceAssetVersionID ||
		descriptor.AssetVersionID != batch.VoiceAssetVersionID ||
		descriptor.LicenseSnapshotID != batch.LicenseSnapshotID ||
		!lowercaseSHA256(descriptor.RevisionInputHash) {
		return speechBatchViolation()
	}
	return nil
}

func (s *Server) validateSpeechBatchSequence(request providercontract.JobRequest) error {
	batch := s.config.SpeechBatchAuthorization
	if batch == nil {
		return nil
	}
	current := -1
	for index, cue := range batch.Cues {
		if cue.JobID == request.JobID {
			current = index
			break
		}
	}
	if current < 0 {
		return speechBatchViolation()
	}
	var consumed int64
	for index := 0; index < current; index++ {
		prior := batch.Cues[index]
		record, found, err := s.loadRecord(prior.JobID)
		if err != nil {
			return err
		}
		if !found || record.Response.State != providercontract.StatusSucceeded ||
			record.Response.JobID != prior.JobID || record.Response.Usage.Unit != "milli_afp" ||
			strings.TrimSpace(record.Response.RequestID) == "" ||
			strings.TrimSpace(record.Response.ConnectID) == "" ||
			strings.TrimSpace(record.Response.LogID) == "" ||
			record.Response.Usage.OutputUnits <= 0 ||
			record.Response.Usage.OutputUnits > prior.MaximumAFPMilli ||
			record.Response.Cost.ActualMicros != nil || record.Response.Cost.Verified ||
			record.Response.Cost.BillingMode != "subscription" {
			return safeError(providercontract.CodeConflict, "speech batch is not ready for this ordered cue", false)
		}
		consumed += record.Response.Usage.OutputUnits
	}
	if consumed+batch.Cues[current].EstimatedAFPMilli > batch.MaximumAFPMilli {
		return safeError(providercontract.CodeBudgetExceeded, "speech batch cumulative AFP ceiling would be exceeded", false)
	}
	return nil
}

func (s *Server) validateSpeechCanaryVoiceDescriptor(voice providercontract.AssetRef) error {
	object, err := s.store.Open(voice.SHA256)
	if err != nil {
		return speechCanaryViolation()
	}
	content, readErr := io.ReadAll(io.LimitReader(object, maxSpeechVoiceDescriptorBytes+1))
	closeErr := object.Close()
	if readErr != nil || closeErr != nil || len(content) == 0 || len(content) > maxSpeechVoiceDescriptorBytes {
		return speechCanaryViolation()
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != voice.SHA256 ||
		(voice.SizeBytes > 0 && voice.SizeBytes != int64(len(content))) {
		return speechCanaryViolation()
	}
	var descriptor speechVoiceDescriptor
	if err := json.Unmarshal(content, &descriptor); err != nil {
		return speechCanaryViolation()
	}
	if descriptor.SchemaVersion != "v2" ||
		descriptor.Provider != "volcengine_ark" ||
		descriptor.ModelID != s.config.SpeechModel ||
		descriptor.ResourceID != AgentPlanTTSResourceID ||
		descriptor.Speaker != s.config.SpeechSpeaker ||
		descriptor.RouteVersion != AgentPlanTTSRouteVersion ||
		descriptor.VoiceClone == nil || *descriptor.VoiceClone ||
		descriptor.InternalMVPOnly == nil || !*descriptor.InternalMVPOnly ||
		descriptor.ParentAssetVersionID != s.config.SpeechCanaryParentVoiceVersion ||
		descriptor.AssetVersionID != s.config.SpeechCanaryVoiceVersion ||
		descriptor.LicenseSnapshotID != s.config.SpeechCanaryLicenseSnapshotID ||
		!lowercaseSHA256(descriptor.RevisionInputHash) {
		return speechCanaryViolation()
	}
	return nil
}

func lowercaseSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func speechCanaryConfigured(config runtimeconfig.VolcengineProvider) bool {
	if config.SpeechCanaryMaximumAFPMilli != 0 || config.SpeechCanaryMaximumCashMicros != 0 {
		return true
	}
	for _, value := range []string{
		config.SpeechCanaryJobID,
		config.SpeechCanaryInputHash,
		config.SpeechCanaryCueID,
		config.SpeechCanaryVoiceAssetID,
		config.SpeechCanaryParentVoiceVersion,
		config.SpeechCanaryVoiceVersion,
		config.SpeechCanaryVoiceHash,
		config.SpeechCanaryLicenseSnapshotID,
		config.SpeechCanaryLicenseHash,
	} {
		if value != "" {
			return true
		}
	}
	return false
}

func completeSpeechCanaryConfig(config runtimeconfig.VolcengineProvider) bool {
	if config.SpeechCanaryMaximumAFPMilli <= 0 || config.SpeechCanaryMaximumCashMicros != 0 {
		return false
	}
	for _, value := range []string{
		config.SpeechCanaryJobID,
		config.SpeechCanaryInputHash,
		config.SpeechCanaryCueID,
		config.SpeechCanaryVoiceAssetID,
		config.SpeechCanaryParentVoiceVersion,
		config.SpeechCanaryVoiceVersion,
		config.SpeechCanaryVoiceHash,
		config.SpeechCanaryLicenseSnapshotID,
		config.SpeechCanaryLicenseHash,
	} {
		if value == "" {
			return false
		}
	}
	return true
}

func speechCanaryViolation() *providercontract.Error {
	return safeError(
		providercontract.CodeInvalidRequest,
		"speech job is outside the frozen single-call canary",
		false,
	)
}

func speechBatchViolation() *providercontract.Error {
	return safeError(
		providercontract.CodeInvalidRequest,
		"speech job is outside the frozen ordered batch authorization",
		false,
	)
}

func acceptedProviderIdentity(provider string) bool {
	return provider == "volcengine_ark" || provider == "VOLCENGINE"
}

func (s *Server) synthesizeSpeech(
	ctx context.Context,
	request providercontract.JobRequest,
	requestHash string,
) (providercontract.JobResponse, *speechInspectionCheckpoint, error) {
	response := providercontract.JobResponse{
		JobID: request.JobID, RunID: request.RunID,
		State: providercontract.StatusUnknown, Model: request.Model,
		Cost: s.subscriptionCost(request),
	}
	result, err := s.speech.Synthesize(ctx, SpeechSynthesisRequest{
		Text: request.Request.Prompt, Speaker: request.Request.ModelHint,
	})
	response.RequestID = result.RequestID
	response.ConnectID = result.ConnectID
	response.LogID = result.LogID
	if result.UsageTokens > 0 {
		response.Usage, _ = TTSUsageAttributes(result.UsageTokens)
	}
	if err != nil {
		return response, nil, err
	}
	if strings.TrimSpace(result.RequestID) == "" || strings.TrimSpace(result.ConnectID) == "" ||
		strings.TrimSpace(result.LogID) == "" {
		return response, nil, safeError(providercontract.CodeUnavailable, "Agent Plan TTS trace evidence is incomplete", false)
	}
	usage, err := TTSUsageAttributes(result.UsageTokens)
	if err != nil {
		return response, nil, safeError(providercontract.CodeUnavailable, "Agent Plan TTS usage evidence is invalid", false)
	}
	if err := s.validateSpeechBatchUsage(request, usage); err != nil {
		return response, nil, err
	}
	if len(result.Audio) == 0 {
		return response, nil, safeError(providercontract.CodeUnavailable, "Agent Plan TTS returned no audio bytes", false)
	}
	artifact := provisionalSpeechArtifact(request, result.Audio, result.MediaType)
	response = providercontract.JobResponse{
		JobID: request.JobID, RunID: request.RunID,
		UpstreamTaskID: result.ConnectID, RequestID: result.RequestID,
		ConnectID: result.ConnectID, LogID: result.LogID,
		State: providercontract.StatusRequiresAction, Model: request.Model,
		Usage: usage, Cost: s.subscriptionCost(request),
		Error: speechInspectionError(),
	}
	inspection := &speechInspectionCheckpoint{
		State: speechInspectionPending, Artifact: artifact,
	}
	receipt := speechInspectionReceipt{
		SchemaVersion: speechInspectionReceiptSchema,
		RequestHash:   requestHash,
		Response:      response,
		Artifact:      artifact,
	}
	// Publish the job-to-digest receipt before committing bytes. A crash can
	// therefore leave a visible missing-object condition, but never an
	// untraceable paid CAS orphan.
	if err := s.createSpeechInspectionReceipt(request.JobID, receipt); err != nil {
		return response, nil, err
	}
	committed, err := s.store.Put(ctx, bytes.NewReader(result.Audio))
	if err != nil {
		return response, inspection, speechInspectionError()
	}
	if committed.Digest != artifact.SHA256 || committed.URI != artifact.URI || committed.Size != artifact.SizeBytes {
		return response, inspection, speechInspectionError()
	}
	return response, inspection, nil
}

func (s *Server) validateSpeechBatchUsage(
	request providercontract.JobRequest,
	usage providercontract.Usage,
) error {
	batch := s.config.SpeechBatchAuthorization
	if batch == nil {
		return nil
	}
	current := -1
	for index, cue := range batch.Cues {
		if cue.JobID == request.JobID {
			current = index
			if usage.OutputUnits > cue.MaximumAFPMilli {
				return safeError(providercontract.CodeBudgetExceeded, "speech cue exceeded its AFP ceiling", false)
			}
			break
		}
	}
	if current < 0 {
		return speechBatchViolation()
	}
	consumed := usage.OutputUnits
	for index := 0; index < current; index++ {
		record, ok, err := s.loadRecord(batch.Cues[index].JobID)
		if err != nil || !ok || record.Response.State != providercontract.StatusSucceeded {
			return safeError(providercontract.CodeConflict, "speech batch prior usage is unavailable", false)
		}
		consumed += record.Response.Usage.OutputUnits
	}
	if consumed > batch.MaximumAFPMilli {
		return safeError(providercontract.CodeBudgetExceeded, "speech batch exceeded its cumulative AFP ceiling", false)
	}
	return nil
}

func newSpeechDurationQC(requested, measured int64) *speechDurationQC {
	delta := measured - requested
	if delta < 0 {
		delta = -delta
	}
	state := "passed"
	if requested <= 0 || measured <= 0 || delta > speechDurationToleranceMillis {
		state = "requires_adjustment"
	}
	return &speechDurationQC{
		RequestedMillis: requested,
		MeasuredMillis:  measured,
		DeltaMillis:     delta,
		ToleranceMillis: speechDurationToleranceMillis,
		State:           state,
	}
}

func validateMeasuredSpeech(measured MediaSpec, expected providercontract.OutputSpec) error {
	if measured.DurationMillis <= 0 || measured.Width != 0 || measured.Height != 0 || measured.FPS != 0 {
		return errors.New("speech media inspection did not return an audio-only duration")
	}
	if expected.Format != "" && measured.Format != expected.Format {
		return errors.New("speech media format differs from the frozen output specification")
	}
	return nil
}

func (s *Server) subscriptionCost(request providercontract.JobRequest) providercontract.Cost {
	return providercontract.Cost{
		EstimatedMicros:  0,
		ActualMicros:     nil,
		Currency:         "",
		PricingVersion:   request.BudgetReservation.PricingVersion,
		Verified:         false,
		BillingMode:      "subscription",
		ProviderReported: false,
	}
}

func (s *Server) commitOutputs(
	ctx context.Context,
	job providercontract.Job,
	expected providercontract.OutputSpec,
) ([]providercontract.AssetRef, error) {
	if job.Output == nil || len(job.Output.Assets) == 0 {
		return nil, safeError(providercontract.CodeUnavailable, "provider succeeded without a downloadable artifact", true)
	}
	result := make([]providercontract.AssetRef, 0, len(job.Output.Assets))
	for _, source := range job.Output.Assets {
		artifact, mediaType, err := s.download(ctx, source)
		if err != nil {
			return nil, err
		}
		committed := source
		committed.URI = artifact.URI
		committed.SHA256 = artifact.Digest
		committed.Revision = artifact.Digest
		committed.MediaType = mediaType
		committed.SizeBytes = artifact.Size
		if source.Kind == providercontract.ModalityVideo {
			spec, err := s.inspector.Inspect(ctx, artifact.Path)
			if err != nil {
				return nil, safeError(providercontract.CodeUnavailable, "downloaded provider video failed media inspection", true)
			}
			if err := validateMeasuredVideo(spec, job.Output.Actual, expected); err != nil {
				return nil, err
			}
			committed.Width = spec.Width
			committed.Height = spec.Height
			committed.FPS = spec.FPS
			committed.DurationMillis = spec.DurationMillis
		}
		result = append(result, committed)
	}
	return result, nil
}

func (s *Server) download(ctx context.Context, source providercontract.AssetRef) (artifactstore.Artifact, string, error) {
	parsed, err := url.Parse(source.URI)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider returned an invalid artifact URL", true)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URI, nil)
	if err != nil {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider artifact download could not be created", true)
	}
	response, err := s.downloadClient.Do(request)
	if err != nil {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider artifact download failed", true)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider artifact download was not successful", true)
	}
	mediaType, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if source.Kind == providercontract.ModalityVideo && mediaType != "video/mp4" && mediaType != "application/octet-stream" {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider artifact has an unexpected media type", true)
	}
	artifact, err := s.store.Put(ctx, &enforcingReader{reader: response.Body, remaining: s.config.MaxDownloadBytes})
	if err != nil {
		return artifactstore.Artifact{}, "", safeError(providercontract.CodeUnavailable, "provider artifact could not be committed to CAS", true)
	}
	if mediaType == "" || mediaType == "application/octet-stream" {
		if source.Kind == providercontract.ModalityVideo {
			mediaType = "video/mp4"
		} else if source.Kind == providercontract.ModalityImage {
			mediaType = "image/jpeg"
		}
	}
	return artifact, mediaType, nil
}

func validateMeasuredVideo(
	measured MediaSpec,
	reported providercontract.OutputSpec,
	expected providercontract.OutputSpec,
) error {
	if measured.Width <= 0 || measured.Height <= 0 || measured.FPS <= 0 || measured.DurationMillis <= 0 {
		return safeError(providercontract.CodeUnavailable, "downloaded provider video has incomplete media metadata", true)
	}
	if reported.FPS > 0 && measured.FPS != reported.FPS {
		return safeError(providercontract.CodeUnavailable, "downloaded provider video FPS differs from provider metadata", true)
	}
	if expected.Width > 0 && measured.Width != expected.Width ||
		expected.Height > 0 && measured.Height != expected.Height ||
		expected.FPS > 0 && measured.FPS != expected.FPS {
		return safeError(providercontract.CodeUnavailable, "downloaded provider video differs from the frozen output specification", true)
	}
	if expected.DurationMillis > 0 {
		delta := measured.DurationMillis - int64(expected.DurationMillis)
		if delta < 0 {
			delta = -delta
		}
		if delta > 500 {
			return safeError(providercontract.CodeUnavailable, "downloaded provider video duration differs from the frozen output specification", true)
		}
	}
	if reported.DurationMillis > 0 {
		delta := measured.DurationMillis - int64(reported.DurationMillis)
		if delta < 0 {
			delta = -delta
		}
		if delta > 500 {
			return safeError(providercontract.CodeUnavailable, "downloaded provider video duration differs from provider metadata", true)
		}
	}
	return nil
}

type enforcingReader struct {
	reader    io.Reader
	remaining int64
}

func (r *enforcingReader) Read(buffer []byte) (int, error) {
	if r.remaining <= 0 {
		var extra [1]byte
		n, err := r.reader.Read(extra[:])
		if n > 0 {
			return 0, errors.New("artifact exceeds configured size limit")
		}
		return 0, err
	}
	if int64(len(buffer)) > r.remaining {
		buffer = buffer[:r.remaining]
	}
	n, err := r.reader.Read(buffer)
	r.remaining -= int64(n)
	return n, err
}

func progressFor(status providercontract.JobStatus) int {
	switch status {
	case providercontract.StatusQueued:
		return 0
	case providercontract.StatusRunning:
		return 50
	case providercontract.StatusSucceeded:
		return 100
	default:
		return 0
	}
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, providerErr *providercontract.Error) {
	writeJSON(w, status, map[string]any{"error": providerErr})
}

func writeProviderError(w http.ResponseWriter, err error) {
	providerErr := providerErrorOrGeneric(err)
	writeError(w, statusFor(providerErr.Code), providerErr)
}

func providerErrorOrGeneric(err error) *providercontract.Error {
	var providerErr *providercontract.Error
	if errors.As(err, &providerErr) {
		return providerErr
	}
	return safeError(providercontract.CodeUnavailable, "live provider adapter is temporarily unavailable", true)
}

func safeError(code providercontract.ErrorCode, message string, retryable bool) *providercontract.Error {
	return &providercontract.Error{
		Code: code, SafeMessage: message, Retryable: retryable,
		RequiresAction:  !retryable,
		SuggestedAction: "inspect the sanitized provider status before retrying",
	}
}

func statusFor(code providercontract.ErrorCode) int {
	switch code {
	case providercontract.CodeUnauthenticated:
		return http.StatusUnauthorized
	case providercontract.CodeForbidden:
		return http.StatusForbidden
	case providercontract.CodeNotFound:
		return http.StatusNotFound
	case providercontract.CodeConflict:
		return http.StatusConflict
	case providercontract.CodeRateLimited:
		return http.StatusTooManyRequests
	case providercontract.CodeQuotaExceeded:
		return http.StatusPaymentRequired
	case providercontract.CodeUnavailable, providercontract.CodeTimeout:
		return http.StatusServiceUnavailable
	case providercontract.CodeInvalidRequest, providercontract.CodeBudgetExceeded,
		providercontract.CodeContentBlocked, providercontract.CodeRegionUnavailable,
		providercontract.CodeModelUnavailable:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

// MarshalRequestHash is kept small and deterministic for adapter recovery
// tests. It deliberately returns only a digest, never request content.
func MarshalRequestHash(request providercontract.JobRequest) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode request: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
