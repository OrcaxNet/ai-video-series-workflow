// Package liveprobe runs one bounded provider pipeline probe through the same
// provider-neutral adapter contract used by Temporal Activities.
package liveprobe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/volcengineprovider"
)

const (
	defaultPrompt = "原创抽象玻璃流体在深蓝背景中缓慢旋转，柔和体积光，固定镜头，无人物、无文字、无品牌"
	modelID       = "doubao-seedance-2.0"
)

type Config struct {
	AdapterURL        string
	ServiceAuthSecret string
	ArtifactRoot      string
	OutputDir         string
	BuildVersion      string
	Region            string
	PlanName          string
	Prompt            string
	PollInterval      time.Duration
	Timeout           time.Duration
	HTTPClient        *http.Client
	Now               func() time.Time
}

type MeasuredOutput struct {
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	FPS            int    `json:"fps"`
	DurationMillis int64  `json:"duration_millis"`
	Format         string `json:"format"`
	MediaType      string `json:"media_type"`
}

type Result struct {
	SchemaVersion  string                 `json:"schema_version"`
	Evidence       string                 `json:"evidence"`
	RunID          string                 `json:"run_id"`
	JobID          string                 `json:"job_id"`
	TaskID         string                 `json:"provider_task_id"`
	RequestID      string                 `json:"provider_request_id"`
	Provider       string                 `json:"provider"`
	Model          string                 `json:"model"`
	Region         string                 `json:"region"`
	Plan           string                 `json:"plan"`
	StartedAt      time.Time              `json:"started_at"`
	CompletedAt    time.Time              `json:"completed_at"`
	LatencyMillis  int64                  `json:"latency_millis"`
	ArtifactSHA256 string                 `json:"artifact_sha256"`
	ArtifactBytes  int64                  `json:"artifact_bytes"`
	Output         MeasuredOutput         `json:"actual_output"`
	Artifacts      []EvidenceArtifact     `json:"artifacts"`
	Usage          providercontract.Usage `json:"usage"`
	Cost           providercontract.Cost  `json:"cost"`
	ManifestSHA256 string                 `json:"manifest_sha256"`
	ServiceBOMHash string                 `json:"service_bom_sha256"`
	Files          map[string]string      `json:"files"`
	Redaction      RedactionEvidence      `json:"redaction"`
}

type EvidenceArtifact struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	Role      string          `json:"role"`
	File      string          `json:"file"`
	SHA256    string          `json:"sha256"`
	Bytes     int64           `json:"bytes"`
	MediaType string          `json:"media_type"`
	Output    *MeasuredOutput `json:"actual_output,omitempty"`
}

type RedactionEvidence struct {
	CredentialPersisted bool `json:"credential_persisted"`
	SignedURLPersisted  bool `json:"signed_url_persisted"`
	PromptPersisted     bool `json:"prompt_persisted"`
}

type ServiceBOM struct {
	SchemaVersion string `json:"schema_version"`
	Evidence      string `json:"evidence"`
	Component     struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"component"`
	Provider struct {
		Name      string `json:"name"`
		Model     string `json:"model"`
		Region    string `json:"region"`
		Plan      string `json:"plan"`
		TaskID    string `json:"task_id"`
		RequestID string `json:"request_id"`
	} `json:"provider"`
	Artifact struct {
		SHA256 string         `json:"sha256"`
		Bytes  int64          `json:"bytes"`
		Output MeasuredOutput `json:"actual_output"`
	} `json:"artifact"`
	Artifacts []EvidenceArtifact     `json:"artifacts"`
	Usage     providercontract.Usage `json:"usage"`
	Cost      providercontract.Cost  `json:"cost"`
	CostNote  string                 `json:"cost_note"`
	Manifest  struct {
		SHA256 string `json:"sha256"`
	} `json:"generation_manifest"`
	Security struct {
		CredentialPersisted bool `json:"credential_persisted"`
		SignedURLPersisted  bool `json:"signed_url_persisted"`
		PromptPersisted     bool `json:"prompt_persisted"`
	} `json:"security"`
	GeneratedAt time.Time `json:"generated_at"`
}

func Run(ctx context.Context, config Config) (Result, error) {
	if err := validateConfig(&config); err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(config.OutputDir, 0o750); err != nil {
		return Result{}, fmt.Errorf("create probe output directory: %w", err)
	}
	runID := "live-probe-" + uuid.NewString()
	jobID := "provider-job-" + runID
	lockPath := filepath.Join(config.OutputDir, ".single-submit.lock")
	if err := writeExclusive(lockPath, []byte(jobID+"\n")); err != nil {
		return Result{}, fmt.Errorf("refuse a second live submission in this output directory: %w", err)
	}

	request, err := buildRequest(runID, jobID, config)
	if err != nil {
		return Result{}, err
	}
	startedAt := config.Now().UTC()
	runCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	authenticatedClient, err := volcengineprovider.AuthenticatedHTTPClient(config.HTTPClient, config.ServiceAuthSecret)
	if err != nil {
		return Result{}, fmt.Errorf("configure live adapter service authentication: %w", err)
	}
	response, err := mockprovider.Submit(runCtx, authenticatedClient, config.AdapterURL, request)
	if err != nil {
		return Result{}, fmt.Errorf("submit single live provider job: %w", err)
	}
	for !providercontract.Terminal(response.State) {
		if response.State == providercontract.StatusRequiresAction {
			if response.Error != nil {
				return Result{}, response.Error
			}
			return Result{}, errors.New("single live provider job requires operator reconciliation")
		}
		if err := sleep(runCtx, config.PollInterval); err != nil {
			return Result{}, fmt.Errorf("wait for single live provider job: %w", err)
		}
		response, err = mockprovider.Get(runCtx, authenticatedClient, config.AdapterURL, jobID)
		if err != nil {
			return Result{}, fmt.Errorf("poll single live provider job: %w", err)
		}
	}
	if response.State != providercontract.StatusSucceeded || len(response.Artifacts) == 0 {
		if response.Error != nil {
			return Result{}, response.Error
		}
		return Result{}, errors.New("single live provider job ended without an artifact")
	}
	if response.UpstreamTaskID == "" || response.RequestID == "" {
		return Result{}, errors.New("live provider response lacks task or request provenance")
	}
	video, err := selectVideo(response.Artifacts)
	if err != nil {
		return Result{}, err
	}
	completedAt := config.Now().UTC()
	if !completedAt.After(startedAt) {
		completedAt = startedAt.Add(time.Millisecond)
	}
	actual := providercontract.OutputSpec{
		Width: video.Width, Height: video.Height,
		Resolution: resolutionFor(video.Height), AspectRatio: aspectRatio(video.Width, video.Height),
		FPS: video.FPS, DurationMillis: int(video.DurationMillis), Format: "mp4",
	}
	providerJob := providercontract.Job{
		ID: response.UpstreamTaskID, RequestID: request.Request.RequestID,
		IdempotencyKey: request.JobID, Status: response.State,
		Provider: "volcengine_ark", ProviderModel: response.Model.ModelID,
		ProviderRegion: config.Region, ProviderRequestID: response.RequestID,
		CreatedAt: startedAt, UpdatedAt: completedAt,
		Output: &providercontract.Output{Assets: response.Artifacts, Actual: actual, Usage: response.Usage},
	}
	manifest, err := providercontract.NewGenerationManifest(providercontract.ManifestBuildInput{
		ManifestID: "manifest-" + runID,
		ShotID:     "shot-" + runID,
		Evidence:   providercontract.EvidenceLiveProvider,
		Request:    request.Request, Job: providerJob, Attempt: 1,
		StartedAt: startedAt, CompletedAt: completedAt,
		ModelSnapshot: &response.Model,
	})
	if err != nil {
		return Result{}, fmt.Errorf("build live Generation Manifest: %w", err)
	}
	manifestPath := filepath.Join(config.OutputDir, "generation-manifest.json")
	manifestHash, err := providercontract.WriteGenerationManifest(manifestPath, manifest)
	if err != nil {
		return Result{}, fmt.Errorf("write live Generation Manifest: %w", err)
	}

	store, err := artifactstore.New(config.ArtifactRoot)
	if err != nil {
		return Result{}, err
	}
	output := MeasuredOutput{
		Width: video.Width, Height: video.Height, FPS: video.FPS,
		DurationMillis: video.DurationMillis, Format: "mp4", MediaType: video.MediaType,
	}
	evidenceArtifacts, artifactFiles, err := exportArtifacts(store, config.OutputDir, response.Artifacts, video, output)
	if err != nil {
		return Result{}, err
	}
	bom := buildBOM(config, response, video, output, evidenceArtifacts, manifestHash, completedAt)
	bomPath := filepath.Join(config.OutputDir, "service-bom.json")
	bomBytes, err := canonicalJSON(bom)
	if err != nil {
		return Result{}, err
	}
	if err := writeExclusive(bomPath, bomBytes); err != nil {
		return Result{}, fmt.Errorf("write Service BOM: %w", err)
	}
	bomSum := sha256.Sum256(bomBytes)
	bomHash := hex.EncodeToString(bomSum[:])

	result := Result{
		SchemaVersion: "1", Evidence: providercontract.EvidenceLiveProvider,
		RunID: runID, JobID: jobID, TaskID: response.UpstreamTaskID, RequestID: response.RequestID,
		Provider: "volcengine_ark", Model: response.Model.ModelID,
		Region: config.Region, Plan: config.PlanName,
		StartedAt: startedAt, CompletedAt: completedAt,
		LatencyMillis:  completedAt.Sub(startedAt).Milliseconds(),
		ArtifactSHA256: video.SHA256, ArtifactBytes: video.SizeBytes,
		Output: output, Artifacts: evidenceArtifacts, Usage: response.Usage, Cost: response.Cost,
		ManifestSHA256: manifestHash, ServiceBOMHash: bomHash,
		Files:     artifactFiles,
		Redaction: RedactionEvidence{},
	}
	result.Files["generation_manifest"] = "generation-manifest.json"
	result.Files["service_bom"] = "service-bom.json"
	result.Files["result"] = "probe-result.json"
	resultPath := filepath.Join(config.OutputDir, "probe-result.json")
	resultBytes, err := canonicalJSON(result)
	if err != nil {
		return Result{}, err
	}
	if err := assertSanitized(config.Prompt, resultBytes, bomBytes, mustRead(manifestPath)); err != nil {
		return Result{}, err
	}
	if err := writeExclusive(resultPath, resultBytes); err != nil {
		return Result{}, fmt.Errorf("write probe result: %w", err)
	}
	return result, nil
}

func validateConfig(config *Config) error {
	if strings.TrimSpace(config.AdapterURL) == "" || strings.TrimSpace(config.ArtifactRoot) == "" ||
		strings.TrimSpace(config.OutputDir) == "" {
		return errors.New("adapter URL, artifact root, and output directory are required")
	}
	if len(config.ServiceAuthSecret) < 32 {
		return errors.New("provider service authentication secret must contain at least 32 bytes")
	}
	if config.Region == "" {
		config.Region = "cn-beijing"
	}
	if config.PlanName == "" {
		config.PlanName = "agent-plan-large"
	}
	if config.Prompt == "" {
		config.Prompt = defaultPrompt
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 3 * time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Minute
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 3 * time.Minute}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return nil
}

func buildRequest(runID, jobID string, config Config) (providercontract.JobRequest, error) {
	capability := sha256.Sum256([]byte("volcengine_ark\x00" + modelID + "\x00agent-plan-large-v1"))
	model := providercontract.ModelSnapshot{
		CapabilityAlias: string(providercontract.CapabilityVideo),
		Provider:        "volcengine_ark", ModelID: modelID,
		RouteVersion: "agent-plan-large-v1", CapabilityHash: hex.EncodeToString(capability[:]),
		Verification: providercontract.PendingKey,
	}
	output := providercontract.OutputSpec{
		Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
		FPS: 24, DurationMillis: 5_000, Format: "mp4", GenerateAudio: false,
	}
	inputMaterial, _ := json.Marshal(struct {
		Prompt string                         `json:"prompt"`
		Model  providercontract.ModelSnapshot `json:"model"`
		Output providercontract.OutputSpec    `json:"output"`
	}{Prompt: config.Prompt, Model: model, Output: output})
	inputSum := sha256.Sum256(inputMaterial)
	inputHash := hex.EncodeToString(inputSum[:])
	budget := providercontract.BudgetEnvelope{
		EstimatedCostMicros: 1_000_000, MaxCostMicros: 1_000_000, MaxAttempts: 1,
	}
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: jobID, RunID: runID,
		Capability: providercontract.CapabilityVideo, InputHash: inputHash, Model: model,
		Request: providercontract.GenerationRequest{
			RequestID: jobID, IdempotencyKey: jobID,
			Modality: providercontract.ModalityVideo, Prompt: config.Prompt,
			PromptSnapshotID: "prompt-" + runID,
			Context: providercontract.ContextRefs{
				SeriesSnapshotID:  "series-flo104-live-probe",
				EpisodeSnapshotID: "episode-flo104-live-probe",
				SceneSnapshotID:   "scene-flo104-live-probe",
				ShotSnapshotID:    "shot-" + runID,
			},
			Output: output, ModelHint: modelID, Budget: budget,
		},
		TraceID: "trace-" + runID,
	}
	reservation, err := providercontract.BindBudgetReservation(providercontract.BudgetReservation{
		ReservationID: "approval-" + runID, Currency: "CNY", AmountMicros: budget.MaxCostMicros,
		PricingVersion: "agent-plan-large-included-v1", ConfirmedBy: "flo104-live-probe",
	}, providercontract.BudgetBindingInput{
		RunID: runID, InputHash: inputHash, Model: model, Budget: budget,
	})
	if err != nil {
		return providercontract.JobRequest{}, err
	}
	request.BudgetReservation = reservation
	return request, request.Validate()
}

func buildBOM(
	config Config,
	response providercontract.JobResponse,
	video providercontract.AssetRef,
	output MeasuredOutput,
	artifacts []EvidenceArtifact,
	manifestHash string,
	completedAt time.Time,
) ServiceBOM {
	var bom ServiceBOM
	bom.SchemaVersion = "1"
	bom.Evidence = providercontract.EvidenceLiveProvider
	bom.Component.Name = "video-volcengine-provider"
	bom.Component.Version = firstNonEmpty(config.BuildVersion, "development")
	bom.Provider.Name = "volcengine_ark"
	bom.Provider.Model = response.Model.ModelID
	bom.Provider.Region = config.Region
	bom.Provider.Plan = config.PlanName
	bom.Provider.TaskID = response.UpstreamTaskID
	bom.Provider.RequestID = response.RequestID
	bom.Artifact.SHA256 = video.SHA256
	bom.Artifact.Bytes = video.SizeBytes
	bom.Artifact.Output = output
	bom.Artifacts = artifacts
	bom.Usage = response.Usage
	bom.Cost = response.Cost
	bom.CostNote = "request included in Agent Plan subscription; provider returned usage units but no per-task monetary amount"
	bom.Manifest.SHA256 = manifestHash
	bom.GeneratedAt = completedAt
	return bom
}

func exportArtifacts(
	store *artifactstore.Store,
	outputDir string,
	artifacts []providercontract.AssetRef,
	video providercontract.AssetRef,
	videoOutput MeasuredOutput,
) ([]EvidenceArtifact, map[string]string, error) {
	evidence := make([]EvidenceArtifact, 0, len(artifacts))
	files := make(map[string]string, len(artifacts)+3)
	for index, artifact := range artifacts {
		if !strings.HasPrefix(artifact.URI, "cas://sha256/") || artifact.SHA256 == "" || artifact.SizeBytes < 1 {
			return nil, nil, fmt.Errorf("provider artifact %d is not an immutable CAS output", index+1)
		}
		key, filename := evidenceArtifactLocation(artifact, index)
		if _, exists := files[key]; exists {
			key = fmt.Sprintf("artifact_%02d", index+1)
			filename = fmt.Sprintf("artifact-%02d%s", index+1, mediaExtension(artifact.MediaType))
		}
		if err := copyArtifact(store, artifact.SHA256, artifact.SizeBytes, filepath.Join(outputDir, filename)); err != nil {
			return nil, nil, fmt.Errorf("export provider artifact %d: %w", index+1, err)
		}
		item := EvidenceArtifact{
			ID: artifact.ID, Kind: string(artifact.Kind), Role: string(artifact.Role),
			File: filename, SHA256: artifact.SHA256, Bytes: artifact.SizeBytes,
			MediaType: artifact.MediaType,
		}
		if artifact.SHA256 == video.SHA256 && artifact.Role == providercontract.AssetRoleOutput {
			measured := videoOutput
			item.Output = &measured
		}
		evidence = append(evidence, item)
		files[key] = filename
	}
	return evidence, files, nil
}

func evidenceArtifactLocation(artifact providercontract.AssetRef, index int) (string, string) {
	if artifact.Kind == providercontract.ModalityVideo && artifact.Role == providercontract.AssetRoleOutput {
		return "video", "probe.mp4"
	}
	if artifact.Role == providercontract.AssetRoleLastFrame {
		return "last_frame", "last-frame" + mediaExtension(artifact.MediaType)
	}
	return fmt.Sprintf("artifact_%02d", index+1), fmt.Sprintf("artifact-%02d%s", index+1, mediaExtension(artifact.MediaType))
}

func mediaExtension(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(strings.Split(mediaType, ";")[0])) {
	case "video/mp4":
		return ".mp4"
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	default:
		return ".bin"
	}
}

func selectVideo(artifacts []providercontract.AssetRef) (providercontract.AssetRef, error) {
	for _, artifact := range artifacts {
		if artifact.Kind == providercontract.ModalityVideo && artifact.Role == providercontract.AssetRoleOutput &&
			strings.HasPrefix(artifact.URI, "cas://sha256/") && artifact.SHA256 != "" &&
			artifact.Width > 0 && artifact.Height > 0 && artifact.FPS > 0 && artifact.DurationMillis > 0 {
			return artifact, nil
		}
	}
	return providercontract.AssetRef{}, errors.New("live provider response lacks a measured CAS video artifact")
}

func copyArtifact(store *artifactstore.Store, digest string, expectedSize int64, destination string) error {
	source, err := store.Open(digest)
	if err != nil {
		return err
	}
	defer source.Close()
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create evidence artifact: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(destination)
		}
	}()
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), source)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("copy evidence artifact: %w", err)
	}
	if written != expectedSize || hex.EncodeToString(hash.Sum(nil)) != digest {
		_ = file.Close()
		return errors.New("exported evidence artifact does not match its declared size and SHA-256")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync evidence artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close evidence artifact: %w", err)
	}
	committed = true
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func writeExclusive(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func assertSanitized(prompt string, values ...[]byte) error {
	for index, value := range values {
		for _, forbidden := range []string{"Authorization", "Bearer ", "output_url", "video_url", "X-Amz-Signature", prompt} {
			if forbidden != "" && bytesContainFold(value, []byte(forbidden)) {
				return fmt.Errorf("evidence document %d contains forbidden transient data", index+1)
			}
		}
	}
	return nil
}

func bytesContainFold(value, needle []byte) bool {
	return strings.Contains(strings.ToLower(string(value)), strings.ToLower(string(needle)))
}

func mustRead(path string) []byte {
	data, _ := os.ReadFile(path)
	return data
}

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func resolutionFor(height int) string {
	if height > 0 {
		return fmt.Sprintf("%dp", height)
	}
	return ""
}

func aspectRatio(width, height int) string {
	if width <= 0 || height <= 0 {
		return ""
	}
	divisor := gcd(width, height)
	return fmt.Sprintf("%d:%d", width/divisor, height/divisor)
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
