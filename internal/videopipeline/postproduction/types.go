// Package postproduction implements provider-neutral speech, subtitle, and
// deterministic FFmpeg finalization for one immutable episode revision.
package postproduction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
	"github.com/google/uuid"
)

const (
	SchemaVersion       = "v1"
	EvidenceMockOnly    = "mock_only"
	EvidenceLive        = "live_provider_call"
	EvidencePendingKey  = "pending_key"
	SpeechIdentityV2    = "speech-v2"
	defaultAudioRate    = 48_000
	defaultAudioChannel = 2
)

type Artifact struct {
	Kind           string `json:"kind"`
	Digest         string `json:"sha256"`
	URI            string `json:"uri"`
	MediaType      string `json:"mediaType"`
	SizeBytes      int64  `json:"sizeBytes"`
	DurationMillis int64  `json:"durationMillis,omitempty"`
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	FPS            int    `json:"fps,omitempty"`
}

func (a Artifact) Validate() error {
	switch {
	case strings.TrimSpace(a.Kind) == "":
		return errors.New("artifact kind is required")
	case !validDigest(a.Digest):
		return fmt.Errorf("%s artifact requires a lowercase SHA-256", a.Kind)
	case a.URI != "cas://sha256/"+a.Digest:
		return fmt.Errorf("%s artifact URI does not match its SHA-256", a.Kind)
	case strings.TrimSpace(a.MediaType) == "":
		return fmt.Errorf("%s artifact media type is required", a.Kind)
	case a.SizeBytes < 0:
		return fmt.Errorf("%s artifact size must be non-negative", a.Kind)
	}
	return nil
}

type Clip struct {
	RunID               string   `json:"runId"`
	ShotSpecRevisionID  string   `json:"shotSpecRevisionId"`
	ShotSpecHash        string   `json:"shotSpecHash"`
	PromptSnapshotID    string   `json:"promptSnapshotId"`
	PromptSnapshotHash  string   `json:"promptSnapshotHash"`
	ContextSnapshotID   string   `json:"contextSnapshotId"`
	ContextSnapshotHash string   `json:"contextSnapshotHash"`
	Artifact            Artifact `json:"artifact"`
	DurationMillis      int64    `json:"durationMillis"`
	LicenseReference    string   `json:"licenseReference"`
}

func (c Clip) Validate() error {
	switch {
	case c.RunID == "" || c.ShotSpecRevisionID == "" || c.PromptSnapshotID == "" ||
		c.ContextSnapshotID == "":
		return errors.New("clip run, shot, prompt, and context identifiers are required")
	case !validDigest(c.ShotSpecHash) || !validDigest(c.PromptSnapshotHash) ||
		!validDigest(c.ContextSnapshotHash):
		return errors.New("clip shot, prompt, and context hashes are required")
	case c.DurationMillis <= 0:
		return errors.New("clip duration must be positive")
	case strings.TrimSpace(c.LicenseReference) == "":
		return errors.New("clip license reference is required")
	}
	if err := c.Artifact.Validate(); err != nil {
		return err
	}
	if c.Artifact.Kind != "shot_video" {
		return errors.New("clip artifact kind must be shot_video")
	}
	return nil
}

type GateBinding struct {
	Gate        string `json:"gate"`
	DecisionID  string `json:"decisionId"`
	Decision    string `json:"decision"`
	ContentHash string `json:"contentHash"`
}

func (g GateBinding) Validate() error {
	if g.Gate == "" || g.DecisionID == "" || g.Decision != "APPROVED" ||
		!validDigest(g.ContentHash) {
		return errors.New("gate binding requires an approved decision and content hash")
	}
	return nil
}

// SpeechVoiceBinding freezes the provider-facing voice configuration separately
// from Cue.VoiceRef. VoiceRef is an immutable product asset version UUID; it is
// never a provider speaker name. The parent binding keeps an auditable link to
// the voice revision that was approved with the already-generated video shots.
type SpeechVoiceBinding struct {
	AssetID              string `json:"assetId"`
	ParentAssetVersionID string `json:"parentAssetVersionId"`
	AssetVersionID       string `json:"assetVersionId"`
	AssetVersionHash     string `json:"assetVersionHash"`
	LicenseSnapshotID    string `json:"licenseSnapshotId"`
	LicenseSnapshotHash  string `json:"licenseSnapshotHash"`
	Provider             string `json:"provider"`
	ModelID              string `json:"modelId"`
	ResourceID           string `json:"resourceId"`
	Speaker              string `json:"speaker"`
}

func (v SpeechVoiceBinding) Validate(route providercontract.ModelSnapshot) error {
	for name, value := range map[string]string{
		"assetId":              v.AssetID,
		"parentAssetVersionId": v.ParentAssetVersionID,
		"assetVersionId":       v.AssetVersionID,
		"licenseSnapshotId":    v.LicenseSnapshotID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("speech voice %s must be a UUID", name)
		}
	}
	if v.ParentAssetVersionID == v.AssetVersionID {
		return errors.New("speech voice revision must differ from its parent")
	}
	if !validDigest(v.AssetVersionHash) || !validDigest(v.LicenseSnapshotHash) {
		return errors.New("speech voice asset and license hashes must be lowercase SHA-256 values")
	}
	if strings.TrimSpace(v.Provider) == "" || strings.TrimSpace(v.ModelID) == "" ||
		strings.TrimSpace(v.ResourceID) == "" || strings.TrimSpace(v.Speaker) == "" {
		return errors.New("speech voice provider, model, resource, and speaker are required")
	}
	if !sameSpeechProvider(v.Provider, route.Provider) || v.ModelID != route.ModelID {
		return errors.New("speech voice provider/model must match the frozen route")
	}
	return nil
}

func sameSpeechProvider(left, right string) bool {
	normalize := func(value string) string {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "volcengine" {
			return "volcengine_ark"
		}
		return value
	}
	return normalize(left) == normalize(right)
}

type SpeechConfig struct {
	Route                            providercontract.ModelSnapshot     `json:"route"`
	ProviderProfileID                string                             `json:"providerProfileId"`
	BudgetApprovalID                 string                             `json:"budgetApprovalId"`
	BudgetMaximumMicros              int64                              `json:"budgetMaximumMicros"`
	BudgetCurrency                   string                             `json:"budgetCurrency"`
	IdentityVersion                  string                             `json:"identityVersion,omitempty"`
	Voice                            *SpeechVoiceBinding                `json:"voice,omitempty"`
	AuthorizedCueID                  string                             `json:"authorizedCueId,omitempty"`
	MaximumAFPMilli                  int64                              `json:"maximumAfpMilli,omitempty"`
	MaximumNonSubscriptionCashMicros int64                              `json:"maximumNonSubscriptionCashMicros,omitempty"`
	MaxAttempts                      int                                `json:"maxAttempts,omitempty"`
	BatchAuthorization               *speechcontract.BatchAuthorization `json:"batchAuthorization,omitempty"`
	CompletedAttempts                []ProviderAttempt                  `json:"completedAttempts,omitempty"`
}

func (s SpeechConfig) Validate() error {
	if err := s.Route.Validate(providercontract.CapabilitySpeech); err != nil {
		return fmt.Errorf("speech route: %w", err)
	}
	switch {
	case strings.TrimSpace(s.ProviderProfileID) == "":
		return errors.New("speech provider profile is required")
	case strings.TrimSpace(s.BudgetApprovalID) == "":
		return errors.New("speech budget approval is required")
	case s.BudgetMaximumMicros <= 0:
		return errors.New("speech budget maximum must be positive")
	case len(s.BudgetCurrency) != 3:
		return errors.New("speech budget currency must be an ISO 4217 code")
	}
	if s.IdentityVersion == "" {
		return nil
	}
	if s.IdentityVersion != SpeechIdentityV2 {
		return errors.New("speech identity version is unsupported")
	}
	if s.Voice == nil {
		return errors.New("speech-v2 requires an immutable VOICE binding")
	}
	if err := s.Voice.Validate(s.Route); err != nil {
		return err
	}
	if s.BatchAuthorization != nil {
		if strings.TrimSpace(s.AuthorizedCueID) != "" || s.MaximumAFPMilli != 0 ||
			s.MaximumNonSubscriptionCashMicros != 0 || s.MaxAttempts != 0 {
			return errors.New("speech-v2 batch cannot retain the single-cue canary fields")
		}
		if err := s.BatchAuthorization.Validate(); err != nil {
			return err
		}
		batch := s.BatchAuthorization
		if !sameSpeechProvider(batch.Provider, s.Route.Provider) ||
			batch.ModelID != s.Route.ModelID || batch.RouteVersion != s.Route.RouteVersion ||
			batch.ResourceID != s.Voice.ResourceID || batch.Speaker != s.Voice.Speaker ||
			batch.VoiceAssetID != s.Voice.AssetID ||
			batch.ParentVoiceAssetVersionID != s.Voice.ParentAssetVersionID ||
			batch.VoiceAssetVersionID != s.Voice.AssetVersionID ||
			batch.VoiceAssetVersionHash != s.Voice.AssetVersionHash ||
			batch.LicenseSnapshotID != s.Voice.LicenseSnapshotID ||
			batch.LicenseSnapshotHash != s.Voice.LicenseSnapshotHash {
			return errors.New("speech-v2 batch route, VOICE, or license drifted from the frozen configuration")
		}
		return nil
	}
	if strings.TrimSpace(s.AuthorizedCueID) == "" {
		return errors.New("speech-v2 requires one explicitly authorized cue")
	}
	if s.MaximumAFPMilli <= 0 {
		return errors.New("speech-v2 requires a positive AFP ceiling")
	}
	if s.MaximumNonSubscriptionCashMicros != 0 {
		return errors.New("speech-v2 canary requires a zero non-subscription cash ceiling")
	}
	if s.MaxAttempts != 1 {
		return errors.New("speech-v2 canary requires MaxAttempts=1")
	}
	return nil
}

type OutputPolicy struct {
	Width              int    `json:"width"`
	Height             int    `json:"height"`
	FPS                int    `json:"fps"`
	Format             string `json:"format"`
	BurnSubtitles      bool   `json:"burnSubtitles"`
	AudioSampleRate    int    `json:"audioSampleRate"`
	AudioChannels      int    `json:"audioChannels"`
	EnforcePoCDuration bool   `json:"enforcePoCDuration"`
}

func (p OutputPolicy) withDefaults() OutputPolicy {
	if p.Width == 0 {
		p.Width = 1280
	}
	if p.Height == 0 {
		p.Height = 720
	}
	if p.FPS == 0 {
		p.FPS = 24
	}
	if p.Format == "" {
		p.Format = "mp4"
	}
	if p.AudioSampleRate == 0 {
		p.AudioSampleRate = defaultAudioRate
	}
	if p.AudioChannels == 0 {
		p.AudioChannels = defaultAudioChannel
	}
	return p
}

func (p OutputPolicy) Validate() error {
	p = p.withDefaults()
	switch {
	case p.Width <= 0 || p.Height <= 0 || p.FPS <= 0:
		return errors.New("positive output dimensions and fps are required")
	case p.Format != "mp4":
		return errors.New("only deterministic mp4 output is supported")
	case p.AudioSampleRate != defaultAudioRate || p.AudioChannels != defaultAudioChannel:
		return errors.New("audio output must be 48 kHz stereo")
	}
	return nil
}

type Request struct {
	SchemaVersion       string           `json:"schemaVersion"`
	Evidence            string           `json:"evidence"`
	EpisodeRevisionID   string           `json:"episodeRevisionId"`
	EpisodeRevisionHash string           `json:"episodeRevisionHash"`
	RunIDs              []string         `json:"runIds"`
	Clips               []Clip           `json:"clips"`
	Subtitle            SubtitleRevision `json:"subtitleRevision"`
	BackgroundAudio     *Artifact        `json:"backgroundAudio,omitempty"`
	Speech              SpeechConfig     `json:"speech"`
	Output              OutputPolicy     `json:"output"`
	Gates               []GateBinding    `json:"gates"`
	TraceID             string           `json:"traceId"`
	// AuthorizePaidSubmit is an activity-local callback and is deliberately
	// excluded from durable request/manifest JSON. The worker invokes it
	// immediately before every provider submission.
	AuthorizePaidSubmit func(context.Context, Cue) error `json:"-"`
}

func (r Request) Validate() error {
	switch {
	case r.SchemaVersion != SchemaVersion:
		return errors.New("post-production schemaVersion must be v1")
	case r.Evidence != EvidenceMockOnly && r.Evidence != EvidenceLive &&
		r.Evidence != EvidencePendingKey:
		return errors.New("post-production evidence must be mock_only, live_provider_call, or pending_key")
	case r.EpisodeRevisionID == "" || !validDigest(r.EpisodeRevisionHash):
		return errors.New("episode revision identifier and hash are required")
	case len(r.RunIDs) == 0 || len(r.RunIDs) != len(r.Clips):
		return errors.New("runIds and clips must be non-empty and have equal length")
	case strings.TrimSpace(r.TraceID) == "":
		return errors.New("traceId is required")
	}
	seen := make(map[string]struct{}, len(r.RunIDs))
	var total int64
	for i, runID := range r.RunIDs {
		if runID == "" || r.Clips[i].RunID != runID {
			return errors.New("runIds must match clips in deterministic shot order")
		}
		if _, duplicate := seen[runID]; duplicate {
			return fmt.Errorf("duplicate runId %q", runID)
		}
		seen[runID] = struct{}{}
		if err := r.Clips[i].Validate(); err != nil {
			return fmt.Errorf("clip %d: %w", i, err)
		}
		total += r.Clips[i].DurationMillis
	}
	if err := r.Subtitle.Validate(total); err != nil {
		return fmt.Errorf("subtitle revision: %w", err)
	}
	if r.BackgroundAudio != nil {
		if err := r.BackgroundAudio.Validate(); err != nil {
			return fmt.Errorf("background audio: %w", err)
		}
		if r.BackgroundAudio.Kind != "background_audio" {
			return errors.New("background audio artifact kind must be background_audio")
		}
	}
	if err := r.Speech.Validate(); err != nil {
		return err
	}
	if err := validateSpeechAuthorizationCoverage(r); err != nil {
		return err
	}
	if err := r.Output.Validate(); err != nil {
		return err
	}
	if r.Output.EnforcePoCDuration && (total < 45_000 || total > 60_000) {
		return fmt.Errorf("PoC duration %dms is outside 45-60 seconds", total)
	}
	for i, gate := range r.Gates {
		if err := gate.Validate(); err != nil {
			return fmt.Errorf("gate %d: %w", i, err)
		}
	}
	if !hasApprovedGate(r.Gates, "G1") || !hasApprovedGate(r.Gates, "G2") {
		return errors.New("approved G1 and G2 bindings are required before post-production")
	}
	return nil
}

func (r Request) DurationMillis() int64 {
	var total int64
	for _, clip := range r.Clips {
		total += clip.DurationMillis
	}
	return total
}

type ProviderAttempt struct {
	CueID          string                         `json:"cueId"`
	JobID          string                         `json:"jobId"`
	RequestID      string                         `json:"requestId"`
	UpstreamTaskID string                         `json:"upstreamTaskId"`
	ConnectID      string                         `json:"connectId,omitempty"`
	LogID          string                         `json:"logId,omitempty"`
	Model          providercontract.ModelSnapshot `json:"modelSnapshot"`
	Usage          providercontract.Usage         `json:"usage"`
	Cost           providercontract.Cost          `json:"cost"`
	Artifact       Artifact                       `json:"artifact"`
	Evidence       string                         `json:"evidence"`
}

type QCReport struct {
	State                     string   `json:"state"`
	SubtitleCERPercent        *float64 `json:"subtitleCerPercent,omitempty"`
	SubtitleBoundaryP95Millis *int64   `json:"subtitleBoundaryP95Millis,omitempty"`
	AudioVideoStartP95Millis  *int64   `json:"audioVideoStartP95Millis,omitempty"`
	ActualDurationMillis      int64    `json:"actualDurationMillis"`
	ManualTimingRequired      bool     `json:"manualTimingRequired"`
	MeasurementEvidence       string   `json:"measurementEvidence"`
}

type ServiceComponent struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Provider   string `json:"provider,omitempty"`
	Model      string `json:"model,omitempty"`
	Version    string `json:"version"`
	Evidence   string `json:"evidence"`
	ConfigHash string `json:"configHash"`
}

type Result struct {
	SchemaVersion     string            `json:"schemaVersion"`
	Evidence          string            `json:"evidence"`
	EpisodeRevisionID string            `json:"episodeRevisionId"`
	Subtitle          Artifact          `json:"subtitle"`
	Dialogue          Artifact          `json:"dialogue"`
	FinalVideo        Artifact          `json:"finalVideo"`
	Manifest          Artifact          `json:"manifest"`
	ServiceBOM        Artifact          `json:"serviceBom"`
	SpeechAttempts    []ProviderAttempt `json:"speechAttempts"`
	QC                QCReport          `json:"qc"`
	CommandPlanHash   string            `json:"commandPlanHash"`
	ManifestHash      string            `json:"manifestHash"`
	ServiceBOMHash    string            `json:"serviceBomHash"`
}

func (r Result) Validate() error {
	if r.SchemaVersion != SchemaVersion || r.EpisodeRevisionID == "" {
		return errors.New("post-production result identity is incomplete")
	}
	if r.Evidence != EvidenceMockOnly && r.Evidence != EvidenceLive {
		return errors.New("completed post-production evidence must be mock_only or live_provider_call")
	}
	for _, artifact := range []Artifact{r.Subtitle, r.Dialogue, r.FinalVideo, r.Manifest, r.ServiceBOM} {
		if err := artifact.Validate(); err != nil {
			return err
		}
	}
	if r.Subtitle.Kind != "subtitle_srt" ||
		r.Dialogue.Kind != "dialogue_audio" ||
		r.FinalVideo.Kind != "final_video" ||
		r.Manifest.Kind != "postproduction_manifest" ||
		r.ServiceBOM.Kind != "service_bom" {
		return errors.New("post-production artifact roles are invalid")
	}
	if r.FinalVideo.DurationMillis <= 0 ||
		r.FinalVideo.Width <= 0 || r.FinalVideo.Height <= 0 || r.FinalVideo.FPS <= 0 {
		return errors.New("post-production final video media specification is incomplete")
	}
	if !validDigest(r.CommandPlanHash) || !validDigest(r.ManifestHash) ||
		!validDigest(r.ServiceBOMHash) {
		return errors.New("post-production command, manifest, and service BOM hashes are required")
	}
	if r.Manifest.Digest != r.ManifestHash || r.ServiceBOM.Digest != r.ServiceBOMHash {
		return errors.New("manifest or service BOM artifact does not match its declared hash")
	}
	if r.QC.State == "" ||
		r.QC.ActualDurationMillis != r.FinalVideo.DurationMillis ||
		r.QC.MeasurementEvidence != r.Evidence {
		return errors.New("post-production QC does not match the final media evidence")
	}
	seenCues := make(map[string]struct{}, len(r.SpeechAttempts))
	for index, attempt := range r.SpeechAttempts {
		if strings.TrimSpace(attempt.CueID) == "" ||
			strings.TrimSpace(attempt.JobID) == "" ||
			strings.TrimSpace(attempt.RequestID) == "" ||
			strings.TrimSpace(attempt.UpstreamTaskID) == "" {
			return fmt.Errorf("speech attempt %d identity is incomplete", index)
		}
		if _, duplicate := seenCues[attempt.CueID]; duplicate {
			return fmt.Errorf("duplicate speech attempt cue %q", attempt.CueID)
		}
		seenCues[attempt.CueID] = struct{}{}
		if err := attempt.Model.Validate(providercontract.CapabilitySpeech); err != nil {
			return fmt.Errorf("speech attempt %d model: %w", index, err)
		}
		if attempt.Evidence != r.Evidence {
			return fmt.Errorf("speech attempt %d evidence does not match the result", index)
		}
		if attempt.Cost.EstimatedMicros < 0 ||
			(attempt.Cost.ActualMicros != nil && *attempt.Cost.ActualMicros < 0) ||
			len(attempt.Cost.Currency) != 3 ||
			strings.TrimSpace(attempt.Cost.PricingVersion) == "" ||
			negativeUsage(attempt.Usage) {
			return fmt.Errorf("speech attempt %d usage or cost is invalid", index)
		}
		if attempt.Artifact.Kind != "dialogue_segment" ||
			attempt.Artifact.DurationMillis <= 0 {
			return fmt.Errorf("speech attempt %d dialogue artifact is invalid", index)
		}
		if err := attempt.Artifact.Validate(); err != nil {
			return fmt.Errorf("speech attempt %d artifact: %w", index, err)
		}
	}
	return nil
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func digestJSON(value any) (string, error) {
	data, err := canonicalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func hasApprovedGate(gates []GateBinding, required string) bool {
	for _, gate := range gates {
		if gate.Gate == required && gate.Decision == "APPROVED" {
			return true
		}
	}
	return false
}
