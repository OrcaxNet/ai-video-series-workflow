// Package providercontract defines the provider-neutral generation boundary
// shared by orchestration code and model provider adapters.
package providercontract

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Modality string

const (
	ModalityText  Modality = "text"
	ModalityImage Modality = "image"
	ModalityVideo Modality = "video"
	ModalityAudio Modality = "audio"
)

type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

func (s JobStatus) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCancelled
}

type AssetRole string

const (
	AssetRoleReferenceImage AssetRole = "reference_image"
	AssetRoleReferenceVideo AssetRole = "reference_video"
	AssetRoleReferenceAudio AssetRole = "reference_audio"
	AssetRoleFirstFrame     AssetRole = "first_frame"
	AssetRoleLastFrame      AssetRole = "last_frame"
	AssetRoleOutput         AssetRole = "output"
)

// AssetRef points to an immutable, authorized asset revision. URI is a
// short-lived runtime location, not an identity.
type AssetRef struct {
	ID               string    `json:"id"`
	Revision         string    `json:"revision"`
	Kind             Modality  `json:"kind"`
	Role             AssetRole `json:"role"`
	URI              string    `json:"uri,omitempty"`
	SHA256           string    `json:"sha256"`
	LicenseReference string    `json:"license_reference"`
}

// ContextRefs pins reusable context snapshots without embedding provider fields
// into Series/Episode/Scene/Shot business records.
type ContextRefs struct {
	SeriesSnapshotID  string `json:"series_snapshot_id"`
	EpisodeSnapshotID string `json:"episode_snapshot_id"`
	SceneSnapshotID   string `json:"scene_snapshot_id"`
	ShotSnapshotID    string `json:"shot_snapshot_id"`
}

type OutputSpec struct {
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	AspectRatio    string `json:"aspect_ratio,omitempty"`
	FPS            int    `json:"fps,omitempty"`
	DurationMillis int    `json:"duration_millis,omitempty"`
	Format         string `json:"format,omitempty"`
	GenerateAudio  bool   `json:"generate_audio,omitempty"`
}

// GenerationRequest is the stable request consumed by every provider adapter.
// Provider-specific request fields belong in adapter code, never here.
type GenerationRequest struct {
	RequestID        string         `json:"request_id"`
	IdempotencyKey   string         `json:"idempotency_key"`
	Modality         Modality       `json:"modality"`
	Prompt           string         `json:"prompt"`
	PromptSnapshotID string         `json:"prompt_snapshot_id"`
	Context          ContextRefs    `json:"context"`
	Assets           []AssetRef     `json:"assets,omitempty"`
	Output           OutputSpec     `json:"output"`
	ModelHint        string         `json:"model_hint,omitempty"`
	CallbackURL      string         `json:"callback_url,omitempty"`
	Budget           BudgetEnvelope `json:"budget"`
}

func (r GenerationRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.RequestID) == "":
		return errors.New("request_id is required")
	case strings.TrimSpace(r.IdempotencyKey) == "":
		return errors.New("idempotency_key is required")
	case r.Modality != ModalityText && r.Modality != ModalityImage &&
		r.Modality != ModalityVideo && r.Modality != ModalityAudio:
		return fmt.Errorf("unsupported modality %q", r.Modality)
	case strings.TrimSpace(r.Prompt) == "":
		return errors.New("prompt is required")
	case strings.TrimSpace(r.PromptSnapshotID) == "":
		return errors.New("prompt_snapshot_id is required")
	case r.Budget.EstimatedCostMicros < 0:
		return errors.New("estimated_cost_micros must be non-negative")
	case r.Budget.MaxCostMicros <= 0:
		return errors.New("max_cost_micros must be positive")
	case r.Budget.EstimatedCostMicros > r.Budget.MaxCostMicros:
		return errors.New("estimated_cost_micros must not exceed max_cost_micros")
	case r.Budget.MaxAttempts < 1:
		return errors.New("max_attempts must be positive")
	}
	for _, asset := range r.Assets {
		if asset.ID == "" || asset.Revision == "" || asset.SHA256 == "" ||
			asset.LicenseReference == "" {
			return errors.New("asset id, revision, sha256, and license_reference are required")
		}
	}
	return nil
}

type Usage struct {
	InputTokens        int64 `json:"input_tokens,omitempty"`
	OutputTokens       int64 `json:"output_tokens,omitempty"`
	VideoTokens        int64 `json:"video_tokens,omitempty"`
	GeneratedImages    int64 `json:"generated_images,omitempty"`
	GeneratedChars     int64 `json:"generated_chars,omitempty"`
	GeneratedMillis    int64 `json:"generated_millis,omitempty"`
	ProviderCostMicros int64 `json:"provider_cost_micros,omitempty"`
}

type Output struct {
	Assets []AssetRef `json:"assets"`
	Usage  Usage      `json:"usage"`
}

// Job contains provider provenance needed by a Generation Manifest. It does
// not change the provider-neutral business model.
type Job struct {
	ID                string    `json:"id"`
	RequestID         string    `json:"request_id"`
	IdempotencyKey    string    `json:"idempotency_key"`
	Status            JobStatus `json:"status"`
	Provider          string    `json:"provider"`
	ProviderModel     string    `json:"provider_model"`
	ProviderRequestID string    `json:"provider_request_id,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	Output            *Output   `json:"output,omitempty"`
	Error             *Error    `json:"error,omitempty"`
}

type Capability struct {
	Provider               string     `json:"provider"`
	ModelFamily            string     `json:"model_family"`
	ModelVersion           string     `json:"model_version,omitempty"`
	InputModalities        []Modality `json:"input_modalities"`
	OutputModality         Modality   `json:"output_modality"`
	Async                  bool       `json:"async"`
	SupportsPolling        bool       `json:"supports_polling"`
	SupportsCallback       bool       `json:"supports_callback"`
	SupportsCancel         bool       `json:"supports_cancel"`
	SupportsReferenceImage bool       `json:"supports_reference_image"`
	SupportsLastFrame      bool       `json:"supports_last_frame"`
	Resolutions            []string   `json:"resolutions,omitempty"`
	AspectRatios           []string   `json:"aspect_ratios,omitempty"`
	MinDurationMillis      int        `json:"min_duration_millis,omitempty"`
	MaxDurationMillis      int        `json:"max_duration_millis,omitempty"`
	NativeFPS              []int      `json:"native_fps,omitempty"`
	Verification           string     `json:"verification"`
}

type Callback struct {
	EventID   string    `json:"event_id"`
	JobID     string    `json:"job_id"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type Provider interface {
	Discover(context.Context) ([]Capability, error)
	Submit(context.Context, GenerationRequest) (Job, error)
	Poll(context.Context, string) (Job, error)
	Cancel(context.Context, string) (Job, error)
}
