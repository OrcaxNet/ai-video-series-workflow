package providercontract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ManifestSchemaVersion = "1"
	EvidenceMockOnly      = "mock_only"
	EvidenceLiveProvider  = "live_provider_call"
)

// AssetEvidence deliberately excludes the runtime URI. Signed provider URLs
// are transport details and must not become durable provenance.
type AssetEvidence struct {
	ID               string    `json:"id"`
	Revision         string    `json:"revision"`
	Kind             Modality  `json:"kind"`
	Role             AssetRole `json:"role"`
	SHA256           string    `json:"sha256"`
	LicenseReference string    `json:"license_reference"`
}

type ProviderEvidence struct {
	Name      string `json:"name"`
	Model     string `json:"model"`
	Region    string `json:"region"`
	RequestID string `json:"request_id"`
	JobID     string `json:"job_id"`
}

type AttemptEvidence struct {
	Number        int       `json:"number"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at"`
	LatencyMillis int64     `json:"latency_millis"`
}

// GenerationManifest is the immutable provider-neutral audit record for one
// provider attempt. It contains snapshot and content hashes, never prompt text
// or signed transport URLs.
type GenerationManifest struct {
	SchemaVersion  string           `json:"schema_version"`
	ManifestID     string           `json:"manifest_id"`
	Evidence       string           `json:"evidence"`
	ShotID         string           `json:"shot_id"`
	RequestID      string           `json:"request_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	Modality       Modality         `json:"modality"`
	PromptSnapshot string           `json:"prompt_snapshot_id"`
	Context        ContextRefs      `json:"context"`
	InputAssets    []AssetEvidence  `json:"input_assets"`
	Requested      OutputSpec       `json:"requested_output"`
	Provider       ProviderEvidence `json:"provider"`
	Attempt        AttemptEvidence  `json:"attempt"`
	Status         JobStatus        `json:"status"`
	Actual         *OutputSpec      `json:"actual_output,omitempty"`
	OutputAssets   []AssetEvidence  `json:"output_assets,omitempty"`
	Usage          Usage            `json:"usage"`
	Budget         BudgetEnvelope   `json:"budget"`
	Error          *Error           `json:"error,omitempty"`
}

type ManifestBuildInput struct {
	ManifestID  string
	ShotID      string
	Evidence    string
	Request     GenerationRequest
	Job         Job
	Attempt     int
	StartedAt   time.Time
	CompletedAt time.Time
}

func NewGenerationManifest(input ManifestBuildInput) (GenerationManifest, error) {
	if err := input.Request.Validate(); err != nil {
		return GenerationManifest{}, fmt.Errorf("validate manifest request: %w", err)
	}
	manifest := GenerationManifest{
		SchemaVersion:  ManifestSchemaVersion,
		ManifestID:     input.ManifestID,
		Evidence:       input.Evidence,
		ShotID:         input.ShotID,
		RequestID:      input.Request.RequestID,
		IdempotencyKey: input.Request.IdempotencyKey,
		Modality:       input.Request.Modality,
		PromptSnapshot: input.Request.PromptSnapshotID,
		Context:        input.Request.Context,
		Requested:      input.Request.Output,
		Provider: ProviderEvidence{
			Name:      input.Job.Provider,
			Model:     input.Job.ProviderModel,
			Region:    input.Job.ProviderRegion,
			RequestID: input.Job.ProviderRequestID,
			JobID:     input.Job.ID,
		},
		Attempt: AttemptEvidence{
			Number:        input.Attempt,
			StartedAt:     input.StartedAt.UTC(),
			CompletedAt:   input.CompletedAt.UTC(),
			LatencyMillis: input.CompletedAt.Sub(input.StartedAt).Milliseconds(),
		},
		Status: input.Job.Status,
		Budget: input.Request.Budget,
		Error:  input.Job.Error,
	}
	for _, asset := range input.Request.Assets {
		manifest.InputAssets = append(manifest.InputAssets, toAssetEvidence(asset))
	}
	if input.Job.Output != nil {
		actual := input.Job.Output.Actual
		manifest.Actual = &actual
		manifest.Usage = input.Job.Output.Usage
		for _, asset := range input.Job.Output.Assets {
			manifest.OutputAssets = append(manifest.OutputAssets, toAssetEvidence(asset))
		}
	}
	if err := manifest.Validate(); err != nil {
		return GenerationManifest{}, err
	}
	return manifest, nil
}

func (m GenerationManifest) Validate() error {
	switch {
	case m.SchemaVersion != ManifestSchemaVersion:
		return errors.New("manifest schema_version must be 1")
	case strings.TrimSpace(m.ManifestID) == "":
		return errors.New("manifest_id is required")
	case m.Evidence != EvidenceMockOnly && m.Evidence != EvidenceLiveProvider:
		return errors.New("manifest evidence must be mock_only or live_provider_call")
	case strings.TrimSpace(m.ShotID) == "":
		return errors.New("shot_id is required")
	case strings.TrimSpace(m.RequestID) == "" || strings.TrimSpace(m.IdempotencyKey) == "":
		return errors.New("request and idempotency identifiers are required")
	case strings.TrimSpace(m.PromptSnapshot) == "":
		return errors.New("prompt_snapshot_id is required")
	case m.Context.SeriesSnapshotID == "" || m.Context.EpisodeSnapshotID == "" ||
		m.Context.SceneSnapshotID == "" || m.Context.ShotSnapshotID == "":
		return errors.New("all context snapshot identifiers are required")
	case m.Provider.Name == "" || m.Provider.Model == "" || m.Provider.Region == "" ||
		m.Provider.RequestID == "" || m.Provider.JobID == "":
		return errors.New("complete provider provenance is required")
	case m.Attempt.Number < 1:
		return errors.New("attempt number must be positive")
	case m.Attempt.StartedAt.IsZero() || !m.Attempt.CompletedAt.After(m.Attempt.StartedAt):
		return errors.New("attempt timestamps are invalid")
	case m.Attempt.LatencyMillis != m.Attempt.CompletedAt.Sub(m.Attempt.StartedAt).Milliseconds():
		return errors.New("attempt latency does not match timestamps")
	case !m.Status.Terminal():
		return errors.New("manifest status must be terminal")
	}
	for _, asset := range append(append([]AssetEvidence{}, m.InputAssets...), m.OutputAssets...) {
		if err := asset.Validate(); err != nil {
			return err
		}
	}
	if m.Status == StatusSucceeded {
		if m.Actual == nil || len(m.OutputAssets) == 0 {
			return errors.New("succeeded manifest requires actual output and output assets")
		}
		if m.Modality == ModalityVideo &&
			(m.Actual.Resolution == "" || m.Actual.AspectRatio == "" ||
				m.Actual.FPS <= 0 || m.Actual.DurationMillis <= 0 || m.Actual.Format == "") {
			return errors.New("succeeded video manifest lacks actual output specification")
		}
	}
	return nil
}

func (a AssetEvidence) Validate() error {
	if a.ID == "" || a.Revision == "" || a.Kind == "" || a.Role == "" ||
		a.LicenseReference == "" {
		return errors.New("manifest asset evidence is incomplete")
	}
	if len(a.SHA256) != sha256.Size*2 || a.SHA256 == "pending_download" {
		return fmt.Errorf("asset %q requires a final SHA-256", a.ID)
	}
	if _, err := hex.DecodeString(a.SHA256); err != nil {
		return fmt.Errorf("asset %q has an invalid SHA-256", a.ID)
	}
	return nil
}

func toAssetEvidence(asset AssetRef) AssetEvidence {
	return AssetEvidence{
		ID:               asset.ID,
		Revision:         asset.Revision,
		Kind:             asset.Kind,
		Role:             asset.Role,
		SHA256:           asset.SHA256,
		LicenseReference: asset.LicenseReference,
	}
}

// WriteGenerationManifest creates one immutable file with restrictive
// permissions and returns the SHA-256 of the exact bytes written.
func WriteGenerationManifest(path string, manifest GenerationManifest) (string, error) {
	data, err := marshalGenerationManifest(manifest)
	if err != nil {
		return "", err
	}
	cleanPath := filepath.Clean(path)
	if _, err := os.Lstat(cleanPath); err == nil {
		return "", &Error{Code: CodeConflict, SafeMessage: "generation manifest already exists"}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect generation manifest path: %w", err)
	}
	parent := filepath.Dir(cleanPath)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", fmt.Errorf("create generation manifest directory: %w", err)
	}
	file, err := os.CreateTemp(parent, ".manifest-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create generation manifest: %w", err)
	}
	tempPath := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}
	if err := file.Chmod(0o600); err != nil {
		cleanup()
		return "", fmt.Errorf("protect generation manifest: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		cleanup()
		return "", fmt.Errorf("write generation manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return "", fmt.Errorf("sync generation manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("close generation manifest: %w", err)
	}
	// A hard link provides atomic create-if-absent semantics on the same
	// filesystem; unlike Rename it cannot overwrite a concurrent writer.
	if err := os.Link(tempPath, cleanPath); err != nil {
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("commit generation manifest: %w", err)
	}
	if err := os.Remove(tempPath); err != nil {
		return "", fmt.Errorf("remove generation manifest staging file: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func marshalGenerationManifest(manifest GenerationManifest) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode generation manifest: %w", err)
	}
	return append(data, '\n'), nil
}
