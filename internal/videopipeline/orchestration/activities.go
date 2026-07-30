package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/production"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// Activities is the executable local skeleton for the Activity boundary.
// Production implementations persist through the control-plane repository;
// this version keeps no business truth and delegates only versioned provider
// jobs. The adapter URL can target the included mock or a remote-provider
// adapter; credentials never enter workflow inputs.
type Activities struct {
	ProviderAdapterURL string
	HTTPClient         *http.Client
	PromptSource       PromptSource
}

// PromptSource loads and compiles exact persisted shot/context/asset
// revisions. The concrete PostgreSQL repository is injected by the control
// plane; no provider credential crosses this boundary.
type PromptSource interface {
	CompilePrompt(context.Context, string, string) (production.PromptSnapshot, error)
}

// NewActivities creates bounded Activity clients.
func NewActivities(providerAdapterURL string) *Activities {
	return &Activities{ProviderAdapterURL: providerAdapterURL, HTTPClient: mockprovider.DefaultHTTPClient()}
}

// ValidateBatch rejects unversioned or unapproved production inputs.
func (a *Activities) ValidateBatch(_ context.Context, input EpisodeProductionInput) error {
	return validateWorkflowInput(input)
}

// CompilePrompt resolves the real immutable production compiler when it is
// configured. The deterministic fallback is explicitly mock-only and exists
// solely for the no-key Compose smoke workflow.
func (a *Activities) CompilePrompt(ctx context.Context, input CompilePromptInput) (PromptSnapshotRef, error) {
	if input.ShotSpecRevisionID == "" || input.GenerationProfileRef == "" {
		return PromptSnapshotRef{}, errors.New("shotSpecRevisionId and generationProfileRef are required")
	}
	if a.PromptSource != nil {
		snapshot, err := a.PromptSource.CompilePrompt(ctx, input.ShotSpecRevisionID, input.GenerationProfileRef)
		if err != nil {
			return PromptSnapshotRef{}, fmt.Errorf("compile production prompt: %w", err)
		}
		if snapshot.ID == "" || snapshot.ContentHash == "" ||
			snapshot.ShotRevision.ID != input.ShotSpecRevisionID ||
			snapshot.GenerationProfileRef != input.GenerationProfileRef {
			return PromptSnapshotRef{}, errors.New("prompt source returned an unpinned snapshot")
		}
		return toPromptSnapshotRef(snapshot), nil
	}
	sum := sha256.Sum256([]byte(input.ShotSpecRevisionID + "\x00" + input.GenerationProfileRef))
	digest := hex.EncodeToString(sum[:])
	contextID := func(scope string) string {
		value := sha256.Sum256([]byte(scope + "\x00" + input.ShotSpecRevisionID))
		return "mock-context-" + scope + "-" + hex.EncodeToString(value[:8])
	}
	return PromptSnapshotRef{
		ID:             "mock-prompt-" + digest[:16],
		Digest:         digest,
		PositivePrompt: "deterministic mock-only shot " + input.ShotSpecRevisionID,
		Context: providercontract.ContextRefs{
			SeriesSnapshotID:  contextID("series"),
			EpisodeSnapshotID: contextID("episode"),
			SceneSnapshotID:   contextID("scene"),
			ShotSnapshotID:    contextID("shot"),
		},
		Output: providercontract.OutputSpec{
			Width: 1280, Height: 720, Resolution: "720p", AspectRatio: "16:9",
			FPS: 24, DurationMillis: 5_000, Format: "mp4",
		},
		InputRevisionHashes: map[string]string{"shot_spec": digest},
	}, nil
}

// CreateRun creates deterministic IDs for the runnable skeleton. A production
// repository makes this an idempotent PostgreSQL transaction with an outbox row.
func (a *Activities) CreateRun(_ context.Context, input CreateRunInput) (GenerationRunRef, error) {
	if input.CreativeAttempt < 1 || input.CreativeAttempt > 2 {
		return GenerationRunRef{}, errors.New("creativeAttempt must be 1 or 2")
	}
	if err := input.Route.Validate(providercontract.CapabilityVideo); err != nil {
		return GenerationRunRef{}, errors.New("a frozen video.primary route is required")
	}
	material := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%d",
		input.ShotSpecRevisionID,
		input.PromptSnapshot.Digest,
		input.GenerationProfileRef,
		input.Route.CapabilityAlias,
		input.Route.Provider,
		input.Route.ModelID,
		input.Route.RouteVersion,
		input.Route.CapabilityHash,
		input.CreativeAttempt,
	)
	sum := sha256.Sum256([]byte(material))
	digest := hex.EncodeToString(sum[:])
	return GenerationRunRef{
		RunID:         "run-" + digest[:16],
		RunSpecDigest: digest,
		Attempt:       input.CreativeAttempt,
	}, nil
}

// ExecuteProviderJob submits and reconciles one idempotent remote API job.
// Activity retries reuse jobId and upstreamTaskId, so they never create a new
// paid attempt. UNKNOWN is polled; it is not treated as a failed generation.
func (a *Activities) ExecuteProviderJob(ctx context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
	if a.HTTPClient == nil {
		return ProviderResult{}, errors.New("provider HTTP client is required")
	}
	activity.RecordHeartbeat(ctx, map[string]any{"phase": "submitting", "runId": input.Run.RunID})
	jobID := "provider-job-" + input.Run.RunID
	promptText := input.Prompt.PositivePrompt
	if input.Prompt.NegativePrompt != "" {
		promptText += "\nNEGATIVE CONSTRAINTS: " + input.Prompt.NegativePrompt
	}
	if promptText == "" {
		return ProviderResult{}, errors.New("immutable compiled prompt text is required")
	}
	outputSpec := input.Prompt.Output
	if outputSpec.Width <= 0 || outputSpec.Height <= 0 || outputSpec.DurationMillis <= 0 {
		return ProviderResult{}, errors.New("immutable compiled output specification is required")
	}
	result, err := mockprovider.Submit(ctx, a.HTTPClient, a.ProviderAdapterURL, providercontract.JobRequest{
		SchemaVersion: "v1",
		JobID:         jobID,
		RunID:         input.Run.RunID,
		Capability:    providercontract.CapabilityVideo,
		InputHash:     input.Run.RunSpecDigest,
		Model:         input.Route,
		Request: providercontract.GenerationRequest{
			RequestID:        jobID,
			IdempotencyKey:   jobID,
			Modality:         providercontract.ModalityVideo,
			Prompt:           promptText,
			PromptSnapshotID: input.Prompt.ID,
			Context:          input.Prompt.Context,
			Assets:           input.Prompt.Assets,
			Output:           outputSpec,
			ModelHint:        input.Route.ModelID,
			Budget: providercontract.BudgetEnvelope{
				EstimatedCostMicros: input.BudgetMaximumMicros,
				MaxCostMicros:       input.BudgetMaximumMicros,
				MaxAttempts:         2,
			},
		},
		BudgetReservation: providercontract.BudgetReservation{
			ReservationID:  input.BudgetApprovalID,
			Currency:       input.BudgetCurrency,
			AmountMicros:   input.BudgetMaximumMicros,
			PricingVersion: "workflow-approved-v1",
			ConfirmedBy:    input.BudgetApprovalID,
		},
		TraceID: input.TraceID,
	})
	if err != nil {
		return ProviderResult{}, classifyProviderError(err)
	}
	activity.RecordHeartbeat(ctx, map[string]any{
		"phase":          "submitted",
		"providerJobId":  result.JobID,
		"upstreamTaskId": result.UpstreamTaskID,
		"state":          result.State,
	})

	for !providercontract.Terminal(result.State) {
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			return ProviderResult{}, err
		}
		result, err = mockprovider.Get(ctx, a.HTTPClient, a.ProviderAdapterURL, result.JobID)
		if err != nil {
			return ProviderResult{}, classifyProviderError(err)
		}
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":          "reconciling",
			"providerJobId":  result.JobID,
			"upstreamTaskId": result.UpstreamTaskID,
			"state":          result.State,
			"progress":       result.Progress,
		})
		if result.State == providercontract.StatusRequiresAction {
			return ProviderResult{}, temporal.NewNonRetryableApplicationError(
				"provider job requires action",
				string(providercontract.CodeInvalidRequest),
				result.Error,
			)
		}
	}
	if result.State != providercontract.StatusSucceeded {
		if result.Error != nil {
			return ProviderResult{}, classifyProviderError(result.Error)
		}
		return ProviderResult{}, temporal.NewNonRetryableApplicationError(
			"provider job ended without an artifact",
			string(providercontract.CodeInvalidRequest),
			nil,
		)
	}
	if len(result.Artifacts) == 0 {
		return ProviderResult{}, errors.New("provider job succeeded without a committed artifact")
	}
	output := result.Artifacts[0]
	activity.RecordHeartbeat(ctx, map[string]any{
		"phase":          "committed",
		"upstreamTaskId": result.UpstreamTaskID,
		"artifactDigest": output.SHA256,
	})
	return ProviderResult{
		UpstreamTaskID: result.UpstreamTaskID,
		RequestID:      result.RequestID,
		ArtifactDigest: output.SHA256,
		ArtifactURI:    output.URI,
		Model:          result.Model,
		Usage:          result.Usage,
		Cost:           result.Cost,
	}, nil
}

func toPromptSnapshotRef(snapshot production.PromptSnapshot) PromptSnapshotRef {
	return PromptSnapshotRef{
		ID:                  snapshot.ID,
		Digest:              snapshot.ContentHash,
		PositivePrompt:      snapshot.PositivePrompt,
		NegativePrompt:      snapshot.NegativePrompt,
		Context:             snapshot.EffectiveContext.RevisionRefs,
		Assets:              snapshot.Assets,
		Output:              snapshot.Output,
		InputRevisionHashes: snapshot.InputRevisionHashes,
	}
}

// RunAutomaticQC is deliberately conservative in the skeleton: a committed,
// correctly addressed mock artifact passes structural QC only.
func (a *Activities) RunAutomaticQC(_ context.Context, input RunQCInput) (QCResult, error) {
	if input.Provider.ArtifactDigest == "" || input.Provider.ArtifactURI == "" {
		return QCResult{Passed: false, FailureCode: "QC_MEDIA_MISSING"}, nil
	}
	return QCResult{Passed: true}, nil
}

// CreateShotReview is a typed boundary for the control-plane ReviewTask write.
func (a *Activities) CreateShotReview(_ context.Context, input CreateReviewInput) error {
	if input.ShotSpecRevisionID == "" || input.RunID == "" || input.ArtifactDigest == "" {
		return errors.New("shot review requires shotSpecRevisionId, runId, and artifactDigest")
	}
	return nil
}

// EscalateShot records that two creative attempts were exhausted.
func (a *Activities) EscalateShot(_ context.Context, input EscalateShotInput) error {
	if input.ShotSpecRevisionID == "" {
		return errors.New("shotSpecRevisionId is required")
	}
	return nil
}

// CreateGate3 creates the final review task after every shot has a reviewable
// immutable artifact.
func (a *Activities) CreateGate3(_ context.Context, input CreateGate3Input) error {
	if input.EpisodeRevisionID == "" || len(input.RunIDs) == 0 {
		return errors.New("G3 review requires episodeRevisionId and runIds")
	}
	return nil
}

func classifyProviderError(err error) error {
	var providerErr *providercontract.Error
	if !errors.As(err, &providerErr) {
		return err
	}
	if providerErr.Retryable {
		return err
	}
	return temporal.NewNonRetryableApplicationError(
		providerErr.SafeMessage,
		string(providerErr.Code),
		err,
	)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ActivityTimeout is exported for manifest/config documentation tests.
const ActivityTimeout = 30 * time.Minute
