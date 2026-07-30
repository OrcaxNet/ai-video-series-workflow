package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// WorkflowStep identifies one durable Temporal Activity execution. ActivityID
// is stable across infrastructure retries, while ActivityType is useful for
// audit and operations views.
type WorkflowStep struct {
	WorkflowID   string
	ActivityID   string
	ActivityType string
	TraceID      string
}

// WorkflowStepJournal is the PostgreSQL durability boundary around Activities.
// Begin returns a previously committed JSON result when a Temporal retry has
// already completed the step. An existing in-progress record is intentionally
// not considered complete: the Activity reconciles its idempotent provider job.
type WorkflowStepJournal interface {
	BeginWorkflowStep(context.Context, WorkflowStep, string) (json.RawMessage, bool, error)
	CompleteWorkflowStep(context.Context, WorkflowStep, string, json.RawMessage) error
}

// ProductionLedger projects durable workflow progress into the normalized
// PostgreSQL product model. Temporal history remains the execution source of
// truth; these methods make every prompt, run, provider job, QC result, review,
// and manifest independently queryable and approvable.
type ProductionLedger interface {
	CompilePromptSnapshot(context.Context, WorkflowStep, CompilePromptInput) (PromptSnapshotRef, error)
	CreateWorkflowRun(context.Context, WorkflowStep, CreateRunInput) (GenerationRunRef, error)
	PrepareProviderJob(context.Context, WorkflowStep, ExecuteProviderJobInput) error
	CompleteProviderJob(context.Context, WorkflowStep, ExecuteProviderJobInput, ProviderResult) error
	RecordAutomaticQC(context.Context, WorkflowStep, RunQCInput, QCResult) error
	OpenShotReview(context.Context, WorkflowStep, CreateReviewInput) error
	RecordShotIntervention(context.Context, WorkflowStep, EscalateShotInput) error
	BuildEpisodeManifest(context.Context, WorkflowStep, CreateGate3Input) ([]byte, error)
	CommitEpisodeManifest(context.Context, WorkflowStep, CreateGate3Input, []byte, artifactstore.Artifact) error
}

// Activities executes side effects at the Temporal Activity boundary. Temporal
// persists orchestration history; Journal persists an independently queryable
// input digest, result, audit record, and outbox event. Credentials never enter
// workflow inputs or the journal.
type Activities struct {
	ProviderAdapterURL string
	HTTPClient         *http.Client
	Journal            WorkflowStepJournal
	Production         ProductionLedger
	Artifacts          *artifactstore.Store
}

// NewActivities creates bounded Activity clients.
func NewActivities(providerAdapterURL string) *Activities {
	return &Activities{ProviderAdapterURL: providerAdapterURL, HTTPClient: mockprovider.DefaultHTTPClient()}
}

// NewActivitiesWithJournal creates production Activities with durable step
// replay. A nil journal remains useful for isolated workflow tests.
func NewActivitiesWithJournal(providerAdapterURL string, journal WorkflowStepJournal) *Activities {
	activities := NewActivities(providerAdapterURL)
	activities.Journal = journal
	return activities
}

// NewProductionActivities enables the normalized product projection and CAS
// manifest writer used by the runnable worker.
func NewProductionActivities(
	providerAdapterURL string,
	journal WorkflowStepJournal,
	production ProductionLedger,
	artifacts *artifactstore.Store,
) *Activities {
	activities := NewActivitiesWithJournal(providerAdapterURL, journal)
	activities.Production = production
	activities.Artifacts = artifacts
	return activities
}

// ValidateBatch rejects unversioned or unapproved production inputs.
func (a *Activities) ValidateBatch(ctx context.Context, input EpisodeProductionInput) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		return struct{}{}, validateWorkflowInput(input)
	})
	return err
}

// CompilePrompt persists the full effective-context and evidence chain in
// production. The deterministic fallback is retained for isolated workflow
// replay tests that intentionally run without the product repository.
func (a *Activities) CompilePrompt(ctx context.Context, input CompilePromptInput) (PromptSnapshotRef, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (PromptSnapshotRef, error) {
		if input.PersistProductTruth {
			if a.Production == nil {
				return PromptSnapshotRef{}, errors.New("production ledger is required")
			}
			step, err := currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return PromptSnapshotRef{}, err
			}
			return a.Production.CompilePromptSnapshot(ctx, step, input)
		}
		if input.ShotSpecRevisionID == "" || input.GenerationProfileRef == "" {
			return PromptSnapshotRef{}, errors.New("shotSpecRevisionId and generationProfileRef are required")
		}
		sum := sha256.Sum256([]byte(input.ShotSpecRevisionID + "\x00" + input.GenerationProfileRef))
		digest := hex.EncodeToString(sum[:])
		return PromptSnapshotRef{ID: "prompt-" + digest[:16], Digest: digest}, nil
	})
}

// CreateRun makes production runs idempotent PostgreSQL transactions with an
// outbox row. Isolated workflow tests use a deterministic in-memory identity.
func (a *Activities) CreateRun(ctx context.Context, input CreateRunInput) (GenerationRunRef, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (GenerationRunRef, error) {
		if input.PersistProductTruth {
			if a.Production == nil {
				return GenerationRunRef{}, errors.New("production ledger is required")
			}
			step, err := currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return GenerationRunRef{}, err
			}
			return a.Production.CreateWorkflowRun(ctx, step, input)
		}
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
	})
}

// ExecuteProviderJob submits and reconciles one idempotent remote API job.
// Activity retries reuse jobId and upstreamTaskId, so they never create a new
// paid attempt. UNKNOWN is polled; it is not treated as a failed generation.
func (a *Activities) ExecuteProviderJob(ctx context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (ProviderResult, error) {
		var step WorkflowStep
		if input.PersistProductTruth {
			if a.Production == nil {
				return ProviderResult{}, errors.New("production ledger is required")
			}
			var err error
			step, err = currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return ProviderResult{}, err
			}
			if err := a.Production.PrepareProviderJob(ctx, step, input); err != nil {
				return ProviderResult{}, err
			}
		}
		result, err := a.executeProviderJob(ctx, input)
		if err != nil {
			return ProviderResult{}, err
		}
		if input.PersistProductTruth {
			if a.Artifacts == nil {
				return ProviderResult{}, errors.New("artifact store is required")
			}
			exists, err := a.Artifacts.Exists(result.ArtifactDigest)
			if err != nil {
				return ProviderResult{}, fmt.Errorf("verify provider artifact in CAS: %w", err)
			}
			if !exists {
				return ProviderResult{}, errors.New("provider result was not committed to CAS")
			}
			if err := a.Production.CompleteProviderJob(ctx, step, input, result); err != nil {
				return ProviderResult{}, err
			}
		}
		return result, nil
	})
}

func (a *Activities) executeProviderJob(ctx context.Context, input ExecuteProviderJobInput) (ProviderResult, error) {
	if a.HTTPClient == nil {
		return ProviderResult{}, errors.New("provider HTTP client is required")
	}
	activity.RecordHeartbeat(ctx, map[string]any{"phase": "submitting", "runId": input.Run.RunID})
	jobID := "provider-job-" + input.Run.RunID
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
			Prompt:           "immutable prompt snapshot " + input.Prompt.Digest,
			PromptSnapshotID: input.Prompt.ID,
			Output: providercontract.OutputSpec{
				Width:          1280,
				Height:         720,
				AspectRatio:    "16:9",
				FPS:            24,
				DurationMillis: 5_000,
				Format:         "mp4",
			},
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
		MediaType:      output.MediaType,
		ArtifactSize:   output.SizeBytes,
		Width:          output.Width,
		Height:         output.Height,
		DurationMillis: output.DurationMillis,
		Model:          result.Model,
		Usage:          result.Usage,
		Cost:           result.Cost,
	}, nil
}

// RunAutomaticQC is deliberately conservative: a committed, correctly
// addressed artifact passes the current structural QC fixture.
func (a *Activities) RunAutomaticQC(ctx context.Context, input RunQCInput) (QCResult, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (QCResult, error) {
		if input.Provider.ArtifactDigest == "" || input.Provider.ArtifactURI == "" {
			result := QCResult{Passed: false, FailureCode: "QC_MEDIA_MISSING"}
			if err := a.recordQC(ctx, input, result); err != nil {
				return QCResult{}, err
			}
			return result, nil
		}
		result := QCResult{Passed: true}
		if err := a.recordQC(ctx, input, result); err != nil {
			return QCResult{}, err
		}
		return result, nil
	})
}

func (a *Activities) recordQC(ctx context.Context, input RunQCInput, result QCResult) error {
	if !input.PersistProductTruth {
		return nil
	}
	if a.Production == nil {
		return errors.New("production ledger is required")
	}
	step, err := currentWorkflowStep(ctx, input.TraceID)
	if err != nil {
		return err
	}
	return a.Production.RecordAutomaticQC(ctx, step, input, result)
}

// CreateShotReview is a typed boundary for the control-plane ReviewTask write.
func (a *Activities) CreateShotReview(ctx context.Context, input CreateReviewInput) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		if input.ShotSpecRevisionID == "" || input.RunID == "" || input.ArtifactDigest == "" {
			return struct{}{}, errors.New("shot review requires shotSpecRevisionId, runId, and artifactDigest")
		}
		if input.PersistProductTruth {
			if a.Production == nil {
				return struct{}{}, errors.New("production ledger is required")
			}
			step, err := currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return struct{}{}, err
			}
			if err := a.Production.OpenShotReview(ctx, step, input); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

// EscalateShot records that two creative attempts were exhausted.
func (a *Activities) EscalateShot(ctx context.Context, input EscalateShotInput) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		if input.ShotSpecRevisionID == "" {
			return struct{}{}, errors.New("shotSpecRevisionId is required")
		}
		if input.PersistProductTruth {
			if a.Production == nil {
				return struct{}{}, errors.New("production ledger is required")
			}
			step, err := currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return struct{}{}, err
			}
			if err := a.Production.RecordShotIntervention(ctx, step, input); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

// CreateGate3 creates the final review task after every shot has a reviewable
// immutable artifact.
func (a *Activities) CreateGate3(ctx context.Context, input CreateGate3Input) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		if input.EpisodeRevisionID == "" || len(input.RunIDs) == 0 {
			return struct{}{}, errors.New("G3 review requires episodeRevisionId and runIds")
		}
		if input.PersistProductTruth {
			if a.Production == nil || a.Artifacts == nil {
				return struct{}{}, errors.New("production ledger and artifact store are required")
			}
			step, err := currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return struct{}{}, err
			}
			manifest, err := a.Production.BuildEpisodeManifest(ctx, step, input)
			if err != nil {
				return struct{}{}, err
			}
			artifact, err := a.Artifacts.Put(ctx, bytes.NewReader(manifest))
			if err != nil {
				return struct{}{}, fmt.Errorf("commit generation manifest to CAS: %w", err)
			}
			if err := a.Production.CommitEpisodeManifest(ctx, step, input, manifest, artifact); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

func journalActivity[T any](
	ctx context.Context,
	journal WorkflowStepJournal,
	traceID string,
	input any,
	execute func() (T, error),
) (T, error) {
	var zero T
	if journal == nil {
		return execute()
	}
	info := activity.GetInfo(ctx)
	step := WorkflowStep{
		WorkflowID:   info.WorkflowExecution.ID,
		ActivityID:   info.ActivityID,
		ActivityType: info.ActivityType.Name,
		TraceID:      traceID,
	}
	if step.WorkflowID == "" || step.ActivityID == "" || step.ActivityType == "" {
		return zero, errors.New("Temporal Activity identity is required for durable journaling")
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return zero, fmt.Errorf("encode Activity journal input: %w", err)
	}
	sum := sha256.Sum256(inputJSON)
	inputHash := hex.EncodeToString(sum[:])
	replay, completed, err := journal.BeginWorkflowStep(ctx, step, inputHash)
	if err != nil {
		return zero, fmt.Errorf("begin Activity journal: %w", err)
	}
	if completed {
		var result T
		if err := json.Unmarshal(replay, &result); err != nil {
			return zero, fmt.Errorf("decode Activity journal result: %w", err)
		}
		return result, nil
	}
	result, err := execute()
	if err != nil {
		return zero, err
	}
	output, err := json.Marshal(result)
	if err != nil {
		return zero, fmt.Errorf("encode Activity journal result: %w", err)
	}
	if err := journal.CompleteWorkflowStep(ctx, step, inputHash, output); err != nil {
		return zero, fmt.Errorf("complete Activity journal: %w", err)
	}
	return result, nil
}

func currentWorkflowStep(ctx context.Context, traceID string) (WorkflowStep, error) {
	info := activity.GetInfo(ctx)
	step := WorkflowStep{
		WorkflowID:   info.WorkflowExecution.ID,
		ActivityID:   info.ActivityID,
		ActivityType: info.ActivityType.Name,
		TraceID:      traceID,
	}
	if step.WorkflowID == "" || step.ActivityID == "" || step.ActivityType == "" || step.TraceID == "" {
		return WorkflowStep{}, errors.New("Temporal Activity identity is required for product persistence")
	}
	return step, nil
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
