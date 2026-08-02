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
	"reflect"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/production"
	"github.com/google/uuid"
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
	ResolvePromptSnapshot(context.Context, string) (PromptSnapshotRef, error)
	CreateWorkflowRun(context.Context, WorkflowStep, CreateRunInput) (GenerationRunRef, error)
	PrepareProviderJob(context.Context, WorkflowStep, ExecuteProviderJobInput) (PreparedProviderJob, error)
	RecordProviderJobObservation(context.Context, WorkflowStep, ExecuteProviderJobInput, ProviderJobObservation) error
	CompleteProviderJob(context.Context, WorkflowStep, ExecuteProviderJobInput, ProviderResult) error
	RecordAutomaticQC(context.Context, WorkflowStep, RunQCInput, QCResult) error
	OpenShotReview(context.Context, WorkflowStep, CreateReviewInput) error
	RecordShotIntervention(context.Context, WorkflowStep, EscalateShotInput) error
	BuildEpisodeManifest(context.Context, WorkflowStep, CreateGate3Input) ([]byte, error)
	CommitEpisodeManifest(context.Context, WorkflowStep, CreateGate3Input, []byte, artifactstore.Artifact) error
	ProviderJobPrepared(context.Context, string) (bool, error)
	RecordProviderCancellation(context.Context, WorkflowStep, CancelProviderJobInput, CancelProviderResult) error
	FinalizeShotRun(context.Context, WorkflowStep, FinalizeShotRunInput) error
}

type PostProductionLedger interface {
	PrepareEpisodePostProduction(
		context.Context,
		WorkflowStep,
		FinalizeEpisodeInput,
	) (postproduction.Request, error)
	AuthorizeEpisodePostProduction(
		context.Context,
		WorkflowStep,
		FinalizeEpisodeInput,
	) error
	CommitEpisodePostProduction(
		context.Context,
		WorkflowStep,
		FinalizeEpisodeInput,
		postproduction.Result,
	) error
}

// Activities executes side effects at the Temporal Activity boundary. Temporal
// persists orchestration history; Journal persists an independently queryable
// input digest, result, audit record, and outbox event. Credentials never enter
// workflow inputs or the journal.
type Activities struct {
	ProviderAdapterURL string
	HTTPClient         *http.Client
	PromptSource       PromptSource
	Journal            WorkflowStepJournal
	Production         ProductionLedger
	Artifacts          *artifactstore.Store
	PostProduction     postproduction.Executor
	PostProductionData PostProductionLedger
}

// PromptSource loads and compiles exact persisted shot/context/asset
// revisions. It is used by isolated compiler integrations; persisted workflow
// executions use ProductionLedger so the full product projection remains
// transactional and queryable.
type PromptSource interface {
	CompilePrompt(context.Context, string, string) (production.PromptSnapshot, error)
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

func (a *Activities) ConfigurePostProduction(
	executor postproduction.Executor,
	ledger PostProductionLedger,
) {
	a.PostProduction = executor
	a.PostProductionData = ledger
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
// production. The deterministic fallback is explicitly mock-only and retained
// for isolated workflow replay tests without the product repository.
func (a *Activities) CompilePrompt(ctx context.Context, input CompilePromptInput) (PromptSnapshotRef, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (PromptSnapshotRef, error) {
		if input.ShotSpecRevisionID == "" || input.GenerationProfileRef == "" {
			return PromptSnapshotRef{}, errors.New("shotSpecRevisionId and generationProfileRef are required")
		}
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
		if a.PromptSource != nil {
			snapshot, err := a.PromptSource.CompilePrompt(
				ctx,
				input.ShotSpecRevisionID,
				input.GenerationProfileRef,
			)
			if err != nil {
				return PromptSnapshotRef{}, fmt.Errorf("compile production prompt: %w", err)
			}
			if err := snapshot.ValidateIntegrity(); err != nil {
				return PromptSnapshotRef{}, fmt.Errorf("verify production prompt snapshot: %w", err)
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
	})
}

// CreateRun makes production runs idempotent PostgreSQL transactions with an
// outbox row. Isolated workflow tests use a deterministic in-memory identity.
func (a *Activities) CreateRun(ctx context.Context, input CreateRunInput) (GenerationRunRef, error) {
	if input.PersistProductTruth {
		if approvalID, err := uuid.Parse(input.BudgetApprovalID); err == nil {
			input.BudgetApprovalID = approvalID.String()
		}
	}
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
	if input.PersistProductTruth {
		if approvalID, err := uuid.Parse(input.BudgetApprovalID); err == nil {
			input.BudgetApprovalID = approvalID.String()
		}
		// The normalized value becomes part of the Activity journal digest and
		// immutable Provider dispatch. Equivalent UUID spellings therefore
		// cannot fork either idempotency or cumulative budget accounting.
	}
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (ProviderResult, error) {
		var step WorkflowStep
		var prepared PreparedProviderJob
		if input.PersistProductTruth {
			if a.Production == nil {
				return ProviderResult{}, errors.New("production ledger is required")
			}
			var err error
			step, err = currentWorkflowStep(ctx, input.TraceID)
			if err != nil {
				return ProviderResult{}, err
			}
			exactPrompt, err := a.Production.ResolvePromptSnapshot(ctx, input.Prompt.ID)
			if err != nil {
				return ProviderResult{}, classifyPostProductionError(err)
			}
			if input.Prompt.ID != exactPrompt.ID || input.Prompt.Digest != exactPrompt.Digest {
				return ProviderResult{}, temporal.NewNonRetryableApplicationError(
					"prompt snapshot identity differs from the persisted immutable record",
					string(controlplane.CodeRevisionConflict),
					nil,
				)
			}
			if promptSnapshotCarriesExecutionFields(input.Prompt) &&
				!reflect.DeepEqual(input.Prompt, exactPrompt) {
				return ProviderResult{}, temporal.NewNonRetryableApplicationError(
					"prompt snapshot content differs from the persisted immutable record",
					string(controlplane.CodeRevisionConflict),
					nil,
				)
			}
			input.Prompt = exactPrompt
			prepared, err = a.Production.PrepareProviderJob(ctx, step, input)
			if err != nil {
				return ProviderResult{}, classifyPostProductionError(err)
			}
		}
		result, err := a.executeProviderJob(ctx, step, input, prepared)
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
				return ProviderResult{}, classifyPostProductionError(err)
			}
		}
		return result, nil
	})
}

func promptSnapshotCarriesExecutionFields(prompt PromptSnapshotRef) bool {
	return prompt.PositivePrompt != "" ||
		prompt.NegativePrompt != "" ||
		prompt.Context != (providercontract.ContextRefs{}) ||
		len(prompt.Assets) != 0 ||
		prompt.Output != (providercontract.OutputSpec{}) ||
		len(prompt.InputRevisionHashes) != 0
}

func (a *Activities) executeProviderJob(
	ctx context.Context,
	step WorkflowStep,
	input ExecuteProviderJobInput,
	prepared PreparedProviderJob,
) (ProviderResult, error) {
	if a.HTTPClient == nil {
		return ProviderResult{}, errors.New("provider HTTP client is required")
	}
	activity.RecordHeartbeat(ctx, map[string]any{"phase": "submitting", "runId": input.Run.RunID})
	budget := prepared.Budget
	budgetReservation := prepared.BudgetReservation
	if !input.PersistProductTruth {
		budget = providercontract.BudgetEnvelope{
			EstimatedCostMicros: input.BudgetMaximumMicros,
			MaxCostMicros:       input.BudgetMaximumMicros,
			MaxAttempts:         2,
		}
		var err error
		budgetReservation, err = providercontract.BindBudgetReservation(
			providercontract.BudgetReservation{
				ReservationID:  input.BudgetApprovalID,
				Currency:       input.BudgetCurrency,
				AmountMicros:   input.BudgetMaximumMicros,
				PricingVersion: "workflow-approved-v1",
				ConfirmedBy:    input.BudgetApprovalID,
			},
			providercontract.BudgetBindingInput{
				RunID:     input.Run.RunID,
				InputHash: input.Run.RunSpecDigest,
				Model:     input.Route,
				Budget:    budget,
			},
		)
		if err != nil {
			return ProviderResult{}, fmt.Errorf("bind budget approval: %w", err)
		}
	}
	jobRequest, err := BuildProviderJobRequest(input, PreparedProviderJob{
		Budget: budget, BudgetReservation: budgetReservation,
	})
	if err != nil {
		return ProviderResult{}, err
	}
	var result providercontract.JobResponse
	if prepared.ReconcileOnly {
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase": "reconciling", "providerJobId": jobRequest.JobID,
		})
		result, err = mockprovider.Get(
			ctx, a.HTTPClient, a.ProviderAdapterURL, jobRequest.JobID,
		)
	} else {
		if input.PersistProductTruth {
			// Freeze the paid-boundary intent before POST. A crash or timeout after
			// this commit can only reconcile the stable JobID; it can never issue a
			// second paid submit.
			if err := a.Production.RecordProviderJobObservation(
				ctx, step, input, ProviderJobObservation{
					State: "UNKNOWN", ErrorCode: "PROVIDER_SUBMISSION_PENDING",
				},
			); err != nil {
				return ProviderResult{}, classifyPostProductionError(err)
			}
		}
		result, err = mockprovider.Submit(
			ctx, a.HTTPClient, a.ProviderAdapterURL, jobRequest,
		)
	}
	if err != nil {
		if input.PersistProductTruth &&
			(prepared.ReconcileOnly || providerSubmissionMayExist(err)) {
			observationErr := a.Production.RecordProviderJobObservation(
				ctx, step, input, ProviderJobObservation{
					State: "UNKNOWN", ErrorCode: "PROVIDER_OUTCOME_UNKNOWN",
				},
			)
			if observationErr != nil {
				// Workflow cancellation can arrive after the provider request has
				// already failed but before this best-effort UNKNOWN projection can
				// open its transaction. Preserve the provider failure in that race:
				// Temporal must record the in-flight ActivityFailure (with
				// RetryState=CancelRequested), while the cancellation compensator
				// owns durable reconciliation of the same stable JobID.
				if ctx.Err() != nil {
					return ProviderResult{}, classifyProviderError(err)
				}
				return ProviderResult{}, classifyPostProductionError(observationErr)
			}
		}
		return ProviderResult{}, classifyProviderError(err)
	}
	if err := validateProviderJobResponse(result, jobRequest); err != nil {
		return ProviderResult{}, err
	}
	if input.PersistProductTruth {
		if err := a.Production.RecordProviderJobObservation(
			ctx, step, input, providerObservation(result),
		); err != nil {
			return ProviderResult{}, classifyPostProductionError(err)
		}
	}
	activity.RecordHeartbeat(ctx, map[string]any{
		"phase":          "submitted",
		"providerJobId":  result.JobID,
		"upstreamTaskId": result.UpstreamTaskID,
		"state":          result.State,
	})

	for !providercontract.Terminal(result.State) {
		if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
			return ProviderResult{}, err
		}
		result, err = mockprovider.Get(ctx, a.HTTPClient, a.ProviderAdapterURL, jobRequest.JobID)
		if err != nil {
			if input.PersistProductTruth {
				observationErr := a.Production.RecordProviderJobObservation(
					ctx, step, input, ProviderJobObservation{
						State: "UNKNOWN", ErrorCode: "PROVIDER_POLL_UNKNOWN",
					},
				)
				if observationErr != nil {
					if ctx.Err() != nil {
						return ProviderResult{}, classifyProviderError(err)
					}
					return ProviderResult{}, classifyPostProductionError(observationErr)
				}
			}
			return ProviderResult{}, classifyProviderError(err)
		}
		if err := validateProviderJobResponse(result, jobRequest); err != nil {
			return ProviderResult{}, err
		}
		if input.PersistProductTruth {
			if err := a.Production.RecordProviderJobObservation(
				ctx, step, input, providerObservation(result),
			); err != nil {
				return ProviderResult{}, classifyPostProductionError(err)
			}
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

func validateProviderJobResponse(
	response providercontract.JobResponse,
	request providercontract.JobRequest,
) error {
	if response.JobID != request.JobID || response.RunID != request.RunID ||
		response.Model != request.Model {
		return temporal.NewNonRetryableApplicationError(
			"provider response differs from the immutable dispatch",
			string(controlplane.CodeRevisionConflict),
			nil,
		)
	}
	return nil
}

func providerSubmissionMayExist(err error) bool {
	switch providercontract.ErrorCodeOf(err) {
	case providercontract.CodeUnavailable, providercontract.CodeTimeout, "":
		return true
	default:
		return false
	}
}

func providerObservation(response providercontract.JobResponse) ProviderJobObservation {
	state := "UNKNOWN"
	switch response.State {
	case providercontract.StatusQueued:
		// A successful POST response proves submission even when a provider does
		// not allocate its upstream task id until later. RUNNING suppresses every
		// future paid POST and forces stable JobID reconciliation.
		state = "RUNNING"
	case providercontract.StatusRunning:
		state = "RUNNING"
	case providercontract.StatusRequiresAction:
		state = "REQUIRES_ACTION"
	case providercontract.StatusSucceeded:
		// Product truth is not terminal until the artifact and cost commit in the
		// same PostgreSQL transaction. RUNNING still suppresses a second submit.
		state = "RUNNING"
	}
	errorCode := ""
	if response.Error != nil {
		errorCode = string(response.Error.Code)
	}
	return ProviderJobObservation{
		State: state, UpstreamTaskID: response.UpstreamTaskID,
		RequestID: response.RequestID, ErrorCode: errorCode,
	}
}

// BuildProviderJobRequest is the single prompt-bearing Provider envelope
// builder shared by Temporal and the formal Stage 1 runner. Callers may only
// invoke it after PostgreSQL PrepareProviderJob returns the durable budget
// reservation; the prompt and output specification must be the exact resolved
// snapshot, never stdin-supplied execution truth.
func BuildProviderJobRequest(
	input ExecuteProviderJobInput,
	prepared PreparedProviderJob,
) (providercontract.JobRequest, error) {
	jobID := "provider-job-" + input.Run.RunID
	promptText := input.Prompt.PositivePrompt
	if input.Prompt.NegativePrompt != "" {
		promptText += "\nNEGATIVE CONSTRAINTS: " + input.Prompt.NegativePrompt
	}
	if promptText == "" {
		return providercontract.JobRequest{}, errors.New("immutable compiled prompt text is required")
	}
	outputSpec := input.Prompt.Output
	if outputSpec.Width <= 0 || outputSpec.Height <= 0 || outputSpec.DurationMillis <= 0 {
		return providercontract.JobRequest{}, errors.New("immutable compiled output specification is required")
	}
	if err := prepared.BudgetReservation.ValidateFor(providercontract.BudgetBindingInput{
		RunID: input.Run.RunID, InputHash: input.Run.RunSpecDigest,
		Model: input.Route, Budget: prepared.Budget,
	}); err != nil {
		return providercontract.JobRequest{}, fmt.Errorf("validate prepared budget allocation: %w", err)
	}
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: jobID, RunID: input.Run.RunID,
		Capability: providercontract.CapabilityVideo, InputHash: input.Run.RunSpecDigest,
		Model: input.Route, TraceID: input.TraceID,
		Request: providercontract.GenerationRequest{
			RequestID: jobID, IdempotencyKey: jobID,
			Modality: providercontract.ModalityVideo, Prompt: promptText,
			PromptSnapshotID: input.Prompt.ID, Context: input.Prompt.Context,
			Assets: input.Prompt.Assets, Output: outputSpec,
			ModelHint: input.Route.ModelID, Budget: prepared.Budget,
		},
		BudgetReservation: prepared.BudgetReservation,
	}
	if err := request.Validate(); err != nil {
		return providercontract.JobRequest{}, fmt.Errorf("validate immutable Provider request: %w", err)
	}
	return request, nil
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

// FinalizeEpisode prepares immutable clip/context/prompt inputs from
// PostgreSQL, performs idempotent speech calls, runs deterministic FFmpeg, and
// commits the resulting evidence before G3 can open.
func (a *Activities) FinalizeEpisode(
	ctx context.Context,
	input FinalizeEpisodeInput,
) (postproduction.Result, error) {
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (postproduction.Result, error) {
		if input.EpisodeRevisionID == "" || len(input.RunIDs) == 0 {
			return postproduction.Result{}, errors.New("episode finalization requires episodeRevisionId and runIds")
		}
		if err := input.Config.Validate(); err != nil {
			return postproduction.Result{}, err
		}
		if a.PostProduction == nil || a.PostProductionData == nil {
			return postproduction.Result{}, errors.New("post-production executor and ledger are required")
		}
		step, err := currentWorkflowStep(ctx, input.TraceID)
		if err != nil {
			return postproduction.Result{}, err
		}
		request, err := a.PostProductionData.PrepareEpisodePostProduction(ctx, step, input)
		if err != nil {
			return postproduction.Result{}, classifyPostProductionError(err)
		}
		// Prepare resolves immutable inputs; this second transactional check is
		// intentionally adjacent to the paid provider boundary so a rights
		// revocation after Prepare fails closed before speech submission.
		if err := a.PostProductionData.AuthorizeEpisodePostProduction(ctx, step, input); err != nil {
			return postproduction.Result{}, classifyPostProductionError(err)
		}
		request.AuthorizePaidSubmit = func(submitCtx context.Context, _ postproduction.Cue) error {
			return classifyPostProductionError(
				a.PostProductionData.AuthorizeEpisodePostProduction(submitCtx, step, input),
			)
		}
		result, err := finalizePostProductionWithHeartbeat(
			ctx, a.PostProduction, request, input.EpisodeRevisionID,
		)
		if errors.Is(err, postproduction.ErrPendingKey) {
			return postproduction.Result{}, temporal.NewNonRetryableApplicationError(
				err.Error(), "PENDING_KEY", err,
			)
		}
		if err != nil {
			return postproduction.Result{}, classifyPostProductionError(err)
		}
		if err := a.PostProductionData.CommitEpisodePostProduction(ctx, step, input, result); err != nil {
			return postproduction.Result{}, classifyPostProductionError(err)
		}
		return result, nil
	})
}

func finalizePostProductionWithHeartbeat(
	ctx context.Context,
	executor postproduction.Executor,
	request postproduction.Request,
	episodeRevisionID string,
) (postproduction.Result, error) {
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase": "post-production", "episodeRevisionId": episodeRevisionID,
		})
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, map[string]any{
					"phase": "post-production", "episodeRevisionId": episodeRevisionID,
				})
			}
		}
	}()
	result, err := executor.Finalize(ctx, request)
	close(stop)
	<-stopped
	return result, err
}

// CreateGate3 creates the final review task after every shot has a reviewable
// immutable artifact.
func (a *Activities) CreateGate3(ctx context.Context, input CreateGate3Input) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		if input.EpisodeRevisionID == "" || len(input.RunIDs) == 0 {
			return struct{}{}, errors.New("G3 review requires episodeRevisionId and runIds")
		}
		if input.PostProductionManifestHash != "" &&
			!validPostProductionManifestHash(input.PostProductionManifestHash) {
			return struct{}{}, errors.New("post-production manifest hash is invalid")
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
				return struct{}{}, classifyPostProductionError(err)
			}
			artifact, err := a.Artifacts.Put(ctx, bytes.NewReader(manifest))
			if err != nil {
				return struct{}{}, fmt.Errorf("commit generation manifest to CAS: %w", err)
			}
			if err := a.Production.CommitEpisodeManifest(ctx, step, input, manifest, artifact); err != nil {
				return struct{}{}, classifyPostProductionError(err)
			}
		}
		return struct{}{}, nil
	})
	return err
}

func validPostProductionManifestHash(value string) bool {
	if len(value) != sha256.Size*2 ||
		value != strings.ToLower(value) ||
		value == strings.Repeat("0", sha256.Size*2) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// CancelProviderJob is an explicit compensation Activity. It uses the same
// stable provider JobID as submit and records cancellation, terminal-success
// races, or an UNKNOWN reconciliation state in PostgreSQL.
func (a *Activities) CancelProviderJob(
	ctx context.Context,
	input CancelProviderJobInput,
) (CancelProviderResult, error) {
	if input.Dispatch.PersistProductTruth {
		if approvalID, err := uuid.Parse(input.Dispatch.BudgetApprovalID); err == nil {
			input.Dispatch.BudgetApprovalID = approvalID.String()
		}
	}
	return journalActivity(ctx, a.Journal, input.TraceID, input, func() (CancelProviderResult, error) {
		step, err := currentWorkflowStep(ctx, input.TraceID)
		if err != nil {
			return CancelProviderResult{}, err
		}
		if input.Dispatch.PersistProductTruth {
			if a.Production == nil {
				return CancelProviderResult{}, errors.New("production ledger is required")
			}
			prepared, err := a.Production.ProviderJobPrepared(ctx, input.Dispatch.Run.RunID)
			if err != nil {
				return CancelProviderResult{}, err
			}
			if !prepared {
				result := CancelProviderResult{State: "CANCELLED", NoRemoteTask: true}
				if err := a.Production.RecordProviderCancellation(ctx, step, input, result); err != nil {
					return CancelProviderResult{}, classifyPostProductionError(err)
				}
				return result, nil
			}
		}
		jobID := "provider-job-" + input.Dispatch.Run.RunID
		response, cancelErr := mockprovider.Cancel(ctx, a.HTTPClient, a.ProviderAdapterURL, jobID)
		result := CancelProviderResult{State: "UNKNOWN", ErrorCode: "CANCEL_NOT_CONFIRMED"}
		if providercontract.ErrorCodeOf(cancelErr) == providercontract.CodeNotFound {
			// The provider's stable task registry is authoritative: a recovered
			// provider reporting that the idempotent JobID does not exist proves
			// there is no upstream task left to cancel.
			result.State = "CANCELLED"
			result.NoRemoteTask = true
			result.ErrorCode = ""
		} else if cancelErr == nil {
			if response.JobID != jobID ||
				response.RunID != input.Dispatch.Run.RunID ||
				response.Model != input.Dispatch.Route {
				return CancelProviderResult{}, temporal.NewNonRetryableApplicationError(
					"provider cancellation response differs from the immutable dispatch",
					string(controlplane.CodeRevisionConflict),
					nil,
				)
			}
			result.UpstreamTaskID = response.UpstreamTaskID
			result.RequestID = response.RequestID
			switch response.State {
			case providercontract.StatusSucceeded:
				if len(response.Artifacts) > 0 {
					result.State = "SUCCEEDED"
					result.ErrorCode = ""
				}
			case providercontract.StatusCancelled:
				result.State = "CANCELLED"
				result.ErrorCode = ""
				result.Usage = response.Usage
				result.Cost = response.Cost
			case providercontract.StatusFailed:
				result.State = "FAILED"
				result.Usage = response.Usage
				result.Cost = response.Cost
				result.ErrorCode = "PROVIDER_FAILED"
				if response.Error != nil && response.Error.Code != "" {
					result.ErrorCode = string(response.Error.Code)
				}
			default:
				result.State = "UNKNOWN"
			}
		}
		if input.Dispatch.PersistProductTruth {
			if result.State == "SUCCEEDED" {
				if a.Artifacts == nil {
					return CancelProviderResult{}, errors.New("artifact store is required")
				}
				providerResult := providerResultFromResponse(response)
				completionDispatch := input.Dispatch
				exactPrompt, err := a.Production.ResolvePromptSnapshot(
					ctx, input.Dispatch.Prompt.ID,
				)
				if err != nil {
					return CancelProviderResult{}, err
				}
				if input.Dispatch.Prompt.ID != exactPrompt.ID ||
					input.Dispatch.Prompt.Digest != exactPrompt.Digest {
					return CancelProviderResult{},
						controlplane.NewConflictError(
							controlplane.CodeRevisionConflict,
							"cancellation race Prompt differs from the persisted immutable record",
						)
				}
				completionDispatch.Prompt = exactPrompt
				exists, err := a.Artifacts.Exists(providerResult.ArtifactDigest)
				if err != nil {
					return CancelProviderResult{}, fmt.Errorf("verify raced provider artifact in CAS: %w", err)
				}
				if !exists {
					return CancelProviderResult{}, errors.New("raced provider result was not committed to CAS")
				}
				if err := a.Production.CompleteProviderJob(
					ctx, step, completionDispatch, providerResult,
				); err != nil {
					return CancelProviderResult{}, err
				}
			}
			if err := a.Production.RecordProviderCancellation(ctx, step, input, result); err != nil {
				return CancelProviderResult{}, classifyPostProductionError(err)
			}
		}
		if result.State == "UNKNOWN" {
			return CancelProviderResult{}, temporal.NewApplicationError(
				"provider cancellation is not yet confirmed",
				"CANCEL_NOT_CONFIRMED",
			)
		}
		return result, nil
	})
}

func providerResultFromResponse(response providercontract.JobResponse) ProviderResult {
	output := response.Artifacts[0]
	return ProviderResult{
		UpstreamTaskID: response.UpstreamTaskID,
		RequestID:      response.RequestID,
		ArtifactDigest: output.SHA256,
		ArtifactURI:    output.URI,
		MediaType:      output.MediaType,
		ArtifactSize:   output.SizeBytes,
		Width:          output.Width,
		Height:         output.Height,
		DurationMillis: output.DurationMillis,
		Model:          response.Model,
		Usage:          response.Usage,
		Cost:           response.Cost,
	}
}

func (a *Activities) FinalizeShotRun(ctx context.Context, input FinalizeShotRunInput) error {
	_, err := journalActivity(ctx, a.Journal, input.TraceID, input, func() (struct{}, error) {
		if a.Production == nil {
			return struct{}{}, errors.New("production ledger is required")
		}
		step, err := currentWorkflowStep(ctx, input.TraceID)
		if err != nil {
			return struct{}{}, err
		}
		if err := a.Production.FinalizeShotRun(ctx, step, input); err != nil {
			return struct{}{}, err
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

func classifyPostProductionError(err error) error {
	if err == nil {
		return nil
	}
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return applicationErr
	}
	var domainErr *controlplane.DomainError
	if errors.As(err, &domainErr) {
		switch domainErr.Code {
		case controlplane.CodeConsentRequired,
			controlplane.CodeLicenseBlocked,
			controlplane.CodeBudgetExceeded,
			controlplane.CodeConflict,
			controlplane.CodeGateRequired,
			controlplane.CodeContentBlocked,
			controlplane.CodeRevisionConflict,
			controlplane.CodeStaleDependency,
			controlplane.CodeCapability:
			return temporal.NewNonRetryableApplicationError(
				domainErr.Error(),
				string(domainErr.Code),
				err,
			)
		}
	}
	return classifyProviderError(err)
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
