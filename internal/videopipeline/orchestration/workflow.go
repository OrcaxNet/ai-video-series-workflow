// Package orchestration defines the Temporal Workflow/Activity boundary for
// episode production. Workflow code owns durable ordering, infrastructure
// recovery, cancellation, creative-attempt limits, and the G3 wait state.
package orchestration

import (
	"errors"
	"fmt"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName        = "video.production.episode.v1"
	DefaultTaskQueue    = "video-production-v1"
	Gate3DecisionSignal = "video.production.gate3-decision.v1"
	StatusQuery         = "video.production.status.v1"

	ActivityValidateBatch      = "video.activity.validate-batch.v1"
	ActivityCompilePrompt      = "video.activity.compile-prompt.v1"
	ActivityCreateRun          = "video.activity.create-run.v1"
	ActivityExecuteProviderJob = "video.activity.execute-provider-job.v1"
	ActivityRunAutomaticQC     = "video.activity.run-automatic-qc.v1"
	ActivityCreateShotReview   = "video.activity.create-shot-review.v1"
	ActivityEscalateShot       = "video.activity.escalate-shot.v1"
	ActivityCreateGate3        = "video.activity.create-gate3-review.v1"
)

// EpisodeProductionInput freezes the exact production batch approved at G2.
type EpisodeProductionInput struct {
	SchemaVersion        string                         `json:"schemaVersion"`
	SeriesID             string                         `json:"seriesId"`
	EpisodeRevisionID    string                         `json:"episodeRevisionId"`
	ShotSpecRevisionIDs  []string                       `json:"shotSpecRevisionIds"`
	GenerationProfileRef string                         `json:"generationProfileRef"`
	Gate2DecisionID      string                         `json:"gate2DecisionId"`
	ProviderRoute        providercontract.ModelSnapshot `json:"providerRoute"`
	BudgetApprovalID     string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros  int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency       string                         `json:"budgetCurrency"`
	TraceID              string                         `json:"traceId"`
}

// EpisodeProductionResult is the durable terminal or intervention state.
type EpisodeProductionResult struct {
	State         string               `json:"state"`
	LockedRunIDs  []string             `json:"lockedRunIds"`
	FailedShotID  string               `json:"failedShotId,omitempty"`
	Gate3Decision *Gate3Decision       `json:"gate3Decision,omitempty"`
	Shots         map[string]ShotState `json:"shots"`
}

// ShotState is exposed through the workflow query handler.
type ShotState struct {
	State           string `json:"state"`
	CreativeAttempt int    `json:"creativeAttempt"`
	RunID           string `json:"runId,omitempty"`
	ArtifactDigest  string `json:"artifactDigest,omitempty"`
	FailureCode     string `json:"failureCode,omitempty"`
}

// WorkflowStatus is safe for UI polling and operational diagnostics.
type WorkflowStatus struct {
	State string               `json:"state"`
	Shots map[string]ShotState `json:"shots"`
}

// Gate3Decision is sent by the control plane after the exact cut, manifest,
// license BOM, consent set, subtitles, audio tracks, and AI marker are reviewed.
type Gate3Decision struct {
	DecisionID string `json:"decisionId"`
	Approved   bool   `json:"approved"`
	ReasonCode string `json:"reasonCode,omitempty"`
	ActorID    string `json:"actorId"`
}

// PromptSnapshotRef identifies an immutable compiled prompt.
type PromptSnapshotRef struct {
	ID                  string                       `json:"id"`
	Digest              string                       `json:"digest"`
	PositivePrompt      string                       `json:"positivePrompt"`
	NegativePrompt      string                       `json:"negativePrompt,omitempty"`
	Context             providercontract.ContextRefs `json:"context"`
	Assets              []providercontract.AssetRef  `json:"assets,omitempty"`
	Output              providercontract.OutputSpec  `json:"output"`
	InputRevisionHashes map[string]string            `json:"inputRevisionHashes"`
}

// GenerationRunRef identifies one creative generation attempt.
type GenerationRunRef struct {
	RunID         string `json:"runId"`
	RunSpecDigest string `json:"runSpecDigest"`
	Attempt       int    `json:"attempt"`
}

// ProviderResult identifies one remote API task and immutable local artifact.
// Temporary provider URLs and credentials are intentionally absent.
type ProviderResult struct {
	UpstreamTaskID string                         `json:"upstreamTaskId"`
	RequestID      string                         `json:"requestId"`
	ArtifactDigest string                         `json:"artifactDigest"`
	ArtifactURI    string                         `json:"artifactUri"`
	Model          providercontract.ModelSnapshot `json:"modelSnapshot"`
	Usage          providercontract.Usage         `json:"usage"`
	Cost           providercontract.Cost          `json:"cost"`
}

// QCResult drives explicit creative retry rather than Activity retry.
type QCResult struct {
	Passed      bool   `json:"passed"`
	FailureCode string `json:"failureCode,omitempty"`
}

// EpisodeProductionWorkflow runs a G2-approved production batch.
func EpisodeProductionWorkflow(ctx workflow.Context, input EpisodeProductionInput) (EpisodeProductionResult, error) {
	if err := validateWorkflowInput(input); err != nil {
		return EpisodeProductionResult{}, temporal.NewNonRetryableApplicationError(err.Error(), "VALIDATION_ERROR", err)
	}

	status := WorkflowStatus{State: "VALIDATING", Shots: make(map[string]ShotState, len(input.ShotSpecRevisionIDs))}
	for _, shotID := range input.ShotSpecRevisionIDs {
		status.Shots[shotID] = ShotState{State: "READY"}
	}
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (WorkflowStatus, error) {
		return status, nil
	}); err != nil {
		return EpisodeProductionResult{}, fmt.Errorf("register status query: %w", err)
	}

	baseOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
			NonRetryableErrorTypes: []string{
				"VALIDATION_ERROR",
				"LICENSE_BLOCKED",
				string(providercontract.CodeBudgetExceeded),
				string(providercontract.CodeUnauthenticated),
				string(providercontract.CodeForbidden),
				string(providercontract.CodeQuotaExceeded),
				string(providercontract.CodeContentBlocked),
				string(providercontract.CodeRegionUnavailable),
				string(providercontract.CodeModelUnavailable),
			},
		},
	}
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityValidateBatch, input).Get(ctx, nil); err != nil {
		return EpisodeProductionResult{}, err
	}

	status.State = "PRODUCING"
	lockedRunIDs := make([]string, 0, len(input.ShotSpecRevisionIDs))
	for _, shotID := range input.ShotSpecRevisionIDs {
		var prompt PromptSnapshotRef
		compileInput := CompilePromptInput{
			ShotSpecRevisionID:   shotID,
			GenerationProfileRef: input.GenerationProfileRef,
			TraceID:              input.TraceID,
		}
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCompilePrompt, compileInput).Get(ctx, &prompt); err != nil {
			return EpisodeProductionResult{}, err
		}

		var accepted bool
		for creativeAttempt := 1; creativeAttempt <= 2; creativeAttempt++ {
			shotStatus := ShotState{State: "CREATING_RUN", CreativeAttempt: creativeAttempt}
			status.Shots[shotID] = shotStatus

			var run GenerationRunRef
			createInput := CreateRunInput{
				ShotSpecRevisionID:   shotID,
				PromptSnapshot:       prompt,
				GenerationProfileRef: input.GenerationProfileRef,
				Route:                input.ProviderRoute,
				CreativeAttempt:      creativeAttempt,
				TraceID:              input.TraceID,
			}
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateRun, createInput).Get(ctx, &run); err != nil {
				return EpisodeProductionResult{}, err
			}
			shotStatus.State = "RUNNING"
			shotStatus.RunID = run.RunID
			status.Shots[shotID] = shotStatus

			providerOptions := baseOptions
			providerOptions.StartToCloseTimeout = 30 * time.Minute
			var generated ProviderResult
			dispatchInput := ExecuteProviderJobInput{
				Run:                 run,
				Prompt:              prompt,
				Route:               input.ProviderRoute,
				BudgetApprovalID:    input.BudgetApprovalID,
				BudgetMaximumMicros: input.BudgetMaximumMicros,
				BudgetCurrency:      input.BudgetCurrency,
				TraceID:             input.TraceID,
			}
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, providerOptions), ActivityExecuteProviderJob, dispatchInput).Get(ctx, &generated); err != nil {
				return EpisodeProductionResult{}, err
			}

			shotStatus.State = "QC_PENDING"
			shotStatus.ArtifactDigest = generated.ArtifactDigest
			status.Shots[shotID] = shotStatus
			var qc QCResult
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityRunAutomaticQC, RunQCInput{
				Run: run, Provider: generated, TraceID: input.TraceID,
			}).Get(ctx, &qc); err != nil {
				return EpisodeProductionResult{}, err
			}
			if qc.Passed {
				if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateShotReview, CreateReviewInput{
					ShotSpecRevisionID: shotID,
					RunID:              run.RunID,
					ArtifactDigest:     generated.ArtifactDigest,
					TraceID:            input.TraceID,
				}).Get(ctx, nil); err != nil {
					return EpisodeProductionResult{}, err
				}
				shotStatus.State = "REVIEW"
				status.Shots[shotID] = shotStatus
				lockedRunIDs = append(lockedRunIDs, run.RunID)
				accepted = true
				break
			}
			shotStatus.State = "AUTO_QC_FAILED"
			shotStatus.FailureCode = qc.FailureCode
			status.Shots[shotID] = shotStatus
		}

		if !accepted {
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityEscalateShot, EscalateShotInput{
				ShotSpecRevisionID: shotID,
				FailureCode:        status.Shots[shotID].FailureCode,
				TraceID:            input.TraceID,
			}).Get(ctx, nil); err != nil {
				return EpisodeProductionResult{}, err
			}
			status.State = "NEEDS_INTERVENTION"
			return EpisodeProductionResult{
				State:        status.State,
				LockedRunIDs: lockedRunIDs,
				FailedShotID: shotID,
				Shots:        status.Shots,
			}, nil
		}
	}

	status.State = "WAITING_G3"
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateGate3, CreateGate3Input{
		EpisodeRevisionID: input.EpisodeRevisionID,
		RunIDs:            lockedRunIDs,
		TraceID:           input.TraceID,
	}).Get(ctx, nil); err != nil {
		return EpisodeProductionResult{}, err
	}

	var gate3 Gate3Decision
	workflow.GetSignalChannel(ctx, Gate3DecisionSignal).Receive(ctx, &gate3)
	if gate3.DecisionID == "" || gate3.ActorID == "" {
		return EpisodeProductionResult{}, temporal.NewNonRetryableApplicationError(
			"invalid G3 decision signal", "VALIDATION_ERROR", errors.New("decisionId and actorId are required"),
		)
	}
	if !gate3.Approved {
		status.State = "G3_REJECTED"
		return EpisodeProductionResult{
			State:         status.State,
			LockedRunIDs:  lockedRunIDs,
			Gate3Decision: &gate3,
			Shots:         status.Shots,
		}, nil
	}

	status.State = "LOCKED"
	return EpisodeProductionResult{
		State:         status.State,
		LockedRunIDs:  lockedRunIDs,
		Gate3Decision: &gate3,
		Shots:         status.Shots,
	}, nil
}

// Activity inputs are explicit and version-independent product contracts.
type CompilePromptInput struct {
	ShotSpecRevisionID   string `json:"shotSpecRevisionId"`
	GenerationProfileRef string `json:"generationProfileRef"`
	TraceID              string `json:"traceId"`
}

type CreateRunInput struct {
	ShotSpecRevisionID   string                         `json:"shotSpecRevisionId"`
	PromptSnapshot       PromptSnapshotRef              `json:"promptSnapshot"`
	GenerationProfileRef string                         `json:"generationProfileRef"`
	Route                providercontract.ModelSnapshot `json:"route"`
	CreativeAttempt      int                            `json:"creativeAttempt"`
	TraceID              string                         `json:"traceId"`
}

type ExecuteProviderJobInput struct {
	Run                 GenerationRunRef               `json:"run"`
	Prompt              PromptSnapshotRef              `json:"prompt"`
	Route               providercontract.ModelSnapshot `json:"route"`
	BudgetApprovalID    string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency      string                         `json:"budgetCurrency"`
	TraceID             string                         `json:"traceId"`
}

type RunQCInput struct {
	Run      GenerationRunRef `json:"run"`
	Provider ProviderResult   `json:"provider"`
	TraceID  string           `json:"traceId"`
}

type CreateReviewInput struct {
	ShotSpecRevisionID string `json:"shotSpecRevisionId"`
	RunID              string `json:"runId"`
	ArtifactDigest     string `json:"artifactDigest"`
	TraceID            string `json:"traceId"`
}

type EscalateShotInput struct {
	ShotSpecRevisionID string `json:"shotSpecRevisionId"`
	FailureCode        string `json:"failureCode"`
	TraceID            string `json:"traceId"`
}

type CreateGate3Input struct {
	EpisodeRevisionID string   `json:"episodeRevisionId"`
	RunIDs            []string `json:"runIds"`
	TraceID           string   `json:"traceId"`
}

func validateWorkflowInput(input EpisodeProductionInput) error {
	if input.SchemaVersion != "v1" {
		return errors.New("schemaVersion must be v1")
	}
	if input.SeriesID == "" || input.EpisodeRevisionID == "" || input.GenerationProfileRef == "" || input.Gate2DecisionID == "" {
		return errors.New("seriesId, episodeRevisionId, generationProfileRef, and gate2DecisionId are required")
	}
	if err := input.ProviderRoute.Validate(providercontract.CapabilityVideo); err != nil {
		return errors.New("a frozen video.primary providerRoute is required")
	}
	if input.BudgetApprovalID == "" || input.BudgetCurrency == "" || input.BudgetMaximumMicros <= 0 {
		return errors.New("budgetApprovalId, budgetCurrency, and positive budgetMaximumMicros are required")
	}
	if len(input.ShotSpecRevisionIDs) == 0 {
		return errors.New("at least one shotSpecRevisionId is required")
	}
	seen := make(map[string]struct{}, len(input.ShotSpecRevisionIDs))
	for _, shotID := range input.ShotSpecRevisionIDs {
		if shotID == "" {
			return errors.New("shotSpecRevisionId cannot be empty")
		}
		if _, ok := seen[shotID]; ok {
			return fmt.Errorf("duplicate shotSpecRevisionId %q", shotID)
		}
		seen[shotID] = struct{}{}
	}
	return nil
}
