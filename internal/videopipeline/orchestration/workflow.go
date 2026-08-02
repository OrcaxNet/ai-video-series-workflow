// Package orchestration defines the Temporal Workflow/Activity boundary for
// episode production. Workflow code owns durable ordering, infrastructure
// recovery, cancellation, creative-attempt limits, and the G3 wait state.
package orchestration

import (
	"errors"
	"fmt"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName                   = "video.production.episode.v1"
	Stage1FinalizationWorkflowName = "video.production.stage1-finalization.v1"
	DefaultTaskQueue               = "video-production-v1"
	Gate3DecisionSignal            = "video.production.gate3-decision.v1"
	ShotDecisionSignal             = "video.production.shot-decision.v1"
	ControlSignal                  = "video.production.control.v1"
	StatusQuery                    = "video.production.status.v1"

	ActivityValidateBatch      = "video.activity.validate-batch.v1"
	ActivityCompilePrompt      = "video.activity.compile-prompt.v1"
	ActivityCreateRun          = "video.activity.create-run.v1"
	ActivityExecuteProviderJob = "video.activity.execute-provider-job.v1"
	ActivityRunAutomaticQC     = "video.activity.run-automatic-qc.v1"
	ActivityCreateShotReview   = "video.activity.create-shot-review.v1"
	ActivityEscalateShot       = "video.activity.escalate-shot.v1"
	ActivityFinalizeEpisode    = "video.activity.finalize-episode.v1"
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
	GenerationPlanID     string                         `json:"generationPlanId,omitempty"`
	ProviderProfileID    string                         `json:"providerProfileId,omitempty"`
	ProviderRoute        providercontract.ModelSnapshot `json:"providerRoute"`
	BudgetApprovalID     string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros  int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency       string                         `json:"budgetCurrency"`
	TraceID              string                         `json:"traceId"`
	RequireShotApproval  bool                           `json:"requireShotApproval,omitempty"`
	PersistProductTruth  bool                           `json:"persistProductTruth,omitempty"`
	PostProduction       *PostProductionConfig          `json:"postProduction,omitempty"`
}

// EpisodeProductionResult is the durable terminal or intervention state.
type EpisodeProductionResult struct {
	State          string                 `json:"state"`
	LockedRunIDs   []string               `json:"lockedRunIds"`
	FailedShotID   string                 `json:"failedShotId,omitempty"`
	PostProduction *postproduction.Result `json:"postProduction,omitempty"`
	Gate3Decision  *Gate3Decision         `json:"gate3Decision,omitempty"`
	Shots          map[string]ShotState   `json:"shots"`
}

// Stage1FinalizationResult is the durable result of the formal Stage 1
// post-production-only workflow. Video submission remains exclusively owned by
// video-stage1-runner; this workflow consumes only its evidence-gated immutable
// FinalizeEpisodeInput and opens G3 after the committed post-production result.
type Stage1FinalizationResult struct {
	PostProduction postproduction.Result `json:"postProduction"`
	Gate3Created   bool                  `json:"gate3Created"`
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
	State  string               `json:"state"`
	Paused bool                 `json:"paused"`
	Shots  map[string]ShotState `json:"shots"`
}

// Gate3Decision is sent by the control plane after the exact cut, manifest,
// license BOM, consent set, subtitles, audio tracks, and AI marker are reviewed.
type Gate3Decision struct {
	DecisionID string `json:"decisionId"`
	Approved   bool   `json:"approved"`
	ReasonCode string `json:"reasonCode,omitempty"`
	ActorID    string `json:"actorId"`
}

// ShotDecision is an immutable Q1 decision for one exact run and artifact.
type ShotDecision struct {
	DecisionID         string `json:"decisionId"`
	ShotSpecRevisionID string `json:"shotSpecRevisionId"`
	RunID              string `json:"runId"`
	Approved           bool   `json:"approved"`
	ReasonCode         string `json:"reasonCode,omitempty"`
	ActorID            string `json:"actorId"`
}

// WorkflowControl pauses or resumes at deterministic Activity boundaries.
// Cancellation continues to use Temporal cancellation so in-flight Activities
// receive context cancellation and their heartbeat details remain recoverable.
type WorkflowControl struct {
	CommandID  string `json:"commandId"`
	Action     string `json:"action"`
	ActorID    string `json:"actorId"`
	ReasonCode string `json:"reasonCode,omitempty"`
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
	MediaType      string                         `json:"mediaType,omitempty"`
	ArtifactSize   int64                          `json:"artifactSizeBytes,omitempty"`
	Width          int                            `json:"width,omitempty"`
	Height         int                            `json:"height,omitempty"`
	DurationMillis int64                          `json:"durationMillis,omitempty"`
	Model          providercontract.ModelSnapshot `json:"modelSnapshot"`
	Usage          providercontract.Usage         `json:"usage"`
	Cost           providercontract.Cost          `json:"cost"`
}

// QCResult drives explicit creative retry rather than Activity retry.
type QCResult struct {
	Passed      bool   `json:"passed"`
	FailureCode string `json:"failureCode,omitempty"`
}

// PostProductionConfig freezes the independent speech route, budget, subtitle
// behavior, and optional licensed background asset before Temporal starts.
type PostProductionConfig struct {
	Enabled                       bool                               `json:"enabled"`
	Evidence                      string                             `json:"evidence"`
	SpeechRoute                   providercontract.ModelSnapshot     `json:"speechRoute"`
	SpeechProviderProfileID       string                             `json:"speechProviderProfileId"`
	SpeechBudgetApprovalID        string                             `json:"speechBudgetApprovalId"`
	SpeechBudgetMaximumMicros     int64                              `json:"speechBudgetMaximumMicros"`
	SpeechBudgetCurrency          string                             `json:"speechBudgetCurrency"`
	SpeechBillingMode             string                             `json:"speechBillingMode,omitempty"`
	SpeechIdentityVersion         string                             `json:"speechIdentityVersion,omitempty"`
	SpeechVoice                   *postproduction.SpeechVoiceBinding `json:"speechVoice,omitempty"`
	SpeechAuthorizedCueID         string                             `json:"speechAuthorizedCueId,omitempty"`
	SpeechMaximumAFPMilli         int64                              `json:"speechMaximumAfpMilli,omitempty"`
	SpeechMaximumCashMicros       int64                              `json:"speechMaximumNonSubscriptionCashMicros,omitempty"`
	SpeechMaxAttempts             int                                `json:"speechMaxAttempts,omitempty"`
	SpeechBatchAuthorization      *speechcontract.BatchAuthorization `json:"speechBatchAuthorization,omitempty"`
	SpeechCompletedAttempts       []postproduction.ProviderAttempt   `json:"speechCompletedAttempts,omitempty"`
	SubtitleLanguage              string                             `json:"subtitleLanguage"`
	BurnSubtitles                 bool                               `json:"burnSubtitles"`
	BackgroundAudioAssetVersionID string                             `json:"backgroundAudioAssetVersionId,omitempty"`
	EnforcePoCDuration            bool                               `json:"enforcePoCDuration"`
}

func (c PostProductionConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if err := c.SpeechRoute.Validate(providercontract.CapabilitySpeech); err != nil {
		return errors.New("a frozen speech.primary route is required")
	}
	if c.Evidence != postproduction.EvidenceMockOnly &&
		c.Evidence != postproduction.EvidenceLive &&
		c.Evidence != postproduction.EvidencePendingKey {
		return errors.New("post-production evidence is invalid")
	}
	subscription := c.SpeechBillingMode == providercontract.BillingModeSubscriptionIncludedOnly
	if c.SpeechProviderProfileID == "" || c.SpeechBudgetApprovalID == "" ||
		c.SpeechBudgetMaximumMicros < 0 ||
		(c.SpeechBudgetMaximumMicros == 0 && !subscription && c.SpeechRoute.Verification != providercontract.PendingKey) ||
		(c.SpeechBillingMode != "" && !subscription) ||
		len(c.SpeechBudgetCurrency) != 3 {
		return errors.New("post-production speech profile and approved budget are required")
	}
	if c.SubtitleLanguage == "" {
		return errors.New("post-production subtitle language is required")
	}
	if err := (postproduction.SpeechConfig{
		Route: c.SpeechRoute, ProviderProfileID: c.SpeechProviderProfileID,
		BudgetApprovalID:    c.SpeechBudgetApprovalID,
		BudgetMaximumMicros: c.SpeechBudgetMaximumMicros,
		BudgetCurrency:      c.SpeechBudgetCurrency,
		BillingMode:         c.SpeechBillingMode,
		IdentityVersion:     c.SpeechIdentityVersion, Voice: c.SpeechVoice,
		AuthorizedCueID:                  c.SpeechAuthorizedCueID,
		MaximumAFPMilli:                  c.SpeechMaximumAFPMilli,
		MaximumNonSubscriptionCashMicros: c.SpeechMaximumCashMicros,
		MaxAttempts:                      c.SpeechMaxAttempts,
		BatchAuthorization:               c.SpeechBatchAuthorization,
		CompletedAttempts:                c.SpeechCompletedAttempts,
	}).Validate(); err != nil {
		return err
	}
	return nil
}

// Stage1FinalizationWorkflow completes the already-generated formal Stage 1
// batch without recompiling prompts, creating Runs, or submitting video jobs.
// The caller cannot select Runs: video-stage1-runner derives this input from its
// immutable execution package and evidence-complete terminal ledger.
func Stage1FinalizationWorkflow(
	ctx workflow.Context,
	input FinalizeEpisodeInput,
) (Stage1FinalizationResult, error) {
	if input.EpisodeRevisionID == "" || len(input.RunIDs) == 0 ||
		input.GenerationPlanID == "" || input.TraceID == "" || !input.PersistProductTruth {
		err := errors.New("formal Stage 1 finalization requires persisted episode, Run, Plan, and trace identity")
		return Stage1FinalizationResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "VALIDATION_ERROR", err,
		)
	}
	if err := input.Config.Validate(); err != nil {
		return Stage1FinalizationResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "VALIDATION_ERROR", err,
		)
	}
	if !input.Config.Enabled || input.Config.Evidence != postproduction.EvidenceLive {
		err := errors.New("formal Stage 1 finalization requires live post-production evidence")
		return Stage1FinalizationResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "VALIDATION_ERROR", err,
		)
	}

	options := workflow.ActivityOptions{
		StartToCloseTimeout: 45 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
			NonRetryableErrorTypes: []string{
				"VALIDATION_ERROR", "LICENSE_BLOCKED", "CONSENT_REQUIRED",
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
	var finalized postproduction.Result
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		ActivityFinalizeEpisode,
		input,
	).Get(ctx, &finalized); err != nil {
		return Stage1FinalizationResult{}, err
	}

	gateOptions := options
	gateOptions.StartToCloseTimeout = 2 * time.Minute
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, gateOptions),
		ActivityCreateGate3,
		CreateGate3Input{
			EpisodeRevisionID:             input.EpisodeRevisionID,
			RunIDs:                        input.RunIDs,
			GenerationPlanID:              input.GenerationPlanID,
			PostProductionManifestHash:    finalized.ManifestHash,
			BackgroundAudioAssetVersionID: input.Config.BackgroundAudioAssetVersionID,
			TraceID:                       input.TraceID,
			PersistProductTruth:           true,
		},
	).Get(ctx, nil); err != nil {
		return Stage1FinalizationResult{}, err
	}
	return Stage1FinalizationResult{PostProduction: finalized, Gate3Created: true}, nil
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
	controlChannel := workflow.GetSignalChannel(ctx, ControlSignal)

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
				"CONSENT_REQUIRED",
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
	waitForResume(ctx, &status, controlChannel)

	status.State = "PRODUCING"
	lockedRunIDs := make([]string, 0, len(input.ShotSpecRevisionIDs))
	for _, shotID := range input.ShotSpecRevisionIDs {
		var prompt PromptSnapshotRef
		compileInput := CompilePromptInput{
			ShotSpecRevisionID:   shotID,
			GenerationProfileRef: input.GenerationProfileRef,
			TraceID:              input.TraceID,
			PersistProductTruth:  input.PersistProductTruth,
		}
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCompilePrompt, compileInput).Get(ctx, &prompt); err != nil {
			return EpisodeProductionResult{}, err
		}
		waitForResume(ctx, &status, controlChannel)

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
				GenerationPlanID:     input.GenerationPlanID,
				BudgetApprovalID:     input.BudgetApprovalID,
				ProviderProfileID:    input.ProviderProfileID,
				CreativeAttempt:      creativeAttempt,
				TraceID:              input.TraceID,
				PersistProductTruth:  input.PersistProductTruth,
			}
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateRun, createInput).Get(ctx, &run); err != nil {
				return EpisodeProductionResult{}, err
			}
			waitForResume(ctx, &status, controlChannel)
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
				ProviderProfileID:   input.ProviderProfileID,
				TraceID:             input.TraceID,
				PersistProductTruth: input.PersistProductTruth,
			}
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, providerOptions), ActivityExecuteProviderJob, dispatchInput).Get(ctx, &generated); err != nil {
				return EpisodeProductionResult{}, err
			}
			waitForResume(ctx, &status, controlChannel)

			shotStatus.State = "QC_PENDING"
			shotStatus.ArtifactDigest = generated.ArtifactDigest
			status.Shots[shotID] = shotStatus
			var qc QCResult
			if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityRunAutomaticQC, RunQCInput{
				Run: run, Provider: generated, TraceID: input.TraceID, PersistProductTruth: input.PersistProductTruth,
			}).Get(ctx, &qc); err != nil {
				return EpisodeProductionResult{}, err
			}
			waitForResume(ctx, &status, controlChannel)
			if qc.Passed {
				if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateShotReview, CreateReviewInput{
					ShotSpecRevisionID:  shotID,
					RunID:               run.RunID,
					ArtifactDigest:      generated.ArtifactDigest,
					TraceID:             input.TraceID,
					PersistProductTruth: input.PersistProductTruth,
				}).Get(ctx, nil); err != nil {
					return EpisodeProductionResult{}, err
				}
				shotStatus.State = "WAITING_Q1"
				status.Shots[shotID] = shotStatus
				if input.RequireShotApproval {
					decision := waitForShotDecision(ctx, &status, controlChannel, shotID, run.RunID)
					if !decision.Approved {
						shotStatus.State = "Q1_REJECTED"
						shotStatus.FailureCode = decision.ReasonCode
						status.Shots[shotID] = shotStatus
						continue
					}
				}
				shotStatus.State = "APPROVED"
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
				ShotSpecRevisionID:  shotID,
				FailureCode:         status.Shots[shotID].FailureCode,
				TraceID:             input.TraceID,
				PersistProductTruth: input.PersistProductTruth,
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

	var delivery *postproduction.Result
	if input.PostProduction != nil && input.PostProduction.Enabled {
		status.State = "POST_PRODUCTION"
		postOptions := baseOptions
		postOptions.StartToCloseTimeout = 45 * time.Minute
		postOptions.HeartbeatTimeout = 30 * time.Second
		var finalized postproduction.Result
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, postOptions),
			ActivityFinalizeEpisode,
			FinalizeEpisodeInput{
				EpisodeRevisionID:   input.EpisodeRevisionID,
				RunIDs:              lockedRunIDs,
				GenerationPlanID:    input.GenerationPlanID,
				Config:              *input.PostProduction,
				TraceID:             input.TraceID,
				PersistProductTruth: input.PersistProductTruth,
			},
		).Get(ctx, &finalized); err != nil {
			return EpisodeProductionResult{
				State: "POST_PRODUCTION_FAILED", LockedRunIDs: lockedRunIDs, Shots: status.Shots,
			}, err
		}
		delivery = &finalized
	}

	status.State = "WAITING_G3"
	postProductionManifestHash := ""
	if delivery != nil {
		postProductionManifestHash = delivery.ManifestHash
	}
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, baseOptions), ActivityCreateGate3, CreateGate3Input{
		EpisodeRevisionID:          input.EpisodeRevisionID,
		RunIDs:                     lockedRunIDs,
		GenerationPlanID:           input.GenerationPlanID,
		PostProductionManifestHash: postProductionManifestHash,
		BackgroundAudioAssetVersionID: func() string {
			if input.PostProduction == nil {
				return ""
			}
			return input.PostProduction.BackgroundAudioAssetVersionID
		}(),
		TraceID:             input.TraceID,
		PersistProductTruth: input.PersistProductTruth,
	}).Get(ctx, nil); err != nil {
		return EpisodeProductionResult{}, err
	}

	gate3 := waitForGate3Decision(ctx, &status, controlChannel)
	if gate3.DecisionID == "" || gate3.ActorID == "" {
		return EpisodeProductionResult{}, temporal.NewNonRetryableApplicationError(
			"invalid G3 decision signal", "VALIDATION_ERROR", errors.New("decisionId and actorId are required"),
		)
	}
	if !gate3.Approved {
		status.State = "G3_REJECTED"
		return EpisodeProductionResult{
			State:          status.State,
			LockedRunIDs:   lockedRunIDs,
			PostProduction: delivery,
			Gate3Decision:  &gate3,
			Shots:          status.Shots,
		}, nil
	}

	status.State = "LOCKED"
	return EpisodeProductionResult{
		State:          status.State,
		LockedRunIDs:   lockedRunIDs,
		PostProduction: delivery,
		Gate3Decision:  &gate3,
		Shots:          status.Shots,
	}, nil
}

func waitForResume(ctx workflow.Context, status *WorkflowStatus, controls workflow.ReceiveChannel) bool {
	var command WorkflowControl
	pauseObserved := status.Paused
	for controls.ReceiveAsync(&command) {
		applyControl(status, command)
		if command.Action == "PAUSE" {
			pauseObserved = true
		}
	}
	if !status.Paused {
		return pauseObserved
	}
	previous := status.State
	status.State = "PAUSED"
	for status.Paused {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(controls, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, &command)
			applyControl(status, command)
		})
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		selector.Select(ctx)
		if ctx.Err() != nil {
			return true
		}
	}
	status.State = previous
	return true
}

func applyControl(status *WorkflowStatus, command WorkflowControl) {
	if command.CommandID == "" || command.ActorID == "" {
		return
	}
	switch command.Action {
	case "PAUSE":
		status.Paused = true
	case "RESUME":
		status.Paused = false
	}
}

func waitForShotDecision(
	ctx workflow.Context,
	status *WorkflowStatus,
	controls workflow.ReceiveChannel,
	shotID string,
	runID string,
) ShotDecision {
	decisions := workflow.GetSignalChannel(ctx, ShotDecisionSignal)
	for {
		waitForResume(ctx, status, controls)
		var decision ShotDecision
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(decisions, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, &decision)
		})
		selector.AddReceive(controls, func(channel workflow.ReceiveChannel, _ bool) {
			var command WorkflowControl
			channel.Receive(ctx, &command)
			applyControl(status, command)
		})
		selector.Select(ctx)
		if status.Paused {
			continue
		}
		if decision.DecisionID != "" &&
			decision.ActorID != "" &&
			decision.ShotSpecRevisionID == shotID &&
			decision.RunID == runID {
			return decision
		}
	}
}

func waitForGate3Decision(
	ctx workflow.Context,
	status *WorkflowStatus,
	controls workflow.ReceiveChannel,
) Gate3Decision {
	decisions := workflow.GetSignalChannel(ctx, Gate3DecisionSignal)
	for {
		waitForResume(ctx, status, controls)
		var decision Gate3Decision
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(decisions, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, &decision)
		})
		selector.AddReceive(controls, func(channel workflow.ReceiveChannel, _ bool) {
			var command WorkflowControl
			channel.Receive(ctx, &command)
			applyControl(status, command)
		})
		selector.Select(ctx)
		if status.Paused {
			continue
		}
		if decision.DecisionID != "" && decision.ActorID != "" {
			return decision
		}
	}
}

// Activity inputs are explicit and version-independent product contracts.
type CompilePromptInput struct {
	ShotSpecRevisionID   string `json:"shotSpecRevisionId"`
	GenerationProfileRef string `json:"generationProfileRef"`
	TraceID              string `json:"traceId"`
	PersistProductTruth  bool   `json:"persistProductTruth,omitempty"`
}

type CreateRunInput struct {
	ShotSpecRevisionID   string                         `json:"shotSpecRevisionId"`
	PromptSnapshot       PromptSnapshotRef              `json:"promptSnapshot"`
	GenerationProfileRef string                         `json:"generationProfileRef"`
	Route                providercontract.ModelSnapshot `json:"route"`
	GenerationPlanID     string                         `json:"generationPlanId,omitempty"`
	BudgetApprovalID     string                         `json:"budgetApprovalId,omitempty"`
	ProviderProfileID    string                         `json:"providerProfileId,omitempty"`
	CreativeAttempt      int                            `json:"creativeAttempt"`
	TraceID              string                         `json:"traceId"`
	PersistProductTruth  bool                           `json:"persistProductTruth,omitempty"`
}

type ExecuteProviderJobInput struct {
	Run                 GenerationRunRef               `json:"run"`
	Prompt              PromptSnapshotRef              `json:"prompt"`
	Route               providercontract.ModelSnapshot `json:"route"`
	BudgetApprovalID    string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency      string                         `json:"budgetCurrency"`
	ProviderProfileID   string                         `json:"providerProfileId,omitempty"`
	TraceID             string                         `json:"traceId"`
	PersistProductTruth bool                           `json:"persistProductTruth,omitempty"`
	// ExpectedProductTruth is set only by the formal Stage 1 runner. The
	// repository compares it with the facts locked in its SERIALIZABLE
	// transaction before it inserts a reservation, Provider job, or cost row.
	ExpectedProductTruth *PreparedProductTruth `json:"expectedProductTruth,omitempty"`
	// SubscriptionQuotaSnapshot is fresh external usage evidence consumed only
	// by the PostgreSQL prepare transaction. It is persisted in the dedicated
	// AFP ledger and deliberately excluded from the immutable Provider request.
	SubscriptionQuotaSnapshot          *SubscriptionQuotaSnapshot `json:"subscriptionQuotaSnapshot,omitempty"`
	ExpectedExecutionPackageHash       string                     `json:"expectedExecutionPackageHash,omitempty"`
	ExpectedControlledRetryPackageHash string                     `json:"expectedControlledRetryPackageHash,omitempty"`
	ExpectedLiveActivationID           string                     `json:"expectedLiveActivationId,omitempty"`
	ExpectedSourceCodeCommit           string                     `json:"expectedSourceCodeCommit,omitempty"`
	EstimatedVideoTokens               int64                      `json:"estimatedVideoTokens,omitempty"`
	PredictedAFPMilli                  int64                      `json:"predictedAfpMilli,omitempty"`
	BillingMode                        string                     `json:"billingMode,omitempty"`
}

// SubscriptionQuotaSnapshot records an authenticated Agent Plan read in AFP
// milli-units. ExternalReservedAFPMilli covers conservative reservations not
// already represented by this database; callers must not include this
// activation's own durable reservation in that value.
type SubscriptionQuotaSnapshot struct {
	SchemaVersion            string    `json:"schemaVersion"`
	Source                   string    `json:"source"`
	CapturedAt               time.Time `json:"capturedAt"`
	AccountID                string    `json:"accountId"`
	Profile                  string    `json:"profile"`
	Region                   string    `json:"region"`
	BillingMode              string    `json:"billingMode"`
	FiveHourUsedAFPMilli     int64     `json:"fiveHourUsedAfpMilli"`
	FiveHourTotalAFPMilli    int64     `json:"fiveHourTotalAfpMilli"`
	WeeklyUsedAFPMilli       int64     `json:"weeklyUsedAfpMilli"`
	WeeklyTotalAFPMilli      int64     `json:"weeklyTotalAfpMilli"`
	MonthlyUsedAFPMilli      int64     `json:"monthlyUsedAfpMilli"`
	MonthlyTotalAFPMilli     int64     `json:"monthlyTotalAfpMilli"`
	ExternalReservedAFPMilli int64     `json:"externalReservedAfpMilli"`
}

// PreparedProviderJob is the durable per-run budget allocation returned by the
// product-truth transaction before any paid Provider request is assembled.
type PreparedProviderJob struct {
	Budget            providercontract.BudgetEnvelope    `json:"budget"`
	BudgetReservation providercontract.BudgetReservation `json:"budgetReservation"`
	ProductTruth      PreparedProductTruth               `json:"productTruth"`
	// ReconcileOnly is derived from the durable Provider job projection on
	// Activity replay. It is intentionally excluded from the immutable request
	// snapshot: once a submit may have crossed the paid boundary, retries poll
	// the stable JobID instead of issuing another POST.
	ReconcileOnly bool `json:"-"`
}

// ProviderJobObservation freezes the paid-boundary state before an Activity
// can fail or retry. UNKNOWN is a reconciliation state, never proof that the
// upstream task failed or can be released.
type ProviderJobObservation struct {
	State          string `json:"state"`
	UpstreamTaskID string `json:"upstreamTaskId,omitempty"`
	RequestID      string `json:"requestId,omitempty"`
	ErrorCode      string `json:"errorCode,omitempty"`
}

// PreparedProductTruth echoes the exact immutable PostgreSQL facts that were
// revalidated in the same transaction as the Provider job and budget
// reservation. A narrow runner can compare it with its signed-off package
// without accepting any caller-reported authorization state.
type PreparedProductTruth struct {
	ShotSpecRevisionID         string                         `json:"shotSpecRevisionId"`
	Run                        GenerationRunRef               `json:"run"`
	PromptSnapshotID           string                         `json:"promptSnapshotId"`
	PromptSnapshotHash         string                         `json:"promptSnapshotHash"`
	GenerationPlanID           string                         `json:"generationPlanId"`
	BudgetApprovalID           string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros        int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency             string                         `json:"budgetCurrency"`
	ProviderProfileID          string                         `json:"providerProfileId"`
	Route                      providercontract.ModelSnapshot `json:"route"`
	LiveActivationID           string                         `json:"liveActivationId,omitempty"`
	ExecutionPackageHash       string                         `json:"executionPackageHash,omitempty"`
	ControlledRetryPackageHash string                         `json:"controlledRetryPackageHash,omitempty"`
	SourceCodeCommit           string                         `json:"sourceCodeCommit,omitempty"`
	EstimatedVideoTokens       int64                          `json:"estimatedVideoTokens,omitempty"`
	PredictedAFPMilli          int64                          `json:"predictedAfpMilli,omitempty"`
	BillingMode                string                         `json:"billingMode,omitempty"`
}

type RunQCInput struct {
	Run                 GenerationRunRef `json:"run"`
	Provider            ProviderResult   `json:"provider"`
	TraceID             string           `json:"traceId"`
	PersistProductTruth bool             `json:"persistProductTruth,omitempty"`
}

type CreateReviewInput struct {
	ShotSpecRevisionID  string `json:"shotSpecRevisionId"`
	RunID               string `json:"runId"`
	ArtifactDigest      string `json:"artifactDigest"`
	TraceID             string `json:"traceId"`
	PersistProductTruth bool   `json:"persistProductTruth,omitempty"`
}

type EscalateShotInput struct {
	ShotSpecRevisionID  string `json:"shotSpecRevisionId"`
	FailureCode         string `json:"failureCode"`
	TraceID             string `json:"traceId"`
	PersistProductTruth bool   `json:"persistProductTruth,omitempty"`
}

type CreateGate3Input struct {
	EpisodeRevisionID             string   `json:"episodeRevisionId"`
	RunIDs                        []string `json:"runIds"`
	GenerationPlanID              string   `json:"generationPlanId,omitempty"`
	PostProductionManifestHash    string   `json:"postProductionManifestHash,omitempty"`
	BackgroundAudioAssetVersionID string   `json:"backgroundAudioAssetVersionId,omitempty"`
	TraceID                       string   `json:"traceId"`
	PersistProductTruth           bool     `json:"persistProductTruth,omitempty"`
}

type FinalizeEpisodeInput struct {
	EpisodeRevisionID   string               `json:"episodeRevisionId"`
	RunIDs              []string             `json:"runIds"`
	GenerationPlanID    string               `json:"generationPlanId,omitempty"`
	Config              PostProductionConfig `json:"config"`
	TraceID             string               `json:"traceId"`
	PersistProductTruth bool                 `json:"persistProductTruth,omitempty"`
}

func validateWorkflowInput(input EpisodeProductionInput) error {
	if input.SchemaVersion != "v1" {
		return errors.New("schemaVersion must be v1")
	}
	if input.SeriesID == "" || input.EpisodeRevisionID == "" || input.GenerationProfileRef == "" || input.Gate2DecisionID == "" {
		return errors.New("seriesId, episodeRevisionId, generationProfileRef, and gate2DecisionId are required")
	}
	if input.PersistProductTruth &&
		(input.GenerationPlanID == "" || input.ProviderProfileID == "") {
		return errors.New("generationPlanId and providerProfileId are required for product persistence")
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
	if input.PostProduction != nil {
		if err := input.PostProduction.Validate(); err != nil {
			return err
		}
	}
	return nil
}
