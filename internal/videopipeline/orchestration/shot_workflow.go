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
	ShotWorkflowName               = "video.production.shot.v1"
	ShotReconciliationWorkflowName = "video.production.shot-reconciliation.v1"
	ActivityCancelProviderJob      = "video.activity.cancel-provider-job.v1"
	ActivityFinalizeShotRun        = "video.activity.finalize-shot-run.v1"
	pauseCompletionRaceVersion     = "shot-provider-pause-completion-race-v1"
)

// ShotProductionInput is reconstructed from PostgreSQL after the public
// shot-run transaction commits. Every identity is immutable and safe to retain
// in Temporal history.
type ShotProductionInput struct {
	OperationID         string                         `json:"operationId"`
	ShotSpecRevisionID  string                         `json:"shotSpecRevisionId"`
	Run                 GenerationRunRef               `json:"run"`
	Prompt              PromptSnapshotRef              `json:"prompt"`
	Route               providercontract.ModelSnapshot `json:"route"`
	ProviderProfileID   string                         `json:"providerProfileId"`
	BudgetApprovalID    string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency      string                         `json:"budgetCurrency"`
	TraceID             string                         `json:"traceId"`
	RequireShotApproval bool                           `json:"requireShotApproval"`
	PersistProductTruth bool                           `json:"persistProductTruth"`
}

type ShotProductionResult struct {
	State    string           `json:"state"`
	Run      GenerationRunRef `json:"run"`
	Provider ProviderResult   `json:"provider,omitempty"`
	QC       QCResult         `json:"qc,omitempty"`
	Decision *ShotDecision    `json:"decision,omitempty"`
}

type CancelProviderJobInput struct {
	OperationID string                  `json:"operationId"`
	Dispatch    ExecuteProviderJobInput `json:"dispatch"`
	ReasonCode  string                  `json:"reasonCode"`
	TraceID     string                  `json:"traceId"`
}

type CancelProviderResult struct {
	State          string                 `json:"state"`
	NoRemoteTask   bool                   `json:"noRemoteTask,omitempty"`
	UpstreamTaskID string                 `json:"upstreamTaskId,omitempty"`
	RequestID      string                 `json:"requestId,omitempty"`
	Usage          providercontract.Usage `json:"usage"`
	Cost           providercontract.Cost  `json:"cost"`
	ErrorCode      string                 `json:"errorCode,omitempty"`
}

type FinalizeShotRunInput struct {
	OperationID  string `json:"operationId"`
	RunID        string `json:"runId"`
	State        string `json:"state"`
	FailureClass string `json:"failureClass,omitempty"`
	FailureCode  string `json:"failureCode,omitempty"`
	TraceID      string `json:"traceId"`
}

// ShotReconciliationInput starts a durable coordinator after the original shot
// Workflow has closed with ambiguous provider cancellation state. The stable
// provider JobID is reconstructed from Dispatch.Run; no paid task is created.
type ShotReconciliationInput struct {
	OperationID string                  `json:"operationId"`
	Dispatch    ExecuteProviderJobInput `json:"dispatch"`
	TraceID     string                  `json:"traceId"`
}

// ShotReconciliationWorkflow retries the idempotent provider cancellation
// boundary until the provider can authoritatively report terminal success,
// cancellation, or absence. PostgreSQL projection and operation convergence
// are committed by CancelProviderJob before this Workflow closes.
func ShotReconciliationWorkflow(
	ctx workflow.Context,
	input ShotReconciliationInput,
) (CancelProviderResult, error) {
	if input.OperationID == "" || input.Dispatch.Run.RunID == "" ||
		input.Dispatch.Run.RunSpecDigest == "" || input.TraceID == "" {
		err := errors.New("shot reconciliation immutable dispatch fields are required")
		return CancelProviderResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "VALIDATION_ERROR", err,
		)
	}
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    0,
		},
	}
	var result CancelProviderResult
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		ActivityCancelProviderJob,
		CancelProviderJobInput{
			OperationID: input.OperationID,
			Dispatch:    input.Dispatch,
			ReasonCode:  "RECONCILE_HISTORY",
			TraceID:     input.TraceID,
		},
	).Get(ctx, &result)
	return result, err
}

// ShotProductionWorkflow executes the exact run already persisted by the
// public API. The stable workflow ID makes an ambiguous start safe to retry.
// Pausing cancels only the polling Activity; resuming reconciles the same
// provider JobID instead of creating a second paid task.
func ShotProductionWorkflow(
	ctx workflow.Context,
	input ShotProductionInput,
) (ShotProductionResult, error) {
	if err := validateShotWorkflowInput(input); err != nil {
		return ShotProductionResult{}, temporal.NewNonRetryableApplicationError(
			err.Error(), "VALIDATION_ERROR", err,
		)
	}
	status := &WorkflowStatus{
		State: "QUEUED",
		Shots: map[string]ShotState{
			input.ShotSpecRevisionID: {
				State: "QUEUED", CreativeAttempt: input.Run.Attempt, RunID: input.Run.RunID,
			},
		},
	}
	if err := workflow.SetQueryHandler(ctx, StatusQuery, func() (WorkflowStatus, error) {
		return *status, nil
	}); err != nil {
		return ShotProductionResult{}, fmt.Errorf("register shot status query: %w", err)
	}
	controls := workflow.GetSignalChannel(ctx, ControlSignal)
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
			NonRetryableErrorTypes: []string{
				"VALIDATION_ERROR",
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
	dispatch := ExecuteProviderJobInput{
		Run: input.Run, Prompt: input.Prompt, Route: input.Route,
		BudgetApprovalID: input.BudgetApprovalID, BudgetMaximumMicros: input.BudgetMaximumMicros,
		BudgetCurrency: input.BudgetCurrency, ProviderProfileID: input.ProviderProfileID,
		TraceID: input.TraceID, PersistProductTruth: input.PersistProductTruth,
	}

	status.State = "RUNNING"
	shot := status.Shots[input.ShotSpecRevisionID]
	shot.State = "RUNNING"
	status.Shots[input.ShotSpecRevisionID] = shot
	generated, err := executeProviderWithControls(ctx, controls, status, options, dispatch)
	if err != nil {
		if workflowCancellationRequested(ctx, err) {
			cancelled := cancelShotAfterWorkflowCancellation(ctx, input, dispatch)
			status.State = cancelled.State
			shot.State = cancelled.State
			shot.FailureCode = cancelled.ErrorCode
			status.Shots[input.ShotSpecRevisionID] = shot
			return ShotProductionResult{
				State: cancelled.State, Run: input.Run,
			}, workflowCancellationError(ctx, err)
		}
		// Submit/poll failures are not proof that the upstream task failed. Keep
		// reconciling the stable JobID until the adapter proves absence, confirms
		// cancellation, or returns a terminal result with exact cost evidence.
		// This path deliberately never calls FinalizeShotRun(FAILED), because
		// doing so would release a task that may still be running and billable.
		reconciled, reconcileErr := reconcileProviderExecutionFailure(
			ctx, input, dispatch,
		)
		if reconcileErr != nil {
			return ShotProductionResult{State: "RECONCILING", Run: input.Run},
				fmt.Errorf("reconcile provider execution failure: %w", reconcileErr)
		}
		status.State = reconciled.State
		return ShotProductionResult{State: status.State, Run: input.Run}, err
	}
	shot.ArtifactDigest = generated.ArtifactDigest
	shot.State = "QC_PENDING"
	status.Shots[input.ShotSpecRevisionID] = shot

	qcOptions := options
	qcOptions.StartToCloseTimeout = 2 * time.Minute
	var qc QCResult
	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, qcOptions),
		ActivityRunAutomaticQC,
		RunQCInput{
			Run: input.Run, Provider: generated, TraceID: input.TraceID,
			PersistProductTruth: input.PersistProductTruth,
		},
	).Get(ctx, &qc); err != nil {
		if workflowCancellationRequested(ctx, err) {
			cancelled := cancelShotAfterWorkflowCancellation(ctx, input, dispatch)
			return ShotProductionResult{
				State: cancelled.State, Run: input.Run, Provider: generated,
			}, workflowCancellationError(ctx, err)
		}
		if finalizeErr := finalizeShot(ctx, qcOptions, FinalizeShotRunInput{
			OperationID: input.OperationID, RunID: input.Run.RunID, State: "FAILED",
			FailureClass: "INFRASTRUCTURE", FailureCode: "QC_EXECUTION_FAILED", TraceID: input.TraceID,
		}); finalizeErr != nil {
			return ShotProductionResult{State: "FAILED", Run: input.Run, Provider: generated},
				fmt.Errorf("finalize QC failure: %w", finalizeErr)
		}
		return ShotProductionResult{State: "FAILED", Run: input.Run, Provider: generated}, err
	}
	if !qc.Passed {
		_ = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, qcOptions),
			ActivityEscalateShot,
			EscalateShotInput{
				ShotSpecRevisionID: input.ShotSpecRevisionID, FailureCode: qc.FailureCode,
				TraceID: input.TraceID, PersistProductTruth: input.PersistProductTruth,
			},
		).Get(ctx, nil)
		if err := finalizeShot(ctx, qcOptions, FinalizeShotRunInput{
			OperationID: input.OperationID, RunID: input.Run.RunID, State: "FAILED",
			FailureClass: "CREATIVE", FailureCode: qc.FailureCode, TraceID: input.TraceID,
		}); err != nil {
			return ShotProductionResult{
				State: "NEEDS_INTERVENTION", Run: input.Run, Provider: generated, QC: qc,
			}, fmt.Errorf("finalize QC rejection: %w", err)
		}
		return ShotProductionResult{State: "NEEDS_INTERVENTION", Run: input.Run, Provider: generated, QC: qc}, nil
	}

	if input.RequireShotApproval {
		status.State = "WAITING_Q1"
		shot.State = "WAITING_Q1"
		status.Shots[input.ShotSpecRevisionID] = shot
		if err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, qcOptions),
			ActivityCreateShotReview,
			CreateReviewInput{
				ShotSpecRevisionID: input.ShotSpecRevisionID, RunID: input.Run.RunID,
				ArtifactDigest: generated.ArtifactDigest, TraceID: input.TraceID,
				PersistProductTruth: input.PersistProductTruth,
			},
		).Get(ctx, nil); err != nil {
			return ShotProductionResult{}, err
		}
		decision, waitErr := waitForExactShotDecision(
			ctx, controls, status, input.ShotSpecRevisionID, input.Run.RunID,
		)
		if waitErr != nil {
			cancelled := cancelShotAfterWorkflowCancellation(ctx, input, dispatch)
			return ShotProductionResult{State: cancelled.State, Run: input.Run, Provider: generated, QC: qc}, waitErr
		}
		if !decision.Approved {
			if err := finalizeShot(ctx, qcOptions, FinalizeShotRunInput{
				OperationID: input.OperationID, RunID: input.Run.RunID, State: "FAILED",
				FailureClass: "CREATIVE", FailureCode: decision.ReasonCode, TraceID: input.TraceID,
			}); err != nil {
				return ShotProductionResult{
					State: "Q1_REJECTED", Run: input.Run, Provider: generated, QC: qc, Decision: &decision,
				}, fmt.Errorf("finalize Q1 rejection: %w", err)
			}
			return ShotProductionResult{
				State: "Q1_REJECTED", Run: input.Run, Provider: generated, QC: qc, Decision: &decision,
			}, nil
		}
		if err := finalizeShot(ctx, qcOptions, FinalizeShotRunInput{
			OperationID: input.OperationID, RunID: input.Run.RunID, State: "SUCCEEDED", TraceID: input.TraceID,
		}); err != nil {
			return ShotProductionResult{
				State: "SUCCEEDED", Run: input.Run, Provider: generated, QC: qc, Decision: &decision,
			}, fmt.Errorf("finalize approved shot: %w", err)
		}
		status.State = "SUCCEEDED"
		return ShotProductionResult{
			State: status.State, Run: input.Run, Provider: generated, QC: qc, Decision: &decision,
		}, nil
	}

	if err := finalizeShot(ctx, qcOptions, FinalizeShotRunInput{
		OperationID: input.OperationID, RunID: input.Run.RunID, State: "SUCCEEDED", TraceID: input.TraceID,
	}); err != nil {
		return ShotProductionResult{State: "SUCCEEDED", Run: input.Run, Provider: generated, QC: qc},
			fmt.Errorf("finalize successful shot: %w", err)
	}
	status.State = "SUCCEEDED"
	return ShotProductionResult{State: status.State, Run: input.Run, Provider: generated, QC: qc}, nil
}

func reconcileProviderExecutionFailure(
	ctx workflow.Context,
	input ShotProductionInput,
	dispatch ExecuteProviderJobInput,
) (CancelProviderResult, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, BackoffCoefficient: 2,
			MaximumInterval: 30 * time.Second, MaximumAttempts: 0,
		},
	}
	var result CancelProviderResult
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, options),
		ActivityCancelProviderJob,
		CancelProviderJobInput{
			OperationID: input.OperationID, Dispatch: dispatch,
			ReasonCode: "RECONCILE_HISTORY", TraceID: input.TraceID,
		},
	).Get(ctx, &result)
	return result, err
}

func executeProviderWithControls(
	ctx workflow.Context,
	controls workflow.ReceiveChannel,
	status *WorkflowStatus,
	options workflow.ActivityOptions,
	input ExecuteProviderJobInput,
) (ProviderResult, error) {
	raceVersion := workflow.GetVersion(
		ctx, pauseCompletionRaceVersion, workflow.DefaultVersion, 1,
	)
	for {
		waitForResume(ctx, status, controls)
		activityCtx, cancelActivity := workflow.WithCancel(workflow.WithActivityOptions(ctx, options))
		future := workflow.ExecuteActivity(activityCtx, ActivityExecuteProviderJob, input)
		var result ProviderResult
		var activityErr error
		completed := false
		pausedCancellation := false
		for !completed {
			selector := workflow.NewSelector(ctx)
			selector.AddFuture(future, func(f workflow.Future) {
				activityErr = f.Get(ctx, &result)
				completed = true
			})
			selector.AddReceive(controls, func(channel workflow.ReceiveChannel, _ bool) {
				var command WorkflowControl
				channel.Receive(ctx, &command)
				applyControl(status, command)
				if status.Paused {
					pausedCancellation = true
					cancelActivity()
				}
			})
			selector.Select(ctx)
		}
		cancelActivity()
		if raceVersion == workflow.DefaultVersion {
			if pausedCancellation && temporal.IsCanceledError(activityErr) {
				waitForResume(ctx, status, controls)
				continue
			}
			return result, activityErr
		}
		// A PAUSE signal and Activity completion can be recorded in the same
		// Workflow task. Drain controls after the Future wins as well as while
		// it is pending. If pause was observed, replay the same idempotent
		// provider job after RESUME so CompleteProviderJob can advance the
		// PostgreSQL run from PAUSED to SUCCEEDED before QC/Q1.
		pauseObserved := pausedCancellation
		if waitForResume(ctx, status, controls) {
			pauseObserved = true
		}
		if pauseObserved && (activityErr == nil || temporal.IsCanceledError(activityErr)) {
			continue
		}
		return result, activityErr
	}
}

// A cancelled Activity normally resolves with CanceledError, but a real
// provider outage can race the cancellation acknowledgement. Temporal then
// returns the provider ActivityFailure with RetryState=CancelRequested while
// the Workflow context is already cancelled. The Workflow cancellation is the
// authoritative control-plane intent in both forms.
func workflowCancellationRequested(ctx workflow.Context, activityErr error) bool {
	return temporal.IsCanceledError(activityErr) || ctx.Err() != nil
}

func workflowCancellationError(ctx workflow.Context, activityErr error) error {
	if cancellationErr := ctx.Err(); cancellationErr != nil {
		return cancellationErr
	}
	return activityErr
}

func waitForExactShotDecision(
	ctx workflow.Context,
	controls workflow.ReceiveChannel,
	status *WorkflowStatus,
	shotID, runID string,
) (ShotDecision, error) {
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
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		selector.Select(ctx)
		if err := ctx.Err(); err != nil {
			return ShotDecision{}, err
		}
		if status.Paused {
			continue
		}
		if decision.DecisionID != "" && decision.ActorID != "" &&
			decision.ShotSpecRevisionID == shotID && decision.RunID == runID {
			return decision, nil
		}
	}
}

func cancelShotAfterWorkflowCancellation(
	ctx workflow.Context,
	input ShotProductionInput,
	dispatch ExecuteProviderJobInput,
) CancelProviderResult {
	disconnected, _ := workflow.NewDisconnectedContext(ctx)
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, MaximumInterval: 10 * time.Second, MaximumAttempts: 3,
		},
	}
	var result CancelProviderResult
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(disconnected, options),
		ActivityCancelProviderJob,
		CancelProviderJobInput{
			OperationID: input.OperationID, Dispatch: dispatch,
			ReasonCode: "WORKFLOW_CANCELLED", TraceID: input.TraceID,
		},
	).Get(disconnected, &result)
	if err != nil {
		return CancelProviderResult{State: "UNKNOWN", ErrorCode: "CANCEL_NOT_CONFIRMED"}
	}
	return result
}

func finalizeShot(ctx workflow.Context, options workflow.ActivityOptions, input FinalizeShotRunInput) error {
	target := ctx
	if ctx.Err() != nil {
		target, _ = workflow.NewDisconnectedContext(ctx)
	}
	return workflow.ExecuteActivity(
		workflow.WithActivityOptions(target, options), ActivityFinalizeShotRun, input,
	).Get(target, nil)
}

func validateShotWorkflowInput(input ShotProductionInput) error {
	if input.OperationID == "" || input.ShotSpecRevisionID == "" ||
		input.Run.RunID == "" || input.Run.RunSpecDigest == "" ||
		input.Prompt.ID == "" || input.Prompt.Digest == "" ||
		input.ProviderProfileID == "" || input.BudgetApprovalID == "" ||
		input.BudgetMaximumMicros <= 0 || input.BudgetCurrency == "" {
		return errors.New("shot workflow immutable dispatch fields are required")
	}
	return input.Route.Validate(providercontract.CapabilityVideo)
}
