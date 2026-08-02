// Package temporalcontrol adapts the narrow control-plane workflow contract to
// Temporal. PostgreSQL operations are always committed before this adapter is
// called, and stable Workflow IDs make command retries safe.
package temporalcontrol

import (
	"context"
	"errors"
	"fmt"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

type Controller struct {
	client    client.Client
	taskQueue string
	store     controlplane.Store
}

func New(temporalClient client.Client, taskQueue string, store controlplane.Store) (*Controller, error) {
	if temporalClient == nil || store == nil || taskQueue == "" {
		return nil, errors.New("Temporal client, task queue, and product store are required")
	}
	return &Controller{client: temporalClient, taskQueue: taskQueue, store: store}, nil
}

func (c *Controller) StartEpisode(
	ctx context.Context,
	operation controlplane.Operation,
	command controlplane.StartProductionCommand,
) (controlplane.WorkflowStart, error) {
	record, err := c.store.GetGenerationPlan(ctx, command.GenerationPlanID)
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("load generation plan for workflow: %w", err)
	}
	if record.BudgetLimit.AmountMicros <= 0 || record.BudgetLimit.Currency == "" {
		return controlplane.WorkflowStart{}, errors.New("generation plan has no positive approved budget limit")
	}
	workflowID := operation.TemporalWorkflowID
	if workflowID == "" {
		workflowID = "episode-production-" + operation.OperationID
	}
	var postProduction *orchestration.PostProductionConfig
	if command.PostProduction != nil {
		post := command.PostProduction
		postProduction = &orchestration.PostProductionConfig{
			Enabled:  post.Enabled,
			Evidence: post.Evidence,
			SpeechRoute: providercontract.ModelSnapshot{
				CapabilityAlias: post.SpeechRouteSnapshot.CapabilityAlias,
				Provider:        post.SpeechRouteSnapshot.Provider,
				ModelID:         post.SpeechRouteSnapshot.ModelID,
				EndpointID:      post.SpeechRouteSnapshot.EndpointID,
				RouteVersion:    post.SpeechRouteSnapshot.RouteVersion,
				CapabilityHash:  post.SpeechRouteSnapshot.CapabilityHash,
				Verification:    "control_plane_capability_snapshot",
			},
			SpeechProviderProfileID:       post.SpeechRouteSnapshot.ProviderProfileID,
			SpeechBudgetApprovalID:        post.SpeechBudgetApprovalID,
			SpeechBudgetMaximumMicros:     post.SpeechBudgetLimit.AmountMicros,
			SpeechBudgetCurrency:          post.SpeechBudgetLimit.Currency,
			SubtitleLanguage:              post.SubtitleLanguage,
			BurnSubtitles:                 post.BurnSubtitles,
			BackgroundAudioAssetVersionID: post.BackgroundAudioAssetVersionID,
			EnforcePoCDuration:            post.EnforcePoCDuration,
		}
	}
	run, err := c.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                c.taskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
	}, orchestration.WorkflowName, orchestration.EpisodeProductionInput{
		SchemaVersion:        command.SchemaVersion,
		SeriesID:             record.SeriesID,
		EpisodeRevisionID:    command.EpisodeRevisionID,
		ShotSpecRevisionIDs:  command.ShotSpecRevisionIDs,
		GenerationProfileRef: command.GenerationProfileRevisionID,
		Gate2DecisionID:      command.Gate2DecisionID,
		GenerationPlanID:     command.GenerationPlanID,
		ProviderProfileID:    command.RouteSnapshot.ProviderProfileID,
		ProviderRoute: providercontract.ModelSnapshot{
			CapabilityAlias: command.RouteSnapshot.CapabilityAlias,
			Provider:        command.RouteSnapshot.Provider,
			ModelID:         command.RouteSnapshot.ModelID,
			EndpointID:      command.RouteSnapshot.EndpointID,
			RouteVersion:    command.RouteSnapshot.RouteVersion,
			CapabilityHash:  command.RouteSnapshot.CapabilityHash,
			Verification:    "control_plane_capability_snapshot",
		},
		BudgetApprovalID:    command.BudgetApprovalID,
		BudgetMaximumMicros: record.BudgetLimit.AmountMicros,
		BudgetCurrency:      record.BudgetLimit.Currency,
		TraceID:             operation.TraceID,
		RequireShotApproval: true,
		PersistProductTruth: true,
		PostProduction:      postProduction,
	})
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("start episode workflow: %w", err)
	}
	return controlplane.WorkflowStart{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (c *Controller) StartShot(
	ctx context.Context,
	operation controlplane.Operation,
) (controlplane.WorkflowStart, error) {
	record, err := c.store.GetShotWorkflowRecord(ctx, operation.AggregateID)
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("load shot workflow projection: %w", err)
	}
	if record.BudgetLimit.AmountMicros <= 0 || record.BudgetLimit.Currency == "" {
		return controlplane.WorkflowStart{}, errors.New("shot generation plan has no positive approved budget limit")
	}
	input := shotProductionInput(operation.OperationID, record)
	if operation.OperationType == "CONFIRM_CREATOR_LIVE_SHOT" {
		input.RequireShotApproval = false
	}
	workflowID := operation.TemporalWorkflowID
	if workflowID == "" {
		workflowID = "shot-generation-" + operation.AggregateID
	}
	run, err := c.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                c.taskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
	}, orchestration.ShotWorkflowName, input)
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("start shot workflow: %w", err)
	}
	return controlplane.WorkflowStart{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func shotProductionInput(
	operationID string,
	record controlplane.ShotWorkflowRecord,
) orchestration.ShotProductionInput {
	return orchestration.ShotProductionInput{
		OperationID:        operationID,
		ShotSpecRevisionID: record.Run.ShotSpecRevisionID,
		Run: orchestration.GenerationRunRef{
			RunID: record.Run.RunID, RunSpecDigest: record.Run.RunSpecDigest,
			Attempt: record.Run.CreativeAttempt,
		},
		Prompt: orchestration.PromptSnapshotRef{
			ID: record.PromptSnapshotID, Digest: record.PromptHash,
		},
		Route: providercontract.ModelSnapshot{
			CapabilityAlias: record.RouteSnapshot.CapabilityAlias,
			Provider:        record.RouteSnapshot.Provider,
			ModelID:         record.RouteSnapshot.ModelID,
			EndpointID:      record.RouteSnapshot.EndpointID,
			RouteVersion:    record.RouteSnapshot.RouteVersion,
			CapabilityHash:  record.RouteSnapshot.CapabilityHash,
			Verification:    "control_plane_capability_snapshot",
		},
		ProviderProfileID:   record.RouteSnapshot.ProviderProfileID,
		BudgetApprovalID:    record.BudgetApprovalID,
		BudgetMaximumMicros: record.BudgetLimit.AmountMicros,
		BudgetCurrency:      record.BudgetLimit.Currency,
		TraceID:             record.Run.TraceID,
		RequireShotApproval: true,
		PersistProductTruth: true,
	}
}

func (c *Controller) Pause(ctx context.Context, workflowID, operationID, reasonCode string) error {
	if workflowID == "" || operationID == "" {
		return errors.New("workflow ID and operation ID are required")
	}
	return c.client.SignalWorkflow(ctx, workflowID, "", orchestration.ControlSignal, orchestration.WorkflowControl{
		CommandID: operationID, Action: "PAUSE", ActorID: "control-plane", ReasonCode: reasonCode,
	})
}

func (c *Controller) Cancel(ctx context.Context, workflowID, _ string) error {
	if workflowID == "" {
		return errors.New("workflow ID is required")
	}
	return c.client.CancelWorkflow(ctx, workflowID, "")
}

func (c *Controller) Resume(
	ctx context.Context,
	workflowID, operationID, recoveryMode string,
) (controlplane.WorkflowStart, error) {
	if workflowID == "" || operationID == "" {
		return controlplane.WorkflowStart{}, errors.New("workflow ID and operation ID are required")
	}
	if recoveryMode == "RESUME_PAUSED" {
		err := c.client.SignalWorkflow(
			ctx, workflowID, "", orchestration.ControlSignal, orchestration.WorkflowControl{
				CommandID:  operationID,
				Action:     "RESUME",
				ActorID:    "control-plane",
				ReasonCode: recoveryMode,
			},
		)
		return controlplane.WorkflowStart{WorkflowID: workflowID}, err
	}
	if recoveryMode != "RECONCILE_HISTORY" && recoveryMode != "RETRY_INFRASTRUCTURE" {
		return controlplane.WorkflowStart{}, fmt.Errorf("unsupported recovery mode %q", recoveryMode)
	}
	operation, err := c.store.GetOperation(ctx, operationID)
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("load recovery operation: %w", err)
	}
	record, err := c.store.GetShotWorkflowRecord(ctx, operation.AggregateID)
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("load shot recovery projection: %w", err)
	}
	input := shotProductionInput(operationID, record)
	recoveryWorkflowID := "shot-recovery-" + operationID
	options := client.StartWorkflowOptions{
		ID:                                       recoveryWorkflowID,
		TaskQueue:                                c.taskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
	}
	var run client.WorkflowRun
	if recoveryMode == "RECONCILE_HISTORY" &&
		record.Run.FailureCode == "CANCEL_NOT_CONFIRMED" {
		run, err = c.client.ExecuteWorkflow(
			ctx,
			options,
			orchestration.ShotReconciliationWorkflowName,
			orchestration.ShotReconciliationInput{
				OperationID: operationID,
				Dispatch: orchestration.ExecuteProviderJobInput{
					Run: input.Run, Prompt: input.Prompt, Route: input.Route,
					BudgetApprovalID:    input.BudgetApprovalID,
					BudgetMaximumMicros: input.BudgetMaximumMicros,
					BudgetCurrency:      input.BudgetCurrency,
					ProviderProfileID:   input.ProviderProfileID,
					TraceID:             input.TraceID,
					PersistProductTruth: input.PersistProductTruth,
				},
				TraceID: input.TraceID,
			},
		)
	} else {
		run, err = c.client.ExecuteWorkflow(
			ctx, options, orchestration.ShotWorkflowName, input,
		)
	}
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("start shot recovery workflow: %w", err)
	}
	return controlplane.WorkflowStart{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (c *Controller) RecordApproval(ctx context.Context, decision controlplane.ApprovalDecision) error {
	if decision.Gate != "Q1" && decision.Gate != "G3" {
		return nil
	}
	if decision.EpisodeID == "" {
		return errors.New("episodeId is required to deliver Q1/G3 workflow decisions")
	}
	approved := decision.Decision == "APPROVED"
	if decision.Gate == "G3" {
		workflowID, err := c.store.FindActiveEpisodeWorkflow(ctx, decision.EpisodeID)
		if err != nil {
			return fmt.Errorf("find G3 approval workflow: %w", err)
		}
		return c.client.SignalWorkflow(ctx, workflowID, "", orchestration.Gate3DecisionSignal, orchestration.Gate3Decision{
			DecisionID: decision.DecisionID,
			Approved:   approved,
			ReasonCode: decision.ReasonCode,
			ActorID:    decision.Actor.ActorID,
		})
	}

	var runID, shotRevisionID string
	for _, binding := range decision.Bindings {
		switch binding.ObjectType {
		case "GENERATION_RUN":
			runID = binding.RevisionID
		case "SHOT_SPEC_REVISION":
			shotRevisionID = binding.RevisionID
		}
	}
	if runID == "" || shotRevisionID == "" {
		return errors.New("Q1 requires GENERATION_RUN and SHOT_SPEC_REVISION bindings")
	}
	run, err := c.store.GetGenerationRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("find Q1 run workflow: %w", err)
	}
	if run.TemporalWorkflowID == "" {
		return errors.New("Q1 generation run has no Temporal workflow ID")
	}
	workflowID := run.TemporalWorkflowID
	return c.client.SignalWorkflow(ctx, workflowID, "", orchestration.ShotDecisionSignal, orchestration.ShotDecision{
		DecisionID:         decision.DecisionID,
		ShotSpecRevisionID: shotRevisionID,
		RunID:              runID,
		Approved:           approved,
		ReasonCode:         decision.ReasonCode,
		ActorID:            decision.Actor.ActorID,
	})
}
