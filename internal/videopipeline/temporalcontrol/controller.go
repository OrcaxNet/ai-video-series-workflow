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
	})
	if err != nil {
		return controlplane.WorkflowStart{}, fmt.Errorf("start episode workflow: %w", err)
	}
	return controlplane.WorkflowStart{WorkflowID: run.GetID(), RunID: run.GetRunID()}, nil
}

func (c *Controller) Cancel(ctx context.Context, workflowID, _ string) error {
	if workflowID == "" {
		return errors.New("workflow ID is required")
	}
	return c.client.CancelWorkflow(ctx, workflowID, "")
}

func (c *Controller) Resume(ctx context.Context, workflowID, recoveryMode string) error {
	if workflowID == "" {
		return errors.New("workflow ID is required")
	}
	return c.client.SignalWorkflow(ctx, workflowID, "", orchestration.ControlSignal, orchestration.WorkflowControl{
		CommandID:  "resume-" + workflowID + "-" + recoveryMode,
		Action:     "RESUME",
		ActorID:    "control-plane",
		ReasonCode: recoveryMode,
	})
}

func (c *Controller) RecordApproval(ctx context.Context, decision controlplane.ApprovalDecision) error {
	if decision.Gate != "Q1" && decision.Gate != "G3" {
		return nil
	}
	if decision.EpisodeID == "" {
		return errors.New("episodeId is required to deliver Q1/G3 workflow decisions")
	}
	workflowID, err := c.store.FindActiveEpisodeWorkflow(ctx, decision.EpisodeID)
	if err != nil {
		return fmt.Errorf("find approval workflow: %w", err)
	}
	approved := decision.Decision == "APPROVED"
	if decision.Gate == "G3" {
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
	return c.client.SignalWorkflow(ctx, workflowID, "", orchestration.ShotDecisionSignal, orchestration.ShotDecision{
		DecisionID:         decision.DecisionID,
		ShotSpecRevisionID: shotRevisionID,
		RunID:              runID,
		Approved:           approved,
		ReasonCode:         decision.ReasonCode,
		ActorID:            decision.Actor.ActorID,
	})
}
