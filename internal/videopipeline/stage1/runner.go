package stage1

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
)

// ArtifactVerifier is deliberately narrower than artifactstore.Store so the
// runner can prove that every successful adapter artifact is present in CAS.
type ArtifactVerifier interface {
	Exists(string) (bool, error)
}

// SubmitInput selects one job from the immutable execution package. The caller
// cannot provide prompts, routes, budgets, assets, or authorization booleans.
type SubmitInput struct {
	ShotID string `json:"shotId"`
}

// ProductTruthPreparer is the PostgreSQL boundary that resolves exact prompt
// lineage and commits the durable paid-attempt identity before any Provider
// POST. Both methods are revalidated for every new formal submit.
type ProductTruthPreparer interface {
	ResolvePromptSnapshot(context.Context, string) (orchestration.PromptSnapshotRef, error)
	PrepareProviderJob(
		context.Context,
		orchestration.WorkflowStep,
		orchestration.ExecuteProviderJobInput,
	) (orchestration.PreparedProviderJob, error)
	CompletePreparedProviderJob(
		context.Context,
		orchestration.WorkflowStep,
		string,
		orchestration.ProviderResult,
	) error
}

// CompleteInput supplies the independently measured Agent Plan AFP delta.
// Provider tokens, cost, task state, content-safety outcome, and artifacts are
// always read back from the authenticated adapter and cannot be overridden.
type CompleteInput struct {
	IdempotencyKey   string `json:"idempotencyKey"`
	ActualAFPMilli   int64  `json:"actualAfpMilli"`
	EvidenceComplete bool   `json:"evidenceComplete"`
}

type CompletionResult struct {
	IdempotencyKey      string `json:"idempotencyKey"`
	ProviderTaskID      string `json:"providerTaskId"`
	State               string `json:"state"`
	ActualVideoTokens   int64  `json:"actualVideoTokens"`
	ActualAFPMilli      int64  `json:"actualAfpMilli"`
	ActualCashMicros    int64  `json:"actualCashMicros"`
	EvidenceComplete    bool   `json:"evidenceComplete"`
	ContentSafetyFailed bool   `json:"contentSafetyFailed"`
}

// Runner is the sole production entry point for formal FLO-104 Stage 1 video
// jobs. Submit always crosses Gate -> AdapterSubmitter -> Executor. Complete
// always reloads terminal truth from the same adapter before freezing it.
type Runner struct {
	gate             *Gate
	adapter          *AdapterSubmitter
	executor         *Executor
	artifacts        ArtifactVerifier
	truth            ProductTruthPreparer
	executionPackage ExecutionPackage
}

func NewRunner(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	executionPackage ExecutionPackage,
) (*Runner, error) {
	if gate == nil || adapter == nil || artifacts == nil || truth == nil {
		return nil, errors.New("stage 1 gate, authenticated adapter, CAS verifier, and product truth are required")
	}
	if err := executionPackage.Validate(gate.Plan()); err != nil {
		return nil, err
	}
	executor, err := NewExecutor(gate, adapter)
	if err != nil {
		return nil, err
	}
	return &Runner{
		gate: gate, adapter: adapter, executor: executor, artifacts: artifacts,
		truth: truth, executionPackage: executionPackage,
	}, nil
}

func (r *Runner) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	frozen, ok := r.executionPackage.Job(input.ShotID)
	if !ok {
		return SubmitResult{}, providerError(providercontract.CodeForbidden, "shot is outside the frozen stage 1 execution package")
	}
	attempt := Attempt{
		AttemptID: frozen.AttemptID, ShotID: frozen.ShotID,
		IdempotencyKey:                     frozen.IdempotencyKey,
		EstimatedVideoTokens:               frozen.EstimatedVideoTokens,
		PredictedAFPMilli:                  frozen.PredictedAFPMilli,
		EstimatedNonSubscriptionCashMicros: frozen.EstimatedNonSubscriptionCashMicros,
	}

	// Existing remote work is recovery, not a new paid submit. It must remain
	// recoverable even if current product truth has since drifted.
	recovered, err := r.adapter.Recover(ctx, attempt.IdempotencyKey)
	if err != nil {
		return SubmitResult{}, err
	}
	decision, err := r.gate.Inspect(attempt)
	if err != nil {
		return SubmitResult{}, err
	}
	if decision == DecisionReplay {
		return SubmitResult{}, providerError(providercontract.CodeConflict, "terminal stage 1 attempt cannot be submitted again")
	}
	if recovered.Found {
		if strings.TrimSpace(recovered.ProviderTaskID) == "" {
			return SubmitResult{}, providerError(providercontract.CodeConflict, "recovered stage 1 job has no provider task")
		}
		return r.executor.Execute(ctx, attempt)
	}
	if decision == DecisionRecoverOnly {
		return SubmitResult{}, providerError(providercontract.CodeUnavailable, "ambiguous stage 1 submit requires operator recovery and cannot be resubmitted")
	}

	jobRequest, err := r.prepareProductTruth(ctx, frozen)
	if err != nil {
		return SubmitResult{}, err
	}
	attempt.JobRequest = &jobRequest
	return r.executor.Execute(ctx, attempt)
}

func (r *Runner) prepareProductTruth(
	ctx context.Context,
	frozen FrozenJob,
) (providercontract.JobRequest, error) {
	prompt, err := r.truth.ResolvePromptSnapshot(ctx, frozen.PromptSnapshotID)
	if err != nil {
		return providercontract.JobRequest{}, err
	}
	if prompt.ID != frozen.PromptSnapshotID || prompt.Digest != frozen.PromptSnapshotHash {
		return providercontract.JobRequest{}, providerError(
			providercontract.CodeConflict,
			"PostgreSQL prompt snapshot differs from the frozen stage 1 package",
		)
	}
	preparation := orchestration.ExecuteProviderJobInput{
		Run: frozen.Run, Prompt: prompt, Route: frozen.Route,
		BudgetApprovalID:    frozen.BudgetApprovalID,
		BudgetMaximumMicros: frozen.BudgetMaximumMicros,
		BudgetCurrency:      frozen.BudgetCurrency,
		ProviderProfileID:   frozen.ProviderProfileID,
		TraceID:             frozen.TraceID, PersistProductTruth: true,
	}
	prepared, err := r.truth.PrepareProviderJob(ctx, orchestration.WorkflowStep{
		WorkflowID: frozen.WorkflowID, ActivityID: frozen.ActivityID,
		ActivityType: orchestration.ActivityExecuteProviderJob, TraceID: frozen.TraceID,
	}, preparation)
	if err != nil {
		return providercontract.JobRequest{}, err
	}
	expectedTruth := orchestration.PreparedProductTruth{
		ShotSpecRevisionID: frozen.ShotSpecRevisionID,
		Run:                frozen.Run, PromptSnapshotID: frozen.PromptSnapshotID,
		PromptSnapshotHash:  frozen.PromptSnapshotHash,
		GenerationPlanID:    frozen.GenerationPlanID,
		BudgetApprovalID:    frozen.BudgetApprovalID,
		BudgetMaximumMicros: frozen.BudgetMaximumMicros,
		BudgetCurrency:      frozen.BudgetCurrency,
		ProviderProfileID:   frozen.ProviderProfileID, Route: frozen.Route,
	}
	if prepared.ProductTruth != expectedTruth {
		return providercontract.JobRequest{}, providerError(
			providercontract.CodeConflict,
			"PostgreSQL product truth differs from the frozen stage 1 package",
		)
	}
	request := providercontract.JobRequest{
		SchemaVersion: "v1", JobID: frozen.IdempotencyKey,
		RunID: frozen.Run.RunID, Capability: providercontract.CapabilityVideo,
		InputHash: frozen.Run.RunSpecDigest, Model: frozen.Route,
		Request: providercontract.GenerationRequest{
			RequestID: frozen.IdempotencyKey, IdempotencyKey: frozen.IdempotencyKey,
			Modality: providercontract.ModalityVideo,
			Prompt:   prompt.PositivePrompt, PromptSnapshotID: prompt.ID,
			Context: prompt.Context, Assets: prompt.Assets, Output: prompt.Output,
			ModelHint: frozen.Route.ModelID, Budget: prepared.Budget,
		},
		BudgetReservation: prepared.BudgetReservation,
		TraceID:           frozen.TraceID,
	}
	if err := request.Validate(); err != nil {
		return providercontract.JobRequest{}, providerError(
			providercontract.CodeConflict,
			"PostgreSQL product truth did not produce an executable stage 1 request",
		)
	}
	if request.JobID != "provider-job-"+request.RunID ||
		request.Model.ModelID != FormalVideoModel ||
		request.Model.Verification != providercontract.PendingKey {
		return providercontract.JobRequest{}, providerError(
			providercontract.CodeConflict,
			"prepared stage 1 request differs from the frozen formal route",
		)
	}
	return request, nil
}

func (r *Runner) Complete(ctx context.Context, input CompleteInput) (CompletionResult, error) {
	if strings.TrimSpace(input.IdempotencyKey) == "" || input.ActualAFPMilli < 0 {
		return CompletionResult{}, providerError(providercontract.CodeInvalidRequest, "stage 1 completion identity and AFP usage are required")
	}
	response, err := r.adapter.Lookup(ctx, input.IdempotencyKey)
	if err != nil {
		return CompletionResult{}, err
	}
	if response.JobID != input.IdempotencyKey ||
		!providercontract.Terminal(response.State) && response.State != providercontract.StatusRequiresAction {
		return CompletionResult{}, &providercontract.Error{
			Code: providercontract.CodeUnavailable, SafeMessage: "stage 1 provider job is not terminal", Retryable: true,
		}
	}
	completion, evidenceErr := r.completionFromProvider(response, input)
	if evidenceErr == nil && completion.State == "TERMINAL_SUCCEEDED" {
		frozen, ok := r.executionPackage.JobByIdempotencyKey(input.IdempotencyKey)
		if !ok {
			return CompletionResult{}, providerError(
				providercontract.CodeForbidden,
				"completion is outside the frozen stage 1 execution package",
			)
		}
		artifact := response.Artifacts[0]
		if err := r.truth.CompletePreparedProviderJob(
			ctx,
			orchestration.WorkflowStep{
				WorkflowID: frozen.WorkflowID, ActivityID: frozen.ActivityID,
				ActivityType: orchestration.ActivityExecuteProviderJob, TraceID: frozen.TraceID,
			},
			frozen.Run.RunID,
			orchestration.ProviderResult{
				UpstreamTaskID: response.UpstreamTaskID, RequestID: response.RequestID,
				ArtifactDigest: artifact.SHA256, ArtifactURI: artifact.URI,
				MediaType: artifact.MediaType, ArtifactSize: artifact.SizeBytes,
				Width: artifact.Width, Height: artifact.Height,
				DurationMillis: artifact.DurationMillis,
				Model:          response.Model, Usage: response.Usage, Cost: response.Cost,
			},
		); err != nil {
			return CompletionResult{}, err
		}
	}
	if err := r.gate.Complete(input.IdempotencyKey, completion); err != nil {
		return CompletionResult{}, err
	}
	result := CompletionResult{
		IdempotencyKey: input.IdempotencyKey, ProviderTaskID: completion.ProviderTaskID,
		State: completion.State, ActualVideoTokens: completion.ActualVideoTokens,
		ActualAFPMilli: completion.ActualAFPMilli, ActualCashMicros: completion.ActualCashMicros,
		EvidenceComplete: completion.EvidenceComplete, ContentSafetyFailed: completion.ContentSafetyFailed,
	}
	if evidenceErr != nil {
		return result, evidenceErr
	}
	return result, nil
}

func (r *Runner) completionFromProvider(
	response providercontract.JobResponse,
	input CompleteInput,
) (Completion, error) {
	completion := Completion{
		ProviderTaskID: response.UpstreamTaskID, State: "TERMINAL_FAILED",
		ActualVideoTokens: response.Usage.VideoTokens, ActualAFPMilli: input.ActualAFPMilli,
		EvidenceComplete: input.EvidenceComplete,
	}
	if response.Cost.ActualMicros != nil {
		completion.ActualCashMicros = *response.Cost.ActualMicros
	}
	if response.State == providercontract.StatusSucceeded {
		completion.State = "TERMINAL_SUCCEEDED"
	}
	if response.Error != nil && response.Error.Code == providercontract.CodeContentBlocked {
		completion.ContentSafetyFailed = true
	}

	var evidenceProblems []string
	if strings.TrimSpace(response.UpstreamTaskID) == "" || strings.TrimSpace(response.RequestID) == "" {
		evidenceProblems = append(evidenceProblems, "provider task/request identity")
	}
	if response.Model.ModelID != FormalVideoModel || response.Model.Verification != providercontract.PendingKey {
		evidenceProblems = append(evidenceProblems, "frozen model route")
	}
	if response.Cost.ActualMicros == nil || response.Cost.Currency != "CNY" {
		evidenceProblems = append(evidenceProblems, "actual cost attribution")
	}
	if response.State == providercontract.StatusSucceeded {
		if response.Usage.VideoTokens <= 0 || input.ActualAFPMilli <= 0 {
			evidenceProblems = append(evidenceProblems, "video token/AFP usage")
		}
		if len(response.Artifacts) == 0 {
			evidenceProblems = append(evidenceProblems, "provider artifacts")
		}
		for _, artifact := range response.Artifacts {
			if !validCASArtifact(artifact) {
				evidenceProblems = append(evidenceProblems, "valid CAS artifact")
				continue
			}
			exists, err := r.artifacts.Exists(artifact.SHA256)
			if err != nil || !exists {
				evidenceProblems = append(evidenceProblems, "CAS artifact presence")
			}
		}
	} else if response.Error == nil {
		evidenceProblems = append(evidenceProblems, "terminal failure classification")
	}
	if !input.EvidenceComplete {
		evidenceProblems = append(evidenceProblems, "operator evidence checklist")
	}
	if len(evidenceProblems) != 0 {
		completion.EvidenceComplete = false
		return completion, providerError(
			providercontract.CodeForbidden,
			fmt.Sprintf("stage 1 terminal evidence is incomplete: %s", strings.Join(evidenceProblems, ", ")),
		)
	}
	return completion, nil
}

func validCASArtifact(artifact providercontract.AssetRef) bool {
	if strings.TrimSpace(artifact.ID) == "" || strings.TrimSpace(artifact.Revision) == "" ||
		strings.TrimSpace(string(artifact.Kind)) == "" || strings.TrimSpace(string(artifact.Role)) == "" ||
		strings.TrimSpace(artifact.LicenseReference) == "" || artifact.SizeBytes <= 0 ||
		len(artifact.SHA256) != 64 || artifact.URI != "cas://sha256/"+artifact.SHA256 {
		return false
	}
	for _, character := range artifact.SHA256 {
		if character < '0' || (character > '9' && character < 'a') || character > 'f' {
			return false
		}
	}
	return true
}
