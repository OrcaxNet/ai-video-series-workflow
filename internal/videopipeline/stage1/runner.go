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
	ShotID        string                                   `json:"shotId"`
	QuotaSnapshot *orchestration.SubscriptionQuotaSnapshot `json:"quotaSnapshot,omitempty"`
}

// PollInput selects an already-prepared immutable attempt. It cannot create a
// provider job or modify the Stage 1 ledger.
type PollInput struct {
	IdempotencyKey string `json:"idempotencyKey"`
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
	FailureClass        string `json:"failureClass,omitempty"`
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
	controlledRetry  *ControlledRetryPackage
	supersession     *FLO167SupersessionPackage
}

func NewRunner(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	executionPackage ExecutionPackage,
) (*Runner, error) {
	return newRunner(gate, adapter, artifacts, truth, executionPackage, nil, nil, nil)
}

// NewRunnerWithExecutionPackageRevision requires both immutable artifacts so
// the gate can prove that the child changes only the approved speech-v2 fields.
func NewRunnerWithExecutionPackageRevision(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	parentExecutionPackage ExecutionPackage,
	executionPackage ExecutionPackage,
) (*Runner, error) {
	return newRunner(
		gate, adapter, artifacts, truth, executionPackage, &parentExecutionPackage, nil, nil,
	)
}

// NewRunnerWithControlledRetry enables the separately sealed +1 path without
// changing the ten-job package already bound to the durable ledger.
func NewRunnerWithControlledRetry(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	executionPackage ExecutionPackage,
	controlledRetry ControlledRetryPackage,
) (*Runner, error) {
	return newRunner(gate, adapter, artifacts, truth, executionPackage, nil, &controlledRetry, nil)
}

// NewRunnerWithFLO167Supersession is the only continuation constructor. It
// keeps the v2 package as immutable parent evidence and requires PostgreSQL to
// independently authorize the exact v3 hash before any paid-boundary insert.
func NewRunnerWithFLO167Supersession(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	legacyExecutionPackage ExecutionPackage,
	supersession FLO167SupersessionPackage,
) (*Runner, error) {
	return newRunner(gate, adapter, artifacts, truth, legacyExecutionPackage, nil, nil, &supersession)
}

func newRunner(
	gate *Gate,
	adapter *AdapterSubmitter,
	artifacts ArtifactVerifier,
	truth ProductTruthPreparer,
	executionPackage ExecutionPackage,
	parentExecutionPackage *ExecutionPackage,
	controlledRetry *ControlledRetryPackage,
	supersession *FLO167SupersessionPackage,
) (*Runner, error) {
	if gate == nil || adapter == nil || artifacts == nil || truth == nil {
		return nil, errors.New("stage 1 gate, authenticated adapter, CAS verifier, and product truth are required")
	}
	if err := executionPackage.Validate(gate.Plan()); err != nil {
		if executionPackage.RequiresRevisionParent() {
			return nil, UnverifiableRevisionParentError(err)
		}
		return nil, err
	}
	if executionPackage.ParentExecutionPackageHash != "" && controlledRetry != nil {
		return nil, errors.New("speech-v2 package revision cannot be combined with a controlled retry package")
	}
	if supersession != nil {
		if controlledRetry != nil || executionPackage.ParentExecutionPackageHash != "" || executionPackage.LiveActivation == nil {
			return nil, errors.New("FLO-167 supersession requires the original v2 live package without another revision")
		}
		if err := supersession.Validate(); err != nil {
			return nil, fmt.Errorf("validate FLO-167 supersession: %w", err)
		}
		if supersession.LegacyExecutionPackageHash != executionPackage.ContentHash ||
			supersession.LegacyAuthorizationHash != executionPackage.LiveActivation.SourceAuthorizationHash {
			return nil, errors.New("FLO-167 supersession is bound to another legacy package or authorization")
		}
	}
	if executionPackage.ParentExecutionPackageHash == "" {
		if parentExecutionPackage != nil {
			return nil, UnverifiableRevisionParentError(errors.New("an original execution package cannot name a revision parent artifact"))
		}
		if err := gate.BindExecutionPackage(executionPackage.ContentHash); err != nil {
			return nil, err
		}
	} else {
		if parentExecutionPackage == nil {
			return nil, UnverifiableRevisionParentError(errors.New("speech-v2 package revision requires its immutable parent artifact"))
		}
		if err := gate.BindExecutionPackageRevision(executionPackage, *parentExecutionPackage); err != nil {
			return nil, err
		}
	}
	if controlledRetry != nil {
		if err := controlledRetry.Validate(gate.Plan(), executionPackage); err != nil {
			return nil, err
		}
		if err := gate.BindControlledRetryPackage(*controlledRetry); err != nil {
			return nil, err
		}
	}
	if supersession != nil {
		if err := gate.BindFLO167Supersession(*supersession); err != nil {
			return nil, err
		}
	}
	executor, err := NewExecutor(gate, adapter)
	if err != nil {
		return nil, err
	}
	return &Runner{
		gate: gate, adapter: adapter, executor: executor, artifacts: artifacts,
		truth: truth, executionPackage: executionPackage, controlledRetry: controlledRetry,
		supersession: supersession,
	}, nil
}

func (r *Runner) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	frozen, ok := r.executionPackage.Job(input.ShotID)
	if !ok {
		return SubmitResult{}, providerError(providercontract.CodeForbidden, "shot is outside the frozen stage 1 execution package")
	}
	if r.supersession != nil {
		binding, ok := r.supersession.Shot(input.ShotID)
		if !ok {
			return SubmitResult{}, providerError(providercontract.CodeForbidden, "shot has no FLO-167 supersession binding")
		}
		if err := r.supersession.AuthorizeSubmit(input.ShotID, binding.Pricing.ExpectedAFPMilli); err != nil {
			return SubmitResult{}, providerError(providercontract.CodeForbidden, err.Error())
		}
		frozen.PredictedAFPMilli = binding.Pricing.ExpectedAFPMilli
	}
	return r.submitFrozen(ctx, frozen, nil, input.QuotaSnapshot)
}

// Poll advances and returns the authenticated Adapter view for an attempt that
// is already present in the immutable package and durable Stage 1 ledger. This
// gives operators a read-only terminal check before supplying independent AFP
// evidence to Complete; using Complete as a poll could accidentally freeze an
// evidence-incomplete terminal result.
func (r *Runner) Poll(ctx context.Context, input PollInput) (providercontract.JobResponse, error) {
	key := strings.TrimSpace(input.IdempotencyKey)
	if key == "" {
		return providercontract.JobResponse{}, providerError(
			providercontract.CodeInvalidRequest, "stage 1 poll identity is required",
		)
	}
	if _, ok := r.jobByIdempotencyKey(key); !ok {
		return providercontract.JobResponse{}, providerError(
			providercontract.CodeForbidden, "poll is outside the frozen stage 1 execution package",
		)
	}
	ledger, err := r.gate.Snapshot()
	if err != nil {
		return providercontract.JobResponse{}, err
	}
	if ledger.Records[key] == nil {
		return providercontract.JobResponse{}, providerError(
			providercontract.CodeForbidden, "stage 1 attempt must be prepared before polling",
		)
	}
	response, err := r.adapter.Lookup(ctx, key)
	if err != nil {
		return providercontract.JobResponse{}, err
	}
	if response.JobID != key {
		return providercontract.JobResponse{}, providerError(
			providercontract.CodeConflict, "polled provider job does not match the immutable attempt",
		)
	}
	return response, nil
}

// SubmitControlledRetry is the only production path to the approved +1. The
// caller selects only the shot; every new identity and approval comes from the
// immutable retry extension.
func (r *Runner) SubmitControlledRetry(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	if r.controlledRetry == nil || r.controlledRetry.Job.ShotID != input.ShotID {
		return SubmitResult{}, providerError(providercontract.CodeForbidden, "shot is outside the frozen stage 1 controlled retry package")
	}
	approval := r.controlledRetry.Approval
	return r.submitFrozen(ctx, r.controlledRetry.Job, &approval, input.QuotaSnapshot)
}

func (r *Runner) submitFrozen(
	ctx context.Context,
	frozen FrozenJob,
	retry *RetryApproval,
	quota *orchestration.SubscriptionQuotaSnapshot,
) (SubmitResult, error) {
	attempt := attemptFromFrozen(frozen, retry)

	return r.executor.ExecutePrepared(ctx, attempt, func(ctx context.Context) (providercontract.JobRequest, error) {
		return r.prepareProductTruth(ctx, frozen, quota)
	})
}

func attemptFromFrozen(frozen FrozenJob, retry *RetryApproval) Attempt {
	return Attempt{
		AttemptID: frozen.AttemptID, ShotID: frozen.ShotID,
		IdempotencyKey:                     frozen.IdempotencyKey,
		EstimatedVideoTokens:               frozen.EstimatedVideoTokens,
		PredictedAFPMilli:                  frozen.PredictedAFPMilli,
		EstimatedNonSubscriptionCashMicros: frozen.EstimatedNonSubscriptionCashMicros,
		Retry:                              retry,
	}
}

func (r *Runner) prepareProductTruth(
	ctx context.Context,
	frozen FrozenJob,
	quota *orchestration.SubscriptionQuotaSnapshot,
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
	if activation := r.executionPackage.LiveActivation; activation != nil {
		expectedTruth.LiveActivationID = activation.ActivationID
		expectedTruth.ExecutionPackageHash = r.executionPackage.ContentHash
		expectedTruth.SourceCodeCommit = activation.SourceCodeCommit
		expectedTruth.EstimatedVideoTokens = frozen.EstimatedVideoTokens
		expectedTruth.PredictedAFPMilli = frozen.PredictedAFPMilli
		expectedTruth.BillingMode = frozen.BillingMode
		if r.controlledRetry != nil && r.controlledRetry.Job.Run.RunID == frozen.Run.RunID {
			expectedTruth.ControlledRetryPackageHash = r.controlledRetry.ContentHash
		}
	}
	if r.supersession != nil {
		binding, ok := r.supersession.Shot(frozen.ShotID)
		if !ok {
			return providercontract.JobRequest{}, providerError(providercontract.CodeForbidden, "shot has no FLO-167 pricing binding")
		}
		wire := orchestration.DurationPricingBinding{
			DurationMS: binding.Pricing.DurationMS, PricingSnapshotID: binding.Pricing.PricingSnapshotID,
			PricingSnapshotDigest: binding.Pricing.PricingSnapshotDigest,
			ReferenceAFPMilli:     binding.Pricing.ReferenceAFPMilli, ReferenceDurationMS: binding.Pricing.ReferenceDurationMS,
			ExpectedAFPMilli: binding.Pricing.ExpectedAFPMilli, NormalizationVersion: binding.Pricing.NormalizationVersion,
			RoundingVersion: binding.Pricing.RoundingVersion, PricingRuleVersion: binding.Pricing.PricingRuleVersion,
			MaximumDriftBPS: binding.Pricing.MaximumDriftBPS,
		}
		expectedTruth.SupersessionPackageHash = r.supersession.ContentHash
		expectedTruth.DurationPricing = &wire
		expectedTruth.RouteBindingHash = binding.RouteHash
		expectedTruth.G1BindingHash = binding.G1Hash
		expectedTruth.G2BindingHash = binding.G2Hash
		expectedTruth.SafetyBindingHash = binding.SafetyHash
		expectedTruth.CanonicalInputHash = binding.CanonicalInputHash
		expectedTruth.SemanticInputHash = binding.SemanticInputHash
	}
	preparation := orchestration.ExecuteProviderJobInput{
		Run: frozen.Run, Prompt: prompt, Route: frozen.Route,
		BudgetApprovalID:    frozen.BudgetApprovalID,
		BudgetMaximumMicros: frozen.BudgetMaximumMicros,
		BudgetCurrency:      frozen.BudgetCurrency,
		ProviderProfileID:   frozen.ProviderProfileID,
		TraceID:             frozen.TraceID, PersistProductTruth: true,
		ExpectedProductTruth:      &expectedTruth,
		SubscriptionQuotaSnapshot: quota,
		EstimatedVideoTokens:      frozen.EstimatedVideoTokens,
		PredictedAFPMilli:         frozen.PredictedAFPMilli,
		BillingMode:               frozen.BillingMode,
	}
	if activation := r.executionPackage.LiveActivation; activation != nil {
		preparation.ExpectedExecutionPackageHash = r.executionPackage.ContentHash
		preparation.ExpectedLiveActivationID = activation.ActivationID
		preparation.ExpectedSourceCodeCommit = activation.SourceCodeCommit
		if r.controlledRetry != nil && r.controlledRetry.Job.Run.RunID == frozen.Run.RunID {
			preparation.ExpectedControlledRetryPackageHash = r.controlledRetry.ContentHash
		}
	}
	if r.supersession != nil {
		preparation.ExpectedSupersessionPackageHash = r.supersession.ContentHash
		preparation.DurationPricing = expectedTruth.DurationPricing
		preparation.RouteBindingHash = expectedTruth.RouteBindingHash
		preparation.G1BindingHash = expectedTruth.G1BindingHash
		preparation.G2BindingHash = expectedTruth.G2BindingHash
		preparation.SafetyBindingHash = expectedTruth.SafetyBindingHash
		preparation.CanonicalInputHash = expectedTruth.CanonicalInputHash
		preparation.SemanticInputHash = expectedTruth.SemanticInputHash
	}
	prepared, err := r.truth.PrepareProviderJob(ctx, orchestration.WorkflowStep{
		WorkflowID: frozen.WorkflowID, ActivityID: frozen.ActivityID,
		ActivityType: orchestration.ActivityExecuteProviderJob, TraceID: frozen.TraceID,
	}, preparation)
	if err != nil {
		return providercontract.JobRequest{}, err
	}
	if prepared.ProductTruth != expectedTruth {
		return providercontract.JobRequest{}, providerError(
			providercontract.CodeConflict,
			"PostgreSQL product truth differs from the frozen stage 1 package",
		)
	}
	request, err := orchestration.BuildProviderJobRequest(preparation, prepared)
	if err != nil {
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
		frozen, ok := r.jobByIdempotencyKey(input.IdempotencyKey)
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
				ActualAFPMilli: input.ActualAFPMilli,
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
		FailureClass: completion.FailureClass,
	}
	if evidenceErr != nil {
		return result, evidenceErr
	}
	return result, nil
}

func (r *Runner) jobByIdempotencyKey(key string) (FrozenJob, bool) {
	if job, ok := r.executionPackage.JobByIdempotencyKey(key); ok {
		return job, true
	}
	if r.controlledRetry != nil && r.controlledRetry.Job.IdempotencyKey == key {
		return r.controlledRetry.Job, true
	}
	return FrozenJob{}, false
}

// FinalizationInput returns the immutable post-production input only when the
// selected ten Runs have evidence-complete successful terminal records. A
// successful controlled retry replaces exactly its failed primary Run.
func (r *Runner) FinalizationInput() (orchestration.FinalizeEpisodeInput, error) {
	ledger, err := r.gate.Snapshot()
	if err != nil {
		return orchestration.FinalizeEpisodeInput{}, err
	}
	failedPrimary := ""
	var failedRecord *Record
	for _, job := range r.executionPackage.PrimaryJobs {
		record := ledger.Records[job.IdempotencyKey]
		if record == nil || !terminalState(record.State) || !record.EvidenceComplete {
			return orchestration.FinalizeEpisodeInput{}, providerError(providercontract.CodeForbidden, "stage 1 finalization requires complete terminal evidence for every primary Run")
		}
		if record.State == "TERMINAL_FAILED" {
			if failedPrimary != "" {
				return orchestration.FinalizeEpisodeInput{}, providerError(providercontract.CodeForbidden, "stage 1 finalization has more than one failed primary Run")
			}
			failedPrimary = job.AttemptID
			failedRecord = record
		}
	}
	if failedPrimary == "" {
		return r.executionPackage.PostProduction, nil
	}
	if r.controlledRetry == nil || ledger.ControlledRetryPackageHash != r.controlledRetry.ContentHash ||
		r.controlledRetry.Approval.OriginalAttemptID != failedPrimary {
		return orchestration.FinalizeEpisodeInput{}, providerError(providercontract.CodeForbidden, "failed Stage 1 primary Run has no bound controlled retry")
	}
	if failedRecord == nil || failedRecord.FailureClass != r.controlledRetry.Approval.FailureClass {
		return orchestration.FinalizeEpisodeInput{}, providerError(providercontract.CodeForbidden, "controlled retry approval differs from the frozen primary failure")
	}
	retryRecord := ledger.Records[r.controlledRetry.Job.IdempotencyKey]
	approval := r.controlledRetry.Approval
	expectedAttempt := attemptFromFrozen(r.controlledRetry.Job, &approval)
	if retryRecord == nil || !sameAttempt(retryRecord.Attempt, expectedAttempt) ||
		retryRecord.State != "TERMINAL_SUCCEEDED" || !retryRecord.EvidenceComplete {
		return orchestration.FinalizeEpisodeInput{}, providerError(providercontract.CodeForbidden, "stage 1 controlled retry is not evidence-complete and successful")
	}
	return r.controlledRetry.PostProduction, nil
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
	if response.Error != nil {
		completion.FailureClass = string(response.Error.Code)
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
	if r.gate.Plan().SubscriptionIncludedOnly &&
		(response.Cost.ActualMicros == nil || *response.Cost.ActualMicros != 0 ||
			response.Cost.BillingMode != "subscription_included") {
		evidenceProblems = append(evidenceProblems, "subscription-included zero-cash attribution")
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
