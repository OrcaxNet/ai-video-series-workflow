package stage1

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

// ArtifactVerifier is deliberately narrower than artifactstore.Store so the
// runner can prove that every successful adapter artifact is present in CAS.
type ArtifactVerifier interface {
	Exists(string) (bool, error)
}

// SubmitInput keeps the prompt-bearing provider envelope outside Attempt. The
// gate persists Attempt only, so neither prompts nor transport URLs enter the
// durable Stage 1 ledger.
type SubmitInput struct {
	Attempt    Attempt                     `json:"attempt"`
	JobRequest providercontract.JobRequest `json:"jobRequest"`
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
	gate      *Gate
	adapter   *AdapterSubmitter
	executor  *Executor
	artifacts ArtifactVerifier
}

func NewRunner(gate *Gate, adapter *AdapterSubmitter, artifacts ArtifactVerifier) (*Runner, error) {
	if gate == nil || adapter == nil || artifacts == nil {
		return nil, errors.New("stage 1 gate, authenticated adapter, and CAS verifier are required")
	}
	executor, err := NewExecutor(gate, adapter)
	if err != nil {
		return nil, err
	}
	return &Runner{gate: gate, adapter: adapter, executor: executor, artifacts: artifacts}, nil
}

func (r *Runner) Submit(ctx context.Context, input SubmitInput) (SubmitResult, error) {
	if err := input.JobRequest.Validate(); err != nil {
		return SubmitResult{}, providerError(providercontract.CodeInvalidRequest, "stage 1 provider job envelope is invalid")
	}
	if input.JobRequest.JobID != input.Attempt.IdempotencyKey ||
		input.JobRequest.Request.IdempotencyKey != input.Attempt.IdempotencyKey ||
		input.JobRequest.Model.ModelID != FormalVideoModel ||
		input.JobRequest.Model.Verification != providercontract.PendingKey ||
		input.JobRequest.Capability != providercontract.CapabilityVideo {
		return SubmitResult{}, providerError(providercontract.CodeInvalidRequest, "stage 1 provider job is not bound to the approved formal route")
	}
	input.Attempt.JobRequest = &input.JobRequest
	return r.executor.Execute(ctx, input.Attempt)
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
