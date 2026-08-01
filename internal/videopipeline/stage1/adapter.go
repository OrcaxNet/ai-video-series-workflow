package stage1

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/mockprovider"
)

// AdapterSubmitter is the production bridge used after readiness approval.
// Executor always invokes Recover before Submit, while the adapter enforces the
// same idempotency key again. No prompt is written to the stage 1 ledger.
type AdapterSubmitter struct {
	endpoint string
	client   *http.Client
}

func NewAdapterSubmitter(endpoint string, client *http.Client) (*AdapterSubmitter, error) {
	if strings.TrimSpace(endpoint) == "" || client == nil {
		return nil, errors.New("provider adapter endpoint and authenticated client are required")
	}
	return &AdapterSubmitter{endpoint: strings.TrimRight(endpoint, "/"), client: client}, nil
}

func (s *AdapterSubmitter) Recover(
	ctx context.Context,
	idempotencyKey string,
) (RecoveryResult, error) {
	response, err := s.Lookup(ctx, idempotencyKey)
	if providercontract.ErrorCodeOf(err) == providercontract.CodeNotFound {
		return RecoveryResult{}, nil
	}
	if err != nil {
		return RecoveryResult{}, err
	}
	if response.JobID != idempotencyKey {
		return RecoveryResult{}, providerError(providercontract.CodeConflict, "recovered provider job does not match the idempotency key")
	}
	if strings.TrimSpace(response.UpstreamTaskID) == "" {
		if response.State == providercontract.StatusRequiresAction && response.Error != nil &&
			response.Error.Code == providercontract.CodeContentBlocked {
			return RecoveryResult{}, providerError(
				providercontract.CodeContentBlocked,
				"stage 1 content safety rejected the job before provider task creation",
			)
		}
		return RecoveryResult{}, providerError(
			providercontract.CodeConflict,
			"recovered stage 1 provider job has no recoverable provider task",
		)
	}
	return RecoveryResult{Found: true, ProviderTaskID: response.UpstreamTaskID}, nil
}

// Lookup returns the adapter's durable job view without submitting. The Stage
// 1 runner uses it both for pre-submit recovery and for terminal completion.
func (s *AdapterSubmitter) Lookup(
	ctx context.Context,
	idempotencyKey string,
) (providercontract.JobResponse, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return providercontract.JobResponse{}, providerError(providercontract.CodeInvalidRequest, "provider job idempotency key is required")
	}
	return mockprovider.Get(ctx, s.client, s.endpoint, idempotencyKey)
}

func (s *AdapterSubmitter) Submit(
	ctx context.Context,
	attempt Attempt,
) (SubmitResult, error) {
	if attempt.JobRequest == nil || attempt.JobRequest.JobID != attempt.IdempotencyKey ||
		attempt.JobRequest.Request.IdempotencyKey != attempt.IdempotencyKey ||
		attempt.JobRequest.Capability != providercontract.CapabilityVideo ||
		attempt.JobRequest.Model.ModelID != FormalVideoModel {
		return SubmitResult{}, providerError(providercontract.CodeInvalidRequest, "stage 1 attempt lacks the exact frozen video job envelope")
	}
	response, err := mockprovider.Submit(ctx, s.client, s.endpoint, *attempt.JobRequest)
	if err != nil {
		return SubmitResult{}, err
	}
	if response.JobID != attempt.IdempotencyKey {
		return SubmitResult{}, providerError(providercontract.CodeConflict, "provider response does not match the stage 1 idempotency key")
	}
	return SubmitResult{ProviderTaskID: response.UpstreamTaskID}, nil
}
