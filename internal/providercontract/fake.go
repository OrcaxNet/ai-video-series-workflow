package providercontract

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type FakeScenario string

const (
	FakeSuccess           FakeScenario = "success"
	FakeUnauthorized      FakeScenario = "unauthorized"
	FakeForbidden         FakeScenario = "forbidden"
	FakeRateLimited       FakeScenario = "rate_limited"
	FakeServerError       FakeScenario = "server_error"
	FakeTimeout           FakeScenario = "timeout"
	FakeQuotaExceeded     FakeScenario = "quota_exceeded"
	FakeContentBlocked    FakeScenario = "content_blocked"
	FakeDuplicateCallback FakeScenario = "duplicate_callback"
	FakeCancelRace        FakeScenario = "cancel_race"
	FakeRecovery          FakeScenario = "recovery"
)

type fakeRecord struct {
	job       Job
	pollCount int
}

// FakeProvider is deterministic and concurrency-safe. It performs no network
// access and must never be interpreted as live provider evidence.
type FakeProvider struct {
	mu             sync.Mutex
	scenario       FakeScenario
	records        map[string]*fakeRecord
	idempotency    map[string]string
	callbackEvents map[string]struct{}
	recoveryFailed map[string]bool
	now            func() time.Time
}

func NewFakeProvider(scenario FakeScenario) *FakeProvider {
	return &FakeProvider{
		scenario:       scenario,
		records:        make(map[string]*fakeRecord),
		idempotency:    make(map[string]string),
		callbackEvents: make(map[string]struct{}),
		recoveryFailed: make(map[string]bool),
		now:            func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
	}
}

func (f *FakeProvider) Discover(context.Context) ([]Capability, error) {
	return []Capability{{
		Provider:               "fake",
		ModelFamily:            "fake-video-v1",
		ModelVersion:           "fixture-1",
		InputModalities:        []Modality{ModalityText, ModalityImage},
		OutputModality:         ModalityVideo,
		Async:                  true,
		SupportsPolling:        true,
		SupportsCallback:       true,
		SupportsCancel:         true,
		SupportsReferenceImage: true,
		SupportsLastFrame:      true,
		Resolutions:            []string{"720p"},
		AspectRatios:           []string{"16:9"},
		MinDurationMillis:      4_000,
		MaxDurationMillis:      6_000,
		NativeFPS:              []int{24},
		Verification:           "mock_only",
	}}, nil
}

func (f *FakeProvider) Submit(ctx context.Context, request GenerationRequest) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, MapContextError(err)
	}
	if err := request.Validate(); err != nil {
		return Job{}, &Error{Code: CodeInvalidRequest, SafeMessage: err.Error()}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if id, ok := f.idempotency[request.IdempotencyKey]; ok {
		return f.records[id].job, nil
	}

	if err := f.scenarioError(); err != nil {
		return Job{}, err
	}

	id := fmt.Sprintf("fake-job-%03d", len(f.records)+1)
	now := f.now()
	job := Job{
		ID:                id,
		RequestID:         request.RequestID,
		IdempotencyKey:    request.IdempotencyKey,
		Status:            StatusQueued,
		Provider:          "fake",
		ProviderModel:     "fake-video-v1",
		ProviderRequestID: "fake-request-" + id,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	f.records[id] = &fakeRecord{job: job}
	f.idempotency[request.IdempotencyKey] = id

	// Recovery simulates an upstream timeout after it accepted the task. The
	// next submit with the same idempotency key returns the one accepted job.
	if f.scenario == FakeRecovery && !f.recoveryFailed[request.IdempotencyKey] {
		f.recoveryFailed[request.IdempotencyKey] = true
		return Job{}, &Error{
			Code:        CodeUnavailable,
			Retryable:   true,
			SafeMessage: "provider service is unavailable",
		}
	}
	return job, nil
}

func (f *FakeProvider) Poll(ctx context.Context, id string) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, MapContextError(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	record, ok := f.records[id]
	if !ok {
		return Job{}, &Error{Code: CodeNotFound, SafeMessage: "provider job was not found"}
	}
	if record.job.Status.Terminal() {
		return record.job, nil
	}
	record.pollCount++
	record.job.UpdatedAt = f.now().Add(time.Duration(record.pollCount) * time.Second)
	if f.scenario == FakeCancelRace || record.pollCount >= 2 {
		record.job.Status = StatusSucceeded
		record.job.Output = &Output{
			Assets: []AssetRef{{
				ID:               "asset-fake-output",
				Revision:         "rev-1",
				Kind:             ModalityVideo,
				Role:             AssetRoleOutput,
				URI:              "https://example.invalid/fake-output.mp4",
				SHA256:           "352b94f7134554825a7c426e579d12943a48c30db1666120353f5f24aa9c59b3",
				LicenseReference: "fixture-license",
			}},
			Usage: Usage{
				VideoTokens:        250_000,
				GeneratedMillis:    5_000,
				ProviderCostMicros: 7_000_000,
			},
		}
	} else {
		record.job.Status = StatusRunning
	}
	return record.job, nil
}

func (f *FakeProvider) Cancel(ctx context.Context, id string) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, MapContextError(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	record, ok := f.records[id]
	if !ok {
		return Job{}, &Error{Code: CodeNotFound, SafeMessage: "provider job was not found"}
	}
	if record.job.Status.Terminal() {
		return record.job, nil
	}
	record.job.Status = StatusCancelled
	record.job.UpdatedAt = f.now().Add(time.Second)
	return record.job, nil
}

// ApplyCallback returns applied=false for duplicate events. A production
// receiver must persist the event ID and then poll the provider for the
// authoritative state before transitioning a durable job.
func (f *FakeProvider) ApplyCallback(callback Callback) (applied bool, job Job, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, duplicate := f.callbackEvents[callback.EventID]; duplicate {
		record, ok := f.records[callback.JobID]
		if !ok {
			return false, Job{}, &Error{Code: CodeNotFound, SafeMessage: "provider job was not found"}
		}
		return false, record.job, nil
	}
	record, ok := f.records[callback.JobID]
	if !ok {
		return false, Job{}, &Error{Code: CodeNotFound, SafeMessage: "provider job was not found"}
	}
	f.callbackEvents[callback.EventID] = struct{}{}
	if !record.job.Status.Terminal() {
		record.job.Status = callback.Status
		record.job.UpdatedAt = callback.CreatedAt
	}
	return true, record.job, nil
}

func (f *FakeProvider) scenarioError() error {
	switch f.scenario {
	case FakeUnauthorized:
		return &Error{Code: CodeUnauthenticated, HTTPStatus: 401, SafeMessage: "provider authentication failed"}
	case FakeForbidden:
		return &Error{Code: CodeForbidden, HTTPStatus: 403, SafeMessage: "provider authorization failed"}
	case FakeRateLimited:
		return &Error{Code: CodeRateLimited, HTTPStatus: 429, Retryable: true, RetryAfter: time.Second, SafeMessage: "provider rate limit exceeded"}
	case FakeServerError:
		return &Error{Code: CodeUnavailable, HTTPStatus: 503, Retryable: true, SafeMessage: "provider service is unavailable"}
	case FakeTimeout:
		return &Error{Code: CodeTimeout, Retryable: true, SafeMessage: "provider request timed out"}
	case FakeQuotaExceeded:
		return &Error{Code: CodeQuotaExceeded, HTTPStatus: 429, SafeMessage: "provider quota is exhausted"}
	case FakeContentBlocked:
		return &Error{Code: CodeContentBlocked, HTTPStatus: 400, SafeMessage: "provider content policy blocked the request"}
	default:
		return nil
	}
}
