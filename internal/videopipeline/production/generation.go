package production

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

type Gate string

const (
	Gate1 Gate = "G1"
	Gate2 Gate = "G2"
	Gate3 Gate = "G3"
)

type GateApproval struct {
	Gate        Gate      `json:"gate"`
	DecisionID  string    `json:"decision_id"`
	Approved    bool      `json:"approved"`
	BindingID   string    `json:"binding_id"`
	BindingHash string    `json:"binding_hash"`
	ActorID     string    `json:"actor_id"`
	DecidedAt   time.Time `json:"decided_at"`
}

func (g GateApproval) Validate(expected Gate, bindingID, bindingHash string) error {
	if g.Gate != expected || !g.Approved || !nonEmpty(g.DecisionID, g.BindingID, g.BindingHash, g.ActorID) ||
		g.BindingID != bindingID || g.BindingHash != bindingHash || !validSHA256(g.BindingHash) ||
		g.DecidedAt.IsZero() {
		return policyf("%s approval is missing, rejected, or bound to different immutable input", expected)
	}
	return nil
}

type AuthorizationEvidence struct {
	SourceRevisionID  string    `json:"source_revision_id"`
	SourceHash        string    `json:"source_hash"`
	LicenseReference  string    `json:"license_reference"`
	Authorized        bool      `json:"authorized"`
	AdaptationAllowed bool      `json:"adaptation_allowed"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
}

func (a AuthorizationEvidence) Validate(source RevisionRef, at time.Time) error {
	if !a.Authorized || !a.AdaptationAllowed ||
		a.SourceRevisionID != source.ID || a.SourceHash != source.ContentHash ||
		!validSHA256(a.SourceHash) || a.LicenseReference == "" {
		return policyf("generation is not bound to current source authorization")
	}
	if !a.ExpiresAt.IsZero() && !a.ExpiresAt.After(at) {
		return policyf("source authorization expired before generation")
	}
	return nil
}

type ArtifactCommitter interface {
	Commit(context.Context, providercontract.AssetRef) (providercontract.AssetRef, error)
}

// RequireCASCommitter accepts only adapter outputs already committed to the
// local content-addressed store. Live adapters download signed URLs and verify
// bytes before returning; temporary URLs never enter durable truth.
type RequireCASCommitter struct{}

func (RequireCASCommitter) Commit(_ context.Context, asset providercontract.AssetRef) (providercontract.AssetRef, error) {
	if !validSHA256(asset.SHA256) ||
		asset.URI != "cas://sha256/"+asset.SHA256 ||
		!nonEmpty(asset.ID, asset.Revision, asset.LicenseReference, asset.MediaType) {
		return providercontract.AssetRef{}, policyf("provider output is not a complete verified CAS artifact")
	}
	return asset, nil
}

type AttemptRecord struct {
	Sequence      int                        `json:"sequence"`
	Kind          string                     `json:"kind"`
	State         providercontract.JobStatus `json:"state"`
	StartedAt     time.Time                  `json:"started_at"`
	FinishedAt    time.Time                  `json:"finished_at"`
	ProviderJobID string                     `json:"provider_job_id,omitempty"`
	ErrorCode     providercontract.ErrorCode `json:"error_code,omitempty"`
	Retryable     bool                       `json:"retryable,omitempty"`
}

type RunState string

const (
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded"
	RunFailed    RunState = "failed"
	RunCancelled RunState = "cancelled"
)

type RunRecord struct {
	RunID               string                             `json:"run_id"`
	IdempotencyKey      string                             `json:"idempotency_key"`
	RequestHash         string                             `json:"request_hash"`
	State               RunState                           `json:"state"`
	Evidence            string                             `json:"evidence"`
	SourceRevision      RevisionRef                        `json:"source_revision"`
	ShotRevision        RevisionRef                        `json:"shot_revision"`
	PromptSnapshotID    string                             `json:"prompt_snapshot_id"`
	PromptSnapshotHash  string                             `json:"prompt_snapshot_hash"`
	InputRevisionHashes map[string]string                  `json:"input_revision_hashes"`
	Capability          providercontract.Capability        `json:"capability"`
	Model               providercontract.ModelSnapshot     `json:"model_snapshot"`
	BudgetReservation   providercontract.BudgetReservation `json:"budget_reservation"`
	Request             providercontract.GenerationRequest `json:"request"`
	Attempts            []AttemptRecord                    `json:"attempts"`
	ProviderJob         *providercontract.Job              `json:"provider_job,omitempty"`
	ManifestID          string                             `json:"manifest_id,omitempty"`
	ManifestHash        string                             `json:"manifest_hash,omitempty"`
	StartedAt           time.Time                          `json:"started_at"`
	FinishedAt          time.Time                          `json:"finished_at,omitempty"`
	ErrorCode           providercontract.ErrorCode         `json:"error_code,omitempty"`
}

type GenerationInput struct {
	RunID             string
	IdempotencyKey    string
	SourceRevision    RevisionRef
	ShotRevision      RevisionRef
	Prompt            PromptSnapshot
	Authorization     AuthorizationEvidence
	Gate1             GateApproval
	Gate2             GateApproval
	Route             providercontract.ModelSnapshot
	Budget            providercontract.BudgetEnvelope
	BudgetPolicy      providercontract.BudgetPolicy
	BudgetReservation providercontract.BudgetReservation
	SpentMicros       int64
	// ReservedMicros is the sum of other outstanding reservations. The
	// reservation for this run is validated separately against its estimate
	// and immutable PromptSnapshot/model binding.
	ReservedMicros int64
	CallbackURL    string
	Evidence       string
	MaxPolls       int
}

type ManifestRecord struct {
	Manifest providercontract.GenerationManifest `json:"manifest"`
	Hash     string                              `json:"hash"`
}

type ManifestStore struct {
	mu   sync.RWMutex
	byID map[string]ManifestRecord
}

func NewManifestStore() *ManifestStore {
	return &ManifestStore{byID: make(map[string]ManifestRecord)}
}

func (s *ManifestStore) Put(manifest providercontract.GenerationManifest) (ManifestRecord, error) {
	if err := manifest.Validate(); err != nil {
		return ManifestRecord{}, err
	}
	digest, err := contentHash(manifest)
	if err != nil {
		return ManifestRecord{}, err
	}
	record := ManifestRecord{Manifest: manifest, Hash: digest}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.byID[manifest.ManifestID]; ok {
		if existing.Hash != digest {
			return ManifestRecord{}, conflictf("manifest %q cannot be mutated", manifest.ManifestID)
		}
		return cloneManifestRecord(existing), nil
	}
	s.byID[manifest.ManifestID] = cloneManifestRecord(record)
	return cloneManifestRecord(record), nil
}

func (s *ManifestStore) Get(id string) (ManifestRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.byID[id]
	return cloneManifestRecord(record), ok
}

func cloneManifestRecord(record ManifestRecord) ManifestRecord {
	data, err := json.Marshal(record)
	if err != nil {
		return ManifestRecord{}
	}
	var cloned ManifestRecord
	if err := json.Unmarshal(data, &cloned); err != nil {
		return ManifestRecord{}
	}
	return cloned
}

type RunLedger struct {
	mu            sync.RWMutex
	byRunID       map[string]RunRecord
	byIdempotency map[string]string
}

func NewRunLedger() *RunLedger {
	return &RunLedger{
		byRunID:       make(map[string]RunRecord),
		byIdempotency: make(map[string]string),
	}
}

func (l *RunLedger) Begin(record RunRecord) (RunRecord, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if runID, ok := l.byIdempotency[record.IdempotencyKey]; ok {
		existing := l.byRunID[runID]
		if existing.RequestHash != record.RequestHash {
			return RunRecord{}, false, conflictf("idempotency key was already used for different generation input")
		}
		return cloneRunRecord(existing), true, nil
	}
	if existing, ok := l.byRunID[record.RunID]; ok {
		if existing.RequestHash != record.RequestHash {
			return RunRecord{}, false, conflictf("run ID was already used for different generation input")
		}
		return cloneRunRecord(existing), true, nil
	}
	l.byRunID[record.RunID] = cloneRunRecord(record)
	l.byIdempotency[record.IdempotencyKey] = record.RunID
	return cloneRunRecord(record), false, nil
}

func (l *RunLedger) Complete(record RunRecord) (RunRecord, error) {
	if record.State == RunRunning || record.FinishedAt.IsZero() {
		return RunRecord{}, validationf("only terminal generation records may be completed")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	existing, ok := l.byRunID[record.RunID]
	if !ok || existing.RequestHash != record.RequestHash {
		return RunRecord{}, conflictf("generation run was not begun with this request")
	}
	if existing.State != RunRunning {
		if existing.ManifestHash != record.ManifestHash || existing.ErrorCode != record.ErrorCode {
			return RunRecord{}, conflictf("generation run already has a different terminal record")
		}
		return cloneRunRecord(existing), nil
	}
	l.byRunID[record.RunID] = cloneRunRecord(record)
	return cloneRunRecord(record), nil
}

func (l *RunLedger) Get(runID string) (RunRecord, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record, ok := l.byRunID[runID]
	return cloneRunRecord(record), ok
}

type GenerationRunner struct {
	Provider  providercontract.Provider
	Committer ArtifactCommitter
	Prompts   PromptSnapshotSource
	Manifests *ManifestStore
	Ledger    *RunLedger
	Now       func() time.Time
	Wait      func(context.Context, time.Duration) error
	mu        sync.Mutex
}

func NewGenerationRunner(provider providercontract.Provider, committer ArtifactCommitter, prompts PromptSnapshotSource) *GenerationRunner {
	return &GenerationRunner{
		Provider:  provider,
		Committer: committer,
		Prompts:   prompts,
		Manifests: NewManifestStore(),
		Ledger:    NewRunLedger(),
		Now:       time.Now,
		Wait:      waitContext,
	}
}

func (r *GenerationRunner) Execute(ctx context.Context, input GenerationInput) (RunRecord, error) {
	// Serialize only the local idempotency transaction. Remote providers also
	// receive the same key, so process recovery preserves at-most-one paid job.
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Provider == nil || r.Committer == nil || r.Prompts == nil ||
		r.Manifests == nil || r.Ledger == nil || r.Now == nil || r.Wait == nil {
		return RunRecord{}, validationf("provider, CAS committer, prompt source, stores, clock, and wait function are required")
	}
	startedAt := r.Now().UTC()
	request, capability, err := r.preflight(ctx, input, startedAt)
	if err != nil {
		return RunRecord{}, err
	}
	requestHash, err := contentHash(struct {
		Request           providercontract.GenerationRequest `json:"request"`
		SourceRevision    RevisionRef                        `json:"source_revision"`
		ShotRevision      RevisionRef                        `json:"shot_revision"`
		PromptHash        string                             `json:"prompt_hash"`
		Authorization     AuthorizationEvidence              `json:"authorization"`
		Gate1             GateApproval                       `json:"gate1"`
		Gate2             GateApproval                       `json:"gate2"`
		Route             providercontract.ModelSnapshot     `json:"route"`
		BudgetReservation providercontract.BudgetReservation `json:"budget_reservation"`
		Evidence          string                             `json:"evidence"`
	}{
		Request:           request,
		SourceRevision:    input.SourceRevision,
		ShotRevision:      input.ShotRevision,
		PromptHash:        input.Prompt.ContentHash,
		Authorization:     input.Authorization,
		Gate1:             input.Gate1,
		Gate2:             input.Gate2,
		Route:             input.Route,
		BudgetReservation: input.BudgetReservation,
		Evidence:          input.Evidence,
	})
	if err != nil {
		return RunRecord{}, err
	}
	record := RunRecord{
		RunID:               input.RunID,
		IdempotencyKey:      input.IdempotencyKey,
		RequestHash:         requestHash,
		State:               RunRunning,
		Evidence:            input.Evidence,
		SourceRevision:      input.SourceRevision,
		ShotRevision:        input.ShotRevision,
		PromptSnapshotID:    input.Prompt.ID,
		PromptSnapshotHash:  input.Prompt.ContentHash,
		InputRevisionHashes: mapsClone(input.Prompt.InputRevisionHashes),
		Capability:          capability,
		Model:               input.Route,
		BudgetReservation:   input.BudgetReservation,
		Request:             request,
		StartedAt:           startedAt,
	}
	if existing, replay, beginErr := r.Ledger.Begin(record); beginErr != nil {
		return RunRecord{}, beginErr
	} else if replay && existing.State != RunRunning {
		if existing.State == RunSucceeded {
			return existing, nil
		}
		if existing.ErrorCode != "" {
			return existing, &providercontract.Error{
				Code:        existing.ErrorCode,
				SafeMessage: "the idempotent generation attempt is already terminal",
			}
		}
		return existing, errors.New("the idempotent generation attempt is already terminal")
	}

	maxAttempts := input.Budget.MaxAttempts
	var job providercontract.Job
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptStarted := r.Now().UTC()
		submitted, submitErr := r.Provider.Submit(ctx, request)
		attemptFinished := later(r.Now().UTC(), attemptStarted)
		attemptRecord := AttemptRecord{
			Sequence:   len(record.Attempts) + 1,
			Kind:       "provider_submit",
			State:      providercontract.StatusFailed,
			StartedAt:  attemptStarted,
			FinishedAt: attemptFinished,
		}
		if submitErr == nil {
			job = submitted
			attemptRecord.State = submitted.Status
			attemptRecord.ProviderJobID = submitted.ID
			record.Attempts = append(record.Attempts, attemptRecord)
			break
		}
		var providerErr *providercontract.Error
		if errors.As(submitErr, &providerErr) {
			attemptRecord.ErrorCode = providerErr.Code
			attemptRecord.Retryable = providerErr.Retryable
		}
		record.Attempts = append(record.Attempts, attemptRecord)
		if !attemptRecord.Retryable || attempt == maxAttempts {
			return r.fail(record, submitErr)
		}
		if waitErr := r.Wait(ctx, retryDelay(providerErr)); waitErr != nil {
			return r.fail(record, waitErr)
		}
	}
	if job.ID == "" {
		return r.fail(record, &providercontract.Error{
			Code:        providercontract.CodeUnavailable,
			Retryable:   true,
			SafeMessage: "provider submission outcome is unknown",
		})
	}

	maxPolls := input.MaxPolls
	if maxPolls <= 0 {
		maxPolls = 60
	}
	for poll := 1; !job.Status.Terminal() && poll <= maxPolls; poll++ {
		pollStarted := r.Now().UTC()
		polled, pollErr := r.Provider.Poll(ctx, job.ID)
		pollFinished := later(r.Now().UTC(), pollStarted)
		attempt := AttemptRecord{
			Sequence:      len(record.Attempts) + 1,
			Kind:          "provider_poll",
			State:         providercontract.StatusFailed,
			StartedAt:     pollStarted,
			FinishedAt:    pollFinished,
			ProviderJobID: job.ID,
		}
		if pollErr != nil {
			var providerErr *providercontract.Error
			if errors.As(pollErr, &providerErr) {
				attempt.ErrorCode = providerErr.Code
				attempt.Retryable = providerErr.Retryable
			}
			record.Attempts = append(record.Attempts, attempt)
			if !attempt.Retryable {
				return r.fail(record, pollErr)
			}
		} else {
			job = polled
			attempt.State = job.Status
			record.Attempts = append(record.Attempts, attempt)
		}
		if !job.Status.Terminal() {
			if waitErr := r.Wait(ctx, 100*time.Millisecond); waitErr != nil {
				_, _ = r.Provider.Cancel(context.WithoutCancel(ctx), job.ID)
				return r.fail(record, waitErr)
			}
		}
	}
	if !job.Status.Terminal() {
		return r.fail(record, &providercontract.Error{
			Code:        providercontract.CodeTimeout,
			Retryable:   true,
			SafeMessage: "provider job did not become terminal before the polling limit",
		})
	}
	if job.Provider != input.Route.Provider || job.ProviderModel != input.Route.ModelID ||
		!nonEmpty(job.ProviderRegion, job.ProviderRequestID) {
		return r.fail(record, conflictf("provider result does not match the frozen model/region/request snapshot"))
	}
	if job.Output != nil {
		actualCost := job.Output.Usage.ProviderCostMicros
		if actualCost < 0 {
			return r.fail(record, validationf("provider returned a negative actual cost"))
		}
		if actualCost > input.BudgetReservation.AmountMicros {
			return r.fail(record, &providercontract.Error{
				Code:        providercontract.CodeBudgetExceeded,
				SafeMessage: "provider actual cost exceeds the approved reservation",
			})
		}
	}

	if job.Status == providercontract.StatusSucceeded {
		if job.Output == nil || len(job.Output.Assets) == 0 {
			return r.fail(record, validationf("provider succeeded without output assets"))
		}
		committed := make([]providercontract.AssetRef, 0, len(job.Output.Assets))
		for _, asset := range job.Output.Assets {
			stored, commitErr := r.Committer.Commit(ctx, asset)
			if commitErr != nil {
				return r.fail(record, commitErr)
			}
			// The Provider MP4 is the immutable source of truth for native
			// dialogue, ambience, effects, and music. Name it explicitly after
			// CAS commit so downstream extraction never mistakes it for a
			// disposable presentation output.
			if request.Output.GenerateAudio && stored.Kind == providercontract.ModalityVideo {
				stored.Role = providercontract.AssetRoleProviderOriginal
			}
			committed = append(committed, stored)
		}
		job.Output.Assets = committed
	}
	completedAt := later(r.Now().UTC(), startedAt)
	manifestID := derivedID("manifest", hashString(input.RunID+"\x00"+requestHash+"\x00"+job.ID))
	manifest, manifestErr := providercontract.NewGenerationManifest(providercontract.ManifestBuildInput{
		ManifestID:          manifestID,
		ShotID:              input.ShotRevision.AggregateID,
		Evidence:            input.Evidence,
		Request:             request,
		Job:                 job,
		Attempt:             countSubmitAttempts(record.Attempts),
		StartedAt:           startedAt,
		CompletedAt:         completedAt,
		InputRevisionHashes: input.Prompt.InputRevisionHashes,
		ModelSnapshot:       &input.Route,
		Gates: []providercontract.GateEvidence{
			toManifestGate(input.Gate1),
			toManifestGate(input.Gate2),
		},
	})
	if manifestErr != nil {
		return r.fail(record, manifestErr)
	}
	manifestRecord, manifestErr := r.Manifests.Put(manifest)
	if manifestErr != nil {
		return r.fail(record, manifestErr)
	}
	record.ProviderJob = &job
	record.ManifestID = manifestRecord.Manifest.ManifestID
	record.ManifestHash = manifestRecord.Hash
	record.FinishedAt = completedAt
	switch job.Status {
	case providercontract.StatusSucceeded:
		record.State = RunSucceeded
	case providercontract.StatusCancelled:
		record.State = RunCancelled
	default:
		record.State = RunFailed
		if job.Error != nil {
			record.ErrorCode = job.Error.Code
		}
	}
	completed, completeErr := r.Ledger.Complete(record)
	if completeErr != nil {
		return RunRecord{}, completeErr
	}
	if completed.State != RunSucceeded {
		return completed, job.Error
	}
	return completed, nil
}

func toManifestGate(gate GateApproval) providercontract.GateEvidence {
	return providercontract.GateEvidence{
		Gate:        string(gate.Gate),
		DecisionID:  gate.DecisionID,
		BindingID:   gate.BindingID,
		BindingHash: gate.BindingHash,
		ActorID:     gate.ActorID,
	}
}

func (r *GenerationRunner) preflight(ctx context.Context, input GenerationInput, at time.Time) (providercontract.GenerationRequest, providercontract.Capability, error) {
	if !nonEmpty(input.RunID, input.IdempotencyKey) || input.RunID != input.IdempotencyKey {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, validationf("run ID and matching idempotency key are required")
	}
	if err := input.SourceRevision.Validate(); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if err := input.ShotRevision.Validate(); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if input.SourceRevision.Kind != KindSource || input.ShotRevision.Kind != KindShotSpec ||
		input.Prompt.ShotRevision.ID != input.ShotRevision.ID {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, conflictf("source, shot, and prompt revisions are not aligned")
	}
	if err := VerifyExactPromptSnapshot(r.Prompts, input.Prompt); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if err := input.Authorization.Validate(input.SourceRevision, at); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if err := input.Gate1.Validate(Gate1, input.SourceRevision.ID, input.SourceRevision.ContentHash); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if err := input.Gate2.Validate(Gate2, input.ShotRevision.ID, input.ShotRevision.ContentHash); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if err := input.Route.Validate(providercontract.CapabilityVideo); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if input.Evidence != providercontract.EvidenceMockOnly && input.Evidence != providercontract.EvidenceLiveProvider {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, validationf("mock or live evidence classification is required")
	}
	if input.Evidence == providercontract.EvidenceMockOnly && input.Route.Verification != "mock_only" {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, conflictf("mock evidence requires a mock-only model snapshot")
	}
	if input.Evidence == providercontract.EvidenceLiveProvider && input.Route.Verification == "mock_only" {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, policyf("mock route cannot produce live provider evidence")
	}
	request, err := BuildGenerationRequest(input.Prompt, input.RunID, input.IdempotencyKey, input.CallbackURL, input.Budget)
	if err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	request.ModelHint = input.Route.ModelID
	if err := input.BudgetReservation.ValidateFor(providercontract.BudgetBindingInput{
		RunID:     input.RunID,
		InputHash: input.Prompt.ContentHash,
		Model:     input.Route,
		Budget:    input.Budget,
	}); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	if _, err := input.BudgetPolicy.Evaluate(input.SpentMicros, input.ReservedMicros, input.Budget); err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	capabilities, err := r.Provider.Discover(ctx)
	if err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	capability, err := selectCapability(capabilities, input.Route, request.Output)
	if err != nil {
		return providercontract.GenerationRequest{}, providercontract.Capability{}, err
	}
	return request, capability, nil
}

func (r *GenerationRunner) fail(record RunRecord, failure error) (RunRecord, error) {
	record.State = RunFailed
	record.FinishedAt = later(r.Now().UTC(), record.StartedAt)
	record.ErrorCode = providercontract.ErrorCodeOf(failure)
	completed, err := r.Ledger.Complete(record)
	if err != nil {
		return RunRecord{}, err
	}
	return completed, failure
}

func selectCapability(capabilities []providercontract.Capability, route providercontract.ModelSnapshot, output providercontract.OutputSpec) (providercontract.Capability, error) {
	for _, capability := range capabilities {
		if capability.Provider != route.Provider || capability.OutputModality != providercontract.ModalityVideo {
			continue
		}
		// Mock routes are exact fixtures. Live discovery may report a model
		// family while the frozen route stores an account-specific endpoint or
		// model ID; the terminal Job must still match that exact frozen ID.
		if capability.ModelFamily != route.ModelID && route.Verification == "mock_only" {
			continue
		}
		if len(capability.Resolutions) > 0 && !slices.Contains(capability.Resolutions, output.Resolution) {
			continue
		}
		if len(capability.AspectRatios) > 0 && !slices.Contains(capability.AspectRatios, output.AspectRatio) {
			continue
		}
		if len(capability.NativeFPS) > 0 && !slices.Contains(capability.NativeFPS, output.FPS) {
			continue
		}
		if capability.MinDurationMillis > 0 && output.DurationMillis < capability.MinDurationMillis {
			continue
		}
		if capability.MaxDurationMillis > 0 && output.DurationMillis > capability.MaxDurationMillis {
			continue
		}
		if err := capability.SupportsOutputAudio(output); err != nil {
			continue
		}
		return capability, nil
	}
	return providercontract.Capability{}, policyf("frozen route does not match current discovered video capability")
}

func retryDelay(providerErr *providercontract.Error) time.Duration {
	if providerErr != nil && providerErr.RetryAfter > 0 {
		return providerErr.RetryAfter
	}
	return 100 * time.Millisecond
}

func later(candidate, floor time.Time) time.Time {
	if candidate.After(floor) {
		return candidate
	}
	return floor.Add(time.Nanosecond)
}

func countSubmitAttempts(attempts []AttemptRecord) int {
	count := 0
	for _, attempt := range attempts {
		if attempt.Kind == "provider_submit" {
			count++
		}
	}
	if count == 0 {
		return 1
	}
	return count
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cloneRunRecord(record RunRecord) RunRecord {
	record.InputRevisionHashes = mapsClone(record.InputRevisionHashes)
	record.Attempts = slices.Clone(record.Attempts)
	record.Request.Assets = slices.Clone(record.Request.Assets)
	if record.ProviderJob != nil {
		job := *record.ProviderJob
		if job.Output != nil {
			output := *job.Output
			output.Assets = slices.Clone(job.Output.Assets)
			job.Output = &output
		}
		record.ProviderJob = &job
	}
	return record
}

func mapsClone(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

type PublicationLock struct {
	ID           string          `json:"id"`
	RunID        string          `json:"run_id"`
	ManifestID   string          `json:"manifest_id"`
	ManifestHash string          `json:"manifest_hash"`
	QC           QualityEvidence `json:"qc"`
	Gate3        GateApproval    `json:"gate3"`
	LockedAt     time.Time       `json:"locked_at"`
}

type QualityEvidence struct {
	ReportID         string    `json:"report_id"`
	RunID            string    `json:"run_id"`
	ManifestID       string    `json:"manifest_id"`
	ManifestHash     string    `json:"manifest_hash"`
	ThresholdVersion string    `json:"threshold_version"`
	ReportHash       string    `json:"report_hash"`
	Passed           bool      `json:"passed"`
	ReasonCodes      []string  `json:"reason_codes,omitempty"`
	CompletedAt      time.Time `json:"completed_at"`
}

func (q QualityEvidence) Validate(run RunRecord) error {
	if !q.Passed || !nonEmpty(q.ReportID, q.RunID, q.ManifestID, q.ManifestHash, q.ThresholdVersion, q.ReportHash) ||
		q.RunID != run.RunID || q.ManifestID != run.ManifestID || q.ManifestHash != run.ManifestHash ||
		!validSHA256(q.ManifestHash) || !validSHA256(q.ReportHash) || q.CompletedAt.IsZero() {
		return policyf("passing QC evidence must bind the exact run and manifest")
	}
	return nil
}

type PublicationLocker struct {
	mu    sync.Mutex
	locks map[string]PublicationLock
}

func NewPublicationLocker() *PublicationLocker {
	return &PublicationLocker{locks: make(map[string]PublicationLock)}
}

func (l *PublicationLocker) Lock(run RunRecord, qc QualityEvidence, gate3 GateApproval, at time.Time) (PublicationLock, error) {
	if run.State != RunSucceeded || !nonEmpty(run.ManifestID, run.ManifestHash) || !validSHA256(run.ManifestHash) {
		return PublicationLock{}, policyf("only a succeeded manifested run may be locked for publication")
	}
	if err := qc.Validate(run); err != nil {
		return PublicationLock{}, err
	}
	if err := gate3.Validate(Gate3, run.ManifestID, run.ManifestHash); err != nil {
		return PublicationLock{}, err
	}
	if at.IsZero() || at.Before(gate3.DecidedAt) || at.Before(qc.CompletedAt) {
		return PublicationLock{}, validationf("publication lock time must follow QC and G3")
	}
	digest := hashString(strings.Join([]string{run.RunID, run.ManifestID, run.ManifestHash, qc.ReportHash, gate3.DecisionID}, "\x00"))
	lock := PublicationLock{
		ID:           derivedID("publication-lock", digest),
		RunID:        run.RunID,
		ManifestID:   run.ManifestID,
		ManifestHash: run.ManifestHash,
		QC:           qc,
		Gate3:        gate3,
		LockedAt:     at.UTC(),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.locks[run.RunID]; ok {
		if existing.ID != lock.ID {
			return PublicationLock{}, conflictf("run already has a different publication lock")
		}
		return existing, nil
	}
	l.locks[run.RunID] = lock
	return lock, nil
}
