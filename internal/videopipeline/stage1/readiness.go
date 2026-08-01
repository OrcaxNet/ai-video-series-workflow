// Package stage1 implements the no-cost readiness gate for FLO-104 sample 1.
// It contains no provider credentials and cannot submit without an injected
// Submitter. Every authorization is durably reserved before the submit call.
package stage1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const (
	SchemaVersion                  = "v1"
	LedgerSchemaVersion            = "v3"
	FormalVideoModel               = "doubao-seedance-2.0"
	RequiredPrimaryJobs            = 10
	MaximumControlledRetries       = 1
	MaximumNewProviderJobs         = 11
	MaximumVideoTokens       int64 = 1_200_000
	MaximumMonthlyAFPMilli   int64 = 38_000_000
	MaximumCashMicros        int64 = 20_000_000
	MaximumDialogueChars     int64 = 600
	MaximumAFPDriftBPS             = 1_000
)

var requiredEvidence = []string{
	"artifact_hashes", "generation_manifest", "license_consent_gate",
	"provider_ids", "qc", "redaction_scan", "service_bom", "usage_cost",
}

type Plan struct {
	SchemaVersion             string       `json:"schemaVersion"`
	BatchID                   string       `json:"batchId"`
	VideoModel                string       `json:"videoModel"`
	PrimaryShotIDs            []string     `json:"primaryShotIds"`
	MaximumNewJobs            int          `json:"maximumNewProviderJobs"`
	MaximumControlledRetries  int          `json:"maximumControlledRetries"`
	MaximumVideoTokens        int64        `json:"maximumVideoTokens"`
	MonthlyBaselineAFPMilli   int64        `json:"monthlyBaselineAfpMilli"`
	MonthlyMaximumAFPMilli    int64        `json:"monthlyMaximumAfpMilli"`
	ReferenceJobAFPMilli      int64        `json:"referenceJobAfpMilli"`
	MaximumAFPDriftBPS        int          `json:"maximumAfpDriftBasisPoints"`
	MaximumCashMicros         int64        `json:"maximumNonSubscriptionCashMicros"`
	MaximumDialogueCharacters int64        `json:"maximumDialogueCharacters"`
	MaximumTTSAFPMilli        int64        `json:"maximumTtsAfpMilli"`
	RequiredEvidence          []string     `json:"requiredEvidence"`
	TTSPreflight              TTSPreflight `json:"ttsPreflight"`
}

type TTSPreflight struct {
	CompletedNoCost     bool   `json:"completedNoCost"`
	Provider            string `json:"provider"`
	Model               string `json:"model"`
	Region              string `json:"region"`
	ResourceID          string `json:"resourceId"`
	CredentialReference string `json:"credentialReference"`
	CredentialAvailable bool   `json:"credentialAvailable"`
	Pricing             string `json:"pricing"`
	UsageAttribution    string `json:"usageAttribution"`
}

func (p Plan) Validate() error {
	switch {
	case p.SchemaVersion != SchemaVersion:
		return errors.New("stage 1 schemaVersion must be v1")
	case strings.TrimSpace(p.BatchID) == "":
		return errors.New("stage 1 batchId is required")
	case p.VideoModel != FormalVideoModel:
		return errors.New("formal sample 1 must use doubao-seedance-2.0")
	case len(p.PrimaryShotIDs) != RequiredPrimaryJobs:
		return fmt.Errorf("stage 1 requires exactly %d primary shots", RequiredPrimaryJobs)
	case p.MaximumNewJobs != MaximumNewProviderJobs || p.MaximumControlledRetries != MaximumControlledRetries:
		return errors.New("stage 1 must be capped at 10 primary jobs plus one controlled retry")
	case p.MaximumVideoTokens != MaximumVideoTokens:
		return errors.New("stage 1 video token cap must equal 1200000")
	case p.MonthlyBaselineAFPMilli < 0 || p.MonthlyBaselineAFPMilli >= p.MonthlyMaximumAFPMilli:
		return errors.New("stage 1 monthly AFP baseline is invalid")
	case p.MonthlyMaximumAFPMilli != MaximumMonthlyAFPMilli:
		return errors.New("stage 1 monthly AFP cap must equal 38000 AFP")
	case p.ReferenceJobAFPMilli <= 0 || p.MaximumAFPDriftBPS != MaximumAFPDriftBPS:
		return errors.New("stage 1 reference AFP and 10 percent drift limit are required")
	case p.MaximumCashMicros != MaximumCashMicros:
		return errors.New("stage 1 non-subscription cash cap must equal 20 CNY")
	case p.MaximumDialogueCharacters != MaximumDialogueChars:
		return errors.New("stage 1 dialogue must be capped at 600 Unicode characters")
	case p.MaximumTTSAFPMilli != MaximumDialogueChars*135:
		return errors.New("stage 1 TTS attribution must be capped at 81 AFP")
	case !p.TTSPreflight.CompletedNoCost || p.TTSPreflight.Provider != "volcengine_ark" ||
		p.TTSPreflight.Model != "doubao-seed-tts-2.0" || p.TTSPreflight.Region != "cn-beijing" ||
		p.TTSPreflight.ResourceID != "seed-tts-2.0" ||
		p.TTSPreflight.CredentialReference != "ARK_API_KEY" || !p.TTSPreflight.CredentialAvailable ||
		p.TTSPreflight.Pricing != "1350_afp_per_10000_chars" ||
		p.TTSPreflight.UsageAttribution != "provider_usage_tokens_per_request":
		return errors.New("stage 1 requires the complete no-cost Agent Plan TTS preflight")
	}
	seen := make(map[string]struct{}, len(p.PrimaryShotIDs))
	for _, shotID := range p.PrimaryShotIDs {
		if strings.TrimSpace(shotID) == "" {
			return errors.New("stage 1 shot IDs cannot be empty")
		}
		if _, duplicate := seen[shotID]; duplicate {
			return fmt.Errorf("duplicate stage 1 shot ID %q", shotID)
		}
		seen[shotID] = struct{}{}
	}
	gotEvidence := append([]string(nil), p.RequiredEvidence...)
	sort.Strings(gotEvidence)
	if strings.Join(gotEvidence, "\x00") != strings.Join(requiredEvidence, "\x00") {
		return errors.New("stage 1 plan does not require the complete evidence set")
	}
	return nil
}

type RetryApproval struct {
	ApprovalID              string `json:"approvalId"`
	OriginalAttemptID       string `json:"originalAttemptId"`
	FailureClass            string `json:"failureClass"`
	DuplicateTaskEvidenceID string `json:"duplicateTaskEvidenceId"`
}

type Attempt struct {
	AttemptID                          string         `json:"attemptId"`
	ShotID                             string         `json:"shotId"`
	IdempotencyKey                     string         `json:"idempotencyKey"`
	EstimatedVideoTokens               int64          `json:"estimatedVideoTokens"`
	PredictedAFPMilli                  int64          `json:"predictedAfpMilli"`
	EstimatedNonSubscriptionCashMicros int64          `json:"estimatedNonSubscriptionCashMicros"`
	Retry                              *RetryApproval `json:"retry,omitempty"`
	// JobRequest exists only in process memory. The gate deliberately excludes
	// prompts and transport input from its durable ledger.
	JobRequest *providercontract.JobRequest `json:"-"`
}

type Record struct {
	Attempt
	State               string `json:"state"`
	TerminalSequence    int64  `json:"terminalSequence,omitempty"`
	ProviderTaskID      string `json:"providerTaskId,omitempty"`
	ActualVideoTokens   int64  `json:"actualVideoTokens,omitempty"`
	ActualAFPMilli      int64  `json:"actualAfpMilli,omitempty"`
	ActualCashMicros    int64  `json:"actualCashMicros,omitempty"`
	EvidenceComplete    bool   `json:"evidenceComplete"`
	ContentSafetyFailed bool   `json:"contentSafetyFailed"`
	FailureClass        string `json:"failureClass,omitempty"`
}

type Ledger struct {
	SchemaVersion                  string             `json:"schemaVersion"`
	BatchID                        string             `json:"batchId"`
	ExecutionPackageHash           string             `json:"executionPackageHash"`
	SupersededExecutionPackageHash string             `json:"supersededExecutionPackageHash,omitempty"`
	ControlledRetryPackageHash     string             `json:"controlledRetryPackageHash,omitempty"`
	Records                        map[string]*Record `json:"records"`
	ReservedVideoTokens            int64              `json:"reservedVideoTokens"`
	ReservedAFPMilli               int64              `json:"reservedAfpMilli"`
	ReservedCashMicros             int64              `json:"reservedCashMicros"`
	ConsecutiveSafetyFailures      int                `json:"consecutiveContentSafetyFailures"`
	NextTerminalSequence           int64              `json:"nextTerminalSequence"`
}

type Decision string

const (
	DecisionSubmit      Decision = "SUBMIT_ONCE"
	DecisionReplay      Decision = "REPLAY_EXISTING"
	DecisionRecoverOnly Decision = "RECOVER_ONLY"
)

type Gate struct {
	plan                       Plan
	path                       string
	executionPackageHash       string
	controlledRetryPackageHash string
	mu                         sync.Mutex
}

func Open(plan Plan, path string) (*Gate, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("stage 1 ledger path is required")
	}
	gate := &Gate{plan: plan, path: path}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	lock, err := gate.acquireFileLock()
	if err != nil {
		return nil, err
	}
	defer releaseFileLock(lock)
	if _, err := gate.loadLocked(); err != nil {
		return nil, err
	}
	return gate, nil
}

func (g *Gate) Plan() Plan { return g.plan }

// BindExecutionPackage permanently binds this ledger to the exact immutable
// execution package. It must happen before the production executor is created.
func (g *Gate) BindExecutionPackage(contentHash string) error {
	if !validLowerDigest(contentHash) {
		return errors.New("stage 1 execution package hash must be a lowercase SHA-256")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return err
	}
	defer releaseFileLock(lock)
	previous := g.executionPackageHash
	g.executionPackageHash = contentHash
	ledger, err := g.loadLocked()
	if err != nil {
		g.executionPackageHash = previous
		return err
	}
	if ledger.ExecutionPackageHash != contentHash {
		g.executionPackageHash = previous
		return errors.New("stage 1 ledger is bound to another execution package")
	}
	if err := g.saveLocked(ledger); err != nil {
		g.executionPackageHash = previous
		return err
	}
	return nil
}

// BindExecutionPackageRevision atomically promotes one speech-v2-only child
// package after comparing it with the complete immutable parent artifact and
// confirming all ten primary video jobs have evidence-complete successful
// terminal records. The parent binding is retained in the ledger, and no
// controlled retry or second package revision may cross this boundary.
//
// parentArtifact is variadic only to keep older callers source-compatible:
// promotion fails closed unless exactly one complete parent package is given.
func (g *Gate) BindExecutionPackageRevision(package_ ExecutionPackage, parentArtifact ...ExecutionPackage) error {
	if len(parentArtifact) != 1 {
		return providerError(providercontract.CodeForbidden, "stage 1 package revision requires exactly one immutable parent artifact")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return err
	}
	defer releaseFileLock(lock)

	previousExpected := g.executionPackageHash
	g.executionPackageHash = ""
	ledger, err := g.loadLocked()
	g.executionPackageHash = previousExpected
	if err != nil {
		return err
	}
	if ledger.ExecutionPackageHash == package_.ContentHash {
		if err := package_.ValidateSpeechV2Revision(g.plan, parentArtifact[0]); err != nil {
			return providerError(providercontract.CodeForbidden, "stage 1 package revision contains non-speech drift")
		}
		if ledger.SupersededExecutionPackageHash != package_.ParentExecutionPackageHash {
			return errors.New("stage 1 ledger revision parent binding is invalid")
		}
		g.executionPackageHash = package_.ContentHash
		return nil
	}
	if ledger.SupersededExecutionPackageHash != "" {
		return providerError(providercontract.CodeConflict, "stage 1 ledger already consumed its package revision")
	}
	if ledger.ExecutionPackageHash != package_.ParentExecutionPackageHash {
		return errors.New("stage 1 ledger is not bound to the revision parent package")
	}
	if err := package_.ValidateSpeechV2Revision(g.plan, parentArtifact[0]); err != nil {
		return providerError(providercontract.CodeForbidden, "stage 1 package revision contains non-speech drift")
	}
	if ledger.ControlledRetryPackageHash != "" {
		return providerError(providercontract.CodeForbidden, "stage 1 package revision cannot replace a controlled retry binding")
	}
	if len(ledger.Records) != len(package_.PrimaryJobs) {
		return providerError(providercontract.CodeForbidden, "stage 1 package revision requires exactly ten completed primary records")
	}
	for _, frozen := range package_.PrimaryJobs {
		record := ledger.Records[frozen.IdempotencyKey]
		if record == nil || !sameAttempt(record.Attempt, attemptFromFrozen(frozen, nil)) ||
			record.State != "TERMINAL_SUCCEEDED" || !record.EvidenceComplete {
			return providerError(providercontract.CodeForbidden, "stage 1 package revision requires unchanged evidence-complete successful primary records")
		}
	}

	ledger.SupersededExecutionPackageHash = package_.ParentExecutionPackageHash
	ledger.ExecutionPackageHash = package_.ContentHash
	g.executionPackageHash = package_.ContentHash
	if err := g.saveLocked(ledger); err != nil {
		g.executionPackageHash = previousExpected
		return err
	}
	return nil
}

// BindControlledRetryPackage persists the one post-failure extension without
// changing the already-bound ten-job package. Competing extensions race under
// the same cross-process lock and only the first hash can win.
func (g *Gate) BindControlledRetryPackage(package_ ControlledRetryPackage) error {
	contentHash := package_.ContentHash
	if !validLowerDigest(contentHash) {
		return errors.New("stage 1 controlled retry package hash must be a lowercase SHA-256")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return err
	}
	defer releaseFileLock(lock)
	ledger, err := g.loadLocked()
	if err != nil {
		return err
	}
	if ledger.ExecutionPackageHash == "" {
		return errors.New("stage 1 execution package must be bound before its controlled retry")
	}
	var original *Record
	for _, record := range ledger.Records {
		if record.AttemptID == package_.Approval.OriginalAttemptID &&
			record.ShotID == package_.Job.ShotID {
			original = record
			break
		}
	}
	if original == nil || original.State != "TERMINAL_FAILED" ||
		!original.EvidenceComplete || original.FailureClass != package_.Approval.FailureClass {
		return providerError(providercontract.CodeForbidden, "controlled retry package does not match an evidence-complete primary failure")
	}
	if ledger.ControlledRetryPackageHash != "" && ledger.ControlledRetryPackageHash != contentHash {
		return providerError(providercontract.CodeConflict, "stage 1 ledger is bound to another controlled retry package")
	}
	ledger.ControlledRetryPackageHash = contentHash
	previous := g.controlledRetryPackageHash
	g.controlledRetryPackageHash = contentHash
	if err := g.saveLocked(ledger); err != nil {
		g.controlledRetryPackageHash = previous
		return err
	}
	return nil
}

func (g *Gate) Authorize(attempt Attempt) (Decision, error) {
	return g.AuthorizePrepared(attempt, nil)
}

// AuthorizePrepared validates the local Stage 1 caps and invokes prepare while
// holding the cross-process ledger lock. Only after PostgreSQL product truth is
// prepared is the exact attempt durably reserved.
func (g *Gate) AuthorizePrepared(attempt Attempt, prepare func() error) (Decision, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return "", err
	}
	defer releaseFileLock(lock)
	ledger, err := g.loadLocked()
	if err != nil {
		return "", err
	}
	if existing := ledger.Records[attempt.IdempotencyKey]; existing != nil {
		if !sameAttempt(existing.Attempt, attempt) {
			return "", providerError(providercontract.CodeConflict, "idempotency key is bound to different stage 1 input")
		}
		if existing.State == "AMBIGUOUS" || existing.State == "PREPARED" {
			return DecisionRecoverOnly, nil
		}
		return DecisionReplay, nil
	}
	if err := g.validateNewAttempt(ledger, attempt); err != nil {
		return "", err
	}
	if prepare != nil {
		if err := prepare(); err != nil {
			return "", err
		}
	}
	ledger.Records[attempt.IdempotencyKey] = &Record{Attempt: attempt, State: "PREPARED"}
	if err := recalculateLedger(&ledger); err != nil {
		return "", err
	}
	if err := g.saveLocked(ledger); err != nil {
		return "", err
	}
	return DecisionSubmit, nil
}

// Inspect performs the exact local ledger decision without reserving a new
// attempt. It is available for no-cost diagnostics; the production runner uses
// AuthorizePrepared so inspection and reservation cannot race across processes.
func (g *Gate) Inspect(attempt Attempt) (Decision, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return "", err
	}
	defer releaseFileLock(lock)
	ledger, err := g.loadLocked()
	if err != nil {
		return "", err
	}
	if existing := ledger.Records[attempt.IdempotencyKey]; existing != nil {
		if !sameAttempt(existing.Attempt, attempt) {
			return "", providerError(providercontract.CodeConflict, "idempotency key is bound to different stage 1 input")
		}
		if existing.State == "AMBIGUOUS" || existing.State == "PREPARED" {
			return DecisionRecoverOnly, nil
		}
		return DecisionReplay, nil
	}
	if err := g.validateNewAttempt(ledger, attempt); err != nil {
		return "", err
	}
	return DecisionSubmit, nil
}

func (g *Gate) validateNewAttempt(ledger Ledger, attempt Attempt) error {
	if strings.TrimSpace(attempt.AttemptID) == "" || strings.TrimSpace(attempt.IdempotencyKey) == "" ||
		strings.TrimSpace(attempt.ShotID) == "" {
		return providerError(providercontract.CodeInvalidRequest, "stage 1 attempt identity is incomplete")
	}
	if !contains(g.plan.PrimaryShotIDs, attempt.ShotID) {
		return providerError(providercontract.CodeForbidden, "shot is outside the approved stage 1 primary set")
	}
	for _, record := range ledger.Records {
		if (record.State == "TERMINAL_SUCCEEDED" || record.State == "TERMINAL_FAILED") &&
			!record.EvidenceComplete {
			return providerError(providercontract.CodeForbidden, "previous stage 1 provider evidence is incomplete")
		}
		if record.ActualAFPMilli > 0 && exceedsDrift(
			record.ActualAFPMilli, g.plan.ReferenceJobAFPMilli, g.plan.MaximumAFPDriftBPS,
		) {
			return providerError(providercontract.CodeBudgetExceeded, "a completed stage 1 job exceeded the 10 percent AFP drift limit")
		}
	}
	if ledger.ReservedVideoTokens > g.plan.MaximumVideoTokens ||
		g.plan.MonthlyBaselineAFPMilli+g.plan.MaximumTTSAFPMilli+ledger.ReservedAFPMilli >
			g.plan.MonthlyMaximumAFPMilli ||
		ledger.ReservedCashMicros > g.plan.MaximumCashMicros {
		return providerError(providercontract.CodeBudgetExceeded, "completed stage 1 usage exceeded an approved hard limit")
	}
	if ledger.ConsecutiveSafetyFailures >= 2 {
		return providerError(providercontract.CodeContentBlocked, "two consecutive content safety failures require reauthorization")
	}
	if len(ledger.Records) >= g.plan.MaximumNewJobs {
		return providerError(providercontract.CodeQuotaExceeded, "stage 1 provider job limit is exhausted")
	}
	if attempt.EstimatedVideoTokens <= 0 ||
		ledger.ReservedVideoTokens+attempt.EstimatedVideoTokens > g.plan.MaximumVideoTokens {
		return providerError(providercontract.CodeBudgetExceeded, "stage 1 video token cap would be exceeded")
	}
	if attempt.PredictedAFPMilli <= 0 || exceedsDrift(
		attempt.PredictedAFPMilli, g.plan.ReferenceJobAFPMilli, g.plan.MaximumAFPDriftBPS,
	) {
		return providerError(providercontract.CodeBudgetExceeded, "next stage 1 job AFP prediction exceeds the 10 percent drift limit")
	}
	if g.plan.MonthlyBaselineAFPMilli+g.plan.MaximumTTSAFPMilli+
		ledger.ReservedAFPMilli+attempt.PredictedAFPMilli >
		g.plan.MonthlyMaximumAFPMilli {
		return providerError(providercontract.CodeBudgetExceeded, "stage 1 monthly AFP cap would be exceeded")
	}
	if attempt.EstimatedNonSubscriptionCashMicros < 0 ||
		ledger.ReservedCashMicros+attempt.EstimatedNonSubscriptionCashMicros > g.plan.MaximumCashMicros {
		return providerError(providercontract.CodeBudgetExceeded, "stage 1 non-subscription cash cap would be exceeded")
	}
	primaryExists := false
	retries := 0
	for _, record := range ledger.Records {
		if record.ShotID == attempt.ShotID && record.Retry == nil {
			primaryExists = true
		}
		if record.Retry != nil {
			retries++
		}
	}
	if attempt.Retry == nil {
		if primaryExists {
			return providerError(providercontract.CodeConflict, "stage 1 primary shot already has a job")
		}
		return nil
	}
	if retries >= g.plan.MaximumControlledRetries || !primaryExists {
		return providerError(providercontract.CodeForbidden, "stage 1 controlled retry is not available")
	}
	retry := attempt.Retry
	if strings.TrimSpace(retry.ApprovalID) == "" || strings.TrimSpace(retry.OriginalAttemptID) == "" ||
		strings.TrimSpace(retry.FailureClass) == "" || strings.TrimSpace(retry.DuplicateTaskEvidenceID) == "" {
		return providerError(providercontract.CodeForbidden, "controlled retry lacks approval, classification, or duplicate-task evidence")
	}
	for _, record := range ledger.Records {
		if record.AttemptID == retry.OriginalAttemptID && record.ShotID == attempt.ShotID &&
			record.State == "TERMINAL_FAILED" && record.EvidenceComplete &&
			record.FailureClass == retry.FailureClass {
			return nil
		}
	}
	return providerError(providercontract.CodeForbidden, "controlled retry does not match the evidence-complete failed terminal original attempt")
}

func (g *Gate) MarkAmbiguous(idempotencyKey string) error {
	return g.updateRecord(idempotencyKey, func(record *Record, _ *Ledger) error {
		switch record.State {
		case "PREPARED":
			record.State = "AMBIGUOUS"
			return nil
		case "AMBIGUOUS":
			return nil
		case "TERMINAL_SUCCEEDED", "TERMINAL_FAILED":
			return providerError(providercontract.CodeConflict, "terminal stage 1 completion cannot be downgraded to ambiguous")
		default:
			return providerError(providercontract.CodeConflict, "stage 1 attempt cannot transition to ambiguous")
		}
	})
}

type Completion struct {
	ProviderTaskID      string
	State               string
	ActualVideoTokens   int64
	ActualAFPMilli      int64
	ActualCashMicros    int64
	EvidenceComplete    bool
	ContentSafetyFailed bool
	FailureClass        string
}

func (g *Gate) Complete(idempotencyKey string, completion Completion) error {
	completion.FailureClass = normalizedFailureClass(completion)
	return g.updateRecord(idempotencyKey, func(record *Record, ledger *Ledger) error {
		if completion.State != "TERMINAL_SUCCEEDED" && completion.State != "TERMINAL_FAILED" {
			return errors.New("stage 1 completion requires a terminal state")
		}
		if strings.TrimSpace(completion.ProviderTaskID) == "" && !tasklessSafetyRejection(completion) {
			return errors.New("stage 1 completion requires a provider task unless safety rejected the request before task creation")
		}
		if completion.ActualVideoTokens < 0 || completion.ActualAFPMilli < 0 || completion.ActualCashMicros < 0 {
			return errors.New("stage 1 actual usage cannot be negative")
		}
		if completion.State == "TERMINAL_SUCCEEDED" && completion.ContentSafetyFailed {
			return errors.New("a succeeded stage 1 completion cannot be a content safety failure")
		}
		if completion.State == "TERMINAL_SUCCEEDED" && completion.FailureClass != "" {
			return errors.New("a succeeded stage 1 completion cannot have a failure class")
		}
		if terminalState(record.State) {
			if sameCompletion(record, completion) {
				return nil
			}
			return providerError(providercontract.CodeConflict, "terminal stage 1 completion is immutable")
		}
		if record.State != "PREPARED" && record.State != "AMBIGUOUS" {
			return providerError(providercontract.CodeConflict, "stage 1 attempt is not completable")
		}
		ledger.NextTerminalSequence++
		record.TerminalSequence = ledger.NextTerminalSequence
		record.ProviderTaskID = completion.ProviderTaskID
		record.State = completion.State
		record.ActualVideoTokens = completion.ActualVideoTokens
		record.ActualAFPMilli = completion.ActualAFPMilli
		record.ActualCashMicros = completion.ActualCashMicros
		record.EvidenceComplete = completion.EvidenceComplete
		record.ContentSafetyFailed = completion.ContentSafetyFailed
		record.FailureClass = completion.FailureClass
		return recalculateLedger(ledger)
	})
}

func (g *Gate) updateRecord(idempotencyKey string, update func(*Record, *Ledger) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return err
	}
	defer releaseFileLock(lock)
	ledger, err := g.loadLocked()
	if err != nil {
		return err
	}
	record := ledger.Records[idempotencyKey]
	if record == nil {
		return errors.New("stage 1 attempt was not prepared")
	}
	if err := update(record, &ledger); err != nil {
		return err
	}
	return g.saveLocked(ledger)
}

func (g *Gate) loadLocked() (Ledger, error) {
	data, err := os.ReadFile(g.path)
	if errors.Is(err, os.ErrNotExist) {
		return Ledger{
			SchemaVersion: LedgerSchemaVersion, BatchID: g.plan.BatchID,
			ExecutionPackageHash: g.executionPackageHash, Records: make(map[string]*Record),
		}, nil
	}
	if err != nil {
		return Ledger{}, fmt.Errorf("read stage 1 ledger: %w", err)
	}
	var ledger Ledger
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil || ledger.SchemaVersion != LedgerSchemaVersion ||
		ledger.BatchID != g.plan.BatchID || ledger.Records == nil ||
		(g.executionPackageHash != "" && ledger.ExecutionPackageHash != g.executionPackageHash) ||
		(g.controlledRetryPackageHash != "" &&
			ledger.ControlledRetryPackageHash != g.controlledRetryPackageHash) {
		return Ledger{}, errors.New("stage 1 ledger is invalid or bound to another batch")
	}
	if ledger.SupersededExecutionPackageHash != "" &&
		(!validLowerDigest(ledger.SupersededExecutionPackageHash) ||
			ledger.SupersededExecutionPackageHash == ledger.ExecutionPackageHash) {
		return Ledger{}, errors.New("stage 1 ledger superseded package binding is invalid")
	}
	if err := validateLedgerDerivedState(ledger); err != nil {
		return Ledger{}, err
	}
	return ledger, nil
}

type derivedLedgerState struct {
	videoTokens         int64
	afpMilli            int64
	cashMicros          int64
	safetyFailures      int
	terminalSequence    int64
	terminalRecordCount int64
}

func deriveLedgerState(ledger Ledger) (derivedLedgerState, error) {
	derived := derivedLedgerState{}
	terminalRecords := make([]*Record, 0, len(ledger.Records))
	sequences := make(map[int64]struct{}, len(ledger.Records))
	for key, record := range ledger.Records {
		if record == nil || record.IdempotencyKey != key {
			return derivedLedgerState{}, errors.New("stage 1 ledger record identity is invalid")
		}
		if terminalState(record.State) {
			if record.TerminalSequence <= 0 ||
				(strings.TrimSpace(record.ProviderTaskID) == "" && !tasklessSafetyRecord(record)) ||
				record.ActualVideoTokens < 0 || record.ActualAFPMilli < 0 || record.ActualCashMicros < 0 {
				return derivedLedgerState{}, errors.New("stage 1 terminal record is incomplete")
			}
			if record.State == "TERMINAL_SUCCEEDED" && record.ContentSafetyFailed {
				return derivedLedgerState{}, errors.New("stage 1 terminal record has an invalid safety outcome")
			}
			if (record.State == "TERMINAL_FAILED" && strings.TrimSpace(record.FailureClass) == "") ||
				(record.State == "TERMINAL_SUCCEEDED" && record.FailureClass != "") {
				return derivedLedgerState{}, errors.New("stage 1 terminal record has an invalid failure classification")
			}
			if _, duplicate := sequences[record.TerminalSequence]; duplicate {
				return derivedLedgerState{}, errors.New("stage 1 terminal sequence is duplicated")
			}
			sequences[record.TerminalSequence] = struct{}{}
			terminalRecords = append(terminalRecords, record)
			var err error
			if derived.videoTokens, err = addUsage(derived.videoTokens, record.ActualVideoTokens); err != nil {
				return derivedLedgerState{}, err
			}
			if derived.afpMilli, err = addUsage(derived.afpMilli, record.ActualAFPMilli); err != nil {
				return derivedLedgerState{}, err
			}
			if derived.cashMicros, err = addUsage(derived.cashMicros, record.ActualCashMicros); err != nil {
				return derivedLedgerState{}, err
			}
			if record.TerminalSequence > derived.terminalSequence {
				derived.terminalSequence = record.TerminalSequence
			}
			continue
		}
		if record.TerminalSequence != 0 || record.State != "PREPARED" && record.State != "AMBIGUOUS" {
			return derivedLedgerState{}, errors.New("stage 1 non-terminal record has an invalid state")
		}
		var err error
		if derived.videoTokens, err = addUsage(derived.videoTokens, record.EstimatedVideoTokens); err != nil {
			return derivedLedgerState{}, err
		}
		if derived.afpMilli, err = addUsage(derived.afpMilli, record.PredictedAFPMilli); err != nil {
			return derivedLedgerState{}, err
		}
		if derived.cashMicros, err = addUsage(derived.cashMicros, record.EstimatedNonSubscriptionCashMicros); err != nil {
			return derivedLedgerState{}, err
		}
	}
	derived.terminalRecordCount = int64(len(terminalRecords))
	if derived.terminalSequence != derived.terminalRecordCount {
		return derivedLedgerState{}, errors.New("stage 1 terminal sequence is not contiguous")
	}
	sort.Slice(terminalRecords, func(i, j int) bool {
		return terminalRecords[i].TerminalSequence < terminalRecords[j].TerminalSequence
	})
	for _, record := range terminalRecords {
		if record.ContentSafetyFailed {
			derived.safetyFailures++
		} else {
			derived.safetyFailures = 0
		}
	}
	return derived, nil
}

func recalculateLedger(ledger *Ledger) error {
	derived, err := deriveLedgerState(*ledger)
	if err != nil {
		return err
	}
	ledger.ReservedVideoTokens = derived.videoTokens
	ledger.ReservedAFPMilli = derived.afpMilli
	ledger.ReservedCashMicros = derived.cashMicros
	ledger.ConsecutiveSafetyFailures = derived.safetyFailures
	ledger.NextTerminalSequence = derived.terminalSequence
	return nil
}

func validateLedgerDerivedState(ledger Ledger) error {
	derived, err := deriveLedgerState(ledger)
	if err != nil {
		return err
	}
	if ledger.ReservedVideoTokens != derived.videoTokens || ledger.ReservedAFPMilli != derived.afpMilli ||
		ledger.ReservedCashMicros != derived.cashMicros ||
		ledger.ConsecutiveSafetyFailures != derived.safetyFailures ||
		ledger.NextTerminalSequence != derived.terminalSequence {
		return errors.New("stage 1 ledger derived state does not match its immutable records")
	}
	return nil
}

// Snapshot returns a deep, immutable view used to select the exact successful
// Run set before Stage 1 post-production. Callers cannot mutate live records.
func (g *Gate) Snapshot() (Ledger, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	lock, err := g.acquireFileLock()
	if err != nil {
		return Ledger{}, err
	}
	defer releaseFileLock(lock)
	ledger, err := g.loadLocked()
	if err != nil {
		return Ledger{}, err
	}
	encoded, err := json.Marshal(ledger)
	if err != nil {
		return Ledger{}, err
	}
	var snapshot Ledger
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return Ledger{}, err
	}
	return snapshot, nil
}

func (g *Gate) saveLocked(ledger Ledger) error {
	if err := os.MkdirAll(filepath.Dir(g.path), 0o750); err != nil {
		return fmt.Errorf("create stage 1 ledger directory: %w", err)
	}
	data, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("encode stage 1 ledger: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(g.path), ".stage1-ledger-*.tmp")
	if err != nil {
		return fmt.Errorf("create stage 1 ledger temp file: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, g.path); err != nil {
		return fmt.Errorf("commit stage 1 ledger: %w", err)
	}
	directory, err := os.Open(filepath.Dir(g.path))
	if err != nil {
		return fmt.Errorf("open stage 1 ledger directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync stage 1 ledger directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close stage 1 ledger directory: %w", err)
	}
	return nil
}

func (g *Gate) acquireFileLock() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(g.path), 0o750); err != nil {
		return nil, fmt.Errorf("create stage 1 ledger directory: %w", err)
	}
	file, err := os.OpenFile(g.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open stage 1 ledger lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock stage 1 ledger: %w", err)
	}
	return file, nil
}

func releaseFileLock(file *os.File) {
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

type RecoveryResult struct {
	Found          bool
	ProviderTaskID string
}

type SubmitResult struct {
	ProviderTaskID string
}

type Submitter interface {
	Recover(context.Context, string) (RecoveryResult, error)
	Submit(context.Context, Attempt) (SubmitResult, error)
}

type Executor struct {
	gate      *Gate
	submitter Submitter
	mu        sync.Mutex
}

func NewExecutor(gate *Gate, submitter Submitter) (*Executor, error) {
	if gate == nil || submitter == nil {
		return nil, errors.New("stage 1 gate and submitter are required")
	}
	return &Executor{gate: gate, submitter: submitter}, nil
}

func (e *Executor) Execute(ctx context.Context, attempt Attempt) (SubmitResult, error) {
	return e.ExecutePrepared(ctx, attempt, nil)
}

// ExecutePrepared performs recovery first and invokes prepare only for a new
// submit. The request returned by prepare remains in memory and never enters
// the prompt-free Stage 1 ledger.
func (e *Executor) ExecutePrepared(
	ctx context.Context,
	attempt Attempt,
	prepare func(context.Context) (providercontract.JobRequest, error),
) (SubmitResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	recovered, err := e.submitter.Recover(ctx, attempt.IdempotencyKey)
	if err != nil {
		return SubmitResult{}, err
	}
	decision, err := e.gate.AuthorizePrepared(attempt, func() error {
		if prepare == nil {
			return nil
		}
		request, prepareErr := prepare(ctx)
		if prepareErr != nil {
			return prepareErr
		}
		attempt.JobRequest = &request
		return nil
	})
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
		return SubmitResult{ProviderTaskID: recovered.ProviderTaskID}, nil
	}
	if decision == DecisionRecoverOnly {
		return SubmitResult{}, providerError(providercontract.CodeUnavailable, "ambiguous stage 1 submit requires operator recovery and cannot be resubmitted")
	}
	result, err := e.submitter.Submit(ctx, attempt)
	if err != nil {
		_ = e.gate.MarkAmbiguous(attempt.IdempotencyKey)
		return SubmitResult{}, err
	}
	if strings.TrimSpace(result.ProviderTaskID) == "" {
		_ = e.gate.MarkAmbiguous(attempt.IdempotencyKey)
		return SubmitResult{}, providerError(providercontract.CodeUnavailable, "stage 1 provider submit returned no task ID")
	}
	return result, nil
}

func ValidateDialogue(texts []string) (characters int64, afpMilli int64, err error) {
	for _, text := range texts {
		characters += int64(len([]rune(strings.TrimSpace(text))))
	}
	if characters <= 0 || characters > MaximumDialogueChars {
		return 0, 0, providerError(providercontract.CodeBudgetExceeded, "stage 1 dialogue must contain 1 to 600 Unicode characters")
	}
	return characters, characters * 135, nil
}

func providerError(code providercontract.ErrorCode, message string) error {
	return &providercontract.Error{Code: code, SafeMessage: message}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func exceedsDrift(value, baseline int64, maximumBPS int) bool {
	delta := value - baseline
	if delta < 0 {
		delta = -delta
	}
	return delta*10_000 > baseline*int64(maximumBPS)
}

func sameAttempt(a, b Attempt) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func sameCompletion(record *Record, completion Completion) bool {
	return record.ProviderTaskID == completion.ProviderTaskID &&
		record.State == completion.State &&
		record.ActualVideoTokens == completion.ActualVideoTokens &&
		record.ActualAFPMilli == completion.ActualAFPMilli &&
		record.ActualCashMicros == completion.ActualCashMicros &&
		record.EvidenceComplete == completion.EvidenceComplete &&
		record.ContentSafetyFailed == completion.ContentSafetyFailed &&
		record.FailureClass == completion.FailureClass
}

func normalizedFailureClass(completion Completion) string {
	if completion.State != "TERMINAL_FAILED" {
		return strings.TrimSpace(completion.FailureClass)
	}
	if value := strings.TrimSpace(completion.FailureClass); value != "" {
		return value
	}
	if completion.ContentSafetyFailed {
		return string(providercontract.CodeContentBlocked)
	}
	return "PROVIDER_TERMINAL_FAILURE"
}

func terminalState(state string) bool {
	return state == "TERMINAL_SUCCEEDED" || state == "TERMINAL_FAILED"
}

func tasklessSafetyRejection(completion Completion) bool {
	return completion.State == "TERMINAL_FAILED" && completion.ContentSafetyFailed && !completion.EvidenceComplete
}

func tasklessSafetyRecord(record *Record) bool {
	return record.State == "TERMINAL_FAILED" && record.ContentSafetyFailed && !record.EvidenceComplete
}

func addUsage(current, value int64) (int64, error) {
	if current < 0 || value < 0 || current > math.MaxInt64-value {
		return 0, errors.New("stage 1 ledger usage overflow")
	}
	return current + value, nil
}
