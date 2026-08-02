package stage1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/google/uuid"
)

const ExecutionPackageSchemaVersion = "v1"

const revisionParentSafeMessage = "stage 1 execution package revision parent is unverifiable"

// ExecutionPackage is the prompt-free, secret-free identity bundle approved for
// one formal Stage 1 run. Runtime product truth remains in PostgreSQL; this file
// pins the exact records that the runner is allowed to resolve and revalidate.
type ExecutionPackage struct {
	SchemaVersion              string                             `json:"schemaVersion"`
	BatchID                    string                             `json:"batchId"`
	ParentExecutionPackageHash string                             `json:"parentExecutionPackageHash,omitempty"`
	ContentHash                string                             `json:"contentHash"`
	NativeEvidence             *NativeExecutionEvidence           `json:"nativeEvidence,omitempty"`
	PrimaryJobs                []FrozenJob                        `json:"primaryJobs"`
	PostProduction             orchestration.FinalizeEpisodeInput `json:"postProduction"`
}

// NativeExecutionEvidence binds a FLO-154 package to the exact no-network
// analyzer installation and input assets verified before any paid boundary.
// FLO-104 packages omit this field and therefore retain their original bytes.
type NativeExecutionEvidence struct {
	CodeCommitSHA            string            `json:"codeCommitSha"`
	BuildSHA256              string            `json:"buildSha256"`
	ProductInputSHA256       string            `json:"productInputSha256"`
	AnalyzerSealSHA256       string            `json:"analyzerSealSha256"`
	AnalyzerExecutableSHA256 string            `json:"analyzerExecutableSha256"`
	AnalyzerConfigSHA256     string            `json:"analyzerConfigSha256"`
	AnalyzerComponentSHA256  map[string]string `json:"analyzerComponentSha256"`
	AssetSHA256              map[string]string `json:"assetSha256"`
}

// FrozenJob contains identifiers and approved limits only. Prompt text, asset
// transport URLs, credentials, and caller-reported authorization state are not
// accepted by the formal runner.
type FrozenJob struct {
	ShotID                             string                         `json:"shotId"`
	ShotSpecRevisionID                 string                         `json:"shotSpecRevisionId"`
	AttemptID                          string                         `json:"attemptId"`
	IdempotencyKey                     string                         `json:"idempotencyKey"`
	Run                                orchestration.GenerationRunRef `json:"run"`
	PromptSnapshotID                   string                         `json:"promptSnapshotId"`
	PromptSnapshotHash                 string                         `json:"promptSnapshotHash"`
	GenerationPlanID                   string                         `json:"generationPlanId"`
	BudgetApprovalID                   string                         `json:"budgetApprovalId"`
	BudgetMaximumMicros                int64                          `json:"budgetMaximumMicros"`
	BudgetCurrency                     string                         `json:"budgetCurrency"`
	ProviderProfileID                  string                         `json:"providerProfileId"`
	Route                              providercontract.ModelSnapshot `json:"route"`
	EstimatedVideoTokens               int64                          `json:"estimatedVideoTokens"`
	PredictedAFPMilli                  int64                          `json:"predictedAfpMilli"`
	EstimatedNonSubscriptionCashMicros int64                          `json:"estimatedNonSubscriptionCashMicros"`
	WorkflowID                         string                         `json:"workflowId"`
	ActivityID                         string                         `json:"activityId"`
	TraceID                            string                         `json:"traceId"`
}

// RequiresRevisionParent reports whether the package claims the speech-v2
// revision contract, including malformed revisions that name a parent while
// carrying another identity version. Callers use it to preserve one
// non-retryable error contract before trusting the package.
func (p ExecutionPackage) RequiresRevisionParent() bool {
	return p.ParentExecutionPackageHash != "" ||
		p.PostProduction.Config.SpeechIdentityVersion == postproduction.SpeechIdentityV2
}

// UnverifiableRevisionParentError normalizes every missing, unreadable,
// malformed, mismatched, or otherwise unverifiable revision parent. The cause
// remains visible to operators while provider-facing classification stays
// forbidden and non-retryable.
func UnverifiableRevisionParentError(cause error) error {
	contract := &providercontract.Error{
		Code:        providercontract.CodeForbidden,
		Retryable:   false,
		SafeMessage: revisionParentSafeMessage,
	}
	if cause == nil {
		return contract
	}
	return fmt.Errorf("%w: %v", contract, cause)
}

func (p ExecutionPackage) Validate(plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	switch {
	case p.SchemaVersion != ExecutionPackageSchemaVersion:
		return errors.New("stage 1 execution package schemaVersion must be v1")
	case p.BatchID != plan.BatchID:
		return errors.New("stage 1 execution package is bound to another batch")
	case len(p.PrimaryJobs) != RequiredPrimaryJobs:
		return fmt.Errorf("stage 1 execution package requires exactly %d primary jobs", RequiredPrimaryJobs)
	case !validLowerDigest(p.ContentHash):
		return errors.New("stage 1 execution package requires a lowercase SHA-256 contentHash")
	}
	if err := p.validateNativeEvidence(plan); err != nil {
		return err
	}
	if p.PostProduction.Config.SpeechIdentityVersion == postproduction.SpeechIdentityV2 &&
		p.ParentExecutionPackageHash == "" {
		return errors.New("speech-v2 execution package requires its immutable parent package hash")
	}
	if p.ParentExecutionPackageHash != "" {
		if !validLowerDigest(p.ParentExecutionPackageHash) ||
			p.ParentExecutionPackageHash == p.ContentHash {
			return errors.New("stage 1 execution package revision requires a distinct parent package hash")
		}
		if p.PostProduction.Config.SpeechIdentityVersion != postproduction.SpeechIdentityV2 {
			return errors.New("only a speech-v2 post-production revision may name a parent package")
		}
	}

	seenShots := make(map[string]struct{}, len(p.PrimaryJobs))
	seenShotSpecs := make(map[string]struct{}, len(p.PrimaryJobs))
	seenRuns := make(map[string]struct{}, len(p.PrimaryJobs))
	seenPrompts := make(map[string]struct{}, len(p.PrimaryJobs))
	runIDs := make([]string, 0, len(p.PrimaryJobs))
	var videoTokens, afpMilli, cashMicros int64
	for index, job := range p.PrimaryJobs {
		if err := job.validate(plan); err != nil {
			return fmt.Errorf("stage 1 primary job %d: %w", index+1, err)
		}
		if _, duplicate := seenShots[job.ShotID]; duplicate {
			return fmt.Errorf("duplicate frozen stage 1 shot %q", job.ShotID)
		}
		if _, duplicate := seenRuns[job.Run.RunID]; duplicate {
			return fmt.Errorf("duplicate frozen stage 1 run %q", job.Run.RunID)
		}
		if _, duplicate := seenShotSpecs[job.ShotSpecRevisionID]; duplicate {
			return fmt.Errorf("duplicate frozen stage 1 shot revision %q", job.ShotSpecRevisionID)
		}
		if _, duplicate := seenPrompts[job.PromptSnapshotID]; duplicate {
			return fmt.Errorf("duplicate frozen stage 1 prompt snapshot %q", job.PromptSnapshotID)
		}
		if job.GenerationPlanID != p.PostProduction.GenerationPlanID {
			return errors.New("every frozen stage 1 run must belong to the post-production generation plan")
		}
		seenShots[job.ShotID] = struct{}{}
		seenShotSpecs[job.ShotSpecRevisionID] = struct{}{}
		seenRuns[job.Run.RunID] = struct{}{}
		seenPrompts[job.PromptSnapshotID] = struct{}{}
		runIDs = append(runIDs, job.Run.RunID)
		videoTokens += job.EstimatedVideoTokens
		afpMilli += job.PredictedAFPMilli
		cashMicros += job.EstimatedNonSubscriptionCashMicros
	}
	for _, shotID := range plan.PrimaryShotIDs {
		if _, ok := seenShots[shotID]; !ok {
			return fmt.Errorf("approved stage 1 shot %q is not frozen", shotID)
		}
	}
	if videoTokens > plan.MaximumVideoTokens ||
		plan.MonthlyBaselineAFPMilli+plan.MaximumTTSAFPMilli+afpMilli > plan.MonthlyMaximumAFPMilli ||
		cashMicros > plan.MaximumCashMicros {
		return errors.New("frozen stage 1 package exceeds an approved aggregate limit")
	}
	post := p.PostProduction
	if !post.PersistProductTruth || post.GenerationPlanID == "" ||
		!equalOrderedStrings(post.RunIDs, runIDs) || strings.TrimSpace(post.TraceID) == "" {
		return errors.New("post-production must freeze the exact ten product-truth runs in order")
	}
	if _, err := uuid.Parse(post.EpisodeRevisionID); err != nil {
		return errors.New("post-production episodeRevisionId must be a UUID")
	}
	if _, err := uuid.Parse(post.GenerationPlanID); err != nil {
		return errors.New("post-production generationPlanId must be a UUID")
	}
	if !post.Config.Enabled || post.Config.Evidence != postproduction.EvidenceLive ||
		!post.Config.BurnSubtitles || !post.Config.EnforcePoCDuration {
		return errors.New("formal stage 1 post-production must freeze live evidence, subtitles, and PoC duration")
	}
	if err := post.Config.Validate(); err != nil {
		return fmt.Errorf("stage 1 post-production package: %w", err)
	}
	wantHash, err := p.digest()
	if err != nil {
		return err
	}
	if p.ContentHash != wantHash {
		return errors.New("stage 1 execution package contentHash does not match its frozen content")
	}
	return nil
}

func (p ExecutionPackage) validateNativeEvidence(plan Plan) error {
	if !plan.IsNativeOnly() {
		if p.NativeEvidence != nil {
			return errors.New("FLO-104 execution package cannot carry FLO-154 native evidence")
		}
		return nil
	}
	if p.NativeEvidence == nil {
		return errors.New("FLO-154 native execution package requires immutable preflight evidence")
	}
	evidence := p.NativeEvidence
	if len(evidence.CodeCommitSHA) != 40 || !validLowerHex(evidence.CodeCommitSHA) {
		return errors.New("FLO-154 native evidence requires a lowercase 40-character code commit SHA")
	}
	for name, value := range map[string]string{
		"build":               evidence.BuildSHA256,
		"product input":       evidence.ProductInputSHA256,
		"analyzer seal":       evidence.AnalyzerSealSHA256,
		"analyzer executable": evidence.AnalyzerExecutableSHA256,
		"analyzer config":     evidence.AnalyzerConfigSHA256,
	} {
		if !validLowerDigest(value) {
			return fmt.Errorf("FLO-154 native evidence %s SHA-256 is invalid", name)
		}
	}
	if evidence.AnalyzerSealSHA256 != plan.NativeAudio.AnalyzerSealSHA256 {
		return errors.New("FLO-154 native analyzer seal differs from the approved plan")
	}
	requiredComponents := []string{
		"asr_model", "tokenizer", "normalizer", "vad", "face_mouth",
		"av_sync", "ffmpeg", "ffprobe", "license_snapshot",
	}
	for _, name := range requiredComponents {
		if !validLowerDigest(evidence.AnalyzerComponentSHA256[name]) {
			return fmt.Errorf("FLO-154 native analyzer component %q is not frozen", name)
		}
	}
	if len(evidence.AssetSHA256) == 0 {
		return errors.New("FLO-154 native input asset hashes are required")
	}
	for name, value := range evidence.AssetSHA256 {
		if strings.TrimSpace(name) == "" || !validLowerDigest(value) {
			return errors.New("FLO-154 native input asset hash is invalid")
		}
	}
	if p.PostProduction.Config.ResolvedAudioStrategy() != providercontract.AudioStrategyNativePreferred ||
		p.PostProduction.Config.RequiresSpeech() ||
		p.PostProduction.Config.AnalyzerSealSHA256 != evidence.AnalyzerSealSHA256 {
		return errors.New("FLO-154 native package must freeze native_preferred with zero Speech configuration")
	}
	return nil
}

// ValidateSpeechV2Revision proves that p is derived from the exact immutable
// parent package and changes only the fields materialized by RevoiceStage1.
// The complete parent artifact is required: its hash alone cannot prove that
// already-executed video lineage or unrelated finalization settings stayed
// frozen.
func (p ExecutionPackage) ValidateSpeechV2Revision(plan Plan, parent ExecutionPackage) error {
	if err := parent.Validate(plan); err != nil {
		return fmt.Errorf("validate parent execution package: %w", err)
	}
	if err := p.Validate(plan); err != nil {
		return fmt.Errorf("validate child execution package: %w", err)
	}
	if p.ParentExecutionPackageHash != parent.ContentHash {
		return errors.New("stage 1 package revision is bound to another parent artifact")
	}
	if parent.PostProduction.Config.SpeechVoice != nil &&
		(p.PostProduction.Config.SpeechVoice == nil ||
			p.PostProduction.Config.SpeechVoice.ParentAssetVersionID !=
				parent.PostProduction.Config.SpeechVoice.AssetVersionID) {
		return errors.New("stage 1 package revision voice does not extend the current parent voice")
	}

	expected := parent
	expected.ParentExecutionPackageHash = parent.ContentHash
	expected.PostProduction.Config.SpeechRoute = p.PostProduction.Config.SpeechRoute
	expected.PostProduction.Config.SpeechProviderProfileID = p.PostProduction.Config.SpeechProviderProfileID
	expected.PostProduction.Config.SpeechIdentityVersion = p.PostProduction.Config.SpeechIdentityVersion
	expected.PostProduction.Config.SpeechVoice = p.PostProduction.Config.SpeechVoice
	expected.PostProduction.Config.SpeechAuthorizedCueID = p.PostProduction.Config.SpeechAuthorizedCueID
	expected.PostProduction.Config.SpeechMaximumAFPMilli = p.PostProduction.Config.SpeechMaximumAFPMilli
	expected.PostProduction.Config.SpeechMaximumCashMicros = p.PostProduction.Config.SpeechMaximumCashMicros
	expected.PostProduction.Config.SpeechMaxAttempts = p.PostProduction.Config.SpeechMaxAttempts
	expected.PostProduction.TraceID = parent.PostProduction.TraceID + "-speech-v2"
	expected, err := SealExecutionPackage(expected)
	if err != nil {
		return err
	}
	if expected.ContentHash != p.ContentHash {
		return errors.New("stage 1 package revision contains fields outside the approved speech-v2 projection")
	}
	return nil
}

// ValidateSpeechBatchRevision proves that a child package only replaces the
// consumed single-cue canary with an ordered batch authorization and immutable
// completed-attempt evidence. The frozen VOICE, license, route, video runs,
// subtitle revision, output policy, and gates must remain byte-for-byte stable.
func (p ExecutionPackage) ValidateSpeechBatchRevision(plan Plan, parent ExecutionPackage) error {
	if err := parent.Validate(plan); err != nil {
		return fmt.Errorf("validate parent execution package: %w", err)
	}
	if err := p.Validate(plan); err != nil {
		return fmt.Errorf("validate child execution package: %w", err)
	}
	if p.ParentExecutionPackageHash != parent.ContentHash {
		return errors.New("speech batch revision is bound to another parent package")
	}
	if p.PostProduction.Config.SpeechBatchAuthorization == nil ||
		p.PostProduction.Config.SpeechBatchAuthorization.ParentExecutionPackageHash != parent.ContentHash {
		return errors.New("speech batch authorization is not bound to the immediate parent package")
	}
	expected := parent
	expected.ParentExecutionPackageHash = parent.ContentHash
	expected.PostProduction.Config.SpeechAuthorizedCueID = ""
	expected.PostProduction.Config.SpeechMaximumAFPMilli = 0
	expected.PostProduction.Config.SpeechMaximumCashMicros = 0
	expected.PostProduction.Config.SpeechMaxAttempts = 0
	expected.PostProduction.Config.SpeechBatchAuthorization =
		p.PostProduction.Config.SpeechBatchAuthorization
	expected.PostProduction.Config.SpeechCompletedAttempts =
		p.PostProduction.Config.SpeechCompletedAttempts
	expected.PostProduction.TraceID = parent.PostProduction.TraceID + "-speech-batch-v1"
	expected, err := SealExecutionPackage(expected)
	if err != nil {
		return err
	}
	if expected.ContentHash != p.ContentHash {
		return errors.New("speech batch revision contains fields outside the approved projection")
	}
	return nil
}

// ValidateRevision dispatches to the exact immutable projection declared by
// the child package while keeping legacy speech-v2 canary revisions readable.
func (p ExecutionPackage) ValidateRevision(plan Plan, parent ExecutionPackage) error {
	if p.PostProduction.Config.SpeechBatchAuthorization != nil {
		return p.ValidateSpeechBatchRevision(plan, parent)
	}
	return p.ValidateSpeechV2Revision(plan, parent)
}

func (j FrozenJob) validate(plan Plan) error {
	for name, value := range map[string]string{
		"shotId": j.ShotID, "attemptId": j.AttemptID, "idempotencyKey": j.IdempotencyKey,
		"promptSnapshotHash": j.PromptSnapshotHash, "budgetCurrency": j.BudgetCurrency,
		"workflowId": j.WorkflowID, "activityId": j.ActivityID, "traceId": j.TraceID,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"shotSpecRevisionId": j.ShotSpecRevisionID,
		"runId":              j.Run.RunID,
		"promptSnapshotId":   j.PromptSnapshotID,
		"generationPlanId":   j.GenerationPlanID,
		"budgetApprovalId":   j.BudgetApprovalID,
		"providerProfileId":  j.ProviderProfileID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID", name)
		}
	}
	if j.IdempotencyKey != "provider-job-"+j.Run.RunID {
		return errors.New("idempotencyKey must match the PostgreSQL provider job identity")
	}
	if j.Run.RunSpecDigest == "" || !validLowerDigest(j.Run.RunSpecDigest) || j.Run.Attempt < 1 {
		return errors.New("run identity and digest are incomplete")
	}
	if !validLowerDigest(j.PromptSnapshotHash) {
		return errors.New("promptSnapshotHash must be a lowercase SHA-256")
	}
	if err := j.Route.Validate(providercontract.CapabilityVideo); err != nil ||
		j.Route.ModelID != plan.VideoModel || j.Route.Verification != providercontract.PendingKey {
		return errors.New("route must freeze the approved formal video capability")
	}
	if j.BudgetMaximumMicros <= 0 || j.BudgetCurrency != "CNY" {
		return errors.New("frozen VIDEO budget must be positive CNY")
	}
	if j.EstimatedVideoTokens <= 0 || j.PredictedAFPMilli <= 0 ||
		j.EstimatedNonSubscriptionCashMicros < 0 {
		return errors.New("frozen token, AFP, and cash estimates are invalid")
	}
	if exceedsDrift(j.PredictedAFPMilli, plan.ReferenceJobAFPMilli, plan.MaximumAFPDriftBPS) {
		return errors.New("frozen AFP estimate exceeds the approved drift")
	}
	return nil
}

func (p ExecutionPackage) Job(shotID string) (FrozenJob, bool) {
	for _, job := range p.PrimaryJobs {
		if job.ShotID == shotID {
			return job, true
		}
	}
	return FrozenJob{}, false
}

func (p ExecutionPackage) JobByIdempotencyKey(key string) (FrozenJob, bool) {
	for _, job := range p.PrimaryJobs {
		if job.IdempotencyKey == key {
			return job, true
		}
	}
	return FrozenJob{}, false
}

func (p ExecutionPackage) digest() (string, error) {
	material := p
	material.ContentHash = ""
	encoded, err := json.Marshal(material)
	if err != nil {
		return "", fmt.Errorf("encode stage 1 execution package: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// SealExecutionPackage is used by the product-truth freeze command or tests
// after all exact identifiers have been selected. It never invents approvals.
func SealExecutionPackage(p ExecutionPackage) (ExecutionPackage, error) {
	p.ContentHash = ""
	digest, err := p.digest()
	if err != nil {
		return ExecutionPackage{}, err
	}
	p.ContentHash = digest
	return p, nil
}

func equalOrderedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validLowerDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func validLowerHex(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return value != ""
}
