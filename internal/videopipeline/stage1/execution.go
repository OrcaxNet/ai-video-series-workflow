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

// ExecutionPackage is the prompt-free, secret-free identity bundle approved for
// one formal Stage 1 run. Runtime product truth remains in PostgreSQL; this file
// pins the exact records that the runner is allowed to resolve and revalidate.
type ExecutionPackage struct {
	SchemaVersion  string                             `json:"schemaVersion"`
	BatchID        string                             `json:"batchId"`
	ContentHash    string                             `json:"contentHash"`
	PrimaryJobs    []FrozenJob                        `json:"primaryJobs"`
	PostProduction orchestration.FinalizeEpisodeInput `json:"postProduction"`
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
