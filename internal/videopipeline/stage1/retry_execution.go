package stage1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
)

const ControlledRetryPackageSchemaVersion = "v1"

// ControlledRetryPackage is an immutable extension created only after one
// primary job has a classified, evidence-complete terminal failure. It cannot
// mutate the ten-job execution package already bound to the Stage 1 ledger.
type ControlledRetryPackage struct {
	SchemaVersion              string                             `json:"schemaVersion"`
	BatchID                    string                             `json:"batchId"`
	ParentExecutionPackageHash string                             `json:"parentExecutionPackageHash"`
	ContentHash                string                             `json:"contentHash"`
	Job                        FrozenJob                          `json:"job"`
	Approval                   RetryApproval                      `json:"approval"`
	PostProduction             orchestration.FinalizeEpisodeInput `json:"postProduction"`
}

func (p ControlledRetryPackage) Validate(plan Plan, primary ExecutionPackage) error {
	if err := primary.Validate(plan); err != nil {
		return fmt.Errorf("primary execution package: %w", err)
	}
	switch {
	case p.SchemaVersion != ControlledRetryPackageSchemaVersion:
		return errors.New("stage 1 controlled retry package schemaVersion must be v1")
	case p.BatchID != plan.BatchID:
		return errors.New("stage 1 controlled retry package is bound to another batch")
	case p.ParentExecutionPackageHash != primary.ContentHash:
		return errors.New("stage 1 controlled retry package is bound to another execution package")
	case !validLowerDigest(p.ContentHash):
		return errors.New("stage 1 controlled retry package requires a lowercase SHA-256 contentHash")
	}
	if err := p.Job.validate(plan); err != nil {
		return fmt.Errorf("stage 1 controlled retry job: %w", err)
	}
	original, ok := primary.Job(p.Job.ShotID)
	if !ok {
		return errors.New("stage 1 controlled retry does not replace a frozen primary shot")
	}
	if p.Job.ShotSpecRevisionID != original.ShotSpecRevisionID ||
		p.Job.GenerationPlanID != original.GenerationPlanID ||
		p.Job.BudgetApprovalID != original.BudgetApprovalID ||
		p.Job.BudgetMaximumMicros != original.BudgetMaximumMicros ||
		p.Job.BudgetCurrency != original.BudgetCurrency ||
		p.Job.ProviderProfileID != original.ProviderProfileID ||
		p.Job.Route != original.Route {
		return errors.New("stage 1 controlled retry drifted from the primary shot plan, budget, or route")
	}
	if p.Job.Run.RunID == original.Run.RunID || p.Job.AttemptID == original.AttemptID ||
		p.Job.IdempotencyKey == original.IdempotencyKey ||
		p.Job.Run.Attempt != original.Run.Attempt+1 {
		return errors.New("stage 1 controlled retry requires a new Run, Attempt, idempotency key, and creative attempt")
	}
	for _, job := range primary.PrimaryJobs {
		if p.Job.Run.RunID == job.Run.RunID || p.Job.AttemptID == job.AttemptID ||
			p.Job.IdempotencyKey == job.IdempotencyKey {
			return errors.New("stage 1 controlled retry identity collides with a primary job")
		}
	}
	if strings.TrimSpace(p.Approval.ApprovalID) == "" ||
		strings.TrimSpace(p.Approval.FailureClass) == "" ||
		strings.TrimSpace(p.Approval.DuplicateTaskEvidenceID) == "" ||
		p.Approval.OriginalAttemptID != original.AttemptID {
		return errors.New("stage 1 controlled retry lacks exact approval, failure, or duplicate-task evidence")
	}
	if _, err := uuid.Parse(p.Approval.ApprovalID); err != nil {
		return errors.New("stage 1 controlled retry approvalId must be a UUID")
	}
	if _, err := uuid.Parse(p.Approval.DuplicateTaskEvidenceID); err != nil {
		return errors.New("stage 1 controlled retry duplicateTaskEvidenceId must be a UUID")
	}

	var videoTokens, afpMilli, cashMicros int64
	for _, job := range primary.PrimaryJobs {
		videoTokens += job.EstimatedVideoTokens
		afpMilli += job.PredictedAFPMilli
		cashMicros += job.EstimatedNonSubscriptionCashMicros
	}
	videoTokens += p.Job.EstimatedVideoTokens
	afpMilli += p.Job.PredictedAFPMilli
	cashMicros += p.Job.EstimatedNonSubscriptionCashMicros
	if videoTokens > plan.MaximumVideoTokens ||
		plan.MonthlyBaselineAFPMilli+plan.MaximumTTSAFPMilli+afpMilli > plan.MonthlyMaximumAFPMilli ||
		cashMicros > plan.MaximumCashMicros {
		return errors.New("stage 1 controlled retry package exceeds an approved aggregate limit")
	}

	expectedPostProduction := primary.PostProduction
	expectedPostProduction.RunIDs = append([]string(nil), primary.PostProduction.RunIDs...)
	for index, runID := range expectedPostProduction.RunIDs {
		if runID == original.Run.RunID {
			expectedPostProduction.RunIDs[index] = p.Job.Run.RunID
		}
	}
	if !reflect.DeepEqual(p.PostProduction, expectedPostProduction) {
		return errors.New("stage 1 controlled retry finalization must replace only the failed primary Run")
	}
	wantHash, err := p.digest()
	if err != nil {
		return err
	}
	if p.ContentHash != wantHash {
		return errors.New("stage 1 controlled retry package contentHash does not match its frozen content")
	}
	return nil
}

func (p ControlledRetryPackage) digest() (string, error) {
	material := p
	material.ContentHash = ""
	encoded, err := json.Marshal(material)
	if err != nil {
		return "", fmt.Errorf("encode stage 1 controlled retry package: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// SealControlledRetryPackage computes the extension hash after the original
// terminal evidence and retry approval have both been frozen.
func SealControlledRetryPackage(p ControlledRetryPackage) (ControlledRetryPackage, error) {
	p.ContentHash = ""
	digest, err := p.digest()
	if err != nil {
		return ControlledRetryPackage{}, err
	}
	p.ContentHash = digest
	return p, nil
}
