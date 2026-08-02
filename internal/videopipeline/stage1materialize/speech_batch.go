package stage1materialize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/speechcontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const SpeechBatchRevisionSchemaVersion = "flo104.speech-batch-revision.v1"

type SpeechBatchCueInput struct {
	CueID             string `json:"cueId"`
	UnicodeCharacters int    `json:"unicodeCharacters"`
	EstimatedAFPMilli int64  `json:"estimatedAfpMilli"`
	MaximumAFPMilli   int64  `json:"maximumAfpMilli"`
	MaxAttempts       int    `json:"maxAttempts"`
}

type SpeechBatchRevisionInput struct {
	SchemaVersion                    string                           `json:"schemaVersion"`
	BatchID                          string                           `json:"batchId"`
	ParentExecutionPackageHash       string                           `json:"parentExecutionPackageHash"`
	MaximumSubmits                   int                              `json:"maximumSubmits"`
	EstimatedAFPMilli                int64                            `json:"estimatedAfpMilli"`
	MaximumAFPMilli                  int64                            `json:"maximumAfpMilli"`
	MaximumNonSubscriptionCashMicros int64                            `json:"maximumNonSubscriptionCashMicros"`
	Cues                             []SpeechBatchCueInput            `json:"cues"`
	CompletedAttempts                []postproduction.ProviderAttempt `json:"completedAttempts"`
}

func (input SpeechBatchRevisionInput) Validate(parent stage1.ExecutionPackage) error {
	switch {
	case input.SchemaVersion != SpeechBatchRevisionSchemaVersion:
		return errors.New("speech batch revision schemaVersion is invalid")
	case input.BatchID != parent.BatchID:
		return errors.New("speech batch revision is bound to another batch")
	case input.ParentExecutionPackageHash != parent.ContentHash:
		return errors.New("speech batch revision parent package hash drifted")
	case parent.PostProduction.Config.SpeechIdentityVersion != postproduction.SpeechIdentityV2 ||
		parent.PostProduction.Config.SpeechVoice == nil:
		return errors.New("speech batch revision requires the current immutable speech-v2 VOICE package")
	case parent.PostProduction.Config.SpeechBatchAuthorization != nil:
		return errors.New("speech batch revision parent already contains a batch authorization")
	case len(input.Cues) == 0 || input.MaximumSubmits != len(input.Cues):
		return errors.New("speech batch maximum submits must equal the cue count")
	case input.MaximumNonSubscriptionCashMicros != 0:
		return errors.New("speech batch revision requires zero non-subscription cash")
	case len(input.CompletedAttempts) == 0:
		return errors.New("speech batch revision must freeze the already completed canary attempt")
	}
	seen := make(map[string]struct{}, len(input.Cues))
	var estimated, maximum int64
	for index, cue := range input.Cues {
		if strings.TrimSpace(cue.CueID) == "" || cue.UnicodeCharacters <= 0 ||
			cue.EstimatedAFPMilli != int64(cue.UnicodeCharacters)*135 ||
			cue.MaximumAFPMilli < cue.EstimatedAFPMilli || cue.MaxAttempts != 1 {
			return fmt.Errorf("speech batch cue %d is invalid", index)
		}
		if _, duplicate := seen[cue.CueID]; duplicate {
			return fmt.Errorf("duplicate speech batch cue %q", cue.CueID)
		}
		seen[cue.CueID] = struct{}{}
		estimated += cue.EstimatedAFPMilli
		maximum += cue.MaximumAFPMilli
	}
	if estimated != input.EstimatedAFPMilli || maximum != input.MaximumAFPMilli {
		return errors.New("speech batch revision aggregate AFP values drifted")
	}
	return nil
}

type SpeechBatchRevisionReport struct {
	SchemaVersion              string                            `json:"schemaVersion"`
	BatchID                    string                            `json:"batchId"`
	RevisionInputHash          string                            `json:"revisionInputHash"`
	ParentExecutionPackageHash string                            `json:"parentExecutionPackageHash"`
	ExecutionPackageHash       string                            `json:"executionPackageHash"`
	Authorization              speechcontract.BatchAuthorization `json:"authorization"`
	CompletedCueIDs            []string                          `json:"completedCueIds"`
	ProviderJobsBefore         int64                             `json:"providerJobsBefore"`
	ProviderJobsAfter          int64                             `json:"providerJobsAfter"`
	ProviderJobDelta           int64                             `json:"providerJobDelta"`
	ProviderCalls              int64                             `json:"providerCalls"`
}

// AuthorizeSpeechBatch materializes a prompt-free child package. It prepares
// product truth only to derive and verify identities; it cannot reach a
// Provider Adapter and must leave provider_jobs unchanged.
func AuthorizeSpeechBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	cas *artifactstore.Store,
	plan stage1.Plan,
	parent stage1.ExecutionPackage,
	input SpeechBatchRevisionInput,
	approval Approval,
) (stage1.ExecutionPackage, SpeechBatchRevisionReport, error) {
	if pool == nil || cas == nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, errors.New("PostgreSQL pool and CAS are required")
	}
	if err := plan.Validate(); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	if err := parent.Validate(plan); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("validate parent execution package: %w", err)
	}
	if err := input.Validate(parent); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	if _, err := uuid.Parse(approval.CommentID); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, errors.New("approval comment must be a UUID")
	}
	if _, err := uuid.Parse(approval.ActorID); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, errors.New("approval actor must be a UUID")
	}
	if !approval.ValidUntil.After(time.Now().UTC()) {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, errors.New("speech batch approval is expired")
	}
	revisionInputHash, err := digest(input)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	var providerJobsBefore int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_jobs`).Scan(&providerJobsBefore); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("count Provider jobs before speech batch revision: %w", err)
	}
	ledger := repository.NewForPool(pool)
	preparedParent, err := ledger.PrepareEpisodePostProduction(ctx, orchestration.WorkflowStep{
		WorkflowID: "stage1-speech-batch-parent-" + parent.ContentHash,
		ActivityID: "prepare-parent", ActivityType: "offline-no-provider",
		TraceID: parent.PostProduction.TraceID,
	}, parent.PostProduction)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("prepare parent post-production: %w", err)
	}
	voice := parent.PostProduction.Config.SpeechVoice
	authorization := speechcontract.BatchAuthorization{
		SchemaVersion:              speechcontract.SchemaVersion,
		ParentExecutionPackageHash: parent.ContentHash,
		ApprovalCommentID:          approval.CommentID, ApprovalActorID: approval.ActorID,
		ValidUntil: approval.ValidUntil.UTC().Format(time.RFC3339),
		Provider:   voice.Provider, ModelID: voice.ModelID,
		RouteVersion: parent.PostProduction.Config.SpeechRoute.RouteVersion,
		ResourceID:   voice.ResourceID, Speaker: voice.Speaker,
		VoiceAssetID: voice.AssetID, ParentVoiceAssetVersionID: voice.ParentAssetVersionID,
		VoiceAssetVersionID: voice.AssetVersionID, VoiceAssetVersionHash: voice.AssetVersionHash,
		LicenseSnapshotID: voice.LicenseSnapshotID, LicenseSnapshotHash: voice.LicenseSnapshotHash,
		MaximumSubmits: input.MaximumSubmits, EstimatedAFPMilli: input.EstimatedAFPMilli,
		MaximumAFPMilli:                  input.MaximumAFPMilli,
		MaximumNonSubscriptionCashMicros: input.MaximumNonSubscriptionCashMicros,
		Cues:                             make([]speechcontract.CueAuthorization, 0, len(input.Cues)),
	}
	for _, cueInput := range input.Cues {
		var cue *postproduction.Cue
		for index := range preparedParent.Subtitle.Cues {
			if preparedParent.Subtitle.Cues[index].ID == cueInput.CueID {
				cue = &preparedParent.Subtitle.Cues[index]
				break
			}
		}
		if cue == nil || len([]rune(strings.TrimSpace(cue.Text))) != cueInput.UnicodeCharacters {
			return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("speech batch cue %q drifted from product truth", cueInput.CueID)
		}
		identity, err := postproduction.DeriveSpeechJobIdentity(postproduction.SpeechRequest{
			EpisodeRevisionID: preparedParent.EpisodeRevisionID,
			SubtitleRevision:  preparedParent.Subtitle, Cue: *cue,
			Config: preparedParent.Speech, Evidence: preparedParent.Evidence,
			TraceID: preparedParent.TraceID, BudgetMicros: preparedParent.Speech.BudgetMaximumMicros,
		})
		if err != nil {
			return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
		}
		authorization.Cues = append(authorization.Cues, speechcontract.CueAuthorization{
			CueID: cueInput.CueID, JobID: identity.JobID, InputHash: identity.InputHash,
			UnicodeCharacters: cueInput.UnicodeCharacters,
			EstimatedAFPMilli: cueInput.EstimatedAFPMilli,
			MaximumAFPMilli:   cueInput.MaximumAFPMilli, MaxAttempts: cueInput.MaxAttempts,
		})
	}
	if err := authorization.Validate(); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	for _, attempt := range input.CompletedAttempts {
		if err := verifyCompletedSpeechArtifact(cas, attempt.Artifact); err != nil {
			return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("completed cue %q CAS artifact is unavailable", attempt.CueID)
		}
	}
	revised := parent
	revised.ParentExecutionPackageHash = parent.ContentHash
	revised.PostProduction.Config.SpeechAuthorizedCueID = ""
	revised.PostProduction.Config.SpeechMaximumAFPMilli = 0
	revised.PostProduction.Config.SpeechMaximumCashMicros = 0
	revised.PostProduction.Config.SpeechMaxAttempts = 0
	revised.PostProduction.Config.SpeechBatchAuthorization = &authorization
	revised.PostProduction.Config.SpeechCompletedAttempts = append(
		[]postproduction.ProviderAttempt(nil), input.CompletedAttempts...,
	)
	revised.PostProduction.TraceID = parent.PostProduction.TraceID + "-speech-batch-v1"
	revised, err = stage1.SealExecutionPackage(revised)
	if err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	if err := revised.ValidateSpeechBatchRevision(plan, parent); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, err
	}
	if _, err := ledger.PrepareEpisodePostProduction(ctx, orchestration.WorkflowStep{
		WorkflowID: "stage1-speech-batch-" + revised.ContentHash,
		ActivityID: "verify-batch", ActivityType: "offline-no-provider",
		TraceID: revised.PostProduction.TraceID,
	}, revised.PostProduction); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("verify speech batch request: %w", err)
	}
	var providerJobsAfter int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.provider_jobs`).Scan(&providerJobsAfter); err != nil {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, fmt.Errorf("count Provider jobs after speech batch revision: %w", err)
	}
	if providerJobsAfter != providerJobsBefore {
		return stage1.ExecutionPackage{}, SpeechBatchRevisionReport{}, errors.New("speech batch revision unexpectedly changed Provider job truth")
	}
	completedCueIDs := make([]string, 0, len(input.CompletedAttempts))
	for _, attempt := range input.CompletedAttempts {
		completedCueIDs = append(completedCueIDs, attempt.CueID)
	}
	return revised, SpeechBatchRevisionReport{
		SchemaVersion: SpeechBatchRevisionSchemaVersion, BatchID: input.BatchID,
		RevisionInputHash:          revisionInputHash,
		ParentExecutionPackageHash: parent.ContentHash, ExecutionPackageHash: revised.ContentHash,
		Authorization: authorization, CompletedCueIDs: completedCueIDs,
		ProviderJobsBefore: providerJobsBefore, ProviderJobsAfter: providerJobsAfter,
		ProviderJobDelta: 0, ProviderCalls: 0,
	}, nil
}

func verifyCompletedSpeechArtifact(cas *artifactstore.Store, artifact postproduction.Artifact) error {
	object, err := cas.Open(artifact.Digest)
	if err != nil {
		return err
	}
	hash := sha256.New()
	size, readErr := io.Copy(hash, object)
	closeErr := object.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if size != artifact.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != artifact.Digest {
		return errors.New("completed speech artifact bytes do not match the frozen CAS identity")
	}
	return nil
}
