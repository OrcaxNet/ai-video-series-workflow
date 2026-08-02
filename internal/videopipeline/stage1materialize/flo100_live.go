package stage1materialize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/postproduction"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/repository"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	liveAuthorizationSchema   = "flo100.batch-a-live-authorization.v1"
	liveProjectionSchema      = "flo100.batch-a-live-projection.v1"
	formalBatchAExecutionHash = "56d21bcec47934b11290f9760c0650e5c37e244ee756b4c76056240fdeebf260"
	liveAuthorizationOutcome  = "AUTHORIZE_BATCH_A_ONLY_WHEN_ALL_PRE_SUBMIT_GATES_PASS"
	liveBillingMode           = providercontract.BillingModeSubscriptionIncludedOnly
	liveSourceReason          = "FLO100_BATCH_A_LIVE_ACTIVATION_V1"
)

// LiveOptions identifies both the immutable offline source and the external
// A-only authority document. SourceCodeCommit is the candidate code revision
// whose live package is being materialized; a mismatching authority can build
// the candidate projection but cannot create submit authorization.
type LiveOptions struct {
	Formal                    FormalOptions
	AuthorizationPath         string
	ExpectedAuthorizationHash string
	SourceCodeCommit          string
}

type LiveReport struct {
	SchemaVersion                 string    `json:"schemaVersion"`
	BatchID                       string    `json:"batchId"`
	SourceCodeCommit              string    `json:"sourceCodeCommit"`
	SourceAuthorizedCommit        string    `json:"sourceAuthorizedCommit"`
	OfflinePackageHash            string    `json:"offlinePackageHash"`
	OfflineExecutionPackageHash   string    `json:"offlineExecutionPackageHash"`
	SourceAuthorizationHash       string    `json:"sourceAuthorizationHash"`
	LiveExecutionPackageHash      string    `json:"liveExecutionPackageHash"`
	LiveProjectionHash            string    `json:"liveProjectionHash"`
	SubmitAuthorized              bool      `json:"submitAuthorized"`
	SubmitAuthorizationValidUntil time.Time `json:"submitAuthorizationValidUntil,omitempty"`
	PrimaryJobs                   int       `json:"primaryJobs"`
	ControlledRetriesMaximum      int       `json:"controlledRetriesMaximum"`
	MaximumVideoTokens            int64     `json:"maximumVideoTokens"`
	VideoAFPMilli                 int64     `json:"videoAfpMilli"`
	SpeechAFPMilli                int64     `json:"speechAfpMilli"`
	NonSubscriptionCashMicros     int64     `json:"nonSubscriptionCashMicros"`
	ProviderJobs                  int64     `json:"providerJobs"`
	BudgetReservations            int64     `json:"budgetReservations"`
	CostLedgerEntries             int64     `json:"costLedgerEntries"`
}

type liveAuthorization struct {
	SchemaVersion string `json:"schemaVersion"`
	IssueID       string `json:"issueId"`
	Decision      struct {
		Outcome                                   string `json:"outcome"`
		BatchAProviderPostAuthorizedConditionally bool   `json:"batchAProviderPostAuthorizedConditionally"`
		BatchBProviderPostAuthorized              bool   `json:"batchBProviderPostAuthorized"`
		BatchCProviderPostAuthorized              bool   `json:"batchCProviderPostAuthorized"`
		Stage4Authorized                          bool   `json:"stage4Authorized"`
	} `json:"decision"`
	Authority struct {
		ActorID    string    `json:"actorId"`
		IssuedAt   time.Time `json:"issuedAt"`
		ValidUntil time.Time `json:"validUntil"`
	} `json:"authority"`
	FixedEvidence struct {
		MergeCommit                      string `json:"mergeCommit"`
		OfflinePackageContentHash        string `json:"offlinePackageContentHash"`
		BatchASealedExecutionPackageHash string `json:"batchASealedExecutionPackageHash"`
	} `json:"fixedEvidence"`
	G1Approval struct {
		Decision            string `json:"decision"`
		LicenseSnapshotHash string `json:"licenseSnapshotHash"`
		SafetyEvidenceHash  string `json:"safetyEvidenceHash"`
		Assets              []struct {
			AssetVersionID string `json:"assetVersionId"`
			ArtifactSHA256 string `json:"artifactSha256"`
			ContentHash    string `json:"contentHash"`
		} `json:"assets"`
	} `json:"g1Approval"`
	G2Approval struct {
		Decision                          string   `json:"decision"`
		BatchID                           string   `json:"batchId"`
		ShotIDs                           []string `json:"shotIds"`
		ProductInputHash                  string   `json:"productInputHash"`
		GenerationPlanID                  string   `json:"generationPlanId"`
		GenerationPlanHash                string   `json:"generationPlanHash"`
		ExecutionIntentHash               string   `json:"executionIntentHash"`
		SealedOfflineExecutionPackageHash string   `json:"sealedOfflineExecutionPackageHash"`
		ShotBindings                      []struct {
			ShotID             string `json:"shotId"`
			ShotSpecRevisionID string `json:"shotSpecRevisionId"`
			InputHash          string `json:"inputHash"`
			PromptHash         string `json:"promptHash"`
		} `json:"shotBindings"`
	} `json:"g2Approval"`
	ProviderRoute struct {
		Provider               string `json:"provider"`
		Profile                string `json:"profile"`
		ModelID                string `json:"modelId"`
		Region                 string `json:"region"`
		BillingMode            string `json:"billingMode"`
		CapabilitySnapshotHash string `json:"capabilitySnapshotHash"`
	} `json:"providerRoute"`
	BatchABudget struct {
		VideoPrimaryJobs                   int     `json:"videoPrimaryJobs"`
		VideoControlledRetriesMaximum      int     `json:"videoControlledRetriesMaximum"`
		VideoMaximumJobs                   int     `json:"videoMaximumJobs"`
		VideoMaximumTokens                 int64   `json:"videoMaximumTokens"`
		VideoMaximumAFPIncludingRetryDrift float64 `json:"videoMaximumAfpIncludingRetryAndDrift"`
		SpeechCharactersMaximum            int64   `json:"speechCharactersMaximum"`
		SpeechMaximumAFP                   float64 `json:"speechMaximumAfp"`
		MaximumNonSubscriptionCashCNY      float64 `json:"maximumNonSubscriptionCashCny"`
		AutomaticRetryAllowed              bool    `json:"automaticRetryAllowed"`
		AutomaticProviderSwitchAllowed     bool    `json:"automaticProviderSwitchAllowed"`
	} `json:"batchABudget"`
}

type preparedLive struct {
	formal            preparedFormal
	batch             preparedFormalBatch
	authorization     liveAuthorization
	authorizationData []byte
	authorizationHash string
	sourceCodeCommit  string
	plan              stage1.Plan
}

type liveProjectionSection struct {
	Name string            `json:"name"`
	Rows []json.RawMessage `json:"rows"`
}

type liveProjectionSnapshot struct {
	SchemaVersion string                  `json:"schemaVersion"`
	ActivationID  string                  `json:"activationId"`
	Sections      []liveProjectionSection `json:"sections"`
}

// MaterializeFLO100Live creates or replays a separate A-only live projection.
// It never constructs a Provider client and asserts a zero paid-boundary count
// before returning. A code-mismatched source authorization deliberately leaves
// the package without a submit-authorization row.
func MaterializeFLO100Live(
	ctx context.Context,
	pool *pgxpool.Pool,
	cas *artifactstore.Store,
	options LiveOptions,
) (stage1.Plan, stage1.ExecutionPackage, LiveReport, error) {
	if pool == nil || cas == nil {
		return stage1.Plan{}, stage1.ExecutionPackage{}, LiveReport{}, errors.New("PostgreSQL pool and CAS are required")
	}
	prepared, err := prepareFLO100Live(options)
	if err != nil {
		return stage1.Plan{}, stage1.ExecutionPackage{}, LiveReport{}, err
	}
	objects, err := ingestFormalCAS(ctx, cas, prepared.formal)
	if err != nil {
		return stage1.Plan{}, stage1.ExecutionPackage{}, LiveReport{}, err
	}
	package_, projectionHash, authorizedUntil, created, err := materializeFLO100LiveDB(
		ctx, pool, prepared, objects,
	)
	if err != nil {
		return stage1.Plan{}, stage1.ExecutionPackage{}, LiveReport{}, err
	}
	report, err := verifyFLO100Live(ctx, pool, prepared, package_, projectionHash, authorizedUntil, created)
	if err != nil {
		return stage1.Plan{}, stage1.ExecutionPackage{}, LiveReport{}, err
	}
	return prepared.plan, package_, report, nil
}

func prepareFLO100Live(options LiveOptions) (preparedLive, error) {
	formal, err := prepareFormal(options.Formal)
	if err != nil {
		return preparedLive{}, err
	}
	if len(formal.batches) != 3 || formal.batches[0].product.BatchID != formalBatchIDs[0] {
		return preparedLive{}, errors.New("FLO-100 offline package has no canonical batch A")
	}
	if !validGitCommit(options.SourceCodeCommit) {
		return preparedLive{}, errors.New("source-code-commit must be a full lowercase Git SHA")
	}
	data, err := os.ReadFile(strings.TrimSpace(options.AuthorizationPath))
	if err != nil || strings.TrimSpace(options.AuthorizationPath) == "" {
		return preparedLive{}, fmt.Errorf("read FLO-100 live authorization: %w", err)
	}
	if len(data) > 1<<20 {
		return preparedLive{}, errors.New("FLO-100 live authorization exceeds the size limit")
	}
	hash := sha256.Sum256(data)
	authorizationHash := hex.EncodeToString(hash[:])
	if options.ExpectedAuthorizationHash != authorizationHash || !validFormalDigest(authorizationHash) {
		return preparedLive{}, errors.New("FLO-100 live authorization hash differs from the independently pinned file")
	}
	var authorization liveAuthorization
	if err := json.Unmarshal(data, &authorization); err != nil {
		return preparedLive{}, fmt.Errorf("decode FLO-100 live authorization: %w", err)
	}
	batch := formal.batches[0]
	if err := validateLiveAuthorization(authorization, authorizationHash, formal, batch); err != nil {
		return preparedLive{}, err
	}
	plan := liveReadinessPlan(batch)
	if err := plan.Validate(); err != nil {
		return preparedLive{}, fmt.Errorf("validate FLO-100 live plan: %w", err)
	}
	return preparedLive{
		formal: formal, batch: batch, authorization: authorization,
		authorizationData: append([]byte(nil), data...), authorizationHash: authorizationHash,
		sourceCodeCommit: options.SourceCodeCommit,
		plan:             plan,
	}, nil
}

func validateLiveAuthorization(
	authorization liveAuthorization,
	authorizationHash string,
	formal preparedFormal,
	batch preparedFormalBatch,
) error {
	if authorization.SchemaVersion != liveAuthorizationSchema || authorization.IssueID != formalIssueID ||
		authorization.Decision.Outcome != liveAuthorizationOutcome ||
		!authorization.Decision.BatchAProviderPostAuthorizedConditionally ||
		authorization.Decision.BatchBProviderPostAuthorized ||
		authorization.Decision.BatchCProviderPostAuthorized || authorization.Decision.Stage4Authorized {
		return errors.New("live authorization is not an explicit A-only, B/C-closed decision")
	}
	if strings.TrimSpace(authorization.Authority.ActorID) == "" || authorization.Authority.IssuedAt.IsZero() ||
		!authorization.Authority.ValidUntil.After(authorization.Authority.IssuedAt) ||
		!authorization.Authority.ValidUntil.After(time.Now().UTC()) {
		return errors.New("live authorization authority or validity is incomplete")
	}
	if !validGitCommit(authorization.FixedEvidence.MergeCommit) ||
		authorization.FixedEvidence.OfflinePackageContentHash != formalManifestHash ||
		authorization.FixedEvidence.BatchASealedExecutionPackageHash != formalBatchAExecutionHash ||
		authorization.G2Approval.SealedOfflineExecutionPackageHash != formalBatchAExecutionHash {
		return errors.New("live authorization fixed code/offline/package evidence drifted")
	}
	if authorization.G1Approval.Decision != "APPROVED_FOR_EXACT_HASHES_INTERNAL_POC_CN" ||
		authorization.G1Approval.LicenseSnapshotHash != formalLicenseHash ||
		authorization.G1Approval.SafetyEvidenceHash != formalSafetyHash ||
		len(authorization.G1Approval.Assets) != 8 {
		return errors.New("live authorization G1 envelope is incomplete")
	}
	assets := make(map[string]formalAssetVersion, len(formal.assets.Versions))
	for _, asset := range formal.assets.Versions {
		assets[asset.AssetVersionID] = asset
	}
	seenAssets := make(map[string]struct{}, 8)
	for _, approved := range authorization.G1Approval.Assets {
		asset, ok := assets[approved.AssetVersionID]
		if !ok || asset.Artifact.SHA256 != approved.ArtifactSHA256 || asset.ContentHash != approved.ContentHash {
			return fmt.Errorf("live authorization G1 AssetVersion %s drifted", approved.AssetVersionID)
		}
		if _, duplicate := seenAssets[approved.AssetVersionID]; duplicate {
			return fmt.Errorf("duplicate live authorization G1 AssetVersion %s", approved.AssetVersionID)
		}
		seenAssets[approved.AssetVersionID] = struct{}{}
	}
	if authorization.G2Approval.Decision != "APPROVED_FOR_BATCH_A_ONLY_WHEN_PRE_SUBMIT_GATES_PASS" ||
		authorization.G2Approval.BatchID != formalBatchIDs[0] ||
		authorization.G2Approval.ProductInputHash != batch.product.ContentHash ||
		authorization.G2Approval.GenerationPlanID != batch.plan.GenerationPlanID ||
		authorization.G2Approval.GenerationPlanHash != batch.plan.ContentHash ||
		authorization.G2Approval.ExecutionIntentHash != batch.intent.ContentHash ||
		len(authorization.G2Approval.ShotIDs) != 10 || len(authorization.G2Approval.ShotBindings) != 10 {
		return errors.New("live authorization G2 product/plan/intent envelope drifted")
	}
	shots := make(map[string]formalShot, len(batch.product.Shots))
	for _, shot := range batch.product.Shots {
		shots[shot.ShotID] = shot
	}
	seenShots := make(map[string]struct{}, 10)
	for index, binding := range authorization.G2Approval.ShotBindings {
		shot, ok := shots[binding.ShotID]
		if !ok || authorization.G2Approval.ShotIDs[index] != binding.ShotID ||
			shot.ShotSpecRevisionID != binding.ShotSpecRevisionID ||
			shot.InputHash != binding.InputHash || shot.PromptHash != binding.PromptHash {
			return fmt.Errorf("live authorization G2 shot binding %d drifted", index+1)
		}
		if _, duplicate := seenShots[binding.ShotID]; duplicate {
			return fmt.Errorf("duplicate live authorization G2 shot %s", binding.ShotID)
		}
		seenShots[binding.ShotID] = struct{}{}
	}
	videoAFP, okVideo := exactAFPMilli(authorization.BatchABudget.VideoMaximumAFPIncludingRetryDrift)
	speechAFP, okSpeech := exactAFPMilli(authorization.BatchABudget.SpeechMaximumAFP)
	if authorization.ProviderRoute.Provider != "volcengine_ark" ||
		authorization.ProviderRoute.Profile != "agent-plan_cn-beijing_personal" ||
		authorization.ProviderRoute.ModelID != stage1.FormalVideoModel ||
		authorization.ProviderRoute.Region != "cn-beijing" ||
		authorization.ProviderRoute.BillingMode != liveBillingMode ||
		authorization.ProviderRoute.CapabilitySnapshotHash != formalVideoHash ||
		authorization.BatchABudget.VideoPrimaryJobs != 10 ||
		authorization.BatchABudget.VideoControlledRetriesMaximum != 1 ||
		authorization.BatchABudget.VideoMaximumJobs != 11 ||
		authorization.BatchABudget.VideoMaximumTokens != stage1.MaximumVideoTokens ||
		!okVideo || videoAFP != stage1.FLO100BatchAVideoAFPMilli ||
		authorization.BatchABudget.SpeechCharactersMaximum != 7 ||
		!okSpeech || speechAFP != stage1.FLO100BatchASpeechAFPMilli ||
		authorization.BatchABudget.MaximumNonSubscriptionCashCNY != 0 ||
		authorization.BatchABudget.AutomaticRetryAllowed ||
		authorization.BatchABudget.AutomaticProviderSwitchAllowed ||
		!validFormalDigest(authorizationHash) {
		return errors.New("live authorization route or subscription budget drifted")
	}
	return nil
}

func liveReadinessPlan(batch preparedFormalBatch) stage1.Plan {
	shotIDs := make([]string, 0, len(batch.product.Shots))
	for _, shot := range batch.product.Shots {
		shotIDs = append(shotIDs, shot.ShotID)
	}
	return stage1.Plan{
		SchemaVersion: stage1.SchemaVersion, BatchID: formalBatchIDs[0],
		VideoModel: stage1.FormalVideoModel, PrimaryShotIDs: shotIDs,
		MaximumNewJobs: 11, MaximumControlledRetries: 1,
		MaximumVideoTokens:      stage1.MaximumVideoTokens,
		MonthlyBaselineAFPMilli: 0, MonthlyMaximumAFPMilli: stage1.FLO100MonthlyAFPCapMilli,
		ReferenceJobAFPMilli: 2_504_700, MaximumAFPDriftBPS: stage1.MaximumAFPDriftBPS,
		MaximumCashMicros: 0, MaximumDialogueCharacters: 7,
		MaximumTTSAFPMilli: stage1.FLO100BatchASpeechAFPMilli,
		RequiredEvidence: []string{
			"artifact_hashes", "generation_manifest", "license_consent_gate", "provider_ids",
			"qc", "redaction_scan", "service_bom", "usage_cost",
		},
		TTSPreflight: stage1.TTSPreflight{
			CompletedNoCost: true, Provider: "volcengine_ark", Model: "doubao-seed-tts-2.0",
			Region: "cn-beijing", ResourceID: "seed-tts-2.0", CredentialReference: "ARK_API_KEY",
			CredentialAvailable: true, Pricing: "1350_afp_per_10000_chars",
			UsageAttribution: "provider_usage_tokens_per_request",
		},
		SubscriptionIncludedOnly: true,
	}
}

func materializeFLO100LiveDB(
	ctx context.Context,
	pool *pgxpool.Pool,
	prepared preparedLive,
	objects map[string]artifactstore.Artifact,
) (stage1.ExecutionPackage, string, time.Time, bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	defer tx.Rollback(ctx)
	if err := verifyFormalProjectionSeal(ctx, tx, prepared.formal, objects); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, fmt.Errorf("verify offline projection before live activation: %w", err)
	}
	offlinePackages, err := loadFormalReplay(ctx, tx, prepared.formal)
	if err != nil || len(offlinePackages) != 3 || offlinePackages[0].ContentHash != formalBatchAExecutionHash {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, errors.New("canonical offline A execution package is missing or drifted")
	}
	activationID := liveUUID(prepared, "activation")
	var existingHash string
	err = tx.QueryRow(ctx, `
		SELECT live_execution_package_hash
		FROM video_pipeline.stage1_live_activations
		WHERE id=$1 FOR SHARE`, activationID).Scan(&existingHash)
	if err == nil {
		package_, projectionHash, err := loadAndVerifyFLO100LiveReplay(ctx, tx, prepared, activationID, objects)
		if err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		authorizedUntil, err := ensureLiveSubmitAuthorization(ctx, tx, prepared, activationID, package_.ContentHash, projectionHash)
		if err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		return package_, projectionHash, authorizedUntil, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}

	now := time.Now().UTC()
	authorization := prepared.authorization
	batch := prepared.batch
	offlinePackage := offlinePackages[0]
	controlSeriesID := liveUUID(prepared, "control-series")
	videoProfileID := liveUUID(prepared, "provider-profile:video")
	speechProfileID := liveUUID(prepared, "provider-profile:speech")
	videoCapabilityID := liveUUID(prepared, "capability:video")
	speechCapabilityID := liveUUID(prepared, "capability:speech")
	livePlanID := liveUUID(prepared, "generation-plan")
	videoBudgetID := liveUUID(prepared, "budget:video")
	speechBudgetID := liveUUID(prepared, "budget:speech")
	g1ID := liveUUID(prepared, "gate:g1")
	g2ID := liveUUID(prepared, "gate:g2")
	safetyID := liveUUID(prepared, "gate:safety")
	traceID := "flo100-live-a-" + prepared.authorizationHash[:12]

	var sourceSeriesID, sourceEpisodeID, sourceEpisodeRevisionID, generationProfileID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT ep.series_id, ep.id, er.id, gr.generation_profile_id
		FROM video_pipeline.generation_runs gr
		JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=gr.shot_spec_revision_id
		JOIN video_pipeline.shots sh ON sh.id=ssr.shot_id
		JOIN video_pipeline.scenes sc ON sc.id=sh.scene_id
		JOIN video_pipeline.episodes ep ON ep.id=sc.episode_id
		JOIN video_pipeline.episode_revisions er ON er.id=$2 AND er.episode_id=ep.id
		WHERE gr.id=$1 FOR SHARE OF gr,ssr,er`,
		mustUUID(offlinePackage.PrimaryJobs[0].Run.RunID),
		mustUUID(offlinePackage.PostProduction.EpisodeRevisionID),
	).Scan(&sourceSeriesID, &sourceEpisodeID, &sourceEpisodeRevisionID, &generationProfileID); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, fmt.Errorf("load canonical A scope: %w", err)
	}

	videoRoute := formalVideoRoute()
	speechRoute := formalSpeechModelRoute()
	jobs := make([]stage1.FrozenJob, 0, len(offlinePackage.PrimaryJobs))
	runIDs := make([]string, 0, len(offlinePackage.PrimaryJobs))
	for index, offlineJob := range offlinePackage.PrimaryJobs {
		liveRunID := liveUUID(prepared, "run:"+offlineJob.Run.RunID)
		runDigest, err := repository.GenerationRunSpecDigest(
			offlineJob.ShotSpecRevisionID, offlineJob.PromptSnapshotID,
			offlineJob.PromptSnapshotHash, generationProfileID.String(),
			livePlanID.String(), videoRoute, 1,
		)
		if err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		job := offlineJob
		job.AttemptID = liveUUID(prepared, fmt.Sprintf("attempt-identity:%02d", index+1)).String()
		job.IdempotencyKey = "provider-job-" + liveRunID.String()
		job.Run = orchestration.GenerationRunRef{RunID: liveRunID.String(), RunSpecDigest: runDigest, Attempt: 1}
		job.GenerationPlanID = livePlanID.String()
		job.BudgetApprovalID = videoBudgetID.String()
		job.BudgetMaximumMicros = 0
		job.BudgetCurrency = "CNY"
		job.ProviderProfileID = videoProfileID.String()
		job.Route = videoRoute
		job.EstimatedNonSubscriptionCashMicros = 0
		job.WorkflowID = "flo100-live-a-" + liveRunID.String()
		job.ActivityID = "submit-" + offlineJob.ShotID
		job.TraceID = traceID + "-" + fmt.Sprintf("%02d", index+1)
		job.BillingMode = liveBillingMode
		jobs = append(jobs, job)
		runIDs = append(runIDs, liveRunID.String())
	}

	post := offlinePackage.PostProduction
	post.GenerationPlanID = livePlanID.String()
	post.RunIDs = runIDs
	post.TraceID = traceID
	post.Config.Evidence = postproduction.EvidenceLive
	post.Config.SpeechRoute = speechRoute
	post.Config.SpeechProviderProfileID = speechProfileID.String()
	post.Config.SpeechBudgetApprovalID = speechBudgetID.String()
	post.Config.SpeechBudgetMaximumMicros = 0
	post.Config.SpeechBudgetCurrency = "CNY"
	post.Config.SpeechBillingMode = liveBillingMode
	post.Config.SpeechMaximumAFPMilli = stage1.FLO100BatchASpeechAFPMilli
	post.Config.SpeechMaximumCashMicros = 0
	post.Config.SpeechMaxAttempts = 1
	livePackage, err := stage1.SealExecutionPackage(stage1.ExecutionPackage{
		SchemaVersion: stage1.ExecutionPackageSchemaVersion,
		BatchID:       formalBatchIDs[0], PrimaryJobs: jobs, PostProduction: post,
		LiveActivation: &stage1.LiveActivationBinding{
			ActivationID:                activationID.String(),
			OfflineExecutionPackageHash: offlinePackage.ContentHash,
			SourceAuthorizationHash:     prepared.authorizationHash,
			SourceCodeCommit:            prepared.FormalSourceCodeCommit(),
			G1DecisionID:                g1ID.String(), G2DecisionID: g2ID.String(), SafetyDecisionID: safetyID.String(),
		},
	})
	if err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := livePackage.Validate(prepared.plan); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, fmt.Errorf("validate FLO-100 live execution package: %w", err)
	}

	executionPolicy := controlplane.ExecutionPolicy{
		TargetTerritory: "CN", ProductForm: "INTERNAL_POC_ACCEPTANCE",
		ContentSafetyPolicyVersion: "flo100-original-fiction-safety-v1",
		ContentSafetyDecisionID:    safetyID.String(),
	}
	livePlanHash, err := digest(map[string]any{
		"schemaVersion": liveProjectionSchema, "batchId": formalBatchIDs[0],
		"sourceCodeCommit":            prepared.FormalSourceCodeCommit(),
		"sourceAuthorizationHash":     prepared.authorizationHash,
		"offlineExecutionPackageHash": offlinePackage.ContentHash,
		"shotSpecRevisionIds":         authorization.G2Approval.ShotBindings,
		"route":                       videoRoute, "executionPolicy": executionPolicy,
		"cashMicros": 0, "videoAfpMilli": stage1.FLO100BatchAVideoAFPMilli,
		"speechAfpMilli": stage1.FLO100BatchASpeechAFPMilli,
	})
	if err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	planRecord := controlplane.GenerationPlan{
		GenerationPlanID: livePlanID.String(), State: "READY", DryRun: false,
		ShotCount: 10, ProviderCallCount: 10,
		RouteSnapshot: controlplane.ModelRouteSnapshot{
			CapabilityAlias: "video.primary", ProviderProfileID: videoProfileID.String(),
			Provider: videoRoute.Provider, ModelID: videoRoute.ModelID,
			RouteVersion: videoRoute.RouteVersion, CapabilityHash: videoRoute.CapabilityHash,
		},
		ExecutionPolicy: executionPolicy,
		Estimate: controlplane.CostEstimate{
			UnitsMinimum: batch.manifest.TargetDuration, UnitsMaximum: batch.manifest.TargetDuration,
			Unit: "video_seconds", Currency: "CNY", PricingRuleVersion: "agent-plan-subscription-v1",
			ValidUntil: authorization.Authority.ValidUntil,
		},
		SpeechBudgetLimit: &controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
		BudgetDecision:    "SUBSCRIPTION_INCLUDED_ONLY", PlanHash: livePlanHash,
	}

	exec := func(label, query string, args ...any) error {
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return nil
	}
	if err := exec("live control series", `INSERT INTO video_pipeline.series
		(id,title,status,rights_declaration,created_by)
		VALUES ($1,$2,'ACTIVE',$3,$4)`, controlSeriesID,
		"FLO-100 Batch A live activation controls "+prepared.FormalSourceCodeCommit()[:12],
		map[string]any{"sourceSeriesId": sourceSeriesID, "batchId": formalBatchIDs[0], "commercialUse": false},
		authorization.Authority.ActorID); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	for _, profile := range []struct {
		id      uuid.UUID
		name    string
		baseURL string
		hash    string
	}{
		{videoProfileID, "FLO-100 A Agent Plan video " + prepared.FormalSourceCodeCommit()[:12], "internal://volcengine-provider", formalVideoHash},
		{speechProfileID, "FLO-100 A Agent Plan speech " + prepared.FormalSourceCodeCommit()[:12], "internal://volcengine-tts-provider", formalSpeechHash},
	} {
		if err := exec("live provider profile", `INSERT INTO video_pipeline.provider_profiles
			(id,provider,display_name,base_url_ref,credential_ref,enabled,mode,health,config_hash)
			VALUES ($1,'VOLCENGINE',$2,$3,'env:ARK_API_KEY',true,'LIVE','READY',$4)`,
			profile.id, profile.name, profile.baseURL, profile.hash); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	limits := map[string]any{
		"billingMode": liveBillingMode, "allowedTerritories": []string{"CN"},
		"productForms":                []string{"INTERNAL_POC_ACCEPTANCE"},
		"contentSafetyPolicyVersions": []string{"flo100-original-fiction-safety-v1"},
		"remainingCalls":              11, "monthlyAccountCapAfpMilli": stage1.FLO100MonthlyAFPCapMilli,
		"maximumVideoAfpMilli":             stage1.FLO100BatchAVideoAFPMilli,
		"maximumSpeechAfpMilli":            stage1.FLO100BatchASpeechAFPMilli,
		"maximumNonSubscriptionCashMicros": 0, "automaticRetry": false, "automaticProviderSwitch": false,
	}
	for _, capability := range []struct {
		id, profile                 uuid.UUID
		alias, model, version, hash string
		inputs                      []string
	}{
		{videoCapabilityID, videoProfileID, "video.primary", stage1.FormalVideoModel, "agent-plan-large-v1", formalVideoHash, []string{"prompt", "reference_image"}},
		{speechCapabilityID, speechProfileID, "speech.primary", "doubao-seed-tts-2.0", "agent-plan-large-tts-v2", formalSpeechHash, []string{"text"}},
	} {
		if err := exec("live capability", `INSERT INTO video_pipeline.provider_capability_snapshots
			(id,provider_profile_id,capability_alias,model_id,route_version,supported_inputs,limits,
			 pricing_rule_version,capability_hash,status,effective_at,expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'agent-plan-subscription-v1',$8,'ACTIVE',$9,$10)`,
			capability.id, capability.profile, capability.alias, capability.model, capability.version,
			capability.inputs, limits, capability.hash, now, authorization.Authority.ValidUntil); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	for _, decision := range []struct {
		id                uuid.UUID
		gate, explanation string
	}{
		{g1ID, "G1", string(mustJSON(map[string]any{"authorizationHash": prepared.authorizationHash, "scope": "exact-eight-assets"}))},
		{g2ID, "G2", string(mustJSON(map[string]any{"authorizationHash": prepared.authorizationHash, "productInputHash": batch.product.ContentHash, "generationPlanHash": batch.plan.ContentHash, "executionIntentHash": batch.intent.ContentHash}))},
		{safetyID, "SAFETY", string(mustJSON(map[string]any{"policyVersion": executionPolicy.ContentSafetyPolicyVersion, "evidenceHash": formalSafetyHash, "validUntil": authorization.Authority.ValidUntil}))},
	} {
		if err := exec("live approval decision", `INSERT INTO video_pipeline.approval_decisions
			(id,series_id,episode_id,gate,decision,reason_code,explanation,actor_id,actor_role,decided_at,trace_id)
			VALUES ($1,$2,NULL,$3,'APPROVED',$4,$5,$6,'ADMIN',$7,$8)`, decision.id, controlSeriesID,
			decision.gate, liveSourceReason, decision.explanation, authorization.Authority.ActorID, now, traceID); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	bind := func(decision uuid.UUID, typ string, id uuid.UUID, hash string) error {
		return exec("live approval binding", `INSERT INTO video_pipeline.approval_bindings
			(decision_id,object_type,revision_id,content_hash) VALUES ($1,$2,$3,$4)`, decision, typ, id, hash)
	}
	for _, approved := range authorization.G1Approval.Assets {
		if err := bind(g1ID, "ASSET_VERSION", mustUUID(approved.AssetVersionID), approved.ArtifactSHA256); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	var episodeHash string
	if err := tx.QueryRow(ctx, `SELECT content_hash FROM video_pipeline.episode_revisions WHERE id=$1 FOR SHARE`, sourceEpisodeRevisionID).Scan(&episodeHash); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := bind(g2ID, "EPISODE_REVISION", sourceEpisodeRevisionID, episodeHash); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := bind(safetyID, "EPISODE_REVISION", sourceEpisodeRevisionID, episodeHash); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	for index, approved := range authorization.G2Approval.ShotBindings {
		var shotHash string
		if err := tx.QueryRow(ctx, `SELECT content_hash FROM video_pipeline.shot_spec_revisions WHERE id=$1 FOR SHARE`, mustUUID(approved.ShotSpecRevisionID)).Scan(&shotHash); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		if err := bind(g2ID, "SHOT_SPEC_REVISION", mustUUID(approved.ShotSpecRevisionID), shotHash); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		if err := bind(g2ID, "PROMPT_SNAPSHOT", mustUUID(offlinePackage.PrimaryJobs[index].PromptSnapshotID), offlinePackage.PrimaryJobs[index].PromptSnapshotHash); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		if err := bind(safetyID, "SHOT_SPEC_REVISION", mustUUID(approved.ShotSpecRevisionID), shotHash); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	for _, name := range []string{"product:" + formalBatchIDs[0], "plan:" + formalBatchIDs[0], "intent:" + formalBatchIDs[0]} {
		object := objects[name]
		artifactID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("stage1-input-artifact:"+object.Digest))
		if err := bind(g2ID, "ARTIFACT", artifactID, object.Digest); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	if err := bind(safetyID, "ARTIFACT", liveUUID(prepared, "safety-artifact"), formalSafetyHash); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := exec("live plan operation", `INSERT INTO video_pipeline.operation_requests
		(id,operation_type,aggregate_type,aggregate_id,state,trace_id,requested_by)
		VALUES ($1,'CREATE_GENERATION_PLAN','SERIES',$2,'SUCCEEDED',$3,$4)`,
		livePlanID, controlSeriesID, traceID, authorization.Authority.ActorID); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := exec("live plan idempotency", `INSERT INTO video_pipeline.idempotency_records
		(scope,idempotency_key,request_hash,operation_id,response_status,response_body,expires_at)
		VALUES ('flo100-live-activate',$1,$2,$3,201,$4,$5)`,
		prepared.FormalSourceCodeCommit(), livePlanHash, livePlanID, planRecord, authorization.Authority.ValidUntil); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	shotIDs := make([]string, len(authorization.G2Approval.ShotBindings))
	for index, binding := range authorization.G2Approval.ShotBindings {
		shotIDs[index] = binding.ShotSpecRevisionID
	}
	if err := exec("live plan audit", `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','generation_plan.created','GENERATION_PLAN',$4,$5,$6,$7)`,
		liveUUID(prepared, "audit:plan"), now, authorization.Authority.ActorID, livePlanID,
		liveSourceReason, traceID, map[string]any{
			"seriesId": controlSeriesID, "sourceSeriesId": sourceSeriesID,
			"episodeRevisionId": sourceEpisodeRevisionID, "shotSpecRevisionIds": shotIDs,
			"candidatesPerShot": 1, "pricingRuleVersion": "agent-plan-subscription-v1",
			"budgetLimit":       controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
			"speechBudgetLimit": controlplane.BudgetLimit{AmountMicros: 0, Currency: "CNY"},
			"executionPolicy":   executionPolicy, "inputPackageHash": batch.product.ContentHash,
			"sourceAuthorizationHash": prepared.authorizationHash,
		}); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	for _, review := range []struct {
		id    uuid.UUID
		scope string
	}{{videoBudgetID, "VIDEO"}, {speechBudgetID, "SPEECH"}} {
		if err := exec("live zero-cash budget approval", `INSERT INTO video_pipeline.review_tasks
			(id,series_id,episode_id,review_type,state,reason_codes,assigned_role,decided_at,
			 generation_plan_id,budget_scope,budget_limit_micros,budget_currency)
			VALUES ($1,$2,NULL,'BUDGET','APPROVED',ARRAY[$3],'ADMIN',$4,$5,$6,0,'CNY')`,
			review.id, controlSeriesID, liveSourceReason, now, livePlanID, review.scope); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	for index, job := range jobs {
		offlineJob := offlinePackage.PrimaryJobs[index]
		if err := exec("live generation run", `INSERT INTO video_pipeline.generation_runs
			(id,shot_spec_revision_id,prompt_snapshot_id,generation_profile_id,temporal_workflow_id,
			 run_spec_digest,creative_attempt,state,dry_run,budget_approval_id,trace_id,created_by)
			VALUES ($1,$2,$3,$4,$5,$6,1,'VALIDATED',false,$7,$8,$9)`,
			mustUUID(job.Run.RunID), mustUUID(job.ShotSpecRevisionID), mustUUID(job.PromptSnapshotID), generationProfileID,
			job.WorkflowID, job.Run.RunSpecDigest, videoBudgetID.String(), job.TraceID, authorization.Authority.ActorID); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		attemptID := liveUUID(prepared, "attempt:"+job.Run.RunID)
		if err := exec("live generation attempt", `INSERT INTO video_pipeline.generation_attempts
			(id,generation_run_id,sequence,attempt_kind,state,input_hash,model_snapshot,parameter_diff)
			VALUES ($1,$2,1,'PROVIDER_REQUEST','VALIDATED',$3,$4,$5)`,
			attemptID, mustUUID(job.Run.RunID), job.Run.RunSpecDigest, videoRoute,
			map[string]any{"sourceOfflineRunId": offlineJob.Run.RunID, "sourceAuthorizationHash": prepared.authorizationHash, "billingMode": liveBillingMode}); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
		if err := exec("live run audit", `INSERT INTO video_pipeline.audit_events
			(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
			VALUES ($1,$2,$3,'ADMIN','generation_run.created','GENERATION_RUN',$4,$5,$6,$7)`,
			liveUUID(prepared, "audit:run:"+job.Run.RunID), now, authorization.Authority.ActorID,
			mustUUID(job.Run.RunID), liveSourceReason, job.TraceID, map[string]any{
				"workflowId": job.WorkflowID, "shotSpecRevisionId": job.ShotSpecRevisionID,
				"promptSnapshotId": job.PromptSnapshotID, "runSpecDigest": job.Run.RunSpecDigest,
				"creativeAttempt": 1, "generationPlanId": livePlanID,
				"sourceOfflineRunId": offlineJob.Run.RunID, "sourceAuthorizationHash": prepared.authorizationHash,
			}); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	if err := exec("live activation", `INSERT INTO video_pipeline.stage1_live_activations
		(id,batch_id,control_series_id,source_series_id,source_episode_id,source_episode_revision_id,
		 source_generation_plan_id,live_generation_plan_id,video_provider_profile_id,video_capability_snapshot_id,
		 speech_provider_profile_id,speech_capability_snapshot_id,video_budget_approval_id,speech_budget_approval_id,
		 g1_decision_id,g2_decision_id,safety_decision_id,offline_package_hash,offline_execution_package_hash,
		 source_authorization_hash,source_authorization,source_authorized_commit,source_code_commit,source_authorization_valid_until,
		 live_plan_hash,live_execution_package_hash,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
		activationID, formalBatchIDs[0], controlSeriesID, sourceSeriesID, sourceEpisodeID, sourceEpisodeRevisionID,
		mustUUID(batch.plan.GenerationPlanID), livePlanID, videoProfileID, videoCapabilityID,
		speechProfileID, speechCapabilityID, videoBudgetID, speechBudgetID, g1ID, g2ID, safetyID,
		formalManifestHash, offlinePackage.ContentHash, prepared.authorizationHash, authorization,
		authorization.FixedEvidence.MergeCommit, prepared.FormalSourceCodeCommit(), authorization.Authority.ValidUntil,
		livePlanHash, livePackage.ContentHash, now); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	for index, job := range jobs {
		offlineJob := offlinePackage.PrimaryJobs[index]
		approved := authorization.G2Approval.ShotBindings[index]
		if err := exec("live activation run", `INSERT INTO video_pipeline.stage1_live_activation_runs
			(activation_id,ordinal,run_id,offline_run_id,shot_spec_revision_id,prompt_snapshot_id,
			 prompt_snapshot_hash,authorized_prompt_hash,intent_input_hash,estimated_video_tokens,predicted_afp_milli)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, activationID, index+1,
			mustUUID(job.Run.RunID), mustUUID(offlineJob.Run.RunID), mustUUID(job.ShotSpecRevisionID),
			mustUUID(job.PromptSnapshotID), job.PromptSnapshotHash, approved.PromptHash, approved.InputHash,
			job.EstimatedVideoTokens, job.PredictedAFPMilli); err != nil {
			return stage1.ExecutionPackage{}, "", time.Time{}, false, err
		}
	}
	if err := exec("live materialization audit", `INSERT INTO video_pipeline.audit_events
		(id,occurred_at,actor_id,actor_role,action,aggregate_type,aggregate_id,reason_code,trace_id,payload)
		VALUES ($1,$2,$3,'ADMIN','flo100.live_execution_package.materialized','STAGE1_LIVE_ACTIVATION',$4,$5,$6,$7)`,
		liveUUID(prepared, "audit:materialize"), now, authorization.Authority.ActorID, activationID,
		liveSourceReason, traceID, map[string]any{
			"executionPackage": livePackage, "executionPackageHash": livePackage.ContentHash,
			"offlineExecutionPackageHash": offlinePackage.ContentHash,
			"sourceAuthorizationHash":     prepared.authorizationHash,
			"sourceCodeCommit":            prepared.FormalSourceCodeCommit(), "providerCalls": 0,
		}); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	projectionHash, err := flo100LiveProjectionHash(ctx, tx, activationID, jobs, []uuid.UUID{g1ID, g2ID, safetyID})
	if err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := exec("live projection seal", `INSERT INTO video_pipeline.stage1_live_projection_seals
		(activation_id,projection_hash,sealed_at,sealed_by) VALUES ($1,$2,$3,$4)`,
		activationID, projectionHash, now, authorization.Authority.ActorID); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	authorizedUntil, err := ensureLiveSubmitAuthorization(ctx, tx, prepared, activationID, livePackage.ContentHash, projectionHash)
	if err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stage1.ExecutionPackage{}, "", time.Time{}, false, err
	}
	return livePackage, projectionHash, authorizedUntil, true, nil
}

func (p preparedLive) FormalSourceCodeCommit() string {
	return p.sourceCodeCommit
}

func loadAndVerifyFLO100LiveReplay(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedLive,
	activationID uuid.UUID,
	objects map[string]artifactstore.Artifact,
) (stage1.ExecutionPackage, string, error) {
	var packageJSON []byte
	if err := tx.QueryRow(ctx, `SELECT payload->'executionPackage'
		FROM video_pipeline.audit_events
		WHERE action='flo100.live_execution_package.materialized'
		  AND aggregate_type='STAGE1_LIVE_ACTIVATION' AND aggregate_id=$1
		  AND reason_code=$2 FOR SHARE`, activationID, liveSourceReason).Scan(&packageJSON); err != nil {
		return stage1.ExecutionPackage{}, "", fmt.Errorf("load FLO-100 live replay package: %w", err)
	}
	var package_ stage1.ExecutionPackage
	if err := json.Unmarshal(packageJSON, &package_); err != nil || package_.Validate(prepared.plan) != nil {
		return stage1.ExecutionPackage{}, "", errors.New("stored FLO-100 live execution package is invalid")
	}
	jobs := package_.PrimaryJobs
	decisions := []uuid.UUID{mustUUID(package_.LiveActivation.G1DecisionID), mustUUID(package_.LiveActivation.G2DecisionID), mustUUID(package_.LiveActivation.SafetyDecisionID)}
	projectionHash, err := flo100LiveProjectionHash(ctx, tx, activationID, jobs, decisions)
	if err != nil {
		return stage1.ExecutionPackage{}, "", err
	}
	var sealedHash string
	if err := tx.QueryRow(ctx, `SELECT projection_hash FROM video_pipeline.stage1_live_projection_seals WHERE activation_id=$1 FOR SHARE`, activationID).Scan(&sealedHash); err != nil {
		return stage1.ExecutionPackage{}, "", fmt.Errorf("load FLO-100 live projection seal: %w", err)
	}
	if projectionHash != sealedHash {
		return stage1.ExecutionPackage{}, "", fmt.Errorf("FLO-100 live projection drift: current %s differs from sealed %s", projectionHash, sealedHash)
	}
	var storedPackageHash, storedSourceCommit, storedOfflineHash string
	if err := tx.QueryRow(ctx, `SELECT live_execution_package_hash,source_code_commit,offline_execution_package_hash
		FROM video_pipeline.stage1_live_activations WHERE id=$1 FOR SHARE`, activationID).
		Scan(&storedPackageHash, &storedSourceCommit, &storedOfflineHash); err != nil {
		return stage1.ExecutionPackage{}, "", err
	}
	if storedPackageHash != package_.ContentHash || storedSourceCommit != prepared.FormalSourceCodeCommit() ||
		storedOfflineHash != formalBatchAExecutionHash {
		return stage1.ExecutionPackage{}, "", errors.New("FLO-100 live activation identity drifted")
	}
	_ = objects
	return package_, projectionHash, nil
}

func ensureLiveSubmitAuthorization(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedLive,
	activationID uuid.UUID,
	packageHash, projectionHash string,
) (time.Time, error) {
	if prepared.authorization.FixedEvidence.MergeCommit != prepared.FormalSourceCodeCommit() {
		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM video_pipeline.stage1_live_submit_authorizations WHERE activation_id=$1`, activationID).Scan(&count); err != nil {
			return time.Time{}, err
		}
		if count != 0 {
			return time.Time{}, errors.New("code-mismatched authority cannot coexist with live submit authorization")
		}
		return time.Time{}, nil
	}
	now := time.Now().UTC()
	if !prepared.authorization.Authority.ValidUntil.After(now) {
		return time.Time{}, errors.New("live submit authorization is expired")
	}
	_, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_submit_authorizations
		(activation_id,authorization_hash,authorization_payload,source_code_commit,execution_package_hash,projection_hash,
		 actor_id,issued_at,valid_until,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (activation_id) DO NOTHING`, activationID, prepared.authorizationHash,
		prepared.authorization, prepared.FormalSourceCodeCommit(), packageHash, projectionHash, prepared.authorization.Authority.ActorID,
		prepared.authorization.Authority.IssuedAt, prepared.authorization.Authority.ValidUntil, now)
	if err != nil {
		return time.Time{}, err
	}
	var storedHash, commit, storedPackage, storedProjection, actor string
	var authorizationExact bool
	var issuedAt, validUntil time.Time
	if err := tx.QueryRow(ctx, `SELECT authorization_hash,source_code_commit,execution_package_hash,
		projection_hash,actor_id,issued_at,valid_until,authorization_payload=$2
		FROM video_pipeline.stage1_live_submit_authorizations WHERE activation_id=$1 FOR SHARE`, activationID, prepared.authorization).
		Scan(&storedHash, &commit, &storedPackage, &storedProjection, &actor, &issuedAt, &validUntil, &authorizationExact); err != nil {
		return time.Time{}, err
	}
	if storedHash != prepared.authorizationHash || commit != prepared.FormalSourceCodeCommit() ||
		storedPackage != packageHash || storedProjection != projectionHash ||
		actor != prepared.authorization.Authority.ActorID || !issuedAt.Equal(prepared.authorization.Authority.IssuedAt) ||
		!validUntil.Equal(prepared.authorization.Authority.ValidUntil) || !authorizationExact {
		return time.Time{}, errors.New("existing live submit authorization differs from the exact immutable authority")
	}
	return validUntil, nil
}

func flo100LiveProjectionHash(
	ctx context.Context,
	tx pgx.Tx,
	activationID uuid.UUID,
	jobs []stage1.FrozenJob,
	decisionIDs []uuid.UUID,
) (string, error) {
	if len(jobs) != 10 || len(decisionIDs) != 3 {
		return "", errors.New("live projection requires ten jobs and three decisions")
	}
	runIDs := make([]uuid.UUID, len(jobs))
	for index, job := range jobs {
		runIDs[index] = mustUUID(job.Run.RunID)
	}
	var controlSeriesID, planID, videoProfileID, speechProfileID, videoCapabilityID, speechCapabilityID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT control_series_id,live_generation_plan_id,video_provider_profile_id,
		speech_provider_profile_id,video_capability_snapshot_id,speech_capability_snapshot_id
		FROM video_pipeline.stage1_live_activations WHERE id=$1`, activationID).
		Scan(&controlSeriesID, &planID, &videoProfileID, &speechProfileID, &videoCapabilityID, &speechCapabilityID); err != nil {
		return "", fmt.Errorf("read live projection identity: %w", err)
	}
	sections := []struct {
		name     string
		expected int
		query    string
		args     []any
	}{
		{"activation", 1, `SELECT to_jsonb(a) FROM video_pipeline.stage1_live_activations a WHERE id=$1`, []any{activationID}},
		{"activation_runs", 10, `SELECT to_jsonb(r) FROM video_pipeline.stage1_live_activation_runs r WHERE activation_id=$1 ORDER BY ordinal`, []any{activationID}},
		{"control_series", 1, `SELECT to_jsonb(s) FROM video_pipeline.series s WHERE id=$1`, []any{controlSeriesID}},
		{"approval_decisions", 3, `SELECT to_jsonb(d) FROM video_pipeline.approval_decisions d WHERE id=ANY($1) ORDER BY id`, []any{decisionIDs}},
		{"approval_bindings", 44, `SELECT to_jsonb(b) FROM video_pipeline.approval_bindings b WHERE decision_id=ANY($1) ORDER BY decision_id,object_type,revision_id`, []any{decisionIDs}},
		{"provider_profiles", 2, `SELECT to_jsonb(p) FROM video_pipeline.provider_profiles p WHERE id=ANY($1) ORDER BY id`, []any{[]uuid.UUID{videoProfileID, speechProfileID}}},
		{"provider_capabilities", 2, `SELECT to_jsonb(c) FROM video_pipeline.provider_capability_snapshots c WHERE id=ANY($1) ORDER BY id`, []any{[]uuid.UUID{videoCapabilityID, speechCapabilityID}}},
		{"plan_operation", 1, `SELECT to_jsonb(o) FROM video_pipeline.operation_requests o WHERE id=$1`, []any{planID}},
		{"plan_idempotency", 1, `SELECT to_jsonb(i) FROM video_pipeline.idempotency_records i WHERE scope='flo100-live-activate' AND operation_id=$1`, []any{planID}},
		{"plan_audit", 1, `SELECT to_jsonb(a) FROM video_pipeline.audit_events a WHERE aggregate_type='GENERATION_PLAN' AND aggregate_id=$1 AND reason_code=$2 ORDER BY id`, []any{planID, liveSourceReason}},
		{"budget_reviews", 2, `SELECT to_jsonb(r) FROM video_pipeline.review_tasks r WHERE generation_plan_id=$1 AND review_type='BUDGET' ORDER BY id`, []any{planID}},
		{"runs", 10, `SELECT jsonb_build_object(
			'id',r.id,'shot_spec_revision_id',r.shot_spec_revision_id,'prompt_snapshot_id',r.prompt_snapshot_id,
			'generation_profile_id',r.generation_profile_id,'temporal_workflow_id',r.temporal_workflow_id,
			'run_spec_digest',r.run_spec_digest,'creative_attempt',r.creative_attempt,'dry_run',r.dry_run,
			'budget_approval_id',r.budget_approval_id,'trace_id',r.trace_id,'created_by',r.created_by)
			FROM video_pipeline.generation_runs r WHERE id=ANY($1) ORDER BY id`, []any{runIDs}},
		{"attempts", 10, `SELECT jsonb_build_object(
			'id',a.id,'generation_run_id',a.generation_run_id,'sequence',a.sequence,'attempt_kind',a.attempt_kind,
			'input_hash',a.input_hash,'model_snapshot',a.model_snapshot,'parameter_diff',a.parameter_diff)
			FROM video_pipeline.generation_attempts a WHERE generation_run_id=ANY($1) ORDER BY id`, []any{runIDs}},
		{"run_audits", 10, `SELECT to_jsonb(a) FROM video_pipeline.audit_events a WHERE aggregate_type='GENERATION_RUN' AND aggregate_id=ANY($1) AND reason_code=$2 ORDER BY id`, []any{runIDs, liveSourceReason}},
		{"materialization_audit", 1, `SELECT to_jsonb(a) FROM video_pipeline.audit_events a WHERE aggregate_type='STAGE1_LIVE_ACTIVATION' AND aggregate_id=$1 AND action='flo100.live_execution_package.materialized' ORDER BY id`, []any{activationID}},
	}
	snapshot := liveProjectionSnapshot{SchemaVersion: liveProjectionSchema, ActivationID: activationID.String()}
	for _, section := range sections {
		rows, err := queryFormalProjectionRows(ctx, tx, section.query, section.args...)
		if err != nil {
			return "", fmt.Errorf("read live projection section %s: %w", section.name, err)
		}
		if len(rows) != section.expected {
			return "", fmt.Errorf("live projection drift: section %s has %d rows, expected %d", section.name, len(rows), section.expected)
		}
		snapshot.Sections = append(snapshot.Sections, liveProjectionSection{Name: section.name, Rows: rows})
	}
	return digest(snapshot)
}

func verifyFLO100Live(
	ctx context.Context,
	pool *pgxpool.Pool,
	prepared preparedLive,
	package_ stage1.ExecutionPackage,
	projectionHash string,
	authorizedUntil time.Time,
	requireProviderFree bool,
) (LiveReport, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
	if err != nil {
		return LiveReport{}, err
	}
	defer tx.Rollback(ctx)
	activationID := mustUUID(package_.LiveActivation.ActivationID)
	decisions := []uuid.UUID{mustUUID(package_.LiveActivation.G1DecisionID), mustUUID(package_.LiveActivation.G2DecisionID), mustUUID(package_.LiveActivation.SafetyDecisionID)}
	currentHash, err := flo100LiveProjectionHash(ctx, tx, activationID, package_.PrimaryJobs, decisions)
	if err != nil {
		return LiveReport{}, err
	}
	if currentHash != projectionHash {
		return LiveReport{}, errors.New("FLO-100 live verification projection hash drifted")
	}
	runIDs := make([]uuid.UUID, len(package_.PrimaryJobs))
	for index, job := range package_.PrimaryJobs {
		runIDs[index] = mustUUID(job.Run.RunID)
	}
	var providerJobs, reservations, cost int64
	if err := tx.QueryRow(ctx, `WITH attempts AS (
		SELECT id FROM video_pipeline.generation_attempts WHERE generation_run_id=ANY($1)
	), jobs AS (
		SELECT pj.id FROM video_pipeline.provider_jobs pj JOIN attempts a ON a.id=pj.generation_attempt_id
	) SELECT (SELECT COUNT(*) FROM jobs),
		(SELECT COUNT(*) FROM video_pipeline.budget_reservations WHERE generation_run_id=ANY($1)),
		(SELECT COUNT(*) FROM video_pipeline.cost_ledger cl JOIN jobs j ON j.id=cl.provider_job_id)`, runIDs).
		Scan(&providerJobs, &reservations, &cost); err != nil {
		return LiveReport{}, err
	}
	if requireProviderFree && (providerJobs != 0 || reservations != 0 || cost != 0) {
		return LiveReport{}, errors.New("live activation materialization crossed a Provider boundary")
	}
	if err := tx.Commit(ctx); err != nil {
		return LiveReport{}, err
	}
	return LiveReport{
		SchemaVersion: "flo100.batch-a-live-materialization-report.v1", BatchID: formalBatchIDs[0],
		SourceCodeCommit:       prepared.FormalSourceCodeCommit(),
		SourceAuthorizedCommit: prepared.authorization.FixedEvidence.MergeCommit,
		OfflinePackageHash:     formalManifestHash, OfflineExecutionPackageHash: formalBatchAExecutionHash,
		SourceAuthorizationHash:  prepared.authorizationHash,
		LiveExecutionPackageHash: package_.ContentHash, LiveProjectionHash: projectionHash,
		SubmitAuthorized: !authorizedUntil.IsZero(), SubmitAuthorizationValidUntil: authorizedUntil,
		PrimaryJobs: 10, ControlledRetriesMaximum: 1, MaximumVideoTokens: stage1.MaximumVideoTokens,
		VideoAFPMilli: stage1.FLO100BatchAVideoAFPMilli, SpeechAFPMilli: stage1.FLO100BatchASpeechAFPMilli,
		NonSubscriptionCashMicros: 0, ProviderJobs: providerJobs,
		BudgetReservations: reservations, CostLedgerEntries: cost,
	}, nil
}

func exactAFPMilli(value float64) (int64, bool) {
	if value < 0 || value > float64(math.MaxInt64)/1000 {
		return 0, false
	}
	scaled := value * 1000
	rounded := math.Round(scaled)
	return int64(rounded), math.Abs(scaled-rounded) < 0.000001
}

func validGitCommit(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func liveUUID(prepared preparedLive, name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("flo100-live-v1:"+prepared.FormalSourceCodeCommit()+":"+name))
}
