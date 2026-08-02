package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	flo100LiveBatchID                = "flo100-gold-a-v1"
	flo100OfflinePackageHash         = "68f0a07e2ea2cd2740da07daca3bb2ce2d1a7572ed9a8756cd73101db7fbd835"
	flo100OfflineExecutionHash       = "56d21bcec47934b11290f9760c0650e5c37e244ee756b4c76056240fdeebf260"
	flo100AgentPlanProfile           = "agent-plan_cn-beijing_personal"
	flo100AgentPlanRegion            = "cn-beijing"
	flo100LiveModel                  = "doubao-seedance-2.0"
	flo100LiveCapabilityHash         = "0d1c97d70c7b332940279be334c127fa068069f83d58840fa57b4d3b10166eca"
	flo100QuotaSchema                = "ark.agent-plan-quota.v1"
	flo100MonthlyCapAFPMilli   int64 = 135_000_000
	flo100VideoAFPMilli        int64 = 30_306_870
	flo100SpeechAFPMilli       int64 = 1_039
	flo100TotalAFPMilli              = flo100VideoAFPMilli + flo100SpeechAFPMilli
)

type stage1LiveAuthority struct {
	ActivationID              uuid.UUID
	ControlSeriesID           uuid.UUID
	SourceSeriesID            uuid.UUID
	SourceEpisodeID           uuid.UUID
	SourceEpisodeRevisionID   uuid.UUID
	LiveGenerationPlanID      uuid.UUID
	VideoProviderProfileID    uuid.UUID
	VideoCapabilitySnapshotID uuid.UUID
	VideoBudgetApprovalID     uuid.UUID
	G1DecisionID              uuid.UUID
	G2DecisionID              uuid.UUID
	SafetyDecisionID          uuid.UUID
	OfflineExecutionHash      string
	SourceAuthorizationHash   string
	SourceAuthorization       json.RawMessage
	SourceCodeCommit          string
	ExecutionPackageHash      string
	AuthorizationValidUntil   time.Time
	ProjectionHash            string
	Run                       stage1LiveRun
}

type stage1LiveRun struct {
	Ordinal              int
	RunID                uuid.UUID
	OfflineRunID         uuid.UUID
	ShotSpecRevisionID   uuid.UUID
	PromptSnapshotID     uuid.UUID
	PromptSnapshotHash   string
	AuthorizedPromptHash string
	IntentInputHash      string
	EstimatedVideoTokens int64
	PredictedAFPMilli    int64
}

type flo100LiveFinalizationAuthority struct {
	ActivationID           uuid.UUID
	ControlSeriesID        uuid.UUID
	SourceSeriesID         uuid.UUID
	SpeechBudgetApprovalID uuid.UUID
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readFLO100LiveFinalizationAuthority(
	ctx context.Context,
	query queryRower,
	episodeRevisionID uuid.UUID,
	planIDRaw string,
	runIDs []uuid.UUID,
	now time.Time,
) (*flo100LiveFinalizationAuthority, error) {
	planID, err := uuid.Parse(planIDRaw)
	if err != nil {
		return nil, nil
	}
	var authority flo100LiveFinalizationAuthority
	var runSetExact, submitExact, decisionsExact, g1Exact, g2Exact, safetyExact bool
	err = query.QueryRow(ctx, `
		SELECT a.id,a.control_series_id,a.source_series_id,a.speech_budget_approval_id,
		  ((SELECT COUNT(*) FROM video_pipeline.stage1_live_activation_runs ar
		    WHERE ar.activation_id=a.id)=10
		   AND (SELECT COUNT(*) FROM video_pipeline.stage1_live_activation_runs ar
		        WHERE ar.activation_id=a.id AND ar.run_id=ANY($3::uuid[]))=10),
		  EXISTS (SELECT 1 FROM video_pipeline.stage1_live_submit_authorizations sa
		          JOIN video_pipeline.stage1_live_projection_seals ps ON ps.activation_id=a.id
		          WHERE sa.activation_id=a.id AND sa.source_code_commit=a.source_code_commit
		            AND sa.execution_package_hash=a.live_execution_package_hash
		            AND sa.projection_hash=ps.projection_hash AND sa.valid_until>$4
			            AND sa.authorization_payload->'fixedEvidence'->>'mergeCommit'=a.source_code_commit
			            AND (sa.authorization_payload->'decision'->>'batchAProviderPostAuthorizedConditionally')::boolean=true
			            AND (sa.authorization_payload->'decision'->>'batchBProviderPostAuthorized')::boolean=false
			            AND (sa.authorization_payload->'decision'->>'batchCProviderPostAuthorized')::boolean=false
			            AND (sa.authorization_payload->'decision'->>'stage4Authorized')::boolean=false),
			  ((SELECT COUNT(*) FROM video_pipeline.approval_decisions d
			    WHERE d.id=ANY(ARRAY[a.g1_decision_id,a.g2_decision_id,a.safety_decision_id])
			      AND d.series_id=a.control_series_id AND d.episode_id IS NULL
			      AND d.decision='APPROVED' AND d.actor_role='ADMIN')=3),
			  ((SELECT COUNT(*) FROM video_pipeline.approval_bindings b
		    WHERE b.decision_id=a.g1_decision_id)=8
		   AND (SELECT COUNT(*)
		        FROM jsonb_array_elements(a.source_authorization->'g1Approval'->'assets') j
		        JOIN video_pipeline.approval_bindings b
		          ON b.decision_id=a.g1_decision_id AND b.object_type='ASSET_VERSION'
		         AND b.revision_id=(j->>'assetVersionId')::uuid
		        JOIN video_pipeline.asset_versions av ON av.id=b.revision_id
			        WHERE b.content_hash=av.content_hash AND av.content_hash=j->>'artifactSha256'
			          AND av.execution_refs->>'metadataContentHash'=j->>'contentHash')=8),
			  ((SELECT COUNT(*) FROM video_pipeline.approval_bindings b
			    WHERE b.decision_id=a.g2_decision_id)=24
			   AND (SELECT COUNT(*)
			        FROM video_pipeline.stage1_live_activation_runs ar
			        JOIN LATERAL (
			          SELECT value
			          FROM jsonb_array_elements(a.source_authorization->'g2Approval'->'shotBindings')
			            WITH ORDINALITY AS x(value,ordinality)
			          WHERE x.ordinality=ar.ordinal
			        ) j ON true
			        WHERE ar.activation_id=a.id
			          AND ar.shot_spec_revision_id=(j.value->>'shotSpecRevisionId')::uuid
			          AND ar.intent_input_hash=j.value->>'inputHash'
			          AND ar.authorized_prompt_hash=j.value->>'promptHash'
			          AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
			                      JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=b.revision_id
			                      WHERE b.decision_id=a.g2_decision_id
			                        AND b.object_type='SHOT_SPEC_REVISION'
			                        AND b.revision_id=ar.shot_spec_revision_id
			                        AND b.content_hash=ssr.content_hash)
			          AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
			                      JOIN video_pipeline.prompt_snapshots ps ON ps.id=b.revision_id
			                      WHERE b.decision_id=a.g2_decision_id
			                        AND b.object_type='PROMPT_SNAPSHOT'
			                        AND b.revision_id=ar.prompt_snapshot_id
			                        AND b.content_hash=ps.content_hash))=10),
			  ((SELECT COUNT(*) FROM video_pipeline.approval_bindings b
			    WHERE b.decision_id=a.safety_decision_id)=12
			   AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
			               WHERE b.decision_id=a.safety_decision_id AND b.object_type='ARTIFACT'
			                 AND b.content_hash=a.source_authorization->'g1Approval'->>'safetyEvidenceHash'))
		FROM video_pipeline.stage1_live_activations a
		WHERE a.live_generation_plan_id=$1 AND a.source_episode_revision_id=$2
		  AND a.batch_id='flo100-gold-a-v1'`, planID, episodeRevisionID, runIDs, now).Scan(
		&authority.ActivationID, &authority.ControlSeriesID, &authority.SourceSeriesID,
		&authority.SpeechBudgetApprovalID, &runSetExact, &submitExact, &decisionsExact,
		&g1Exact, &g2Exact, &safetyExact,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read FLO-100 live finalization authority: %w", err)
	}
	if len(runIDs) != 10 || !runSetExact || !submitExact || !decisionsExact ||
		!g1Exact || !g2Exact || !safetyExact {
		return nil, controlplane.NewPolicyError(
			controlplane.CodeForbidden,
			"FLO-100 finalization differs from its exact A-only live authority",
			"use the exact ten authorized runs and current submit authorization",
		)
	}
	return &authority, nil
}

type stage1LiveAuthorizationEnvelope struct {
	Decision struct {
		A      bool `json:"batchAProviderPostAuthorizedConditionally"`
		B      bool `json:"batchBProviderPostAuthorized"`
		C      bool `json:"batchCProviderPostAuthorized"`
		Stage4 bool `json:"stage4Authorized"`
	} `json:"decision"`
	G1Approval struct {
		Decision            string `json:"decision"`
		LicenseSnapshotHash string `json:"licenseSnapshotHash"`
		SafetyEvidenceHash  string `json:"safetyEvidenceHash"`
		Assets              []any  `json:"assets"`
	} `json:"g1Approval"`
	G2Approval struct {
		Decision     string `json:"decision"`
		BatchID      string `json:"batchId"`
		ShotIDs      []any  `json:"shotIds"`
		ShotBindings []any  `json:"shotBindings"`
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
		PrimaryJobs      int   `json:"videoPrimaryJobs"`
		RetriesMaximum   int   `json:"videoControlledRetriesMaximum"`
		MaximumJobs      int   `json:"videoMaximumJobs"`
		MaximumTokens    int64 `json:"videoMaximumTokens"`
		SpeechCharacters int64 `json:"speechCharactersMaximum"`
		AutomaticRetry   bool  `json:"automaticRetryAllowed"`
		AutomaticSwitch  bool  `json:"automaticProviderSwitchAllowed"`
	} `json:"batchABudget"`
}

func readStage1LiveAuthority(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
) (*stage1LiveAuthority, error) {
	var authority stage1LiveAuthority
	var submitValidUntil *time.Time
	err := tx.QueryRow(ctx, `
		SELECT a.id,a.control_series_id,a.source_series_id,a.source_episode_id,
		       a.source_episode_revision_id,a.live_generation_plan_id,
		       a.video_provider_profile_id,a.video_capability_snapshot_id,
		       a.video_budget_approval_id,a.g1_decision_id,a.g2_decision_id,a.safety_decision_id,
		       a.offline_execution_package_hash,a.source_authorization_hash,a.source_authorization,
		       a.source_code_commit,a.live_execution_package_hash,
		       sa.valid_until,COALESCE(ps.projection_hash,''),
		       ar.ordinal,ar.run_id,ar.offline_run_id,ar.shot_spec_revision_id,
		       ar.prompt_snapshot_id,ar.prompt_snapshot_hash,ar.authorized_prompt_hash,ar.intent_input_hash,
		       ar.estimated_video_tokens,ar.predicted_afp_milli
		FROM video_pipeline.stage1_live_activation_runs ar
		JOIN video_pipeline.stage1_live_activations a ON a.id=ar.activation_id
		LEFT JOIN video_pipeline.stage1_live_submit_authorizations sa ON sa.activation_id=a.id
		LEFT JOIN video_pipeline.stage1_live_projection_seals ps ON ps.activation_id=a.id
		WHERE ar.run_id=$1
		FOR SHARE OF a,ar`, runID).Scan(
		&authority.ActivationID, &authority.ControlSeriesID, &authority.SourceSeriesID,
		&authority.SourceEpisodeID, &authority.SourceEpisodeRevisionID,
		&authority.LiveGenerationPlanID, &authority.VideoProviderProfileID,
		&authority.VideoCapabilitySnapshotID, &authority.VideoBudgetApprovalID,
		&authority.G1DecisionID, &authority.G2DecisionID, &authority.SafetyDecisionID,
		&authority.OfflineExecutionHash, &authority.SourceAuthorizationHash,
		&authority.SourceAuthorization, &authority.SourceCodeCommit,
		&authority.ExecutionPackageHash, &submitValidUntil,
		&authority.ProjectionHash, &authority.Run.Ordinal, &authority.Run.RunID,
		&authority.Run.OfflineRunID, &authority.Run.ShotSpecRevisionID,
		&authority.Run.PromptSnapshotID, &authority.Run.PromptSnapshotHash,
		&authority.Run.AuthorizedPromptHash, &authority.Run.IntentInputHash,
		&authority.Run.EstimatedVideoTokens,
		&authority.Run.PredictedAFPMilli,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read FLO-100 live authority: %w", err)
	}
	if submitValidUntil != nil {
		authority.AuthorizationValidUntil = *submitValidUntil
	}
	return &authority, nil
}

func requireStage1LiveAuthority(
	ctx context.Context,
	tx pgx.Tx,
	authority *stage1LiveAuthority,
	input orchestration.ExecuteProviderJobInput,
	plan controlplane.GenerationPlanRecord,
	seriesID, episodeID, episodeRevisionID, shotID, promptID, profileID uuid.UUID,
	promptHash string,
	now time.Time,
) error {
	if authority == nil {
		return controlplane.NewPolicyError(
			controlplane.CodeForbidden,
			"subscription dispatch has no FLO-100 A-only live activation",
			"materialize and authorize an exact live execution package",
		)
	}
	if authority.SourceSeriesID != seriesID || authority.SourceEpisodeID != episodeID ||
		authority.SourceEpisodeRevisionID != episodeRevisionID ||
		authority.LiveGenerationPlanID.String() != plan.Plan.GenerationPlanID ||
		authority.ControlSeriesID.String() != plan.SeriesID ||
		authority.VideoProviderProfileID.String() != input.ProviderProfileID ||
		authority.VideoBudgetApprovalID.String() != input.BudgetApprovalID ||
		authority.Run.RunID.String() != input.Run.RunID || authority.Run.ShotSpecRevisionID != shotID ||
		authority.Run.PromptSnapshotID != promptID || authority.Run.PromptSnapshotHash != promptHash ||
		authority.Run.EstimatedVideoTokens != input.EstimatedVideoTokens ||
		authority.Run.PredictedAFPMilli != input.PredictedAFPMilli ||
		authority.OfflineExecutionHash != flo100OfflineExecutionHash ||
		input.ExpectedLiveActivationID != authority.ActivationID.String() ||
		input.ExpectedExecutionPackageHash != authority.ExecutionPackageHash ||
		input.ExpectedSourceCodeCommit != authority.SourceCodeCommit ||
		input.BillingMode != providercontract.BillingModeSubscriptionIncludedOnly ||
		!authority.AuthorizationValidUntil.After(now) || authority.ProjectionHash == "" {
		return controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"FLO-100 live dispatch differs from its exact A-only authority",
		)
	}
	var authorization stage1LiveAuthorizationEnvelope
	if err := json.Unmarshal(authority.SourceAuthorization, &authorization); err != nil {
		return controlplane.NewConflictError(controlplane.CodeRevisionConflict, "stored FLO-100 live authorization is invalid")
	}
	if !authorization.Decision.A || authorization.Decision.B || authorization.Decision.C ||
		authorization.Decision.Stage4 || authorization.G1Approval.Decision != "APPROVED_FOR_EXACT_HASHES_INTERNAL_POC_CN" ||
		authorization.G2Approval.Decision != "APPROVED_FOR_BATCH_A_ONLY_WHEN_PRE_SUBMIT_GATES_PASS" ||
		authorization.G2Approval.BatchID != flo100LiveBatchID || len(authorization.G1Approval.Assets) != 8 ||
		len(authorization.G2Approval.ShotIDs) != 10 || len(authorization.G2Approval.ShotBindings) != 10 ||
		authorization.ProviderRoute.Provider != "volcengine_ark" ||
		authorization.ProviderRoute.Profile != flo100AgentPlanProfile ||
		authorization.ProviderRoute.ModelID != flo100LiveModel ||
		authorization.ProviderRoute.Region != flo100AgentPlanRegion ||
		authorization.ProviderRoute.BillingMode != providercontract.BillingModeSubscriptionIncludedOnly ||
		authorization.ProviderRoute.CapabilitySnapshotHash != flo100LiveCapabilityHash ||
		authorization.BatchABudget.PrimaryJobs != 10 || authorization.BatchABudget.RetriesMaximum != 1 ||
		authorization.BatchABudget.MaximumJobs != 11 || authorization.BatchABudget.MaximumTokens != 1_200_000 ||
		authorization.BatchABudget.SpeechCharacters != 7 || authorization.BatchABudget.AutomaticRetry ||
		authorization.BatchABudget.AutomaticSwitch {
		return controlplane.NewPolicyError(
			controlplane.CodeForbidden,
			"stored authority is not the exact FLO-100 Batch A subscription envelope",
			"obtain a new exact A-only authorization",
		)
	}

	var submitExact, activationExact, decisionsExact, g1Exact, g2Exact, safetyExact bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM video_pipeline.stage1_live_submit_authorizations sa
		  WHERE sa.activation_id=$1 AND sa.source_code_commit=$2
		    AND sa.execution_package_hash=$3 AND sa.projection_hash=$4 AND sa.valid_until>$5
		    AND sa.authorization_payload->'fixedEvidence'->>'mergeCommit'=$2
		    AND (sa.authorization_payload->'decision'->>'batchAProviderPostAuthorizedConditionally')::boolean=true
		    AND (sa.authorization_payload->'decision'->>'batchBProviderPostAuthorized')::boolean=false
		    AND (sa.authorization_payload->'decision'->>'batchCProviderPostAuthorized')::boolean=false
		    AND (sa.authorization_payload->'decision'->>'stage4Authorized')::boolean=false
		), EXISTS (
		  SELECT 1 FROM video_pipeline.stage1_live_activations a
		  WHERE a.id=$1 AND a.batch_id='flo100-gold-a-v1'
		    AND a.offline_package_hash=$6 AND a.offline_execution_package_hash=$7
		    AND a.live_execution_package_hash=$3 AND a.source_code_commit=$2
		), (
		  SELECT COUNT(*)=3 FROM video_pipeline.approval_decisions d
		  WHERE d.id=ANY($8::uuid[]) AND d.series_id=$9 AND d.episode_id IS NULL
		    AND d.decision='APPROVED' AND d.actor_role='ADMIN'
		), (
		  SELECT COUNT(*)=8
		  FROM jsonb_array_elements((SELECT source_authorization->'g1Approval'->'assets'
		                             FROM video_pipeline.stage1_live_activations WHERE id=$1)) j
		  JOIN video_pipeline.approval_bindings b
		    ON b.decision_id=$10 AND b.object_type='ASSET_VERSION'
		   AND b.revision_id=(j->>'assetVersionId')::uuid
		  JOIN video_pipeline.asset_versions av ON av.id=b.revision_id
		  WHERE b.content_hash=av.content_hash AND av.content_hash=j->>'artifactSha256'
		    AND av.execution_refs->>'metadataContentHash'=j->>'contentHash'
		), (
		  SELECT COUNT(*)=10
		  FROM video_pipeline.stage1_live_activation_runs ar
		  JOIN LATERAL (
		    SELECT value FROM jsonb_array_elements(
		      (SELECT source_authorization->'g2Approval'->'shotBindings'
		       FROM video_pipeline.stage1_live_activations WHERE id=$1)
		    ) WITH ORDINALITY AS x(value,ordinality) WHERE x.ordinality=ar.ordinal
		  ) j ON true
		  WHERE ar.activation_id=$1 AND ar.shot_spec_revision_id=(j.value->>'shotSpecRevisionId')::uuid
		    AND ar.intent_input_hash=j.value->>'inputHash' AND ar.authorized_prompt_hash=j.value->>'promptHash'
		    AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
		                JOIN video_pipeline.shot_spec_revisions ssr ON ssr.id=b.revision_id
		                WHERE b.decision_id=$11 AND b.object_type='SHOT_SPEC_REVISION'
		                  AND b.revision_id=ar.shot_spec_revision_id AND b.content_hash=ssr.content_hash)
		    AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
		                JOIN video_pipeline.prompt_snapshots ps ON ps.id=b.revision_id
		                WHERE b.decision_id=$11 AND b.object_type='PROMPT_SNAPSHOT'
		                  AND b.revision_id=ar.prompt_snapshot_id AND b.content_hash=ps.content_hash)
		), (
		  SELECT (SELECT COUNT(*) FROM video_pipeline.approval_bindings WHERE decision_id=$10)=8
		     AND (SELECT COUNT(*) FROM video_pipeline.approval_bindings WHERE decision_id=$11)=24
		     AND (SELECT COUNT(*) FROM video_pipeline.approval_bindings WHERE decision_id=$12)=12
		     AND EXISTS (SELECT 1 FROM video_pipeline.approval_bindings b
		                 WHERE b.decision_id=$12 AND b.object_type='ARTIFACT'
		                   AND b.content_hash=(SELECT source_authorization->'g1Approval'->>'safetyEvidenceHash'
		                                       FROM video_pipeline.stage1_live_activations WHERE id=$1))
		)`, authority.ActivationID, authority.SourceCodeCommit, authority.ExecutionPackageHash,
		authority.ProjectionHash, now, flo100OfflinePackageHash, flo100OfflineExecutionHash,
		[]uuid.UUID{authority.G1DecisionID, authority.G2DecisionID, authority.SafetyDecisionID},
		authority.ControlSeriesID, authority.G1DecisionID, authority.G2DecisionID,
		authority.SafetyDecisionID).Scan(
		&submitExact, &activationExact, &decisionsExact, &g1Exact, &g2Exact, &safetyExact,
	); err != nil {
		return fmt.Errorf("verify FLO-100 live authority projection: %w", err)
	}
	if !submitExact || !activationExact || !decisionsExact || !g1Exact || !g2Exact || !safetyExact {
		return controlplane.NewPolicyError(
			controlplane.CodeForbidden,
			fmt.Sprintf("FLO-100 live authority evidence drifted (submit=%t activation=%t decisions=%t g1=%t g2=%t safety=%t)",
				submitExact, activationExact, decisionsExact, g1Exact, g2Exact, safetyExact),
			"rematerialize and independently authorize the exact live projection",
		)
	}
	var routeExact bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM video_pipeline.provider_capability_snapshots pcs
		JOIN video_pipeline.provider_profiles pp ON pp.id=pcs.provider_profile_id
		WHERE pcs.id=$1 AND pp.id=$2 AND pp.enabled AND pp.mode='LIVE' AND pp.health='READY'
		  AND pcs.status='ACTIVE' AND pcs.expires_at>$3 AND pcs.capability_alias='video.primary'
		  AND pcs.model_id=$4 AND pcs.capability_hash=$5
		  AND pcs.limits->>'billingMode'=$6
		  AND (pcs.limits->>'remainingCalls')::bigint=11
		  AND (pcs.limits->>'monthlyAccountCapAfpMilli')::bigint=$7
		  AND (pcs.limits->>'maximumVideoAfpMilli')::bigint=$8
		  AND (pcs.limits->>'maximumSpeechAfpMilli')::bigint=$9
		  AND (pcs.limits->>'maximumNonSubscriptionCashMicros')::bigint=0
		  AND (pcs.limits->>'automaticRetry')::boolean=false
		  AND (pcs.limits->>'automaticProviderSwitch')::boolean=false
	)`, authority.VideoCapabilitySnapshotID, authority.VideoProviderProfileID, now,
		flo100LiveModel, flo100LiveCapabilityHash,
		providercontract.BillingModeSubscriptionIncludedOnly, flo100MonthlyCapAFPMilli,
		flo100VideoAFPMilli, flo100SpeechAFPMilli).Scan(&routeExact); err != nil {
		return fmt.Errorf("verify FLO-100 live route: %w", err)
	}
	if !routeExact || profileID == uuid.Nil {
		return controlplane.NewPolicyError(
			controlplane.CodeCapability,
			"FLO-100 live provider profile or immutable capability drifted",
			"obtain a new exact route authorization",
		)
	}
	return nil
}

func reserveStage1LiveAFP(
	ctx context.Context,
	tx pgx.Tx,
	authority *stage1LiveAuthority,
	input orchestration.ExecuteProviderJobInput,
	now time.Time,
) error {
	if authority == nil {
		return nil
	}
	snapshot := input.SubscriptionQuotaSnapshot
	if snapshot == nil {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"a fresh Agent Plan quota snapshot is required before live submission",
			"read authenticated 5-hour, weekly, and monthly quota and retry",
		)
	}
	if snapshot.SchemaVersion != flo100QuotaSchema || strings.TrimSpace(snapshot.Source) == "" ||
		strings.TrimSpace(snapshot.AccountID) == "" || snapshot.Profile != flo100AgentPlanProfile ||
		snapshot.Region != flo100AgentPlanRegion ||
		snapshot.BillingMode != providercontract.BillingModeSubscriptionIncludedOnly ||
		snapshot.CapturedAt.IsZero() || snapshot.CapturedAt.After(now.Add(30*time.Second)) ||
		now.Sub(snapshot.CapturedAt) > 300*time.Second ||
		snapshot.FiveHourUsedAFPMilli < 0 || snapshot.FiveHourTotalAFPMilli <= 0 ||
		snapshot.WeeklyUsedAFPMilli < 0 || snapshot.WeeklyTotalAFPMilli <= 0 ||
		snapshot.MonthlyUsedAFPMilli < 0 || snapshot.MonthlyTotalAFPMilli <= 0 ||
		snapshot.ExternalReservedAFPMilli < 0 ||
		snapshot.FiveHourUsedAFPMilli > snapshot.FiveHourTotalAFPMilli ||
		snapshot.WeeklyUsedAFPMilli > snapshot.WeeklyTotalAFPMilli ||
		snapshot.MonthlyUsedAFPMilli > snapshot.MonthlyTotalAFPMilli {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"Agent Plan quota snapshot is stale, incomplete, or belongs to another profile",
			"read a complete authenticated snapshot no older than 300 seconds",
		)
	}
	if input.PredictedAFPMilli <= 0 ||
		snapshot.FiveHourUsedAFPMilli > snapshot.FiveHourTotalAFPMilli-input.PredictedAFPMilli ||
		snapshot.WeeklyUsedAFPMilli > snapshot.WeeklyTotalAFPMilli-flo100TotalAFPMilli {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"Agent Plan 5-hour or weekly quota cannot cover the authorized Batch A reservation",
			"wait for quota recovery or obtain a new authorization",
		)
	}
	// Serialize every account-level Agent Plan reservation in this database.
	// The fixed advisory key is deliberately process-independent and avoids a
	// check-then-insert race between separate activation transactions.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7100165)`); err != nil {
		return fmt.Errorf("lock Agent Plan AFP ledger: %w", err)
	}
	var otherReserved int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(total_afp_milli),0)
		FROM video_pipeline.stage1_agent_plan_afp_reservations
		WHERE account_id=$1 AND profile=$2 AND region=$3 AND status='RESERVED'
		  AND activation_id<>$4`, snapshot.AccountID, snapshot.Profile,
		snapshot.Region, authority.ActivationID).Scan(&otherReserved); err != nil {
		return fmt.Errorf("read Agent Plan AFP reservations: %w", err)
	}
	accountLimit := flo100MonthlyCapAFPMilli
	if snapshot.MonthlyTotalAFPMilli < accountLimit {
		accountLimit = snapshot.MonthlyTotalAFPMilli
	}
	remaining := accountLimit
	monthlyFits := true
	for _, allocation := range []int64{
		snapshot.MonthlyUsedAFPMilli, otherReserved,
		snapshot.ExternalReservedAFPMilli, flo100TotalAFPMilli,
	} {
		if allocation < 0 || allocation > remaining {
			monthlyFits = false
			break
		}
		remaining -= allocation
	}
	if !monthlyFits {
		return controlplane.NewPolicyError(
			controlplane.CodeBudgetExceeded,
			"monthly Agent Plan usage plus all reservations would exceed 135000 AFP",
			"release reservations, wait for renewal, or obtain a smaller authorized batch",
		)
	}
	snapshotHash, err := digestValue(snapshot)
	if err != nil {
		return err
	}
	snapshotID := uuid.NewSHA1(authority.ActivationID, []byte("quota:"+input.Run.RunID+":"+snapshotHash))
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_agent_plan_quota_snapshots
		(id,activation_id,run_id,snapshot_hash,source,captured_at,recorded_at,account_id,profile,region,
		 billing_mode,five_hour_used_afp_milli,five_hour_total_afp_milli,weekly_used_afp_milli,
		 weekly_total_afp_milli,monthly_used_afp_milli,monthly_total_afp_milli,external_reserved_afp_milli)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (id) DO NOTHING`, snapshotID, authority.ActivationID, authority.Run.RunID,
		snapshotHash, snapshot.Source, snapshot.CapturedAt, now, snapshot.AccountID,
		snapshot.Profile, snapshot.Region, snapshot.BillingMode,
		snapshot.FiveHourUsedAFPMilli, snapshot.FiveHourTotalAFPMilli,
		snapshot.WeeklyUsedAFPMilli, snapshot.WeeklyTotalAFPMilli,
		snapshot.MonthlyUsedAFPMilli, snapshot.MonthlyTotalAFPMilli,
		snapshot.ExternalReservedAFPMilli); err != nil {
		return fmt.Errorf("persist Agent Plan quota snapshot: %w", err)
	}
	reservationID := uuid.NewSHA1(authority.ActivationID, []byte("agent-plan-afp-reservation"))
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_agent_plan_afp_reservations
		(id,activation_id,quota_snapshot_id,account_id,profile,region,video_afp_milli,speech_afp_milli,
		 total_afp_milli,account_cap_afp_milli,monthly_used_afp_milli,external_reserved_afp_milli,status,reserved_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'RESERVED',$13)
		ON CONFLICT (activation_id) DO NOTHING`, reservationID, authority.ActivationID, snapshotID,
		snapshot.AccountID, snapshot.Profile, snapshot.Region, flo100VideoAFPMilli,
		flo100SpeechAFPMilli, flo100TotalAFPMilli, flo100MonthlyCapAFPMilli,
		snapshot.MonthlyUsedAFPMilli, snapshot.ExternalReservedAFPMilli, now); err != nil {
		return fmt.Errorf("reserve Agent Plan AFP: %w", err)
	}
	var storedID uuid.UUID
	var storedAccount, storedProfile, storedRegion, status string
	var video, speech, total, cap int64
	if err := tx.QueryRow(ctx, `SELECT id,account_id,profile,region,video_afp_milli,speech_afp_milli,
		total_afp_milli,account_cap_afp_milli,status
		FROM video_pipeline.stage1_agent_plan_afp_reservations WHERE activation_id=$1 FOR SHARE`,
		authority.ActivationID).Scan(&storedID, &storedAccount, &storedProfile, &storedRegion,
		&video, &speech, &total, &cap, &status); err != nil {
		return fmt.Errorf("verify Agent Plan AFP reservation: %w", err)
	}
	if storedID != reservationID || storedAccount != snapshot.AccountID ||
		storedProfile != snapshot.Profile || storedRegion != snapshot.Region ||
		video != flo100VideoAFPMilli || speech != flo100SpeechAFPMilli ||
		total != flo100TotalAFPMilli || cap != flo100MonthlyCapAFPMilli || status != "RESERVED" {
		return controlplane.NewConflictError(
			controlplane.CodeRevisionConflict,
			"existing Agent Plan AFP reservation differs from the immutable Batch A allocation",
		)
	}
	return nil
}

func canonicalProviderInput(input orchestration.ExecuteProviderJobInput) orchestration.ExecuteProviderJobInput {
	input.SubscriptionQuotaSnapshot = nil
	return input
}

// loadFLO100LivePromptAssetEvidence is the narrow exception for immutable
// offline AssetVersions promoted by the separate exact-hash G1 authority. It
// never changes the source asset/license rows and accepts only assets present
// in both the stored external authorization and the live G1 bindings.
func loadFLO100LivePromptAssetEvidence(
	ctx context.Context,
	tx pgx.Tx,
	promptID uuid.UUID,
	assetVersionIDs []uuid.UUID,
) ([]providercontract.AssetRef, map[string]string, error) {
	if len(assetVersionIDs) == 0 {
		return nil, map[string]string{}, nil
	}
	var activationID, g1DecisionID uuid.UUID
	var exactCount int
	if err := tx.QueryRow(ctx, `
		SELECT a.id,a.g1_decision_id,(
		  SELECT COUNT(*)
		  FROM unnest($2::uuid[]) requested(id)
		  JOIN jsonb_array_elements(a.source_authorization->'g1Approval'->'assets') j
		    ON (j->>'assetVersionId')::uuid=requested.id
		  JOIN video_pipeline.approval_bindings b
		    ON b.decision_id=a.g1_decision_id AND b.object_type='ASSET_VERSION'
		   AND b.revision_id=requested.id
		  JOIN video_pipeline.asset_versions av ON av.id=requested.id
		  WHERE b.content_hash=av.content_hash AND av.content_hash=j->>'artifactSha256'
		    AND av.execution_refs->>'metadataContentHash'=j->>'contentHash'
		)
		FROM video_pipeline.stage1_live_activations a
		JOIN video_pipeline.stage1_live_activation_runs ar ON ar.activation_id=a.id
		JOIN video_pipeline.approval_decisions d ON d.id=a.g1_decision_id
		WHERE ar.prompt_snapshot_id=$1 AND d.gate='G1' AND d.decision='APPROVED'
		FOR SHARE OF a,ar,d`, promptID, assetVersionIDs).
		Scan(&activationID, &g1DecisionID, &exactCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"prompt assets are unavailable and have no exact FLO-100 live G1 authority",
				"obtain exact-hash G1 approval before live submission",
			)
		}
		return nil, nil, fmt.Errorf("read FLO-100 live prompt authority: %w", err)
	}
	if activationID == uuid.Nil || g1DecisionID == uuid.Nil || exactCount != len(assetVersionIDs) {
		return nil, nil, controlplane.NewPolicyError(
			controlplane.CodeLicenseBlocked,
			"FLO-100 live G1 does not bind every exact prompt asset",
			"obtain exact-hash G1 approval for the complete prompt asset set",
		)
	}
	rows, err := tx.Query(ctx, `
		SELECT av.id,av.asset_id,av.content_hash,av.artifact_uri,av.media_type,
		       art.size_bytes,av.dimensions,ls.license_id,ls.license_hash
		FROM video_pipeline.asset_versions av
		JOIN video_pipeline.license_snapshots ls ON ls.id=av.license_snapshot_id
		JOIN video_pipeline.artifacts art ON art.content_hash=av.content_hash
		 AND art.artifact_uri=av.artifact_uri AND art.media_type=av.media_type
		WHERE av.id=ANY($1::uuid[]) AND art.status='ACTIVE' AND art.size_bytes>0
		FOR SHARE OF av,ls,art`, assetVersionIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("read exact FLO-100 live prompt assets: %w", err)
	}
	defer rows.Close()
	type evidence struct {
		ref  providercontract.AssetRef
		hash string
	}
	byID := make(map[uuid.UUID]evidence, len(assetVersionIDs))
	for rows.Next() {
		var versionID, assetID uuid.UUID
		var contentHash, uri, mediaType, licenseID, licenseHash string
		var sizeBytes int64
		var dimensions []byte
		if err := rows.Scan(&versionID, &assetID, &contentHash, &uri, &mediaType,
			&sizeBytes, &dimensions, &licenseID, &licenseHash); err != nil {
			return nil, nil, fmt.Errorf("scan exact FLO-100 live prompt asset: %w", err)
		}
		modality, role, err := promptAssetType(mediaType)
		if err != nil {
			return nil, nil, err
		}
		var media struct {
			Width          int   `json:"width"`
			Height         int   `json:"height"`
			DurationMillis int64 `json:"durationMillis"`
		}
		if len(dimensions) != 0 {
			if err := json.Unmarshal(dimensions, &media); err != nil {
				return nil, nil, fmt.Errorf("decode exact FLO-100 live asset dimensions: %w", err)
			}
		}
		byID[versionID] = evidence{hash: contentHash, ref: providercontract.AssetRef{
			ID: assetID.String(), Revision: versionID.String(), Kind: modality, Role: role,
			URI: uri, SHA256: contentHash, LicenseReference: licenseID + ":" + licenseHash,
			MediaType: mediaType, SizeBytes: sizeBytes, Width: media.Width, Height: media.Height,
			DurationMillis: media.DurationMillis,
		}}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate exact FLO-100 live prompt assets: %w", err)
	}
	refs := make([]providercontract.AssetRef, 0, len(assetVersionIDs))
	hashes := make(map[string]string, len(assetVersionIDs))
	for _, id := range assetVersionIDs {
		item, ok := byID[id]
		if !ok {
			return nil, nil, controlplane.NewPolicyError(
				controlplane.CodeLicenseBlocked,
				"an exact FLO-100 live prompt asset is unavailable from CAS",
				"restore the exact authorized artifact",
			)
		}
		refs = append(refs, item.ref)
		hashes["asset:"+id.String()] = item.hash
	}
	return refs, hashes, nil
}
