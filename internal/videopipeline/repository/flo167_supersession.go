package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/controlplane"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/orchestration"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	flo167A01ActualAFPMilli      int64 = 2_007_900
	flo167RemainingVideoAFPMilli int64 = 28_298_970
	flo167LegacyTerminalHash           = "7ea9cfb63b3c54fa46583cf5abdb0bc67d323eead9ab4f45e9187e9700dcf0e0"
)

type flo167PaidBoundary struct {
	SupersessionID    uuid.UUID
	PackageHash       string
	ProjectionHash    string
	AuthorizationHash string
	Shot              stage1.FLO167ShotBinding
	CompletedSetHash  string
	QuotaSnapshotID   uuid.UUID
}

type flo167AuthorizationEnvelope struct {
	SchemaVersion           string `json:"schemaVersion"`
	SupersessionPackageHash string `json:"supersessionPackageHash"`
	CanonicalProjectionHash string `json:"canonicalProjectionHash"`
	Decision                struct {
		A02A10 bool `json:"a02A10ProviderPostAuthorizedConditionally"`
		B      bool `json:"batchBProviderPostAuthorized"`
		C      bool `json:"batchCProviderPostAuthorized"`
		Stage4 bool `json:"stage4Authorized"`
	} `json:"decision"`
}

// requireFLO167Supersession locks and validates the complete continuation
// projection. It is called inside PrepareProviderJob's SERIALIZABLE transaction
// before any budget reservation, cost ledger, Provider job, or network request.
func requireFLO167Supersession(
	ctx context.Context,
	tx pgx.Tx,
	authority *stage1LiveAuthority,
	input orchestration.ExecuteProviderJobInput,
	now time.Time,
) (*flo167PaidBoundary, error) {
	if authority == nil {
		if input.ExpectedSupersessionPackageHash != "" || input.DurationPricing != nil {
			return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 supersession has no legacy activation")
		}
		return nil, nil
	}
	var result flo167PaidBoundary
	var state, packageHash, projectionHash string
	var legacyAuthorizationHash, legacyExecutionHash, legacyProjectionHash, legacyStopHash string
	var packageJSON, projectionJSON, completedSetJSON, allowedSubmitSetJSON []byte
	var authHash *string
	var authPackageHash, authProjectionHash *string
	var rootAuthorizationHash, authPricingDigest *string
	var authPayload []byte
	var authValidUntil *time.Time
	var pricing stage1.DurationPricingBinding
	var shot stage1.FLO167ShotBinding
	err := tx.QueryRow(ctx, `
		SELECT s.id,s.state,s.execution_package_hash,s.canonical_projection_hash,
		       s.legacy_authorization_hash,s.legacy_execution_package_hash,
		       s.legacy_projection_hash,s.legacy_stop_evidence_hash,s.package,s.canonical_projection,
		       s.completed_set,s.allowed_submit_set,
		       ss.shot_id,ss.duration_ms,ss.pricing_snapshot_id,ss.pricing_snapshot_digest,
		       ss.reference_afp_milli,ss.reference_duration_ms,ss.expected_afp_milli,
		       ss.pricing_rule_version,ss.maximum_drift_basis_points,
		       ss.normalization_version,ss.rounding_version,ss.route_hash,ss.g1_hash,ss.g2_hash,
		       ss.safety_hash,ss.canonical_input_hash,ss.semantic_input_hash,
		       s.authorization_hash,sa.authorization_hash,sa.execution_package_hash,sa.projection_hash,
		       sa.pricing_snapshot_digest,sa.payload,sa.valid_until
		FROM video_pipeline.stage1_live_supersessions s
		JOIN video_pipeline.stage1_live_supersession_shots ss
		  ON ss.supersession_id=s.id AND ss.ordinal=$2
		LEFT JOIN video_pipeline.stage1_live_supersession_authorizations sa ON sa.supersession_id=s.id
		WHERE s.legacy_activation_id=$1
		FOR UPDATE OF s`, authority.ActivationID, authority.Run.Ordinal).Scan(
		&result.SupersessionID, &state, &packageHash, &projectionHash,
		&legacyAuthorizationHash, &legacyExecutionHash, &legacyProjectionHash, &legacyStopHash,
		&packageJSON, &projectionJSON, &completedSetJSON, &allowedSubmitSetJSON, &shot.ShotID, &pricing.DurationMS,
		&pricing.PricingSnapshotID, &pricing.PricingSnapshotDigest, &pricing.ReferenceAFPMilli,
		&pricing.ReferenceDurationMS, &pricing.ExpectedAFPMilli, &pricing.PricingRuleVersion,
		&pricing.MaximumDriftBPS, &pricing.NormalizationVersion, &pricing.RoundingVersion,
		&shot.RouteHash, &shot.G1Hash, &shot.G2Hash, &shot.SafetyHash,
		&shot.CanonicalInputHash, &shot.SemanticInputHash, &rootAuthorizationHash, &authHash, &authPackageHash,
		&authProjectionHash, &authPricingDigest, &authPayload, &authValidUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if input.ExpectedSupersessionPackageHash != "" || input.DurationPricing != nil {
			return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 supersession projection is missing")
		}
		var a01CrossedPaidBoundary bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM video_pipeline.stage1_live_activation_runs ar
			JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id=ar.run_id
			JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id=ga.id
			WHERE ar.activation_id=$1 AND ar.ordinal=1
			  AND (pj.upstream_task_id IS NOT NULL OR pj.state IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN'))
		)`, authority.ActivationID).Scan(&a01CrossedPaidBoundary); err != nil {
			return nil, fmt.Errorf("verify FLO-167 A01 stop boundary: %w", err)
		}
		if a01CrossedPaidBoundary {
			return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "FLO-167 continuation is disabled until v3 supersession is materialized and authorized", "materialize the exact provider-free v3 package")
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lock FLO-167 supersession: %w", err)
	}
	shot.Pricing = pricing
	result.PackageHash, result.ProjectionHash, result.Shot = packageHash, projectionHash, shot
	if legacyAuthorizationHash != authority.SourceAuthorizationHash ||
		legacyExecutionHash != authority.ExecutionPackageHash || legacyProjectionHash != authority.ProjectionHash ||
		legacyStopHash != "dd1954608254791425d7574fe7333d1c1d7cd77cb843572f79d84a3dbeadea76" {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 legacy lineage drifted")
	}
	var package_ stage1.FLO167SupersessionPackage
	var projection stage1.FLO167CanonicalProjection
	if err := json.Unmarshal(packageJSON, &package_); err != nil ||
		json.Unmarshal(projectionJSON, &projection) != nil || stage1.ValidateFLO167Artifacts(package_, projection) != nil ||
		projection.ContentHash != projectionHash || projection.SupersessionPackageHash != packageHash ||
		!reflect.DeepEqual(projection.Shots, package_.Shots) ||
		package_.ContentHash != packageHash || package_.LegacyExecutionPackageHash != authority.ExecutionPackageHash ||
		package_.LegacyProjectionHash != authority.ProjectionHash || package_.LegacyAuthorizationHash != authority.SourceAuthorizationHash ||
		package_.LegacyTerminalLedgerHash != flo167LegacyTerminalHash {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 package is invalid or differs from legacy lineage")
	}
	packageShot, ok := package_.Shot(shot.ShotID)
	if !ok || !reflect.DeepEqual(packageShot, shot) || input.ExpectedSupersessionPackageHash != packageHash {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 shot/package binding drifted")
	}
	wire := orchestration.DurationPricingBinding{
		DurationMS: pricing.DurationMS, PricingSnapshotID: pricing.PricingSnapshotID,
		PricingSnapshotDigest: pricing.PricingSnapshotDigest, ReferenceAFPMilli: pricing.ReferenceAFPMilli,
		ReferenceDurationMS: pricing.ReferenceDurationMS, ExpectedAFPMilli: pricing.ExpectedAFPMilli,
		PricingRuleVersion: pricing.PricingRuleVersion, MaximumDriftBPS: pricing.MaximumDriftBPS,
		NormalizationVersion: pricing.NormalizationVersion, RoundingVersion: pricing.RoundingVersion,
	}
	if input.DurationPricing == nil || *input.DurationPricing != wire || input.PredictedAFPMilli != pricing.ExpectedAFPMilli ||
		input.RouteBindingHash != shot.RouteHash || input.G1BindingHash != shot.G1Hash || input.G2BindingHash != shot.G2Hash ||
		input.SafetyBindingHash != shot.SafetyHash || input.CanonicalInputHash != shot.CanonicalInputHash ||
		input.SemanticInputHash != shot.SemanticInputHash {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 paid-boundary input drifted")
	}
	if input.ExpectedProductTruth == nil {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 submit is missing the complete frozen product-truth envelope")
	}
	if shot.ShotID == "GOLD-A01" {
		return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "GOLD-A01 is terminal and cannot be resubmitted", "continue from GOLD-A02")
	}
	if authHash == nil || authPackageHash == nil || authProjectionHash == nil || authValidUntil == nil ||
		rootAuthorizationHash == nil || authPricingDigest == nil || *rootAuthorizationHash != *authHash ||
		state == "supersession_package_pending_v3" || *authPackageHash != packageHash || *authProjectionHash != projectionHash ||
		*authPricingDigest != pricing.PricingSnapshotDigest ||
		!authValidUntil.After(now) {
		return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "FLO-167 v3 authorization is absent, expired, or drifted", "obtain an exact v3 authorization")
	}
	var envelope flo167AuthorizationEnvelope
	if json.Unmarshal(authPayload, &envelope) != nil || envelope.SchemaVersion != "flo100.batch-a-continuation-authorization.v3" ||
		envelope.SupersessionPackageHash != packageHash || envelope.CanonicalProjectionHash != projectionHash ||
		!envelope.Decision.A02A10 || envelope.Decision.B || envelope.Decision.C || envelope.Decision.Stage4 {
		return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "FLO-167 v3 authorization scope drifted", "obtain an A02-A10-only authorization")
	}
	if state == "v3_authorized_A02_A10" && shot.ShotID != "GOLD-A02" {
		return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "FLO-167 continuation must begin with GOLD-A02", "submit the exact next shot")
	}
	if authority.Run.Ordinal > 2 {
		var priorTerminalCount int
		if err := tx.QueryRow(ctx, `SELECT count(*)
			FROM video_pipeline.stage1_live_supersession_terminal_ledger tl
			JOIN video_pipeline.stage1_live_supersession_shots ss
			  ON ss.supersession_id=tl.supersession_id AND ss.shot_id=tl.shot_id
			WHERE tl.supersession_id=$1 AND ss.ordinal >= 2 AND ss.ordinal < $2`,
			result.SupersessionID, authority.Run.Ordinal).Scan(&priorTerminalCount); err != nil {
			return nil, fmt.Errorf("verify FLO-167 continuation order: %w", err)
		}
		if priorTerminalCount != authority.Run.Ordinal-2 {
			return nil, controlplane.NewPolicyError(controlplane.CodeForbidden, "FLO-167 continuation has an unfinished prior shot", "complete each shot in ordinal order")
		}
	}
	var completedSet, allowedSubmitSet []string
	if json.Unmarshal(completedSetJSON, &completedSet) != nil ||
		json.Unmarshal(allowedSubmitSetJSON, &allowedSubmitSet) != nil ||
		!reflect.DeepEqual(completedSet, package_.Authorization.CompletedSet) ||
		!reflect.DeepEqual(completedSet, projection.CompletedSet) ||
		!reflect.DeepEqual(allowedSubmitSet, package_.Authorization.AllowedSubmitSet) ||
		!reflect.DeepEqual(allowedSubmitSet, projection.AllowedSubmitSet) {
		return nil, controlplane.NewConflictError(controlplane.CodeRevisionConflict, "FLO-167 completed or allowed-submit set drifted")
	}
	completedHash, err := digestValue(completedSet)
	if err != nil {
		return nil, err
	}
	result.AuthorizationHash, result.CompletedSetHash = *authHash, completedHash
	return &result, nil
}

func reserveFLO167AFP(
	ctx context.Context,
	tx pgx.Tx,
	boundary *flo167PaidBoundary,
	authority *stage1LiveAuthority,
	input orchestration.ExecuteProviderJobInput,
	now time.Time,
) error {
	if boundary == nil {
		return nil
	}
	snapshot := input.SubscriptionQuotaSnapshot
	if snapshot == nil || snapshot.SchemaVersion != flo100QuotaSchema || strings.TrimSpace(snapshot.Source) == "" || snapshot.CapturedAt.IsZero() ||
		snapshot.CapturedAt.After(now.Add(30*time.Second)) || now.Sub(snapshot.CapturedAt) > 300*time.Second ||
		snapshot.AccountID == "" || snapshot.Profile != flo100AgentPlanProfile || snapshot.Region != flo100AgentPlanRegion ||
		snapshot.BillingMode != "subscription_included_only" ||
		snapshot.MonthlyUsedAFPMilli < 0 || snapshot.MonthlyTotalAFPMilli <= 0 || snapshot.ExternalReservedAFPMilli < 0 ||
		snapshot.FiveHourUsedAFPMilli < 0 || snapshot.FiveHourTotalAFPMilli <= 0 ||
		snapshot.WeeklyUsedAFPMilli < 0 || snapshot.WeeklyTotalAFPMilli <= 0 ||
		snapshot.FiveHourUsedAFPMilli > snapshot.FiveHourTotalAFPMilli ||
		snapshot.WeeklyUsedAFPMilli > snapshot.WeeklyTotalAFPMilli ||
		snapshot.MonthlyUsedAFPMilli > snapshot.MonthlyTotalAFPMilli {
		return controlplane.NewPolicyError(controlplane.CodeBudgetExceeded, "FLO-167 quota snapshot is stale or incomplete", "refresh quota within 300 seconds")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7100165)`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE video_pipeline.stage1_agent_plan_afp_reservations
		SET status='RELEASED' WHERE activation_id=$1 AND status='RESERVED'`, authority.ActivationID); err != nil {
		return fmt.Errorf("release superseded FLO-100 AFP reservation: %w", err)
	}
	var otherReserved int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(total_afp_milli),0) FROM (
		SELECT total_afp_milli FROM video_pipeline.stage1_agent_plan_afp_reservations
		 WHERE account_id=$1 AND profile=$2 AND region=$3 AND status='RESERVED' AND activation_id<>$5
		UNION ALL
		SELECT total_afp_milli FROM video_pipeline.stage1_live_supersession_afp_reservations
		 WHERE account_id=$1 AND profile=$2 AND region=$3 AND status='RESERVED' AND supersession_id<>$4
		) reservations`, snapshot.AccountID, snapshot.Profile, snapshot.Region, boundary.SupersessionID,
		authority.ActivationID).Scan(&otherReserved); err != nil {
		return fmt.Errorf("read FLO-167 concurrent reservations: %w", err)
	}
	required := flo167RemainingVideoAFPMilli + flo100SpeechAFPMilli
	monthlyTotal := snapshot.MonthlyTotalAFPMilli
	if monthlyTotal > flo100MonthlyCapAFPMilli {
		monthlyTotal = flo100MonthlyCapAFPMilli
	}
	for _, window := range []struct {
		used, total int64
	}{
		{snapshot.FiveHourUsedAFPMilli, snapshot.FiveHourTotalAFPMilli},
		{snapshot.WeeklyUsedAFPMilli, snapshot.WeeklyTotalAFPMilli},
		{snapshot.MonthlyUsedAFPMilli, monthlyTotal},
	} {
		available := window.total - window.used - snapshot.ExternalReservedAFPMilli - otherReserved
		if available < required {
			return controlplane.NewPolicyError(controlplane.CodeBudgetExceeded, "FLO-167 quota cannot cover inherited remaining budget", "wait for quota recovery")
		}
	}
	snapshotHash, err := digestValue(snapshot)
	if err != nil {
		return err
	}
	snapshotID := uuid.NewSHA1(boundary.SupersessionID, []byte("quota:"+input.Run.RunID+":"+snapshotHash))
	boundary.QuotaSnapshotID = snapshotID
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_agent_plan_quota_snapshots
		(id,activation_id,run_id,snapshot_hash,source,captured_at,recorded_at,account_id,profile,region,billing_mode,
		 five_hour_used_afp_milli,five_hour_total_afp_milli,weekly_used_afp_milli,weekly_total_afp_milli,
		 monthly_used_afp_milli,monthly_total_afp_milli,external_reserved_afp_milli)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
		ON CONFLICT (id) DO NOTHING`, snapshotID, authority.ActivationID, authority.Run.RunID, snapshotHash,
		snapshot.Source, snapshot.CapturedAt, now, snapshot.AccountID, snapshot.Profile, snapshot.Region, snapshot.BillingMode,
		snapshot.FiveHourUsedAFPMilli, snapshot.FiveHourTotalAFPMilli, snapshot.WeeklyUsedAFPMilli,
		snapshot.WeeklyTotalAFPMilli, snapshot.MonthlyUsedAFPMilli, snapshot.MonthlyTotalAFPMilli,
		snapshot.ExternalReservedAFPMilli); err != nil {
		return fmt.Errorf("persist FLO-167 quota: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersession_afp_reservations
		(supersession_id,quota_snapshot_id,account_id,profile,region,a01_settled_afp_milli,
		 remaining_video_afp_milli,speech_afp_milli,total_afp_milli,status,reserved_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'RESERVED',$10) ON CONFLICT (supersession_id) DO NOTHING`,
		boundary.SupersessionID, snapshotID, snapshot.AccountID, snapshot.Profile, snapshot.Region,
		flo167A01ActualAFPMilli, flo167RemainingVideoAFPMilli, flo100SpeechAFPMilli,
		flo167RemainingVideoAFPMilli+flo100SpeechAFPMilli, now); err != nil {
		return fmt.Errorf("reserve FLO-167 inherited AFP: %w", err)
	}
	var storedSnapshotID uuid.UUID
	var storedAccount, storedProfile, storedRegion, storedStatus string
	var storedA01, storedVideo, storedSpeech int64
	if err := tx.QueryRow(ctx, `SELECT quota_snapshot_id,account_id,profile,region,
		a01_settled_afp_milli,remaining_video_afp_milli,speech_afp_milli,status
		FROM video_pipeline.stage1_live_supersession_afp_reservations WHERE supersession_id=$1 FOR SHARE`,
		boundary.SupersessionID).Scan(&storedSnapshotID, &storedAccount, &storedProfile, &storedRegion,
		&storedA01, &storedVideo, &storedSpeech, &storedStatus); err != nil {
		return fmt.Errorf("verify FLO-167 inherited reservation: %w", err)
	}
	if storedAccount != snapshot.AccountID || storedProfile != snapshot.Profile || storedRegion != snapshot.Region ||
		storedA01 != flo167A01ActualAFPMilli || storedVideo != flo167RemainingVideoAFPMilli ||
		storedSpeech != flo100SpeechAFPMilli || storedStatus != "RESERVED" {
		return controlplane.NewConflictError(controlplane.CodeRevisionConflict, "existing FLO-167 inherited reservation drifted")
	}
	// The reservation retains its first evidence snapshot; the submission uses
	// the fresh snapshot captured for this exact paid-boundary transaction.
	if _, err := tx.Exec(ctx, `UPDATE video_pipeline.stage1_live_supersessions
		SET state='quota_reserved' WHERE id=$1 AND state='v3_authorized_A02_A10'`, boundary.SupersessionID); err != nil {
		return err
	}
	return nil
}

func recordFLO167Submission(ctx context.Context, tx pgx.Tx, boundary *flo167PaidBoundary, attemptID uuid.UUID, input orchestration.ExecuteProviderJobInput, now time.Time) error {
	if boundary == nil {
		return nil
	}
	if boundary.QuotaSnapshotID == uuid.Nil {
		return errors.New("FLO-167 submission has no same-transaction quota snapshot")
	}
	b := boundary.Shot
	idempotencyKey := "provider-job-" + input.Run.RunID
	_, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersession_submissions
		(supersession_id,shot_id,attempt_id,quota_snapshot_id,duration_ms,pricing_snapshot_id,pricing_snapshot_digest,
		 reference_afp_milli,reference_duration_ms,expected_afp_milli,pricing_rule_version,maximum_drift_basis_points,
		 normalization_version,rounding_version,route_hash,g1_hash,g2_hash,safety_hash,completed_set_hash,
		 canonical_input_hash,semantic_input_hash,authorization_hash,idempotency_key,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
		ON CONFLICT (supersession_id,shot_id) DO NOTHING`, boundary.SupersessionID, b.ShotID, attemptID,
		boundary.QuotaSnapshotID, b.Pricing.DurationMS, b.Pricing.PricingSnapshotID, b.Pricing.PricingSnapshotDigest,
		b.Pricing.ReferenceAFPMilli, b.Pricing.ReferenceDurationMS, b.Pricing.ExpectedAFPMilli,
		b.Pricing.PricingRuleVersion, b.Pricing.MaximumDriftBPS, b.Pricing.NormalizationVersion,
		b.Pricing.RoundingVersion, b.RouteHash, b.G1Hash, b.G2Hash, b.SafetyHash, boundary.CompletedSetHash,
		b.CanonicalInputHash, b.SemanticInputHash, boundary.AuthorizationHash, idempotencyKey, now)
	if err != nil {
		return fmt.Errorf("record FLO-167 submission: %w", err)
	}
	var storedAttempt, storedQuota uuid.UUID
	var storedPricingDigest, storedCompletedHash, storedCanonicalHash, storedSemanticHash, storedAuthHash, storedKey string
	if err := tx.QueryRow(ctx, `SELECT attempt_id,quota_snapshot_id,pricing_snapshot_digest,
		completed_set_hash,canonical_input_hash,semantic_input_hash,authorization_hash,idempotency_key
		FROM video_pipeline.stage1_live_supersession_submissions
		WHERE supersession_id=$1 AND shot_id=$2 FOR SHARE`, boundary.SupersessionID, b.ShotID).Scan(
		&storedAttempt, &storedQuota, &storedPricingDigest, &storedCompletedHash, &storedCanonicalHash,
		&storedSemanticHash, &storedAuthHash, &storedKey); err != nil {
		return fmt.Errorf("verify FLO-167 submission: %w", err)
	}
	if storedAttempt != attemptID || storedQuota != boundary.QuotaSnapshotID ||
		storedPricingDigest != b.Pricing.PricingSnapshotDigest || storedCompletedHash != boundary.CompletedSetHash ||
		storedCanonicalHash != b.CanonicalInputHash || storedSemanticHash != b.SemanticInputHash ||
		storedAuthHash != boundary.AuthorizationHash || storedKey != idempotencyKey {
		return controlplane.NewConflictError(controlplane.CodeRevisionConflict, "existing FLO-167 submission drifted")
	}
	if b.ShotID == "GOLD-A02" {
		if _, err := tx.Exec(ctx, `UPDATE video_pipeline.stage1_live_supersessions SET state='A02_submitted'
			WHERE id=$1 AND state='quota_reserved'`, boundary.SupersessionID); err != nil {
			return err
		}
	}
	return nil
}

func recordFLO167Terminal(
	ctx context.Context,
	tx pgx.Tx,
	attemptID, jobID uuid.UUID,
	result orchestration.ProviderResult,
	now time.Time,
) error {
	var supersessionID uuid.UUID
	var shotID string
	var duration, referenceAFP, referenceDuration, expectedAFP, maximumDrift int64
	var pricingID, pricingDigest, pricingRule, normalization, rounding string
	var routeHash, g1Hash, g2Hash, safetyHash, completedHash, canonicalHash, semanticHash string
	err := tx.QueryRow(ctx, `SELECT supersession_id,shot_id,duration_ms,pricing_snapshot_id,
		pricing_snapshot_digest,reference_afp_milli,reference_duration_ms,expected_afp_milli,
		pricing_rule_version,maximum_drift_basis_points,normalization_version,rounding_version,
		route_hash,g1_hash,g2_hash,safety_hash,completed_set_hash,canonical_input_hash,semantic_input_hash
		FROM video_pipeline.stage1_live_supersession_submissions WHERE attempt_id=$1 FOR SHARE`, attemptID).Scan(
		&supersessionID, &shotID, &duration, &pricingID, &pricingDigest, &referenceAFP,
		&referenceDuration, &expectedAFP, &pricingRule, &maximumDrift, &normalization, &rounding,
		&routeHash, &g1Hash, &g2Hash, &safetyHash, &completedHash, &canonicalHash, &semanticHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read FLO-167 terminal binding: %w", err)
	}
	if result.ActualAFPMilli <= 0 || result.Usage.VideoTokens <= 0 || result.Cost.ActualMicros == nil || *result.Cost.ActualMicros != 0 {
		return controlplane.NewPolicyError(controlplane.CodeBudgetExceeded, "FLO-167 terminal usage evidence is incomplete", "supply independently measured AFP, video tokens, and zero-cash attribution")
	}
	withinDrift, driftErr := stage1.AFPWithinDrift(result.ActualAFPMilli, expectedAFP, maximumDrift)
	if driftErr != nil || !withinDrift {
		return controlplane.NewPolicyError(controlplane.CodeBudgetExceeded, "FLO-167 terminal AFP exceeded the frozen duration-normalized drift boundary", "stop the continuation and reconcile independently measured AFP")
	}
	terminal := struct {
		SupersessionID    string `json:"supersessionId"`
		ShotID            string `json:"shotId"`
		AttemptID         string `json:"attemptId"`
		ProviderJobID     string `json:"providerJobId"`
		ActualAFPMilli    int64  `json:"actualAfpMilli"`
		ActualVideoTokens int64  `json:"actualVideoTokens"`
		ActualCashMicros  int64  `json:"actualCashMicros"`
		ArtifactDigest    string `json:"artifactDigest"`
		UpstreamTaskID    string `json:"upstreamTaskId"`
		RequestID         string `json:"requestId"`
	}{supersessionID.String(), shotID, attemptID.String(), jobID.String(), result.ActualAFPMilli,
		result.Usage.VideoTokens, *result.Cost.ActualMicros, result.ArtifactDigest, result.UpstreamTaskID, result.RequestID}
	terminalHash, err := digestValue(terminal)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersession_terminal_ledger
		(supersession_id,shot_id,attempt_id,provider_job_id,duration_ms,pricing_snapshot_id,
		 pricing_snapshot_digest,reference_afp_milli,reference_duration_ms,expected_afp_milli,
		 pricing_rule_version,maximum_drift_basis_points,actual_afp_milli,actual_video_tokens,
		 actual_cash_micros,normalization_version,rounding_version,route_hash,g1_hash,g2_hash,
		 safety_hash,completed_set_hash,canonical_input_hash,semantic_input_hash,terminal_hash,terminal_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
		ON CONFLICT (supersession_id,shot_id) DO NOTHING`, supersessionID, shotID, attemptID, jobID,
		duration, pricingID, pricingDigest, referenceAFP, referenceDuration, expectedAFP, pricingRule,
		maximumDrift, result.ActualAFPMilli, result.Usage.VideoTokens, *result.Cost.ActualMicros,
		normalization, rounding, routeHash, g1Hash, g2Hash, safetyHash, completedHash, canonicalHash,
		semanticHash, terminalHash, now); err != nil {
		return fmt.Errorf("record FLO-167 terminal ledger: %w", err)
	}
	var storedHash string
	if err := tx.QueryRow(ctx, `SELECT terminal_hash FROM video_pipeline.stage1_live_supersession_terminal_ledger
		WHERE supersession_id=$1 AND shot_id=$2 FOR SHARE`, supersessionID, shotID).Scan(&storedHash); err != nil {
		return fmt.Errorf("verify FLO-167 terminal ledger: %w", err)
	}
	if storedHash != terminalHash {
		return controlplane.NewConflictError(controlplane.CodeRevisionConflict, "existing FLO-167 terminal ledger drifted")
	}
	return nil
}
