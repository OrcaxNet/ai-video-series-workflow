package stage1materialize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/stage1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	flo167LegacyAuthorizationHash = "7bf55cad2a4f81f54eb6617bbab81fd21f789785ce1176213014f5833ce4ac25"
	flo167LegacyExecutionHash     = "6a7c03ed869c427d23cc6b669e7938ba271c8343b3e4627e85cd93ea50fffd2e"
	flo167LegacyProjectionHash    = "c0d2d316867c79d1ebc419dec3a68fe29c947cd184d21a2b11e45c6224013202"
	flo167LegacyTerminalHash      = "7ea9cfb63b3c54fa46583cf5abdb0bc67d323eead9ab4f45e9187e9700dcf0e0"
	flo167LegacyStopHash          = "dd1954608254791425d7574fe7333d1c1d7cd77cb843572f79d84a3dbeadea76"
)

var lowerSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

type FLO167Materialization struct {
	LegacyActivationID string
	Projection         stage1.FLO167CanonicalProjection
	Package            stage1.FLO167SupersessionPackage
	CreatedAt          time.Time
}

type FLO167Authorization struct {
	LegacyActivationID string
	Payload            json.RawMessage
	IssuedAt           time.Time
	ValidUntil         time.Time
}

type flo167AuthorizationPayload struct {
	SchemaVersion           string `json:"schemaVersion"`
	SupersessionPackageHash string `json:"supersessionPackageHash"`
	CanonicalProjectionHash string `json:"canonicalProjectionHash"`
	PricingSnapshotDigest   string `json:"pricingSnapshotDigest"`
	Decision                struct {
		A02A10 bool `json:"a02A10ProviderPostAuthorizedConditionally"`
		B      bool `json:"batchBProviderPostAuthorized"`
		C      bool `json:"batchCProviderPostAuthorized"`
		Stage4 bool `json:"stage4Authorized"`
	} `json:"decision"`
}

// MaterializeFLO167Supersession persists a complete provider-free v3 package.
// It verifies the immutable v2 activation and seal in the same SERIALIZABLE
// transaction and cannot create a budget reservation, cost row, or Provider job.
func MaterializeFLO167Supersession(ctx context.Context, pool *pgxpool.Pool, input FLO167Materialization) error {
	activationID, err := uuid.Parse(input.LegacyActivationID)
	if err != nil {
		return errors.New("legacy activation id must be a UUID")
	}
	if err := stage1.ValidateFLO167Artifacts(input.Package, input.Projection); err != nil {
		return fmt.Errorf("validate FLO-167 artifacts: %w", err)
	}
	if input.CreatedAt.IsZero() {
		return errors.New("FLO-167 projection hash or creation time is invalid")
	}
	if input.Package.LegacyAuthorizationHash != flo167LegacyAuthorizationHash ||
		input.Package.LegacyExecutionPackageHash != flo167LegacyExecutionHash ||
		input.Package.LegacyProjectionHash != flo167LegacyProjectionHash ||
		input.Package.LegacyTerminalLedgerHash != flo167LegacyTerminalHash ||
		input.Package.LegacyStopEvidenceHash != flo167LegacyStopHash {
		return errors.New("FLO-167 package does not reference the frozen v2 lineage")
	}
	packageJSON, err := json.Marshal(input.Package)
	if err != nil {
		return err
	}
	projectionJSON, err := json.Marshal(input.Projection)
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var authHash, executionHash, projectionHash string
	if err := tx.QueryRow(ctx, `SELECT a.source_authorization_hash,a.live_execution_package_hash,ps.projection_hash
		FROM video_pipeline.stage1_live_activations a
		JOIN video_pipeline.stage1_live_projection_seals ps ON ps.activation_id=a.id
		WHERE a.id=$1 AND a.batch_id='flo100-gold-a-v1' FOR SHARE OF a,ps`, activationID).Scan(
		&authHash, &executionHash, &projectionHash); err != nil {
		return fmt.Errorf("verify frozen FLO-167 legacy activation: %w", err)
	}
	if authHash != flo167LegacyAuthorizationHash || executionHash != flo167LegacyExecutionHash || projectionHash != flo167LegacyProjectionHash {
		return errors.New("stored FLO-167 legacy activation drifted")
	}
	if err := verifyFLO167LegacyTerminal(ctx, tx, activationID, input.Projection.A01Terminal); err != nil {
		return err
	}
	supersessionID := uuid.NewSHA1(activationID, []byte("flo167-duration-normalized-v3"))
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersessions
		(id,legacy_activation_id,schema_version,state,legacy_authorization_hash,
		 legacy_execution_package_hash,legacy_projection_hash,legacy_terminal_ledger_hash,
		 legacy_stop_evidence_hash,execution_package_hash,canonical_projection_hash,canonical_projection,
		 completed_set,allowed_submit_set,package,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (legacy_activation_id) DO NOTHING`, supersessionID, activationID,
		input.Package.SchemaVersion, input.Package.State, input.Package.LegacyAuthorizationHash,
		input.Package.LegacyExecutionPackageHash, input.Package.LegacyProjectionHash,
		input.Package.LegacyTerminalLedgerHash, input.Package.LegacyStopEvidenceHash,
		input.Package.ContentHash, input.Projection.ContentHash, projectionJSON, input.Package.Authorization.CompletedSet,
		input.Package.Authorization.AllowedSubmitSet, packageJSON, input.CreatedAt); err != nil {
		return fmt.Errorf("insert FLO-167 supersession: %w", err)
	}
	for ordinal, shot := range input.Package.Shots {
		p := shot.Pricing
		var storedShotID, routeHash, g1Hash, g2Hash, safetyHash, canonicalHash, semanticHash string
		if err := tx.QueryRow(ctx, `SELECT binding.value->>'shotId',pcs.capability_hash,
			a.source_authorization->'g1Approval'->>'licenseSnapshotHash',ar.intent_input_hash,
			a.source_authorization->'g1Approval'->>'safetyEvidenceHash',gr.run_spec_digest,ar.prompt_snapshot_hash
			FROM video_pipeline.stage1_live_activations a
			JOIN video_pipeline.stage1_live_activation_runs ar ON ar.activation_id=a.id AND ar.ordinal=$2
			JOIN video_pipeline.generation_runs gr ON gr.id=ar.run_id
			JOIN video_pipeline.provider_capability_snapshots pcs ON pcs.id=a.video_capability_snapshot_id
			JOIN LATERAL (SELECT value FROM jsonb_array_elements(a.source_authorization->'g2Approval'->'shotBindings')
				WITH ORDINALITY AS item(value,position) WHERE item.position=$2) binding ON true
			WHERE a.id=$1 FOR SHARE OF a,ar,gr,pcs`, activationID, ordinal+1).Scan(
			&storedShotID, &routeHash, &g1Hash, &g2Hash, &safetyHash, &canonicalHash, &semanticHash); err != nil {
			return fmt.Errorf("verify FLO-167 legacy shot %d: %w", ordinal+1, err)
		}
		if storedShotID != shot.ShotID || routeHash != shot.RouteHash || g1Hash != shot.G1Hash ||
			g2Hash != shot.G2Hash || safetyHash != shot.SafetyHash || canonicalHash != shot.CanonicalInputHash ||
			semanticHash != shot.SemanticInputHash {
			return fmt.Errorf("FLO-167 shot %s differs from frozen PostgreSQL lineage", shot.ShotID)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersession_shots
			(supersession_id,ordinal,shot_id,duration_ms,pricing_snapshot_id,pricing_snapshot_digest,
			 reference_afp_milli,reference_duration_ms,expected_afp_milli,pricing_rule_version,
			 maximum_drift_basis_points,normalization_version,rounding_version,route_hash,g1_hash,g2_hash,
			 safety_hash,canonical_input_hash,semantic_input_hash)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
			ON CONFLICT (supersession_id,ordinal) DO NOTHING`, supersessionID, ordinal+1, shot.ShotID,
			p.DurationMS, p.PricingSnapshotID, p.PricingSnapshotDigest, p.ReferenceAFPMilli,
			p.ReferenceDurationMS, p.ExpectedAFPMilli, p.PricingRuleVersion, p.MaximumDriftBPS,
			p.NormalizationVersion, p.RoundingVersion, shot.RouteHash, shot.G1Hash, shot.G2Hash,
			shot.SafetyHash, shot.CanonicalInputHash, shot.SemanticInputHash); err != nil {
			return fmt.Errorf("insert FLO-167 shot %s: %w", shot.ShotID, err)
		}
	}
	var storedHash, storedProjection string
	var storedPackageJSON, storedProjectionJSON []byte
	if err := tx.QueryRow(ctx, `SELECT execution_package_hash,canonical_projection_hash,package,canonical_projection
		FROM video_pipeline.stage1_live_supersessions WHERE id=$1 FOR SHARE`, supersessionID).Scan(
		&storedHash, &storedProjection, &storedPackageJSON, &storedProjectionJSON); err != nil {
		return err
	}
	var storedPackage stage1.FLO167SupersessionPackage
	var storedProjectionValue stage1.FLO167CanonicalProjection
	if json.Unmarshal(storedPackageJSON, &storedPackage) != nil || storedHash != input.Package.ContentHash ||
		json.Unmarshal(storedProjectionJSON, &storedProjectionValue) != nil ||
		storedProjection != input.Projection.ContentHash || !reflect.DeepEqual(storedPackage, input.Package) ||
		!reflect.DeepEqual(storedProjectionValue, input.Projection) {
		return errors.New("existing FLO-167 materialization differs from exact replay")
	}
	var shotCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM video_pipeline.stage1_live_supersession_shots WHERE supersession_id=$1`, supersessionID).Scan(&shotCount); err != nil {
		return err
	}
	if shotCount != 10 {
		return errors.New("FLO-167 materialization does not contain exactly ten shots")
	}
	return tx.Commit(ctx)
}

func verifyFLO167LegacyTerminal(
	ctx context.Context,
	tx pgx.Tx,
	activationID uuid.UUID,
	expected stage1.FLO167LegacyTerminal,
) error {
	var rowCount int
	var taskID, requestID, state, artifactHash string
	var videoTokens, actualCashMicros int64
	err := tx.QueryRow(ctx, `
		SELECT count(*) OVER (),pj.upstream_task_id,pj.upstream_request_id,pj.state,
		       a.content_hash,
		       COALESCE((a.media_spec->'usage'->>'video_tokens')::bigint,0),
		       cl.amount_micros
		FROM video_pipeline.stage1_live_activation_runs ar
		JOIN video_pipeline.generation_runs gr ON gr.id=ar.run_id
		JOIN video_pipeline.generation_attempts ga ON ga.generation_run_id=ar.run_id
		JOIN video_pipeline.provider_jobs pj ON pj.generation_attempt_id=ga.id
		JOIN video_pipeline.budget_reservations br ON br.id=pj.budget_reservation_id
		JOIN video_pipeline.run_artifacts ra ON ra.generation_run_id=ar.run_id AND ra.role='OUTPUT'
		JOIN video_pipeline.artifacts a ON a.id=ra.artifact_id
		JOIN video_pipeline.cost_ledger cl ON cl.provider_job_id=pj.id AND cl.entry_type='ACTUAL' AND cl.verified
		WHERE ar.activation_id=$1 AND ar.ordinal=1
		  AND gr.state='SUCCEEDED' AND ga.state='SUCCEEDED' AND pj.state='SUCCEEDED'
		  AND br.status='SETTLED' AND a.status='ACTIVE'
		  AND a.artifact_uri='cas://sha256/'||a.content_hash
		  AND cl.budget_reservation_id=br.id AND cl.currency='CNY'
		  AND cl.units=$2 AND cl.unit_name='video_tokens'
		  AND cl.pricing_rule_version=br.pricing_rule_version
		  AND (SELECT count(*) FROM video_pipeline.provider_jobs p WHERE p.generation_attempt_id=ga.id)=1
		  AND (SELECT count(*) FROM video_pipeline.run_artifacts r WHERE r.generation_run_id=ar.run_id AND r.role='OUTPUT')=1
		  AND (SELECT count(*) FROM video_pipeline.cost_ledger c
		       WHERE c.provider_job_id=pj.id AND c.entry_type='ACTUAL')=1`, activationID, expected.ActualVideoTokens).Scan(
		&rowCount, &taskID, &requestID, &state, &artifactHash, &videoTokens, &actualCashMicros,
	)
	if err != nil {
		return fmt.Errorf("verify frozen FLO-167 A01 terminal facts: %w", err)
	}
	if rowCount != 1 || state != "SUCCEEDED" || taskID != expected.ProviderTaskID ||
		requestID != expected.ProviderRequestID || artifactHash != expected.ArtifactSHA256 ||
		videoTokens != expected.ActualVideoTokens || actualCashMicros != expected.ActualCashMicros {
		return errors.New("stored FLO-167 A01 terminal facts differ from the canonical projection")
	}
	return nil
}

// AuthorizeFLO167Supersession imports an independently issued, exact-scope v3
// authorization and performs the only pending -> authorized transition.
func AuthorizeFLO167Supersession(ctx context.Context, pool *pgxpool.Pool, input FLO167Authorization) error {
	activationID, err := uuid.Parse(input.LegacyActivationID)
	if err != nil {
		return errors.New("legacy activation id must be a UUID")
	}
	if input.IssuedAt.IsZero() || !input.ValidUntil.After(input.IssuedAt) {
		return errors.New("FLO-167 authorization validity is invalid")
	}
	var payload flo167AuthorizationPayload
	var canonicalPayload any
	if err := json.Unmarshal(input.Payload, &payload); err != nil || json.Unmarshal(input.Payload, &canonicalPayload) != nil {
		return errors.New("FLO-167 authorization payload is invalid JSON")
	}
	if payload.SchemaVersion != "flo100.batch-a-continuation-authorization.v3" ||
		!payload.Decision.A02A10 || payload.Decision.B || payload.Decision.C || payload.Decision.Stage4 ||
		!lowerSHA256.MatchString(payload.SupersessionPackageHash) ||
		!lowerSHA256.MatchString(payload.CanonicalProjectionHash) ||
		!lowerSHA256.MatchString(payload.PricingSnapshotDigest) {
		return errors.New("FLO-167 authorization has invalid identity or scope")
	}
	authorizationHash, err := digest(canonicalPayload)
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var supersessionID uuid.UUID
	var state, packageHash, projectionHash, pricingDigest string
	var pricingDigestCount int
	if err := tx.QueryRow(ctx, `SELECT s.id,s.state,s.execution_package_hash,s.canonical_projection_hash,
		(SELECT min(pricing_snapshot_digest) FROM video_pipeline.stage1_live_supersession_shots WHERE supersession_id=s.id),
		(SELECT count(DISTINCT pricing_snapshot_digest) FROM video_pipeline.stage1_live_supersession_shots WHERE supersession_id=s.id)
		FROM video_pipeline.stage1_live_supersessions s WHERE s.legacy_activation_id=$1 FOR UPDATE`, activationID).Scan(
		&supersessionID, &state, &packageHash, &projectionHash, &pricingDigest, &pricingDigestCount); err != nil {
		return fmt.Errorf("lock FLO-167 supersession for authorization: %w", err)
	}
	if pricingDigestCount != 1 || payload.SupersessionPackageHash != packageHash || payload.CanonicalProjectionHash != projectionHash ||
		payload.PricingSnapshotDigest != pricingDigest {
		return errors.New("FLO-167 authorization differs from the materialized package")
	}
	if state != "supersession_package_pending_v3" && state != "v3_authorized_A02_A10" {
		return errors.New("FLO-167 authorization cannot replace a started continuation")
	}
	if _, err := tx.Exec(ctx, `INSERT INTO video_pipeline.stage1_live_supersession_authorizations
		(supersession_id,authorization_hash,execution_package_hash,projection_hash,
		 pricing_snapshot_digest,valid_until,payload,authorized_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (supersession_id) DO NOTHING`,
		supersessionID, authorizationHash, packageHash, projectionHash, pricingDigest,
		input.ValidUntil, input.Payload, input.IssuedAt); err != nil {
		return fmt.Errorf("insert FLO-167 authorization: %w", err)
	}
	var storedHash string
	var storedPayload []byte
	var storedValidUntil, storedAuthorizedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT authorization_hash,payload,valid_until,authorized_at FROM video_pipeline.stage1_live_supersession_authorizations
		WHERE supersession_id=$1 FOR SHARE`, supersessionID).Scan(&storedHash, &storedPayload, &storedValidUntil, &storedAuthorizedAt); err != nil {
		return err
	}
	var storedCanonical any
	if json.Unmarshal(storedPayload, &storedCanonical) != nil || storedHash != authorizationHash ||
		!reflect.DeepEqual(storedCanonical, canonicalPayload) || !storedValidUntil.Equal(input.ValidUntil) ||
		!storedAuthorizedAt.Equal(input.IssuedAt) {
		return errors.New("existing FLO-167 authorization differs from exact replay")
	}
	if state == "supersession_package_pending_v3" {
		if _, err := tx.Exec(ctx, `UPDATE video_pipeline.stage1_live_supersessions
			SET authorization_hash=$2,state='v3_authorized_A02_A10' WHERE id=$1`, supersessionID, authorizationHash); err != nil {
			return fmt.Errorf("authorize FLO-167 supersession: %w", err)
		}
	}
	return tx.Commit(ctx)
}
