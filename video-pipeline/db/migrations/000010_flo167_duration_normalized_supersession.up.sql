-- FLO-167: duration-normalized AFP supersession. The v2 activation and A01
-- terminal ledger remain immutable and are referenced by digest only.
SET search_path TO video_pipeline, public;

CREATE TABLE stage1_live_supersessions (
    id UUID PRIMARY KEY,
    legacy_activation_id UUID NOT NULL UNIQUE REFERENCES stage1_live_activations(id),
    schema_version TEXT NOT NULL CHECK (schema_version = 'flo100.batch-a-supersession.v3'),
    state TEXT NOT NULL CHECK (state IN ('supersession_package_pending_v3','v3_authorized_A02_A10','quota_reserved','A02_submitted')),
    legacy_authorization_hash CHAR(64) NOT NULL CHECK (legacy_authorization_hash ~ '^[0-9a-f]{64}$'),
    legacy_execution_package_hash CHAR(64) NOT NULL CHECK (legacy_execution_package_hash ~ '^[0-9a-f]{64}$'),
    legacy_projection_hash CHAR(64) NOT NULL CHECK (legacy_projection_hash ~ '^[0-9a-f]{64}$'),
    legacy_terminal_ledger_hash CHAR(64) NOT NULL CHECK (legacy_terminal_ledger_hash ~ '^[0-9a-f]{64}$'),
    legacy_stop_evidence_hash CHAR(64) NOT NULL CHECK (legacy_stop_evidence_hash ~ '^[0-9a-f]{64}$'),
    execution_package_hash CHAR(64) NOT NULL UNIQUE CHECK (execution_package_hash ~ '^[0-9a-f]{64}$'),
    canonical_projection_hash CHAR(64) NOT NULL CHECK (canonical_projection_hash ~ '^[0-9a-f]{64}$'),
    canonical_projection JSONB NOT NULL,
    authorization_hash CHAR(64) CHECK (authorization_hash ~ '^[0-9a-f]{64}$'),
    completed_set JSONB NOT NULL CHECK (completed_set = '["GOLD-A01"]'::jsonb),
    allowed_submit_set JSONB NOT NULL CHECK (allowed_submit_set = '["GOLD-A02","GOLD-A03","GOLD-A04","GOLD-A05","GOLD-A06","GOLD-A07","GOLD-A08","GOLD-A09","GOLD-A10"]'::jsonb),
    package JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE stage1_live_supersession_shots (
    supersession_id UUID NOT NULL REFERENCES stage1_live_supersessions(id),
    ordinal SMALLINT NOT NULL CHECK (ordinal BETWEEN 1 AND 10),
    shot_id TEXT NOT NULL CHECK (shot_id ~ '^GOLD-A(0[1-9]|10)$'),
    duration_ms BIGINT NOT NULL CHECK (duration_ms > 0),
    pricing_snapshot_id TEXT NOT NULL,
    pricing_snapshot_digest CHAR(64) NOT NULL CHECK (pricing_snapshot_digest ~ '^[0-9a-f]{64}$'),
    reference_afp_milli BIGINT NOT NULL CHECK (reference_afp_milli = 2504700),
    reference_duration_ms BIGINT NOT NULL CHECK (reference_duration_ms = 5000),
    expected_afp_milli BIGINT NOT NULL CHECK (expected_afp_milli >= 0),
    pricing_rule_version TEXT NOT NULL CHECK (pricing_rule_version = 'agent-plan-subscription-v1'),
    maximum_drift_basis_points BIGINT NOT NULL CHECK (maximum_drift_basis_points = 1000),
    normalization_version TEXT NOT NULL CHECK (normalization_version = 'duration-normalized-afp/v1'),
    rounding_version TEXT NOT NULL CHECK (rounding_version = 'half-up-nonnegative-integer/v1'),
    route_hash CHAR(64) NOT NULL CHECK (route_hash ~ '^[0-9a-f]{64}$'),
    g1_hash CHAR(64) NOT NULL CHECK (g1_hash ~ '^[0-9a-f]{64}$'),
    g2_hash CHAR(64) NOT NULL CHECK (g2_hash ~ '^[0-9a-f]{64}$'),
    safety_hash CHAR(64) NOT NULL CHECK (safety_hash ~ '^[0-9a-f]{64}$'),
    canonical_input_hash CHAR(64) NOT NULL CHECK (canonical_input_hash ~ '^[0-9a-f]{64}$'),
    semantic_input_hash CHAR(64) NOT NULL CHECK (semantic_input_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (supersession_id, ordinal),
    UNIQUE (supersession_id, shot_id),
    CHECK ((shot_id IN ('GOLD-A01','GOLD-A05','GOLD-A09') AND duration_ms=4000 AND expected_afp_milli=2003760)
        OR (shot_id IN ('GOLD-A02','GOLD-A07','GOLD-A10') AND duration_ms=4500 AND expected_afp_milli=2254230)
        OR (shot_id IN ('GOLD-A03','GOLD-A06') AND duration_ms=5000 AND expected_afp_milli=2504700)
        OR (shot_id IN ('GOLD-A04','GOLD-A08') AND duration_ms=5500 AND expected_afp_milli=2755170))
);

CREATE TABLE stage1_live_supersession_authorizations (
    supersession_id UUID PRIMARY KEY REFERENCES stage1_live_supersessions(id),
    authorization_hash CHAR(64) NOT NULL UNIQUE CHECK (authorization_hash ~ '^[0-9a-f]{64}$'),
    execution_package_hash CHAR(64) NOT NULL CHECK (execution_package_hash ~ '^[0-9a-f]{64}$'),
    projection_hash CHAR(64) NOT NULL CHECK (projection_hash ~ '^[0-9a-f]{64}$'),
    pricing_snapshot_digest CHAR(64) NOT NULL CHECK (pricing_snapshot_digest ~ '^[0-9a-f]{64}$'),
    valid_until TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    authorized_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE stage1_live_supersession_afp_reservations (
    supersession_id UUID PRIMARY KEY REFERENCES stage1_live_supersessions(id),
    quota_snapshot_id UUID NOT NULL REFERENCES stage1_agent_plan_quota_snapshots(id),
    account_id TEXT NOT NULL,
    profile TEXT NOT NULL,
    region TEXT NOT NULL,
    a01_settled_afp_milli BIGINT NOT NULL CHECK (a01_settled_afp_milli = 2007900),
    remaining_video_afp_milli BIGINT NOT NULL CHECK (remaining_video_afp_milli = 28298970),
    speech_afp_milli BIGINT NOT NULL CHECK (speech_afp_milli = 1039),
    total_afp_milli BIGINT NOT NULL CHECK (total_afp_milli = remaining_video_afp_milli + speech_afp_milli),
    status TEXT NOT NULL CHECK (status IN ('RESERVED','SETTLED','RELEASED')),
    reserved_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE stage1_live_supersession_submissions (
    supersession_id UUID NOT NULL REFERENCES stage1_live_supersessions(id),
    shot_id TEXT NOT NULL CHECK (shot_id IN ('GOLD-A02','GOLD-A03','GOLD-A04','GOLD-A05','GOLD-A06','GOLD-A07','GOLD-A08','GOLD-A09','GOLD-A10')),
    attempt_id UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    quota_snapshot_id UUID NOT NULL REFERENCES stage1_agent_plan_quota_snapshots(id),
    duration_ms BIGINT NOT NULL CHECK (duration_ms > 0),
    pricing_snapshot_id TEXT NOT NULL,
    pricing_snapshot_digest CHAR(64) NOT NULL CHECK (pricing_snapshot_digest ~ '^[0-9a-f]{64}$'),
    reference_afp_milli BIGINT NOT NULL CHECK (reference_afp_milli = 2504700),
    reference_duration_ms BIGINT NOT NULL CHECK (reference_duration_ms = 5000),
    expected_afp_milli BIGINT NOT NULL CHECK (expected_afp_milli >= 0),
    pricing_rule_version TEXT NOT NULL CHECK (pricing_rule_version = 'agent-plan-subscription-v1'),
    maximum_drift_basis_points BIGINT NOT NULL CHECK (maximum_drift_basis_points = 1000),
    normalization_version TEXT NOT NULL CHECK (normalization_version = 'duration-normalized-afp/v1'),
    rounding_version TEXT NOT NULL CHECK (rounding_version = 'half-up-nonnegative-integer/v1'),
    route_hash CHAR(64) NOT NULL CHECK (route_hash ~ '^[0-9a-f]{64}$'),
    g1_hash CHAR(64) NOT NULL CHECK (g1_hash ~ '^[0-9a-f]{64}$'),
    g2_hash CHAR(64) NOT NULL CHECK (g2_hash ~ '^[0-9a-f]{64}$'),
    safety_hash CHAR(64) NOT NULL CHECK (safety_hash ~ '^[0-9a-f]{64}$'),
    completed_set_hash CHAR(64) NOT NULL CHECK (completed_set_hash ~ '^[0-9a-f]{64}$'),
    canonical_input_hash CHAR(64) NOT NULL CHECK (canonical_input_hash ~ '^[0-9a-f]{64}$'),
    semantic_input_hash CHAR(64) NOT NULL CHECK (semantic_input_hash ~ '^[0-9a-f]{64}$'),
    authorization_hash CHAR(64) NOT NULL CHECK (authorization_hash ~ '^[0-9a-f]{64}$'),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (supersession_id, shot_id)
);

CREATE TABLE stage1_live_supersession_terminal_ledger (
    supersession_id UUID NOT NULL REFERENCES stage1_live_supersessions(id),
    shot_id TEXT NOT NULL,
    attempt_id UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    provider_job_id UUID NOT NULL UNIQUE REFERENCES provider_jobs(id),
    duration_ms BIGINT NOT NULL CHECK (duration_ms > 0),
    pricing_snapshot_id TEXT NOT NULL,
    pricing_snapshot_digest CHAR(64) NOT NULL CHECK (pricing_snapshot_digest ~ '^[0-9a-f]{64}$'),
    reference_afp_milli BIGINT NOT NULL CHECK (reference_afp_milli = 2504700),
    reference_duration_ms BIGINT NOT NULL CHECK (reference_duration_ms = 5000),
    expected_afp_milli BIGINT NOT NULL CHECK (expected_afp_milli >= 0),
    pricing_rule_version TEXT NOT NULL CHECK (pricing_rule_version = 'agent-plan-subscription-v1'),
    maximum_drift_basis_points BIGINT NOT NULL CHECK (maximum_drift_basis_points = 1000),
    actual_afp_milli BIGINT NOT NULL CHECK (actual_afp_milli >= 0),
    actual_video_tokens BIGINT NOT NULL CHECK (actual_video_tokens >= 0),
    actual_cash_micros BIGINT NOT NULL CHECK (actual_cash_micros = 0),
    normalization_version TEXT NOT NULL CHECK (normalization_version = 'duration-normalized-afp/v1'),
    rounding_version TEXT NOT NULL CHECK (rounding_version = 'half-up-nonnegative-integer/v1'),
    route_hash CHAR(64) NOT NULL CHECK (route_hash ~ '^[0-9a-f]{64}$'),
    g1_hash CHAR(64) NOT NULL CHECK (g1_hash ~ '^[0-9a-f]{64}$'),
    g2_hash CHAR(64) NOT NULL CHECK (g2_hash ~ '^[0-9a-f]{64}$'),
    safety_hash CHAR(64) NOT NULL CHECK (safety_hash ~ '^[0-9a-f]{64}$'),
    completed_set_hash CHAR(64) NOT NULL CHECK (completed_set_hash ~ '^[0-9a-f]{64}$'),
    canonical_input_hash CHAR(64) NOT NULL CHECK (canonical_input_hash ~ '^[0-9a-f]{64}$'),
    semantic_input_hash CHAR(64) NOT NULL CHECK (semantic_input_hash ~ '^[0-9a-f]{64}$'),
    terminal_hash CHAR(64) NOT NULL UNIQUE CHECK (terminal_hash ~ '^[0-9a-f]{64}$'),
    terminal_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (supersession_id, shot_id)
);

CREATE TRIGGER stage1_live_supersession_immutable_update BEFORE UPDATE ON stage1_live_supersessions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('state','authorization_hash');
CREATE TRIGGER stage1_live_supersession_shot_immutable_update BEFORE UPDATE ON stage1_live_supersession_shots
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_authorization_immutable_update BEFORE UPDATE ON stage1_live_supersession_authorizations
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_submission_immutable_update BEFORE UPDATE ON stage1_live_supersession_submissions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_terminal_immutable_update BEFORE UPDATE ON stage1_live_supersession_terminal_ledger
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_afp_immutable_update BEFORE UPDATE ON stage1_live_supersession_afp_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER stage1_live_supersession_immutable_delete BEFORE DELETE ON stage1_live_supersessions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_shot_immutable_delete BEFORE DELETE ON stage1_live_supersession_shots
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_authorization_immutable_delete BEFORE DELETE ON stage1_live_supersession_authorizations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_submission_immutable_delete BEFORE DELETE ON stage1_live_supersession_submissions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_terminal_immutable_delete BEFORE DELETE ON stage1_live_supersession_terminal_ledger
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_afp_immutable_delete BEFORE DELETE ON stage1_live_supersession_afp_reservations
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();

CREATE FUNCTION guard_flo167_state_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT ((OLD.state='supersession_package_pending_v3' AND NEW.state='v3_authorized_A02_A10')
        OR (OLD.state='v3_authorized_A02_A10' AND NEW.state='quota_reserved')
        OR (OLD.state='quota_reserved' AND NEW.state='A02_submitted')) THEN
        RAISE EXCEPTION 'invalid FLO-167 state transition from % to %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER stage1_live_supersession_state_transition
    BEFORE UPDATE OF state ON stage1_live_supersessions
    FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state)
    EXECUTE FUNCTION guard_flo167_state_transition();
