-- FLO-165: independent FLO-100 A-only live authority and Agent Plan AFP ledger.
SET search_path TO video_pipeline, public;

CREATE TABLE stage1_live_activations (
    id                              UUID PRIMARY KEY,
    batch_id                        TEXT NOT NULL CHECK (batch_id = 'flo100-gold-a-v1'),
    control_series_id               UUID NOT NULL REFERENCES series(id),
    source_series_id                UUID NOT NULL REFERENCES series(id),
    source_episode_id               UUID NOT NULL REFERENCES episodes(id),
    source_episode_revision_id      UUID NOT NULL REFERENCES episode_revisions(id),
    source_generation_plan_id       UUID NOT NULL REFERENCES operation_requests(id),
    live_generation_plan_id         UUID NOT NULL UNIQUE REFERENCES operation_requests(id),
    video_provider_profile_id       UUID NOT NULL REFERENCES provider_profiles(id),
    video_capability_snapshot_id    UUID NOT NULL REFERENCES provider_capability_snapshots(id),
    speech_provider_profile_id      UUID NOT NULL REFERENCES provider_profiles(id),
    speech_capability_snapshot_id   UUID NOT NULL REFERENCES provider_capability_snapshots(id),
    video_budget_approval_id        UUID NOT NULL UNIQUE REFERENCES review_tasks(id),
    speech_budget_approval_id       UUID NOT NULL UNIQUE REFERENCES review_tasks(id),
    g1_decision_id                  UUID NOT NULL UNIQUE REFERENCES approval_decisions(id),
    g2_decision_id                  UUID NOT NULL UNIQUE REFERENCES approval_decisions(id),
    safety_decision_id              UUID NOT NULL UNIQUE REFERENCES approval_decisions(id),
    offline_package_hash            CHAR(64) NOT NULL CHECK (offline_package_hash ~ '^[0-9a-f]{64}$'),
    offline_execution_package_hash  CHAR(64) NOT NULL CHECK (offline_execution_package_hash ~ '^[0-9a-f]{64}$'),
    source_authorization_hash       CHAR(64) NOT NULL CHECK (source_authorization_hash ~ '^[0-9a-f]{64}$'),
    source_authorization            JSONB NOT NULL,
    source_authorized_commit        CHAR(40) NOT NULL CHECK (source_authorized_commit ~ '^[0-9a-f]{40}$'),
    source_code_commit              CHAR(40) NOT NULL CHECK (source_code_commit ~ '^[0-9a-f]{40}$'),
    source_authorization_valid_until TIMESTAMPTZ NOT NULL,
    live_plan_hash                  CHAR(64) NOT NULL CHECK (live_plan_hash ~ '^[0-9a-f]{64}$'),
    live_execution_package_hash     CHAR(64) NOT NULL CHECK (live_execution_package_hash ~ '^[0-9a-f]{64}$'),
    created_at                      TIMESTAMPTZ NOT NULL,
    UNIQUE (batch_id, source_code_commit)
);

CREATE TABLE stage1_live_activation_runs (
    activation_id          UUID NOT NULL REFERENCES stage1_live_activations(id),
    ordinal                INTEGER NOT NULL CHECK (ordinal BETWEEN 1 AND 10),
    run_id                 UUID NOT NULL UNIQUE REFERENCES generation_runs(id),
    offline_run_id         UUID NOT NULL REFERENCES generation_runs(id),
    shot_spec_revision_id  UUID NOT NULL REFERENCES shot_spec_revisions(id),
    prompt_snapshot_id     UUID NOT NULL REFERENCES prompt_snapshots(id),
    prompt_snapshot_hash   CHAR(64) NOT NULL CHECK (prompt_snapshot_hash ~ '^[0-9a-f]{64}$'),
    authorized_prompt_hash CHAR(64) NOT NULL CHECK (authorized_prompt_hash ~ '^[0-9a-f]{64}$'),
    intent_input_hash      CHAR(64) NOT NULL CHECK (intent_input_hash ~ '^[0-9a-f]{64}$'),
    estimated_video_tokens BIGINT NOT NULL CHECK (estimated_video_tokens > 0),
    predicted_afp_milli    BIGINT NOT NULL CHECK (predicted_afp_milli > 0),
    PRIMARY KEY (activation_id, ordinal),
    UNIQUE (activation_id, offline_run_id),
    UNIQUE (activation_id, shot_spec_revision_id),
    UNIQUE (activation_id, prompt_snapshot_id)
);

CREATE TABLE stage1_live_projection_seals (
    activation_id  UUID PRIMARY KEY REFERENCES stage1_live_activations(id),
    projection_hash CHAR(64) NOT NULL CHECK (projection_hash ~ '^[0-9a-f]{64}$'),
    sealed_at       TIMESTAMPTZ NOT NULL,
    sealed_by       TEXT NOT NULL
);

CREATE TABLE stage1_live_submit_authorizations (
    activation_id             UUID PRIMARY KEY REFERENCES stage1_live_activations(id),
    authorization_hash        CHAR(64) NOT NULL CHECK (authorization_hash ~ '^[0-9a-f]{64}$'),
    authorization_payload     JSONB NOT NULL,
    source_code_commit        CHAR(40) NOT NULL CHECK (source_code_commit ~ '^[0-9a-f]{40}$'),
    execution_package_hash    CHAR(64) NOT NULL CHECK (execution_package_hash ~ '^[0-9a-f]{64}$'),
    projection_hash           CHAR(64) NOT NULL CHECK (projection_hash ~ '^[0-9a-f]{64}$'),
    actor_id                  TEXT NOT NULL,
    issued_at                 TIMESTAMPTZ NOT NULL,
    valid_until               TIMESTAMPTZ NOT NULL CHECK (valid_until > issued_at),
    created_at                TIMESTAMPTZ NOT NULL
);

CREATE TABLE stage1_agent_plan_quota_snapshots (
    id                          UUID PRIMARY KEY,
    activation_id               UUID NOT NULL REFERENCES stage1_live_activations(id),
    run_id                      UUID NOT NULL REFERENCES generation_runs(id),
    snapshot_hash               CHAR(64) NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
    source                      TEXT NOT NULL,
    captured_at                 TIMESTAMPTZ NOT NULL,
    recorded_at                 TIMESTAMPTZ NOT NULL,
    account_id                  TEXT NOT NULL,
    profile                     TEXT NOT NULL,
    region                      TEXT NOT NULL,
    billing_mode                TEXT NOT NULL CHECK (billing_mode = 'subscription_included_only'),
    five_hour_used_afp_milli    BIGINT NOT NULL CHECK (five_hour_used_afp_milli >= 0),
    five_hour_total_afp_milli   BIGINT NOT NULL CHECK (five_hour_total_afp_milli > 0),
    weekly_used_afp_milli       BIGINT NOT NULL CHECK (weekly_used_afp_milli >= 0),
    weekly_total_afp_milli      BIGINT NOT NULL CHECK (weekly_total_afp_milli > 0),
    monthly_used_afp_milli      BIGINT NOT NULL CHECK (monthly_used_afp_milli >= 0),
    monthly_total_afp_milli     BIGINT NOT NULL CHECK (monthly_total_afp_milli > 0),
    external_reserved_afp_milli BIGINT NOT NULL CHECK (external_reserved_afp_milli >= 0),
    CHECK (five_hour_used_afp_milli <= five_hour_total_afp_milli),
    CHECK (weekly_used_afp_milli <= weekly_total_afp_milli),
    CHECK (monthly_used_afp_milli <= monthly_total_afp_milli)
);

CREATE TABLE stage1_agent_plan_afp_reservations (
    id                          UUID PRIMARY KEY,
    activation_id               UUID NOT NULL UNIQUE REFERENCES stage1_live_activations(id),
    quota_snapshot_id           UUID NOT NULL REFERENCES stage1_agent_plan_quota_snapshots(id),
    account_id                  TEXT NOT NULL,
    profile                     TEXT NOT NULL,
    region                      TEXT NOT NULL,
    video_afp_milli             BIGINT NOT NULL CHECK (video_afp_milli = 30306870),
    speech_afp_milli            BIGINT NOT NULL CHECK (speech_afp_milli = 1039),
    total_afp_milli             BIGINT NOT NULL CHECK (total_afp_milli = video_afp_milli + speech_afp_milli),
    account_cap_afp_milli       BIGINT NOT NULL CHECK (account_cap_afp_milli = 135000000),
    monthly_used_afp_milli      BIGINT NOT NULL CHECK (monthly_used_afp_milli >= 0),
    external_reserved_afp_milli BIGINT NOT NULL CHECK (external_reserved_afp_milli >= 0),
    status                      TEXT NOT NULL CHECK (status IN ('RESERVED', 'SETTLED', 'RELEASED')),
    reserved_at                 TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_stage1_quota_activation_captured
    ON stage1_agent_plan_quota_snapshots (activation_id, captured_at DESC);
CREATE INDEX ix_stage1_afp_reservation_account
    ON stage1_agent_plan_afp_reservations (account_id, profile, region, status);

CREATE TRIGGER stage1_live_activation_immutable_update
    BEFORE UPDATE ON stage1_live_activations
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_activation_run_immutable_update
    BEFORE UPDATE ON stage1_live_activation_runs
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_projection_seal_immutable_update
    BEFORE UPDATE ON stage1_live_projection_seals
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_submit_authorization_immutable_update
    BEFORE UPDATE ON stage1_live_submit_authorizations
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_quota_snapshot_immutable_update
    BEFORE UPDATE ON stage1_agent_plan_quota_snapshots
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_afp_reservation_immutable_update
    BEFORE UPDATE ON stage1_agent_plan_afp_reservations
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');

CREATE TRIGGER stage1_live_activation_immutable_delete
    BEFORE DELETE ON stage1_live_activations FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_activation_run_immutable_delete
    BEFORE DELETE ON stage1_live_activation_runs FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_projection_seal_immutable_delete
    BEFORE DELETE ON stage1_live_projection_seals FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_submit_authorization_immutable_delete
    BEFORE DELETE ON stage1_live_submit_authorizations FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_quota_snapshot_immutable_delete
    BEFORE DELETE ON stage1_agent_plan_quota_snapshots FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_afp_reservation_immutable_delete
    BEFORE DELETE ON stage1_agent_plan_afp_reservations FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();

COMMENT ON TABLE stage1_live_activations IS
    'Independent A-only live authority projection; never mutates the FLO-100 offline canonical seal.';
COMMENT ON TABLE stage1_live_submit_authorizations IS
    'Post-implementation authorization bound to the exact code, live package, and live projection hashes.';
COMMENT ON TABLE stage1_agent_plan_afp_reservations IS
    'Account-level Agent Plan AFP reservation, separate from the zero-CNY provider cost ledger.';
