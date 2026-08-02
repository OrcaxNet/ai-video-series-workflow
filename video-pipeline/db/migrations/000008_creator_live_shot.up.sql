-- FLO-161: subscription-only Studio live-shot intent and evidence ledger.
SET search_path TO video_pipeline, public;

CREATE TABLE creator_live_shot_plans (
    id                              UUID PRIMARY KEY,
    series_id                       UUID NOT NULL REFERENCES series(id),
    source_revision_id              UUID NOT NULL REFERENCES source_revisions(id),
    episode_revision_id             UUID NOT NULL REFERENCES episode_revisions(id),
    scene_revision_id               UUID NOT NULL REFERENCES scene_revisions(id),
    shot_spec_revision_id           UUID NOT NULL REFERENCES shot_spec_revisions(id),
    prompt_snapshot_id              UUID NOT NULL REFERENCES prompt_snapshots(id),
    generation_profile_revision_id  UUID NOT NULL REFERENCES generation_profiles(id),
    generation_plan_id              UUID NOT NULL REFERENCES operation_requests(id),
    budget_approval_id              UUID NOT NULL REFERENCES review_tasks(id),
    safety_decision_id              UUID NOT NULL REFERENCES approval_decisions(id),
    provider_profile_id             UUID NOT NULL REFERENCES provider_profiles(id),
    title                           TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 80),
    scene_text                      TEXT NOT NULL CHECK (char_length(scene_text) BETWEEN 1 AND 800),
    scene_text_hash                 CHAR(64) NOT NULL CHECK (scene_text_hash ~ '^[0-9a-f]{64}$'),
    source_artifact_uri             TEXT NOT NULL CHECK (source_artifact_uri LIKE 'cas://sha256/%'),
    aspect_ratio                    TEXT NOT NULL CHECK (aspect_ratio IN ('16:9', '9:16')),
    output_spec                     JSONB NOT NULL,
    context_snapshot                JSONB NOT NULL,
    rights_snapshot                 JSONB NOT NULL,
    safety_snapshot                 JSONB NOT NULL,
    generation_profile              JSONB NOT NULL,
    execution_policy                JSONB NOT NULL,
    route_snapshot                  JSONB NOT NULL,
    capability_snapshot             JSONB NOT NULL,
    project_tasks_used              INTEGER NOT NULL CHECK (project_tasks_used BETWEEN 0 AND 3),
    project_video_tokens_used       BIGINT NOT NULL CHECK (project_video_tokens_used BETWEEN 0 AND 3000000),
    project_active_runs             INTEGER NOT NULL CHECK (project_active_runs BETWEEN 0 AND 1),
    provider_call_count             INTEGER NOT NULL CHECK (provider_call_count = 1),
    provider_submit_count           INTEGER NOT NULL CHECK (provider_submit_count = 0),
    plan_hash                       CHAR(64) NOT NULL CHECK (plan_hash ~ '^[0-9a-f]{64}$'),
    projection                      JSONB NOT NULL,
    state                           TEXT NOT NULL CHECK (state IN ('READY', 'CONFIRMED', 'EXPIRED')),
    actor_id                        TEXT NOT NULL,
    expires_at                      TIMESTAMPTZ NOT NULL,
    confirmed_at                    TIMESTAMPTZ,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (shot_spec_revision_id),
    CHECK (capability_snapshot::text !~* '"(authorization|api[_-]?key|access[_-]?token|cookie|secret|signed[_-]?url)"[[:space:]]*:'),
    CHECK (projection::text !~* '"(authorization|api[_-]?key|access[_-]?token|cookie|secret|signed[_-]?url)"[[:space:]]*:')
);

CREATE TABLE creator_live_shot_runs (
    id                  UUID PRIMARY KEY,
    plan_id             UUID NOT NULL UNIQUE REFERENCES creator_live_shot_plans(id),
    operation_id        UUID NOT NULL UNIQUE REFERENCES operation_requests(id),
    provider_job_id     UUID NOT NULL UNIQUE,
    workflow_id         TEXT NOT NULL UNIQUE,
    run_spec_digest     CHAR(64) NOT NULL CHECK (run_spec_digest ~ '^[0-9a-f]{64}$'),
    request_hash        CHAR(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    request_snapshot    JSONB NOT NULL,
    reservation_id      UUID NOT NULL UNIQUE,
    reserved_tasks      INTEGER NOT NULL CHECK (reserved_tasks = 1),
    reserved_video_tokens BIGINT NOT NULL CHECK (reserved_video_tokens BETWEEN 1 AND 1000000),
    submit_count        INTEGER NOT NULL DEFAULT 0 CHECK (submit_count BETWEEN 0 AND 1),
    progress            INTEGER CHECK (progress BETWEEN 0 AND 100),
    state               TEXT NOT NULL CHECK (state IN (
        'QUEUED','RUNNING','UNKNOWN','RECONCILING','SUCCEEDED','FAILED','CANCELLED','REQUIRES_ACTION'
    )),
    upstream_task_id    TEXT,
    upstream_request_id TEXT,
    error_code          TEXT,
    output_hash         CHAR(64) CHECK (output_hash ~ '^[0-9a-f]{64}$'),
    output_media_type   TEXT,
    output_size_bytes   BIGINT CHECK (output_size_bytes >= 0),
    output_width        INTEGER,
    output_height       INTEGER,
    output_duration_ms  BIGINT,
    usage               JSONB NOT NULL DEFAULT '{}',
    cash_cost           JSONB NOT NULL DEFAULT '{"amountMicros":null,"verified":false,"billingMode":"subscription"}',
    manifest            JSONB,
    manifest_hash       CHAR(64) CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    trace_id            TEXT NOT NULL,
    actor_id            TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    terminal_at         TIMESTAMPTZ,
    CHECK (request_snapshot::text !~* '"(authorization|api[_-]?key|access[_-]?token|cookie|secret|signed[_-]?url)"[[:space:]]*:'),
    CHECK ((state = 'SUCCEEDED' AND output_hash IS NOT NULL AND manifest IS NOT NULL AND manifest_hash IS NOT NULL)
        OR state <> 'SUCCEEDED'),
    UNIQUE (run_spec_digest)
);

CREATE INDEX ix_creator_live_shot_runs_series_state
    ON creator_live_shot_runs (state, created_at);

CREATE TABLE creator_live_shot_idempotency (
    scope             TEXT NOT NULL,
    idempotency_key   UUID NOT NULL,
    request_hash      CHAR(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    response_status   INTEGER,
    response_body     JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, idempotency_key)
);

CREATE TRIGGER creator_live_shot_plan_immutable_update
    BEFORE UPDATE ON creator_live_shot_plans
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('state', 'confirmed_at');

CREATE TRIGGER creator_live_shot_run_immutable_update
    BEFORE UPDATE ON creator_live_shot_runs
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update(
        'state', 'submit_count', 'progress', 'upstream_task_id', 'upstream_request_id', 'error_code',
        'output_hash', 'output_media_type', 'output_size_bytes', 'output_width',
        'output_height', 'output_duration_ms', 'usage', 'cash_cost', 'manifest',
        'manifest_hash', 'updated_at', 'terminal_at'
    );

COMMENT ON TABLE creator_live_shot_plans IS
    'Zero-submit, 15-minute immutable Studio plan with server-derived production bindings.';
COMMENT ON TABLE creator_live_shot_runs IS
    'Subscription task/token reservation, provider intent, CAS output, and live-provider manifest in one monotonic projection.';
