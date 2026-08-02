-- FLO-165: immutable, one-shot controlled retry authority for FLO-100 Batch A.
SET search_path TO video_pipeline, public;

CREATE TABLE stage1_live_controlled_retries (
    activation_id                    UUID PRIMARY KEY REFERENCES stage1_live_activations(id),
    primary_run_id                   UUID NOT NULL UNIQUE REFERENCES generation_runs(id),
    primary_attempt_id               UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    primary_provider_job_id          UUID NOT NULL UNIQUE REFERENCES provider_jobs(id),
    primary_attempt_identity         TEXT NOT NULL,
    primary_failure_class            TEXT NOT NULL,
    primary_failure_evidence_hash    CHAR(64) NOT NULL CHECK (primary_failure_evidence_hash ~ '^[0-9a-f]{64}$'),
    retry_run_id                     UUID NOT NULL UNIQUE REFERENCES generation_runs(id),
    retry_attempt_id                 UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    retry_approval_id                UUID NOT NULL UNIQUE,
    duplicate_task_evidence_id       UUID NOT NULL UNIQUE,
    controlled_retry_package_hash    CHAR(64) NOT NULL UNIQUE CHECK (controlled_retry_package_hash ~ '^[0-9a-f]{64}$'),
    controlled_retry_package         JSONB NOT NULL,
    source_execution_package_hash    CHAR(64) NOT NULL CHECK (source_execution_package_hash ~ '^[0-9a-f]{64}$'),
    created_by                       TEXT NOT NULL,
    created_at                       TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER stage1_live_controlled_retry_immutable_update
    BEFORE UPDATE ON stage1_live_controlled_retries
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_controlled_retry_immutable_delete
    BEFORE DELETE ON stage1_live_controlled_retries
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
