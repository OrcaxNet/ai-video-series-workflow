-- FLO-172: bind the single Batch A controlled retry to the immutable FLO-167
-- supersession, its exact authorization, and the failed primary submission.
SET search_path TO video_pipeline, public;

CREATE TABLE stage1_live_supersession_controlled_retries (
    supersession_id UUID PRIMARY KEY REFERENCES stage1_live_supersessions(id),
    activation_id UUID NOT NULL UNIQUE REFERENCES stage1_live_controlled_retries(activation_id),
    shot_id TEXT NOT NULL CHECK (shot_id IN ('GOLD-A02','GOLD-A03','GOLD-A04','GOLD-A05','GOLD-A06','GOLD-A07','GOLD-A08','GOLD-A09','GOLD-A10')),
    primary_attempt_id UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    retry_attempt_id UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    retry_approval_id UUID NOT NULL UNIQUE,
    duplicate_task_evidence_id UUID NOT NULL UNIQUE,
    controlled_retry_package_hash CHAR(64) NOT NULL UNIQUE CHECK (controlled_retry_package_hash ~ '^[0-9a-f]{64}$'),
    supersession_package_hash CHAR(64) NOT NULL CHECK (supersession_package_hash ~ '^[0-9a-f]{64}$'),
    canonical_projection_hash CHAR(64) NOT NULL CHECK (canonical_projection_hash ~ '^[0-9a-f]{64}$'),
    authorization_hash CHAR(64) NOT NULL CHECK (authorization_hash ~ '^[0-9a-f]{64}$'),
    pricing_snapshot_digest CHAR(64) NOT NULL CHECK (pricing_snapshot_digest ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (supersession_id, shot_id)
        REFERENCES stage1_live_supersession_submissions(supersession_id, shot_id)
);

CREATE TABLE stage1_live_supersession_controlled_retry_submissions (
    supersession_id UUID PRIMARY KEY REFERENCES stage1_live_supersession_controlled_retries(supersession_id),
    retry_attempt_id UUID NOT NULL UNIQUE REFERENCES generation_attempts(id),
    quota_snapshot_id UUID NOT NULL REFERENCES stage1_agent_plan_quota_snapshots(id),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TRIGGER stage1_live_supersession_retry_immutable_update
    BEFORE UPDATE ON stage1_live_supersession_controlled_retries
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_retry_immutable_delete
    BEFORE DELETE ON stage1_live_supersession_controlled_retries
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER stage1_live_supersession_retry_submission_immutable_update
    BEFORE UPDATE ON stage1_live_supersession_controlled_retry_submissions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER stage1_live_supersession_retry_submission_immutable_delete
    BEFORE DELETE ON stage1_live_supersession_controlled_retry_submissions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
