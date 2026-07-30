SET search_path TO video_pipeline, public;

-- A rollback cannot preserve a state unknown to v2. PAUSED runs are moved to
-- explicit recovery so the previous worker reconciles rather than dispatching
-- a duplicate provider task.
UPDATE generation_runs
SET state = 'RECONCILING',
    failure_class = 'INFRASTRUCTURE',
    failure_code = 'ROLLBACK_FROM_PAUSED'
WHERE state = 'PAUSED';

DROP INDEX ux_generation_runs_active_digest;
CREATE UNIQUE INDEX ux_generation_runs_active_digest
    ON generation_runs (run_spec_digest)
    WHERE state IN (
        'VALIDATED', 'QUEUED', 'RUNNING', 'UNKNOWN',
        'RECONCILING', 'REQUIRES_ACTION', 'SUCCEEDED'
    );

ALTER TABLE generation_runs
    DROP CONSTRAINT generation_runs_state_check,
    ADD CONSTRAINT generation_runs_state_check
        CHECK (state IN (
            'DRAFT', 'VALIDATED', 'QUEUED', 'RUNNING', 'UNKNOWN',
            'RECONCILING', 'REQUIRES_ACTION', 'CANCEL_REQUESTED', 'CANCELLED',
            'SUCCEEDED', 'FAILED'
        ));
