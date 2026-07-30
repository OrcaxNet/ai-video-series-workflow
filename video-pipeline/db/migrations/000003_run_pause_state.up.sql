SET search_path TO video_pipeline, public;

ALTER TABLE generation_runs
    DROP CONSTRAINT generation_runs_state_check,
    ADD CONSTRAINT generation_runs_state_check
        CHECK (state IN (
            'DRAFT', 'VALIDATED', 'QUEUED', 'RUNNING', 'PAUSED', 'UNKNOWN',
            'RECONCILING', 'REQUIRES_ACTION', 'CANCEL_REQUESTED', 'CANCELLED',
            'SUCCEEDED', 'FAILED'
        ));

DROP INDEX ux_generation_runs_active_digest;
CREATE UNIQUE INDEX ux_generation_runs_active_digest
    ON generation_runs (run_spec_digest)
    WHERE state IN (
        'VALIDATED', 'QUEUED', 'RUNNING', 'PAUSED', 'UNKNOWN',
        'RECONCILING', 'REQUIRES_ACTION', 'SUCCEEDED'
    );
