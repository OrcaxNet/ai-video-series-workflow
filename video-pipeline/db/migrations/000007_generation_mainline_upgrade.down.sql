SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM prompt_snapshot_inputs
        WHERE input_type = 'GENERATION_PROFILE'
    ) THEN
        RAISE EXCEPTION
            'cannot roll back generation mainline upgrade while GENERATION_PROFILE lineage exists';
    END IF;
END
$$;

ALTER TABLE prompt_snapshot_inputs
    DROP CONSTRAINT IF EXISTS prompt_snapshot_inputs_input_type_check;

ALTER TABLE prompt_snapshot_inputs
    ADD CONSTRAINT prompt_snapshot_inputs_input_type_check CHECK (input_type IN (
        'SOURCE', 'WORLD', 'CHARACTER', 'RELATIONSHIP', 'LOCATION', 'PROP',
        'EPISODE', 'SCENE', 'SHOT_SPEC', 'CONTEXT',
        'PREVIOUS_PROMPT', 'TAIL_FRAME'
    ));

-- This intentionally fails when more than one Run locked the same Manifest;
-- operators must preserve the v7 schema or remove only disposable test data.
ALTER TABLE publication_locks
    ADD CONSTRAINT publication_locks_manifest_id_key UNIQUE (manifest_id);

