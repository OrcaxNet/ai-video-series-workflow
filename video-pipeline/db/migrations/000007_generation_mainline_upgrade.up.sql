-- FLO-102 review hardening for databases that already applied migration 000006.
SET search_path TO video_pipeline, public;

ALTER TABLE prompt_snapshot_inputs
    DROP CONSTRAINT IF EXISTS prompt_snapshot_inputs_input_type_check;

ALTER TABLE prompt_snapshot_inputs
    ADD CONSTRAINT prompt_snapshot_inputs_input_type_check CHECK (input_type IN (
        'SOURCE', 'WORLD', 'CHARACTER', 'RELATIONSHIP', 'LOCATION', 'PROP',
        'EPISODE', 'SCENE', 'SHOT_SPEC', 'GENERATION_PROFILE', 'CONTEXT',
        'PREVIOUS_PROMPT', 'TAIL_FRAME'
    ));

-- One episode Manifest may legitimately cover several successful shot Runs.
ALTER TABLE publication_locks
    DROP CONSTRAINT IF EXISTS publication_locks_manifest_id_key;

