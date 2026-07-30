SET search_path TO video_pipeline, public;

DROP TABLE IF EXISTS publication_locks;
DROP TABLE IF EXISTS prompt_snapshot_assets;
DROP TABLE IF EXISTS prompt_snapshot_inputs;

ALTER TABLE prompt_snapshots
    DROP COLUMN IF EXISTS input_revision_hashes,
    DROP COLUMN IF EXISTS output_spec,
    DROP COLUMN IF EXISTS previous_prompt_snapshot_id;

DROP TABLE IF EXISTS content_compilation_runs;
