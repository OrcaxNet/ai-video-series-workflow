-- FLO-102: executable content compilation, prompt input lineage, and G3 lock.
SET search_path TO video_pipeline, public;

CREATE TABLE content_compilation_runs (
    id                    UUID PRIMARY KEY,
    series_id             UUID NOT NULL REFERENCES series(id),
    source_revision_id    UUID NOT NULL REFERENCES source_revisions(id),
    stage                 TEXT NOT NULL CHECK (stage IN ('STRUCTURE', 'EPISODES', 'SCENES', 'SHOTS')),
    generator_job_id      UUID REFERENCES provider_jobs(id),
    generator_model       JSONB NOT NULL,
    input_hash            CHAR(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    output_revision_ids   UUID[] NOT NULL DEFAULT '{}',
    output_hash           CHAR(64) CHECK (output_hash ~ '^[0-9a-f]{64}$'),
    evidence_ids          UUID[] NOT NULL DEFAULT '{}',
    state                 TEXT NOT NULL CHECK (state IN ('VALIDATED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'REQUIRES_ACTION')),
    error_code            TEXT,
    trace_id              TEXT NOT NULL,
    created_by            TEXT NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at           TIMESTAMPTZ,
    UNIQUE (source_revision_id, stage, input_hash),
    CHECK (
        (state = 'SUCCEEDED' AND output_hash IS NOT NULL AND cardinality(output_revision_ids) > 0)
        OR state <> 'SUCCEEDED'
    ),
    CHECK (generator_model::text !~* '"(authorization|api[_-]?key|access[_-]?token|cookie|secret)"[[:space:]]*:')
);

ALTER TABLE prompt_snapshots
    ADD COLUMN previous_prompt_snapshot_id UUID REFERENCES prompt_snapshots(id),
    ADD COLUMN output_spec JSONB NOT NULL DEFAULT '{}',
    ADD COLUMN input_revision_hashes JSONB NOT NULL DEFAULT '{}';

CREATE TABLE prompt_snapshot_inputs (
    prompt_snapshot_id UUID NOT NULL REFERENCES prompt_snapshots(id),
    input_type         TEXT NOT NULL CHECK (input_type IN (
        'SOURCE', 'WORLD', 'CHARACTER', 'RELATIONSHIP', 'LOCATION', 'PROP',
        'EPISODE', 'SCENE', 'SHOT_SPEC', 'GENERATION_PROFILE', 'CONTEXT',
        'PREVIOUS_PROMPT', 'TAIL_FRAME'
    )),
    input_revision_id  UUID NOT NULL,
    input_hash         CHAR(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    dependency_role    TEXT NOT NULL,
    PRIMARY KEY (prompt_snapshot_id, input_type, input_revision_id, dependency_role)
);

CREATE TABLE prompt_snapshot_assets (
    prompt_snapshot_id UUID NOT NULL REFERENCES prompt_snapshots(id),
    alias               TEXT NOT NULL,
    asset_version_id    UUID NOT NULL REFERENCES asset_versions(id),
    asset_hash          CHAR(64) NOT NULL CHECK (asset_hash ~ '^[0-9a-f]{64}$'),
    provider_role       TEXT NOT NULL CHECK (provider_role IN (
        'reference_image', 'reference_video', 'reference_audio', 'first_frame', 'last_frame'
    )),
    PRIMARY KEY (prompt_snapshot_id, alias),
    UNIQUE (prompt_snapshot_id, asset_version_id, provider_role)
);

CREATE TABLE publication_locks (
    id                 UUID PRIMARY KEY,
    generation_run_id  UUID NOT NULL UNIQUE REFERENCES generation_runs(id),
    manifest_id        UUID NOT NULL REFERENCES generation_manifests(id),
    manifest_hash      CHAR(64) NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    qc_report_id       UUID NOT NULL UNIQUE REFERENCES qc_reports(id),
    qc_report_hash     CHAR(64) NOT NULL CHECK (qc_report_hash ~ '^[0-9a-f]{64}$'),
    gate3_decision_id  UUID NOT NULL REFERENCES approval_decisions(id),
    locked_by          TEXT NOT NULL,
    locked_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_content_compilation_reconcile
    ON content_compilation_runs (state, created_at)
    WHERE state IN ('RUNNING', 'REQUIRES_ACTION');
CREATE INDEX ix_prompt_inputs_revision
    ON prompt_snapshot_inputs (input_type, input_revision_id);
CREATE INDEX ix_prompt_assets_version
    ON prompt_snapshot_assets (asset_version_id);

COMMENT ON TABLE content_compilation_runs IS
    'Provider-neutral structured text generation attempts; source text and credentials are not copied into this ledger.';
COMMENT ON TABLE prompt_snapshot_inputs IS
    'Exact revision/hash dependency set used by deterministic stale propagation and prompt diffs.';
COMMENT ON TABLE publication_locks IS
    'A successful manifested run becomes publishable only after passing QC and a G3 decision bound to the exact manifest hash.';
