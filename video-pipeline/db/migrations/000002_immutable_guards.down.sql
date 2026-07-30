SET search_path TO video_pipeline, public;

DROP TRIGGER IF EXISTS artifact_retention_delete ON artifacts;
DROP TRIGGER IF EXISTS generation_manifest_immutable_delete ON generation_manifests;
DROP TRIGGER IF EXISTS audit_event_immutable_delete ON audit_events;
DROP TRIGGER IF EXISTS approval_binding_immutable_delete ON approval_bindings;
DROP TRIGGER IF EXISTS approval_decision_immutable_delete ON approval_decisions;
DROP TRIGGER IF EXISTS prompt_snapshot_immutable_delete ON prompt_snapshots;
DROP TRIGGER IF EXISTS effective_context_snapshot_immutable_delete ON effective_context_snapshots;
DROP TRIGGER IF EXISTS shot_spec_revision_immutable_delete ON shot_spec_revisions;
DROP TRIGGER IF EXISTS storyboard_revision_immutable_delete ON storyboard_revisions;
DROP TRIGGER IF EXISTS episode_script_revision_immutable_delete ON episode_script_revisions;
DROP TRIGGER IF EXISTS asset_version_immutable_delete ON asset_versions;
DROP TRIGGER IF EXISTS context_revision_immutable_delete ON context_revisions;
DROP TRIGGER IF EXISTS entity_revision_immutable_delete ON entity_revisions;
DROP TRIGGER IF EXISTS scene_revision_immutable_delete ON scene_revisions;
DROP TRIGGER IF EXISTS episode_revision_immutable_delete ON episode_revisions;
DROP TRIGGER IF EXISTS source_revision_immutable_delete ON source_revisions;

DROP TRIGGER IF EXISTS generation_manifest_lock_transition ON generation_manifests;
DROP TRIGGER IF EXISTS generation_manifest_immutable_update ON generation_manifests;
DROP TRIGGER IF EXISTS audit_event_immutable_update ON audit_events;
DROP TRIGGER IF EXISTS approval_binding_immutable_update ON approval_bindings;
DROP TRIGGER IF EXISTS approval_decision_immutable_update ON approval_decisions;
DROP TRIGGER IF EXISTS prompt_snapshot_immutable_update ON prompt_snapshots;
DROP TRIGGER IF EXISTS effective_context_snapshot_immutable_update ON effective_context_snapshots;
DROP TRIGGER IF EXISTS shot_spec_revision_immutable_update ON shot_spec_revisions;
DROP TRIGGER IF EXISTS storyboard_revision_immutable_update ON storyboard_revisions;
DROP TRIGGER IF EXISTS episode_script_revision_immutable_update ON episode_script_revisions;
DROP TRIGGER IF EXISTS asset_version_immutable_update ON asset_versions;
DROP TRIGGER IF EXISTS context_revision_immutable_update ON context_revisions;
DROP TRIGGER IF EXISTS entity_revision_immutable_update ON entity_revisions;
DROP TRIGGER IF EXISTS scene_revision_immutable_update ON scene_revisions;
DROP TRIGGER IF EXISTS episode_revision_immutable_update ON episode_revisions;
DROP TRIGGER IF EXISTS source_revision_immutable_update ON source_revisions;

DROP FUNCTION IF EXISTS guard_manifest_lock_update();
DROP FUNCTION IF EXISTS guard_artifact_delete();
DROP FUNCTION IF EXISTS reject_immutable_delete();
DROP FUNCTION IF EXISTS guard_immutable_payload_update();

ALTER TABLE generation_manifests
    DROP CONSTRAINT IF EXISTS ck_manifest_lock_has_gate,
    DROP CONSTRAINT IF EXISTS ck_manifest_no_secret_fields;

ALTER TABLE artifacts
    DROP CONSTRAINT IF EXISTS ck_artifact_orphan_retention,
    DROP COLUMN IF EXISTS retention_until,
    DROP COLUMN IF EXISTS orphaned_at;
