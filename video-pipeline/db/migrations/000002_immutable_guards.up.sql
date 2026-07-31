SET search_path TO video_pipeline, public;

-- CAS deletion is a two-step, time-bounded operation. Existing foreign keys
-- remain the final protection for referenced bytes.
ALTER TABLE artifacts
    ADD COLUMN orphaned_at TIMESTAMPTZ,
    ADD COLUMN retention_until TIMESTAMPTZ,
    ADD CONSTRAINT ck_artifact_orphan_retention
        CHECK (
            status <> 'ORPHAN_CANDIDATE'
            OR (orphaned_at IS NOT NULL AND retention_until IS NOT NULL AND retention_until >= orphaned_at)
        );

ALTER TABLE generation_manifests
    ADD CONSTRAINT ck_manifest_no_secret_fields
        CHECK (
            payload::text !~* '"(authorization|proxy-authorization|api[_-]?key|access[_-]?token|cookie|credential|signed[_-]?url)"[[:space:]]*:'
        ),
    ADD CONSTRAINT ck_manifest_lock_has_gate
        CHECK (locked_at IS NULL OR gate_decision_id IS NOT NULL);

CREATE FUNCTION guard_immutable_payload_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    old_payload JSONB := to_jsonb(OLD);
    new_payload JSONB := to_jsonb(NEW);
    allowed_column TEXT;
BEGIN
    FOREACH allowed_column IN ARRAY TG_ARGV LOOP
        old_payload := old_payload - allowed_column;
        new_payload := new_payload - allowed_column;
    END LOOP;
    IF old_payload IS DISTINCT FROM new_payload THEN
        RAISE EXCEPTION '% immutable payload cannot be updated', TG_TABLE_NAME
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION reject_immutable_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% immutable record cannot be deleted', TG_TABLE_NAME
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE FUNCTION guard_artifact_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status <> 'ORPHAN_CANDIDATE'
       OR OLD.orphaned_at IS NULL
       OR OLD.retention_until IS NULL
       OR OLD.retention_until > now() THEN
        RAISE EXCEPTION 'artifact is active or inside its retention window'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN OLD;
END;
$$;

CREATE FUNCTION guard_manifest_lock_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.locked_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'locked generation manifest cannot be updated'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.locked_at IS NULL
       AND NEW.gate_decision_id IS NOT NULL
       AND NEW.locked_at IS NULL THEN
        RAISE EXCEPTION 'manifest gate decision and lock timestamp must commit together'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_revision_immutable_update
    BEFORE UPDATE ON source_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER episode_revision_immutable_update
    BEFORE UPDATE ON episode_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER scene_revision_immutable_update
    BEFORE UPDATE ON scene_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER entity_revision_immutable_update
    BEFORE UPDATE ON entity_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER context_revision_immutable_update
    BEFORE UPDATE ON context_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER asset_version_immutable_update
    BEFORE UPDATE ON asset_versions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status', 'approval_decision_id', 'generation_run_id', 'archived_at');
CREATE TRIGGER episode_script_revision_immutable_update
    BEFORE UPDATE ON episode_script_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER storyboard_revision_immutable_update
    BEFORE UPDATE ON storyboard_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('status');
CREATE TRIGGER shot_spec_revision_immutable_update
    BEFORE UPDATE ON shot_spec_revisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('lifecycle_state', 'freshness');
CREATE TRIGGER effective_context_snapshot_immutable_update
    BEFORE UPDATE ON effective_context_snapshots
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER prompt_snapshot_immutable_update
    BEFORE UPDATE ON prompt_snapshots
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER approval_decision_immutable_update
    BEFORE UPDATE ON approval_decisions
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER approval_binding_immutable_update
    BEFORE UPDATE ON approval_bindings
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER audit_event_immutable_update
    BEFORE UPDATE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update();
CREATE TRIGGER generation_manifest_immutable_update
    BEFORE UPDATE ON generation_manifests
    FOR EACH ROW EXECUTE FUNCTION guard_immutable_payload_update('gate_decision_id', 'locked_at');
CREATE TRIGGER generation_manifest_lock_transition
    BEFORE UPDATE ON generation_manifests
    FOR EACH ROW EXECUTE FUNCTION guard_manifest_lock_update();

CREATE TRIGGER source_revision_immutable_delete
    BEFORE DELETE ON source_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER episode_revision_immutable_delete
    BEFORE DELETE ON episode_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER scene_revision_immutable_delete
    BEFORE DELETE ON scene_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER entity_revision_immutable_delete
    BEFORE DELETE ON entity_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER context_revision_immutable_delete
    BEFORE DELETE ON context_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER asset_version_immutable_delete
    BEFORE DELETE ON asset_versions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER episode_script_revision_immutable_delete
    BEFORE DELETE ON episode_script_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER storyboard_revision_immutable_delete
    BEFORE DELETE ON storyboard_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER shot_spec_revision_immutable_delete
    BEFORE DELETE ON shot_spec_revisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER effective_context_snapshot_immutable_delete
    BEFORE DELETE ON effective_context_snapshots FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER prompt_snapshot_immutable_delete
    BEFORE DELETE ON prompt_snapshots FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER approval_decision_immutable_delete
    BEFORE DELETE ON approval_decisions FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER approval_binding_immutable_delete
    BEFORE DELETE ON approval_bindings FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER audit_event_immutable_delete
    BEFORE DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER generation_manifest_immutable_delete
    BEFORE DELETE ON generation_manifests FOR EACH ROW EXECUTE FUNCTION reject_immutable_delete();
CREATE TRIGGER artifact_retention_delete
    BEFORE DELETE ON artifacts FOR EACH ROW EXECUTE FUNCTION guard_artifact_delete();

COMMENT ON COLUMN artifacts.retention_until IS
    'Earliest physical deletion time for an explicit ORPHAN_CANDIDATE; FK references still block deletion.';
