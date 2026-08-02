SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage1_live_activations) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'refusing FLO-100 live activation rollback while immutable live projections exist',
            HINT = 'disable live execution and retain migration 8; do not orphan LIVE profiles, plans, or approval bindings';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS stage1_afp_reservation_immutable_delete ON stage1_agent_plan_afp_reservations;
DROP TRIGGER IF EXISTS stage1_quota_snapshot_immutable_delete ON stage1_agent_plan_quota_snapshots;
DROP TRIGGER IF EXISTS stage1_live_submit_authorization_immutable_delete ON stage1_live_submit_authorizations;
DROP TRIGGER IF EXISTS stage1_live_projection_seal_immutable_delete ON stage1_live_projection_seals;
DROP TRIGGER IF EXISTS stage1_live_activation_run_immutable_delete ON stage1_live_activation_runs;
DROP TRIGGER IF EXISTS stage1_live_activation_immutable_delete ON stage1_live_activations;
DROP TRIGGER IF EXISTS stage1_afp_reservation_immutable_update ON stage1_agent_plan_afp_reservations;
DROP TRIGGER IF EXISTS stage1_quota_snapshot_immutable_update ON stage1_agent_plan_quota_snapshots;
DROP TRIGGER IF EXISTS stage1_live_submit_authorization_immutable_update ON stage1_live_submit_authorizations;
DROP TRIGGER IF EXISTS stage1_live_projection_seal_immutable_update ON stage1_live_projection_seals;
DROP TRIGGER IF EXISTS stage1_live_activation_run_immutable_update ON stage1_live_activation_runs;
DROP TRIGGER IF EXISTS stage1_live_activation_immutable_update ON stage1_live_activations;

DROP TABLE IF EXISTS stage1_agent_plan_afp_reservations;
DROP TABLE IF EXISTS stage1_agent_plan_quota_snapshots;
DROP TABLE IF EXISTS stage1_live_submit_authorizations;
DROP TABLE IF EXISTS stage1_live_projection_seals;
DROP TABLE IF EXISTS stage1_live_activation_runs;
DROP TABLE IF EXISTS stage1_live_activations;
