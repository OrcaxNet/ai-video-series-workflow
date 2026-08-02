SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF to_regclass('video_pipeline.creator_live_shot_plans') IS NOT NULL
       AND EXISTS (SELECT 1 FROM creator_live_shot_plans) THEN
        RAISE EXCEPTION 'cannot roll back creator live-shot migration while immutable plans exist';
    END IF;
END;
$$;

DROP TABLE IF EXISTS creator_live_shot_idempotency;
DROP TABLE IF EXISTS creator_live_shot_runs;
DROP TABLE IF EXISTS creator_live_shot_plans;
