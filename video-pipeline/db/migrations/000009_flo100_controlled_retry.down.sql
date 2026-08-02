SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage1_live_controlled_retries) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'refusing FLO-100 controlled retry rollback while immutable retry authorities exist',
            HINT = 'retain migration 9 so retry approval and terminal-failure evidence remain auditable';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS stage1_live_controlled_retry_immutable_delete ON stage1_live_controlled_retries;
DROP TRIGGER IF EXISTS stage1_live_controlled_retry_immutable_update ON stage1_live_controlled_retries;
DROP TABLE IF EXISTS stage1_live_controlled_retries;
