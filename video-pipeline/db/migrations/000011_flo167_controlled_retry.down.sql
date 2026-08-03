SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage1_live_supersession_controlled_retries)
       OR EXISTS (SELECT 1 FROM stage1_live_supersession_controlled_retry_submissions) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'refusing FLO-167 controlled retry rollback while immutable authorities exist',
            HINT = 'retain migration 11 so v3 retry approval and supersession lineage remain auditable';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS stage1_live_supersession_retry_submission_immutable_delete ON stage1_live_supersession_controlled_retry_submissions;
DROP TRIGGER IF EXISTS stage1_live_supersession_retry_submission_immutable_update ON stage1_live_supersession_controlled_retry_submissions;
DROP TRIGGER IF EXISTS stage1_live_supersession_retry_immutable_delete ON stage1_live_supersession_controlled_retries;
DROP TRIGGER IF EXISTS stage1_live_supersession_retry_immutable_update ON stage1_live_supersession_controlled_retries;
DROP TABLE IF EXISTS stage1_live_supersession_controlled_retry_submissions;
DROP TABLE IF EXISTS stage1_live_supersession_controlled_retries;
