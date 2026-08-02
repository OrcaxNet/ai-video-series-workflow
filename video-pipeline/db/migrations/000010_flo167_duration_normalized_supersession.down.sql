SET search_path TO video_pipeline, public;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage1_live_supersessions) THEN
        RAISE EXCEPTION 'cannot roll back FLO-167 while immutable supersession lineage exists';
    END IF;
END $$;

DROP TABLE stage1_live_supersession_terminal_ledger;
DROP TABLE stage1_live_supersession_submissions;
DROP TABLE stage1_live_supersession_authorizations;
DROP TABLE stage1_live_supersession_shots;
DROP TABLE stage1_live_supersessions;
