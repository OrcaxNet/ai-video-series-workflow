SET search_path TO video_pipeline, public;

ALTER TABLE approval_decisions
    DROP CONSTRAINT approval_decisions_gate_check,
    ADD CONSTRAINT approval_decisions_gate_check
        CHECK (gate IN ('G1', 'G2', 'Q1', 'G3', 'SAFETY'));
