SET search_path TO video_pipeline, public;

-- SAFETY decisions are immutable audit evidence. Keep the backward-compatible
-- enum value on downgrade so rollback never deletes or rewrites an approval.
-- This schema downgrade does not make an older application binary safe for
-- provider execution; deployment rollback must retain the v4 safety gate or
-- disable provider execution externally.
ALTER TABLE approval_decisions
    DROP CONSTRAINT approval_decisions_gate_check,
    ADD CONSTRAINT approval_decisions_gate_check
        CHECK (gate IN ('G1', 'G2', 'Q1', 'G3', 'SAFETY'));
