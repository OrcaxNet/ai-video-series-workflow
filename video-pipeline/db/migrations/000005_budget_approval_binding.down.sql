SET search_path TO video_pipeline, public;

DROP INDEX IF EXISTS ix_review_budget_plan;

ALTER TABLE review_tasks
    DROP CONSTRAINT IF EXISTS ck_review_budget_binding_pair,
    DROP COLUMN IF EXISTS budget_currency,
    DROP COLUMN IF EXISTS budget_limit_micros,
    DROP COLUMN IF EXISTS budget_scope,
    DROP COLUMN IF EXISTS generation_plan_id;
