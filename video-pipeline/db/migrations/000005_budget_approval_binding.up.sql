SET search_path TO video_pipeline, public;

-- Legacy budget reviews remain readable but are deliberately unbound. Paid
-- submission authorization rejects NULL bindings until a new review is
-- approved for the exact immutable generation plan, amount, and currency.
ALTER TABLE review_tasks
    ADD COLUMN generation_plan_id UUID REFERENCES operation_requests(id),
    ADD COLUMN budget_scope TEXT,
    ADD COLUMN budget_limit_micros BIGINT,
    ADD COLUMN budget_currency CHAR(3),
    ADD CONSTRAINT ck_review_budget_binding_pair
        CHECK (
            (generation_plan_id IS NULL
             AND budget_scope IS NULL
             AND budget_limit_micros IS NULL
             AND budget_currency IS NULL)
            OR
            (generation_plan_id IS NOT NULL
             AND budget_scope IN ('VIDEO', 'SPEECH')
             AND budget_limit_micros IS NOT NULL
             AND budget_limit_micros >= 0
             AND budget_currency ~ '^[A-Z]{3}$')
        );

CREATE INDEX ix_review_budget_plan
    ON review_tasks(generation_plan_id, budget_scope, budget_currency, budget_limit_micros)
    WHERE review_type = 'BUDGET' AND state = 'APPROVED';

COMMENT ON COLUMN review_tasks.generation_plan_id IS
    'Exact immutable generation plan approved by a BUDGET review; NULL legacy approvals fail closed.';
COMMENT ON COLUMN review_tasks.budget_scope IS
    'VIDEO or SPEECH; prevents a video envelope from authorizing TTS submission.';
COMMENT ON COLUMN review_tasks.budget_limit_micros IS
    'Maximum approved spend in micros for the bound generation plan.';
COMMENT ON COLUMN review_tasks.budget_currency IS
    'ISO 4217 currency for the bound budget envelope.';
