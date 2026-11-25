-- Slice A7: deadline-awareness (ADR-012).
--
-- Why: each run may carry deadline_at (from the pipeline's deadline rule).
-- The evaluator marks runs at risk (expected remaining work no longer fits)
-- and missed (deadline passed while active, or finished after it). Each mark
-- is a timestamp set at most once (`... IS NULL` in the guarded UPDATE), so
-- every late run has exactly one run.deadline_missed event.

ALTER TABLE workflow_runs
    ADD COLUMN deadline_at_risk_at timestamptz,
    ADD COLUMN deadline_missed_at  timestamptz,
    ADD COLUMN finished_at         timestamptz,   -- set when the run reaches a terminal status
    ADD COLUMN fallback_run_id     uuid REFERENCES workflow_runs (id);

-- Declared duration, used for at-risk estimates until there is history.
ALTER TABLE tasks ADD COLUMN estimate_seconds double precision CHECK (estimate_seconds >= 0);

-- Evaluator scans only active runs that have a deadline.
CREATE INDEX runs_open_deadlines ON workflow_runs (deadline_at)
    WHERE status IN ('PENDING', 'RUNNING') AND deadline_at IS NOT NULL;
-- Duration history per pipeline (p95 of recent successful attempts).
CREATE INDEX runs_pipeline_created ON workflow_runs (pipeline_name, created_at);
CREATE INDEX task_attempts_task ON task_attempts (task_id);
