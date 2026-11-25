DROP INDEX IF EXISTS task_attempts_task;
DROP INDEX IF EXISTS runs_pipeline_created;
DROP INDEX IF EXISTS runs_open_deadlines;
ALTER TABLE tasks DROP COLUMN IF EXISTS estimate_seconds;
ALTER TABLE workflow_runs
    DROP COLUMN IF EXISTS fallback_run_id,
    DROP COLUMN IF EXISTS finished_at,
    DROP COLUMN IF EXISTS deadline_missed_at,
    DROP COLUMN IF EXISTS deadline_at_risk_at;
