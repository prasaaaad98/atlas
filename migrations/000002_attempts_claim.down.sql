DROP INDEX IF EXISTS tasks_claim;
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_current_attempt_fk;
DROP TABLE IF EXISTS task_attempts;
ALTER TABLE tasks
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS current_attempt_id,
    DROP COLUMN IF EXISTS priority_rank,
    DROP CONSTRAINT IF EXISTS tasks_priority_class_valid;
