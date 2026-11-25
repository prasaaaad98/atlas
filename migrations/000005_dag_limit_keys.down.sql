DROP INDEX IF EXISTS task_edges_parent;
DROP INDEX IF EXISTS task_attempts_limit_key_recent;
DROP INDEX IF EXISTS tasks_live_by_limit_key;
ALTER TABLE task_attempts DROP COLUMN IF EXISTS limit_key;
ALTER TABLE tasks DROP COLUMN IF EXISTS limit_key;
DROP TABLE IF EXISTS limit_keys;
