DROP INDEX IF EXISTS tasks_lease_expiry;
ALTER TABLE tasks DROP COLUMN IF EXISTS checkpoint;
