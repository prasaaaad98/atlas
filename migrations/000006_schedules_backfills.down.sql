DROP INDEX IF EXISTS runs_active_by_backfill;
ALTER TABLE workflow_runs DROP COLUMN IF EXISTS backfill_id;
DROP TABLE IF EXISTS backfills;
DROP TABLE IF EXISTS schedules;
