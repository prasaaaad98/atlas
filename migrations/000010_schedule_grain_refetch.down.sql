-- Re-fetch runs are history and are never deleted here: if any exist, adding the old
-- constraint fails and the down migration stops (delete them deliberately first).
ALTER TABLE workflow_runs DROP CONSTRAINT workflow_runs_trigger_check;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_runs_trigger_check
    CHECK (trigger IN ('schedule', 'backfill', 'manual'));
ALTER TABLE backfills DROP COLUMN partition_grain;
ALTER TABLE schedules
    DROP COLUMN refetch_after_days,
    DROP COLUMN partition_offset_months,
    DROP COLUMN partition_grain;
