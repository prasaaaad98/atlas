-- Slice E6: month partitions and re-fetch on schedules (PLAN_EIA_TRACK.md E6).
--
-- Why: EIA data is revised after first publication (re-fetch at +3 and +30 days), and some
-- pipelines are partitioned by month (Henry Hub per trade month, storage inventory per
-- report month, which lags ~2 months: beyond the ±31-day partition offset).
--
-- partition_grain          day | month. A month partition key is the month's first day.
-- partition_offset_months  month grain only: partition = month of the local fire date + this.
-- refetch_after_days       partition P is submitted again, once per d, on the first fire at
--                          least d days after P ENDS. Key refetch:<pipeline>:<P>:<d> is
--                          deterministic, so crashes and overlapping ticks replay, never duplicate.
-- Month-grain primary runs use key schedule:<pipeline>:<P>:<local fire date>: the current
-- month is partial by nature, so each fire rebuilds it (one active run per partition still holds).
ALTER TABLE schedules
    ADD COLUMN partition_grain text NOT NULL DEFAULT 'day' CHECK (partition_grain IN ('day', 'month')),
    ADD COLUMN partition_offset_months integer NOT NULL DEFAULT 0 CHECK (partition_offset_months BETWEEN -24 AND 24),
    ADD COLUMN refetch_after_days integer[] NOT NULL DEFAULT '{}',
    ADD CHECK (partition_grain = 'month' OR partition_offset_months = 0),
    ADD CHECK (partition_grain = 'day' OR partition_offset_days = 0),
    -- 1..400 days, at most 4 re-fetches; NULL elements are rejected by the array comparison.
    ADD CHECK (cardinality(refetch_after_days) <= 4 AND 1 <= ALL (refetch_after_days) AND 400 >= ALL (refetch_after_days));

-- Month backfills step one month at a time (from/to must be month starts; checked in Go).
ALTER TABLE backfills
    ADD COLUMN partition_grain text NOT NULL DEFAULT 'day' CHECK (partition_grain IN ('day', 'month'));

-- A re-fetch is its own trigger, so schedule reports (lag, source waits) count first fetches only.
ALTER TABLE workflow_runs DROP CONSTRAINT workflow_runs_trigger_check;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_runs_trigger_check
    CHECK (trigger IN ('schedule', 'backfill', 'manual', 'refetch'));
