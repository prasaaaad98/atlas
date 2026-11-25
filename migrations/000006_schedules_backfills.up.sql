-- Slice A6: schedules, catchup, backfills.
--
-- Why: scheduled runs are created by a ticker from a cron in the pipeline's
-- time zone; backfills create runs lazily, oldest first, never more than
-- max_concurrent_runs at once. Both submit through the normal durable submit
-- path with deterministic idempotency keys:
--   schedule:<pipeline>:<partition>   backfill:<backfill id>:<partition>
-- so a restart in the middle of a tick can't create a run twice
-- (runs_idempotency_key), and the one-active-run-per-partition index keeps a
-- backfill and a schedule from rebuilding the same day concurrently.

-- One schedule per pipeline NAME (it survives new versions of the definition).
CREATE TABLE schedules (
    pipeline_name         text        PRIMARY KEY,
    cron                  text        NOT NULL,
    tz                    text        NOT NULL,                  -- IANA zone the cron is read in
    partition_offset_days integer     NOT NULL DEFAULT 0,        -- partition = local fire date + offset
    catchup               boolean     NOT NULL DEFAULT false,
    start_partition       text,                                  -- first partition for catchup
    enabled               boolean     NOT NULL DEFAULT true,
    cursor                timestamptz,                           -- last fire time already submitted
    updated_at            timestamptz NOT NULL DEFAULT now(),
    CHECK (partition_offset_days BETWEEN -31 AND 31),
    CHECK (NOT catchup OR start_partition IS NOT NULL)
);

CREATE TABLE backfills (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name       text        NOT NULL,
    from_partition      text        NOT NULL,
    to_partition        text        NOT NULL,
    next_partition      text        NOT NULL,                    -- oldest partition not yet submitted
    max_concurrent_runs integer     NOT NULL CHECK (max_concurrent_runs BETWEEN 1 AND 1000),
    status              text        NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'CANCELED')),
    created_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    CHECK (from_partition <= to_partition)
);

ALTER TABLE workflow_runs ADD COLUMN backfill_id uuid REFERENCES backfills (id);
-- Concurrency count per backfill: its active runs only.
CREATE INDEX runs_active_by_backfill ON workflow_runs (backfill_id) WHERE status IN ('PENDING', 'RUNNING');
