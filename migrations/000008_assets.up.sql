-- Slice A8: assets, materializations, checks, current pointers, freshness (ADR-013).
--
-- Why: Atlas, not the worker, decides which output of a (pipeline, partition)
-- is "current". A completion's materializations and check results are
-- inserted in the same fenced transaction that marks the task SUCCEEDED, and
-- asset_partitions.current_materialization_id moves only when no blocking
-- check failed. Readers resolve data only through that pointer, so they see
-- the old version or the new one, never a partial or fenced-out attempt's file.

CREATE TABLE assets (
    key              text        PRIMARY KEY CHECK (key ~ '^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$'),
    pipeline_name    text        NOT NULL,      -- the pipeline that produces it (latest registration)
    partition_kind   text        NOT NULL DEFAULT 'daily',
    freshness_policy jsonb,                     -- {deadline_cron, tz, expected_partition_offset, severity}
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Append-only. Only fenced completions insert here (attempt id + fencing token recorded).
CREATE TABLE materializations (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_key      text        NOT NULL REFERENCES assets (key),
    partition_key  text        NOT NULL,
    run_id         uuid        NOT NULL REFERENCES workflow_runs (id),
    task_id        uuid        NOT NULL REFERENCES tasks (id),
    attempt_id     uuid        NOT NULL REFERENCES task_attempts (id),
    fencing_token  bigint      NOT NULL,
    uri            text        NOT NULL,
    row_count      bigint      NOT NULL CHECK (row_count >= 0),
    content_hash   text        NOT NULL CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
    schema_version text        NOT NULL,
    code_version   text        NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, asset_key)
);
CREATE INDEX materializations_asset_partition ON materializations (asset_key, partition_key, created_at);

-- Check results reported with a completion. They apply to every materialization
-- of that attempt (a task's checks gate its outputs together, as in the local runner).
CREATE TABLE asset_checks (
    id            bigserial   PRIMARY KEY,
    attempt_id    uuid        NOT NULL REFERENCES task_attempts (id),
    run_id        uuid        NOT NULL REFERENCES workflow_runs (id),
    partition_key text        NOT NULL,
    check_name    text        NOT NULL,
    severity      text        NOT NULL CHECK (severity IN ('blocking', 'warn')),
    passed        boolean     NOT NULL,
    metrics       jsonb       NOT NULL DEFAULT '{}',
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX asset_checks_attempt ON asset_checks (attempt_id);

-- The current pointer per (asset, partition).
CREATE TABLE asset_partitions (
    asset_key                  text        NOT NULL REFERENCES assets (key),
    partition_key              text        NOT NULL,
    current_materialization_id uuid        REFERENCES materializations (id),
    status                     text        NOT NULL CHECK (status IN ('missing', 'fresh', 'late', 'failed_check')),
    late_since                 timestamptz,
    updated_at                 timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_key, partition_key)
);
