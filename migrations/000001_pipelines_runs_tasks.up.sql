-- Slice A1: pipelines, runs, tasks, edges, events.
--
-- Why: these tables hold Atlas's durable state. A run and all of its tasks are
-- inserted in one transaction (ADR-003 decision 6), and the constraints below
-- are what make the A1 guarantees true even under concurrent submits:
--   * one run per (tenant_id, idempotency_key)        -> runs_idempotency_key
--   * one ACTIVE run per (pipeline_name, partition_key) -> runs_one_active_per_partition
-- Status values match internal/domain exactly.

CREATE TABLE pipelines (
    name            text        NOT NULL,
    version         integer     NOT NULL CHECK (version >= 1),
    definition      jsonb       NOT NULL,           -- tasks, edges, asset outputs, retry policy (later)
    definition_hash text        NOT NULL,           -- sha256 of the canonical definition; detects re-registration
    partition_kind  text        NOT NULL CHECK (partition_kind IN ('daily')),
    partition_tz    text        NOT NULL,           -- IANA zone the partition date is defined in
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version),
    UNIQUE (name, definition_hash)
);

CREATE TABLE workflow_runs (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           text        NOT NULL DEFAULT 'default',
    idempotency_key     text        NOT NULL,
    request_hash        text        NOT NULL,       -- same key + different request = conflict, not a replay
    pipeline_name       text        NOT NULL,
    pipeline_version    integer     NOT NULL,
    partition_key       text        NOT NULL,
    data_interval_start timestamptz NOT NULL,
    data_interval_end   timestamptz NOT NULL,
    trigger             text        NOT NULL CHECK (trigger IN ('schedule', 'backfill', 'manual')),
    priority_class      text        NOT NULL CHECK (priority_class IN ('live', 'backfill', 'adhoc')),
    status              text        NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    deadline_at         timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (pipeline_name, pipeline_version) REFERENCES pipelines (name, version),
    CHECK (data_interval_end > data_interval_start)
);

-- Invariant: a duplicate submission never creates a second run.
CREATE UNIQUE INDEX runs_idempotency_key ON workflow_runs (tenant_id, idempotency_key);

-- Invariant: at most one active run per pipeline partition, so a schedule tick,
-- a manual trigger and a backfill can't rebuild the same day concurrently
-- (Airflow 3 dropped this; we keep it deliberately, ADR-010). Finished runs are
-- outside the index, so a partition can be re-run after its previous run ends.
CREATE UNIQUE INDEX runs_one_active_per_partition
    ON workflow_runs (pipeline_name, partition_key)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE TABLE tasks (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id         uuid        NOT NULL REFERENCES workflow_runs (id),
    name           text        NOT NULL,
    status         text        NOT NULL CHECK (status IN
                       ('BLOCKED', 'READY', 'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED_TERMINAL', 'CANCELED')),
    priority_class text        NOT NULL,           -- copied from the run so claim needn't join
    deadline_at    timestamptz,                    -- copied from the run (A7)
    available_at   timestamptz NOT NULL DEFAULT now(),
    attempt_count  integer     NOT NULL DEFAULT 0,
    max_attempts   integer     NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    fencing_token  bigint      NOT NULL DEFAULT 0,
    asset_outputs  text[]      NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, name)
);

CREATE TABLE task_edges (
    run_id         uuid NOT NULL REFERENCES workflow_runs (id),
    parent_task_id uuid NOT NULL REFERENCES tasks (id),
    child_task_id  uuid NOT NULL REFERENCES tasks (id),
    PRIMARY KEY (parent_task_id, child_task_id),
    CHECK (parent_task_id <> child_task_id)
);
CREATE INDEX task_edges_child ON task_edges (child_task_id);

-- Append-only transition log; the timeline page (A9) renders it.
CREATE TABLE events (
    id         bigserial   PRIMARY KEY,
    run_id     uuid        REFERENCES workflow_runs (id),
    task_id    uuid        REFERENCES tasks (id),
    type       text        NOT NULL,
    payload    jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_run ON events (run_id, id);
