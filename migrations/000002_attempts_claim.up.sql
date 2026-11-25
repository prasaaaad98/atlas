-- Slice A2: task attempts and the claim path.
--
-- Why: a worker claims a READY task, starts it, then completes or fails it.
-- Every claim creates one append-only attempt row (never overwritten), and the
-- task carries the fencing token of its current attempt. Constraints below are
-- the database-level backstops for the A2 guarantees:
--   * a task is never leased to two live attempts  -> task_attempts_one_live
--   * a fencing token is never reused for a task   -> UNIQUE (task_id, fencing_token)
--   * claim order is served by one partial index   -> tasks_claim
-- Status values match internal/domain exactly.

ALTER TABLE tasks
    ADD CONSTRAINT tasks_priority_class_valid CHECK (priority_class IN ('live', 'backfill', 'adhoc')),
    -- Claim order by class: live first, then ad-hoc, then backfill, so a large
    -- backfill can never starve live runs (PHASE_AB_PLAN.md §2.4). Stored so the
    -- claim index can sort on it directly.
    ADD COLUMN priority_rank smallint GENERATED ALWAYS AS (
        CASE priority_class WHEN 'live' THEN 0 WHEN 'adhoc' THEN 1 ELSE 2 END) STORED,
    ADD COLUMN current_attempt_id uuid,          -- the attempt holding the lease; NULL when not leased
    ADD COLUMN lease_expires_at   timestamptz;   -- database clock; the reaper (A3) reads it

CREATE TABLE task_attempts (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id          uuid        NOT NULL REFERENCES tasks (id),
    run_id           uuid        NOT NULL REFERENCES workflow_runs (id),
    attempt_number   integer     NOT NULL CHECK (attempt_number >= 1),
    fencing_token    bigint      NOT NULL CHECK (fencing_token >= 1),
    worker_id        text        NOT NULL,
    status           text        NOT NULL CHECK (status IN
                         ('LEASED', 'RUNNING', 'SUCCEEDED', 'CHECK_FAILED', 'FAILED', 'LEASE_EXPIRED', 'CANCELED')),
    outcome          text        CHECK (outcome IN ('succeeded', 'failed_definite', 'unknown')),
    error_class      text,
    error_detail     text,
    retryable        boolean,
    leased_at        timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz NOT NULL,
    started_at       timestamptz,
    finished_at      timestamptz,
    UNIQUE (task_id, attempt_number),
    UNIQUE (task_id, fencing_token)
);

-- Invariant backstop: at most one live (LEASED or RUNNING) attempt per task.
-- The claim's row lock already prevents a double claim; if a future change
-- broke that, this index turns the bug into an error instead of two workers
-- running the same task.
CREATE UNIQUE INDEX task_attempts_one_live ON task_attempts (task_id) WHERE status IN ('LEASED', 'RUNNING');
CREATE INDEX task_attempts_run ON task_attempts (run_id);

ALTER TABLE tasks
    ADD CONSTRAINT tasks_current_attempt_fk FOREIGN KEY (current_attempt_id) REFERENCES task_attempts (id);

-- Claim index: only READY rows, in exactly the claim's ORDER BY, so claiming
-- reads the first unlocked entry instead of scanning finished tasks.
CREATE INDEX tasks_claim ON tasks (priority_rank, deadline_at NULLS LAST, available_at, id) WHERE status = 'READY';
