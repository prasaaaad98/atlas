-- Slice A5: limit keys (source politeness) and the indexes DAG readiness needs.
--
-- Why: tasks that call the same external source share a limit key; Atlas
-- admits a claim only while the key is under its concurrency and
-- requests-per-minute limits and not paused (after a 429). Enforced at claim,
-- best effort for the source itself (ADR-008): Atlas bounds its own requests,
-- it can't control what else uses the source.

CREATE TABLE limit_keys (
    key             text        PRIMARY KEY CHECK (key ~ '^[a-z][a-z0-9_]*$'),
    max_concurrency integer     NOT NULL CHECK (max_concurrency >= 1),
    rpm             integer     NOT NULL CHECK (rpm >= 1),   -- claims per rolling minute
    paused_until    timestamptz,                             -- set on 429; database clock
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- A task's key must exist, so a typo can't make a task unclaimable forever.
ALTER TABLE tasks ADD COLUMN limit_key text REFERENCES limit_keys (key);
ALTER TABLE task_attempts ADD COLUMN limit_key text;

-- Admission counts: live attempts per key, and claims in the last minute.
CREATE INDEX tasks_live_by_limit_key ON tasks (limit_key) WHERE status IN ('LEASED', 'RUNNING') AND limit_key IS NOT NULL;
CREATE INDEX task_attempts_limit_key_recent ON task_attempts (limit_key, leased_at) WHERE limit_key IS NOT NULL;

-- Readiness: find a task's parents (child index exists since 000001) and children.
CREATE INDEX task_edges_parent ON task_edges (parent_task_id);
