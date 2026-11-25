-- Slice A3: heartbeats with checkpoints, lease expiry, reaper.
--
-- Why: a worker can crash, hang or be paused. Its lease (database clock) runs
-- out unless heartbeats extend it; the reaper then re-queues the task, and the
-- old attempt's token stops working (ADR-004). A heartbeat may carry a small
-- checkpoint that the next attempt receives at claim (Temporal heartbeat details).

-- Last checkpoint from a heartbeat of the attempt holding the task. Written only
-- under the fencing condition, so a stale worker can't overwrite a newer one.
ALTER TABLE tasks ADD COLUMN checkpoint jsonb
    CHECK (checkpoint IS NULL OR octet_length(checkpoint::text) <= 8192);

-- Reaper index: only leased/running rows, ordered by expiry.
CREATE INDEX tasks_lease_expiry ON tasks (lease_expires_at) WHERE status IN ('LEASED', 'RUNNING');
