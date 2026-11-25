-- Slice A4: per-task retry policy (ADR-005).
--
-- Why: when an attempt fails or its lease lapses, the task's policy decides
-- the backoff (full jitter, domain.RetryPolicy.Backoff) and which error
-- classes are never retried. NULL means domain.DefaultRetryPolicy.
ALTER TABLE tasks ADD COLUMN retry_policy jsonb;
