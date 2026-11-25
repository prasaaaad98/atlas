-- B7 (14-day schedule): an uptime record, so freshness lag caused by the
-- machine being off or asleep is never mistaken for a late source.
--
-- One row per minute in which the control plane was running, with the number
-- of distinct workers that asked for work in that minute (long-poll claims
-- arrive every ≤20 s per idle worker, so an idle but alive worker is counted).
-- A missing minute = the control plane was not running (or the machine slept).
CREATE TABLE uptime_minutes (
    minute     timestamptz PRIMARY KEY,   -- date_trunc('minute', now()), database clock
    instance   text        NOT NULL,      -- control plane process that wrote it
    workers    integer     NOT NULL CHECK (workers >= 0)
);
