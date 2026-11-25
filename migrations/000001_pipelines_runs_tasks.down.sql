-- Undo Slice A1. Drops all orchestration state; only for local development.
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS task_edges;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS workflow_runs;
DROP TABLE IF EXISTS pipelines;
