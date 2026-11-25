#!/usr/bin/env bash
# Slice A3 demo: kill -9 a worker mid-task; the reaper re-queues the task when
# its lease lapses, and a new worker resumes from the last heartbeat checkpoint.
#
# Shows: attempt 1 LEASE_EXPIRED with outcome unknown, attempt 2 SUCCEEDED,
# resumed_from = the checkpoint, and the killed attempt's late report refused.
# The paused-worker (SIGSTOP) case is TestPausedWorkerCannotOverwriteNewAttempt;
# Windows has no SIGSTOP for native processes.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a3.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_REAPER_INTERVAL=1s
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
export ATLAS_DEMO_CHUNK_SECONDS=1
LAKE=bin/demo-a3   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
DAY="$((2100 + RANDOM % 800))-01-01"

go build -o "$LAKE/atlas$(go env GOEXE)" ./cmd/atlas
"$LAKE/atlas$(go env GOEXE)" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

$ATLAS register engine_chunked
$ATLAS submit engine_chunked --partition "$DAY" --priority live

echo "== worker A starts the 6-chunk task (1 s per chunk, lease 10 s), killed after ~3.5 s"
$ATLAS worker --worker-id worker-A --lease-seconds 10 --max-tasks 1 > "$LAKE/a.log" 2>&1 &
A=$!
sleep 3.5
kill -9 $A
echo "killed worker A (pid $A)"
$PSQL -c "SELECT t.status, t.checkpoint, t.lease_expires_at > now() AS lease_live FROM tasks t
          JOIN workflow_runs r ON r.id = t.run_id WHERE r.pipeline_name='engine_chunked' AND r.partition_key='$DAY'"

echo "== waiting for the lease to lapse and the reaper to re-queue the task"
for _ in $(seq 40); do
  st=$($PSQL -Atc "SELECT a.status FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
                   WHERE r.pipeline_name='engine_chunked' AND r.partition_key='$DAY' AND a.attempt_number=1")
  [ "$st" = "LEASE_EXPIRED" ] && break; sleep 1
done
echo "attempt 1: $st"

echo "== worker B claims attempt 2 and resumes from the checkpoint"
# --max-tasks 1, not --exit-when-idle: the re-run is claimable only after its backoff (A4).
$ATLAS worker --worker-id worker-B --lease-seconds 10 --max-tasks 1

$PSQL -c "SELECT a.attempt_number, a.worker_id, a.fencing_token, a.status, a.outcome, a.error_class
          FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
          WHERE r.pipeline_name='engine_chunked' AND r.partition_key='$DAY' ORDER BY a.attempt_number"
$PSQL -Atc "SELECT 'attempt 2 check: ' || (e.payload->'checks'->0->'metrics')::text FROM events e
            JOIN workflow_runs r ON r.id=e.run_id WHERE r.pipeline_name='engine_chunked'
            AND r.partition_key='$DAY' AND e.type='task.completed'"

echo "== the killed attempt's late completion is refused"
ID=$($PSQL -Atc "SELECT a.id FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
                 WHERE r.pipeline_name='engine_chunked' AND r.partition_key='$DAY' AND a.attempt_number=1")
curl -s -X POST "$ATLAS_URL/attempts/$ID/complete" -d '{"fencing_token": 1}'; echo
