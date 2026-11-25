#!/usr/bin/env bash
# Slice A5 demo: DAG readiness with worker kills, run cancellation, limit-key admission.
#
#  1. 20 partitions of engine_diamond (fetch → parse_a, parse_b → publish), 3
#     workers, one killed with kill -9 mid-run: every run SUCCEEDED, none BLOCKED.
#  2. engine_chunked started, then canceled: the worker stops at its next
#     heartbeat, its report is refused, the run is CANCELED.
#  3. engine_limited on key demo_one (concurrency 1), 3 workers: attempts never overlap.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a5.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_REAPER_INTERVAL=1s
export PYTHONUNBUFFERED=1  # worker logs appear as they happen
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
export ATLAS_DEMO_CHUNK_SECONDS=1
LAKE=bin/demo-a5   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
Y=$((2100 + RANDOM % 800))

go build -o "$LAKE/atlas$(go env GOEXE)" ./cmd/atlas
"$LAKE/atlas$(go env GOEXE)" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
WORKERS=()
cleanup() { kill "${WORKERS[@]}" $SERVER 2>/dev/null || true; }
trap cleanup EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

echo "== 1. diamond DAG, 20 partitions, 3 workers, one killed"
$ATLAS register engine_diamond
$ATLAS submit engine_diamond --partition "$Y-01-01" --to "$Y-01-20" --priority backfill > /dev/null
for w in 1 2 3; do
  $ATLAS worker --worker-id "w$w" --lease-seconds 10 > "$LAKE/w$w.log" 2>&1 &
  WORKERS+=($!)
done
sleep 4
kill -9 "${WORKERS[0]}"; echo "killed w1 (pid ${WORKERS[0]})"
for _ in $(seq 120); do
  active=$($PSQL -Atc "SELECT count(*) FROM workflow_runs WHERE pipeline_name='engine_diamond'
                       AND partition_key LIKE '$Y-%' AND status IN ('PENDING','RUNNING')")
  [ "$active" = "0" ] && break; sleep 1
done
$PSQL -c "SELECT r.status AS run, count(DISTINCT r.id) AS runs, count(t.id) AS tasks,
                 sum((t.status <> 'SUCCEEDED')::int) AS unfinished_tasks, sum(t.attempt_count) AS attempts
          FROM workflow_runs r JOIN tasks t ON t.run_id = r.id
          WHERE r.pipeline_name='engine_diamond' AND r.partition_key LIKE '$Y-%' GROUP BY 1"
$PSQL -c "SELECT a.worker_id, a.status, a.outcome, count(*) FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
          WHERE r.pipeline_name='engine_diamond' AND r.partition_key LIKE '$Y-%' GROUP BY 1,2,3 ORDER BY 1,2"

echo "== 2. cancel a running run"
$ATLAS register engine_chunked
RUN=$($ATLAS submit engine_chunked --partition "$Y-02-01" --priority live | sed -n 's/.*run=\([0-9a-f-]*\).*/\1/p')
sleep 3
$ATLAS cancel "$RUN"
sleep 3   # the worker learns at its next ctx.heartbeat (after the current chunk)
$PSQL -c "SELECT r.status AS run, t.status AS task, a.status AS attempt, a.outcome, a.error_class
          FROM workflow_runs r JOIN tasks t ON t.run_id=r.id JOIN task_attempts a ON a.task_id=t.id WHERE r.id='$RUN'"
grep -h "engine_chunked" "$LAKE"/w*.log || true

echo "== 3. limit key demo_one (concurrency 1), 6 tasks, 2 live workers"
curl -s -X PUT "$ATLAS_URL/limit-keys/demo_one" -d '{"max_concurrency": 1, "rpm": 600}'; echo
$ATLAS register engine_limited
$ATLAS submit engine_limited --partition "$Y-03-01" --to "$Y-03-06" --priority live > /dev/null
for _ in $(seq 60); do
  active=$($PSQL -Atc "SELECT count(*) FROM workflow_runs WHERE pipeline_name='engine_limited'
                       AND partition_key LIKE '$Y-%' AND status IN ('PENDING','RUNNING')")
  [ "$active" = "0" ] && break; sleep 1
done
$PSQL -c "SELECT a.worker_id, to_char(a.leased_at, 'HH24:MI:SS.MS') AS leased, to_char(a.finished_at, 'HH24:MI:SS.MS') AS finished
          FROM task_attempts a WHERE a.limit_key = 'demo_one' AND a.run_id IN
          (SELECT id FROM workflow_runs WHERE pipeline_name='engine_limited' AND partition_key LIKE '$Y-%') ORDER BY a.leased_at"
$PSQL -Atc "SELECT 'overlapping attempts on demo_one: ' || count(*) FROM task_attempts a JOIN task_attempts b ON a.id < b.id
            WHERE a.limit_key='demo_one' AND b.limit_key='demo_one' AND a.leased_at < b.finished_at AND b.leased_at < a.finished_at
            AND a.run_id IN (SELECT id FROM workflow_runs WHERE partition_key LIKE '$Y-%')
            AND b.run_id IN (SELECT id FROM workflow_runs WHERE partition_key LIKE '$Y-%')"
