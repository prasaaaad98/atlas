#!/usr/bin/env bash
# Slice A6 demo: schedules with catchup across a control-plane crash, and a
# concurrency-bounded backfill that doesn't delay a live run.
#
#  1. engine_quick gets an hourly schedule in Europe/Brussels with catchup from
#     10 days ago. The control plane is kill -9'd right after the first tick and
#     restarted: every partition exists exactly once (deterministic keys).
#  2. A 60-day backfill with max 3 concurrent runs; a live run submitted midway is
#     claimed before the waiting backfill tasks. Progress is printed as it goes.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a6.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_SCHEDULER_INTERVAL=1s
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-a6   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
BIN="$LAKE/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas
start_server() {
  "$BIN" >> "$LAKE/atlas.log" 2>&1 &
  SERVER=$!
  for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done
}
WORKER=
trap 'kill $SERVER $WORKER 2>/dev/null || true' EXIT
start_server

# Fresh state for the demo pipeline so repeated demos show the same picture.
$PSQL -qc "DELETE FROM schedules WHERE pipeline_name = 'engine_quick'"
$ATLAS register engine_quick
START=$(date -u -d '10 days ago' +%Y-%m-%d)

echo "== 1. hourly schedule with catchup from $START; control plane killed after the first tick"
BEFORE=$($PSQL -Atc "SELECT count(*) FROM workflow_runs WHERE pipeline_name='engine_quick' AND trigger='schedule'")
$ATLAS schedule engine_quick --cron "0 * * * *" --tz Europe/Brussels --catchup --start "$START"
sleep 1.5
kill -9 $SERVER; echo "killed control plane (pid $SERVER)"
start_server; echo "restarted"
sleep 3
$PSQL -c "SELECT count(*) AS scheduled_runs, count(DISTINCT partition_key) AS distinct_partitions,
                 min(partition_key) AS first, max(partition_key) AS last
          FROM workflow_runs WHERE pipeline_name='engine_quick' AND trigger='schedule' AND partition_key >= '$START'"
echo "(24 hourly fires per local day map to one partition each; scheduled runs before this demo: $BEFORE)"

echo "== 2. backfill 60 days, max 3 concurrent; a live run is submitted midway"
$ATLAS worker --worker-id w1 > "$LAKE/w1.log" 2>&1 &
WORKER=$!
Y=$((2100 + RANDOM % 800))
BF=$($ATLAS backfill engine_quick --from "$Y-01-01" --to "$Y-03-01" --max-concurrent 3 | sed -n 's/.*id=\([0-9a-f-]*\).*/\1/p')
for i in $(seq 120); do
  read -r status total active ok < <($PSQL -Atc "SELECT b.status, count(r.id), count(r.id) FILTER (WHERE r.status IN ('PENDING','RUNNING')),
         count(r.id) FILTER (WHERE r.status='SUCCEEDED') FROM backfills b LEFT JOIN workflow_runs r ON r.backfill_id=b.id
         WHERE b.id='$BF' GROUP BY b.status" | tr '|' ' ')
  printf "  %-9s submitted=%-3s active=%s succeeded=%s\n" "$status" "$total" "$active" "$ok"
  [ "$i" = 3 ] && $ATLAS submit engine_quick --partition "$Y-12-31" --priority live
  [ "$status" = "SUCCEEDED" ] && break
  sleep 1
done
$PSQL -c "SELECT r.priority_class, r.partition_key, to_char(a.leased_at,'HH24:MI:SS.MS') AS claimed
          FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
          WHERE r.pipeline_name='engine_quick' AND r.partition_key LIKE '$Y-%'
            AND a.leased_at >= (SELECT a2.leased_at FROM task_attempts a2 JOIN workflow_runs r2 ON r2.id=a2.run_id
                                WHERE r2.partition_key='$Y-12-31' AND r2.pipeline_name='engine_quick') - interval '1 second'
          ORDER BY a.leased_at LIMIT 6"
