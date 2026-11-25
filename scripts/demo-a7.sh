#!/usr/bin/env bash
# Slice A7 demo: earliest-deadline-first, at-risk before miss, late policy.
#
#  1. Four 4-second engine_slow runs, one worker that starts ~6 s after the first
#     submit (T). Deadlines T+18, T+9, none, T+16, submitted in that order.
#     Claims follow EDF: T+9, T+16, T+18, then none. T+9 can't make it (claimed
#     ~T+6, 4 s of work): at risk first, then missed. T+16 is on time. T+18 waits
#     behind two runs: flagged at risk BEFORE it misses.
#  2. engine_strict (deadline = end of the partition day, late_policy=cancel,
#     fallback engine_quick) for a past day: missed → canceled → engine_quick
#     submitted for the same day.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a7.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_DEADLINE_INTERVAL=1s
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-a7   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
BIN="$LAKE/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas
"$BIN" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done
Y=$((2100 + RANDOM % 800))

for p in engine_slow engine_strict engine_quick; do $ATLAS register $p > /dev/null; done
# Cancel runs left active by earlier demos (e.g. a fallback), so the worker only sees this demo's runs.
for id in $($PSQL -Atc "SELECT id FROM workflow_runs WHERE pipeline_name LIKE 'engine_%' AND status IN ('PENDING','RUNNING')"); do
  curl -s -X POST "$ATLAS_URL/runs/$id/cancel" > /dev/null
done
T=$(date +%s)
echo "== 1. four runs, one worker; deadlines T+18, T+9, none, T+16"
$ATLAS submit engine_slow --partition "$Y-01-18" --priority live --deadline-at $((T + 18)) > /dev/null
$ATLAS submit engine_slow --partition "$Y-01-09" --priority live --deadline-at $((T + 9)) > /dev/null
$ATLAS submit engine_slow --partition "$Y-02-01" --priority live > /dev/null
$ATLAS submit engine_slow --partition "$Y-01-16" --priority live --deadline-at $((T + 16)) > /dev/null
sleep $(( T + 5 - $(date +%s) > 0 ? T + 5 - $(date +%s) : 0 ))
$ATLAS worker --worker-id w1 --max-tasks 4
sleep 1.5
$PSQL -c "SELECT r.partition_key AS run,
                 'T+' || round(extract(epoch FROM r.deadline_at) - $T) AS deadline,
                 'T+' || round(extract(epoch FROM a.leased_at) - $T, 1) AS claimed_t,
                 'T+' || round(extract(epoch FROM r.finished_at) - $T, 1) AS finished_t,
                 'T+' || round(extract(epoch FROM r.deadline_at_risk_at) - $T, 1) AS at_risk,
                 'T+' || round(extract(epoch FROM r.deadline_missed_at) - $T, 1) AS missed,
                 (SELECT count(*) FROM events e WHERE e.run_id=r.id AND e.type='run.deadline_missed') AS miss_events
          FROM workflow_runs r JOIN tasks t ON t.run_id=r.id JOIN task_attempts a ON a.task_id=t.id
          WHERE r.pipeline_name='engine_slow' AND r.partition_key LIKE '$Y-%' ORDER BY a.leased_at"

echo "== 2. late policy: engine_strict for a past day (its deadline, the end of that day, has passed)"
D=$(date -u -d "$((RANDOM % 3000 + 30)) days ago" +%Y-%m-%d)   # a different past day per demo run
$ATLAS submit engine_strict --partition "$D" --priority live > /dev/null
sleep 3
$PSQL -c "SELECT r.pipeline_name, r.partition_key, r.status, to_char(r.deadline_missed_at, 'MI:SS') AS missed,
                 f.pipeline_name AS fallback, f.partition_key AS fallback_partition, f.status AS fallback_status
          FROM workflow_runs r LEFT JOIN workflow_runs f ON f.id = r.fallback_run_id
          WHERE r.pipeline_name='engine_strict' AND r.partition_key='$D'"
