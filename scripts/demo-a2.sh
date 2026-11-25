#!/usr/bin/env bash
# Slice A2 demo: the real control plane hands tasks to two Python workers.
#
# What it shows: pipelines registered from Python, 6 runs submitted (2 live,
# 4 backfill), two workers claiming concurrently, live tasks first, every run
# SUCCEEDED, one attempt row per task, and a stale report rejected (409).
# Needs: make up && make migrate (Postgres in Docker). No external network.
# Who calls it: make demo-a2.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT"
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
# Repo-relative (bin/ is gitignored): Windows Go and Python would resolve /tmp against the drive.
LAKE=bin/demo-a2
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
P=demo_grid_entsoe_dam
# Unique partitions per demo run, so re-running the demo submits new runs.
YEAR=$((2100 + RANDOM % 800))

go build -o "$LAKE/atlas$(go env GOEXE)" ./cmd/atlas
"$LAKE/atlas$(go env GOEXE)" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

echo "== register and submit (backfill first, then live)"
$ATLAS register $P
$ATLAS submit $P --partition "$YEAR-01-01" --to "$YEAR-01-04" --priority backfill
$ATLAS submit $P --partition "$YEAR-06-01" --to "$YEAR-06-02" --priority live

echo "== two workers, concurrently"
$ATLAS worker --worker-id worker-a --exit-when-idle > "$LAKE/a.log" &
A=$!
$ATLAS worker --worker-id worker-b --exit-when-idle > "$LAKE/b.log" &
B=$!
wait $A $B
cat "$LAKE/a.log" "$LAKE/b.log"

echo "== claim order and attempts (from Postgres)"
docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas -c "
  SELECT r.partition_key, r.priority_class, r.status AS run, a.worker_id, a.attempt_number, a.fencing_token, a.status AS attempt
  FROM task_attempts a JOIN workflow_runs r ON r.id = a.run_id
  WHERE r.pipeline_name = '$P' AND r.partition_key LIKE '$YEAR-%'
  ORDER BY a.leased_at"

echo "== a stale report is rejected: complete a finished attempt with a wrong fencing token"
ID=$(docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas -Atc "
  SELECT a.id FROM task_attempts a JOIN workflow_runs r ON r.id = a.run_id
  WHERE r.partition_key = '$YEAR-06-01' AND r.pipeline_name = '$P' LIMIT 1")
curl -s -X POST "$ATLAS_URL/attempts/$ID/complete" -d '{"fencing_token": 99}'
echo
