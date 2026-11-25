#!/usr/bin/env bash
# Slice A4 demo: retry policies, full-jitter backoff, the error classifier.
#
# Shows: engine_flaky fails twice (server_error) and succeeds on attempt 3,
# with randomised gaps bounded by the policy (cap 2 s, then 4 s); engine_fatal
# fails with `auth` and is never retried although the worker calls it retryable.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a4.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT"
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-a4   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
DAY="$((2100 + RANDOM % 800))-01-01"

go build -o "$LAKE/atlas$(go env GOEXE)" ./cmd/atlas
"$LAKE/atlas$(go env GOEXE)" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

for p in engine_flaky engine_fatal; do
  $ATLAS register $p
  $ATLAS submit $p --partition "$DAY" --priority live
done
echo "== one worker: flaky needs 3 attempts, fatal gets 1"
$ATLAS worker --worker-id worker-1 --max-tasks 4

$PSQL -c "SELECT r.pipeline_name, a.attempt_number, a.status, a.outcome, a.error_class, a.retryable,
                 round(extract(epoch FROM a.leased_at - lag(a.finished_at) OVER (PARTITION BY a.task_id ORDER BY a.attempt_number))::numeric, 2) AS waited_s
          FROM task_attempts a JOIN workflow_runs r ON r.id = a.run_id
          WHERE r.partition_key = '$DAY' AND r.pipeline_name IN ('engine_flaky','engine_fatal')
          ORDER BY r.pipeline_name, a.attempt_number"
$PSQL -c "SELECT pipeline_name, status FROM workflow_runs WHERE partition_key = '$DAY'
          AND pipeline_name IN ('engine_flaky','engine_fatal') ORDER BY 1"
echo "(waited_s includes up to 2 s of worker polling; the backoff itself is <= 2 s then <= 4 s)"
