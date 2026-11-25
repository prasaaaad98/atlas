#!/usr/bin/env bash
# Slice A9 demo: metrics, timeline and freshness pages, long-poll workers.
#
# Starts the control plane, runs a little of everything with two long-polling
# Python workers (a diamond DAG, a flaky task with retries, an asset build),
# prints the atlas_* metrics, and leaves the server up for 60 s so the pages
# can be opened:  /metrics   /ui/runs/<id>   /ui/assets
# Needs: make up && make migrate. No external network. Who calls it: make demo-a9.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_DEADLINE_INTERVAL=1s ATLAS_FRESHNESS_INTERVAL=2s
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-a9   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
BIN="$LAKE/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas
"$BIN" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
W=()
trap 'kill "${W[@]}" $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done
Y=$((2100 + RANDOM % 800))

for p in engine_diamond engine_flaky engine_assets; do $ATLAS register $p > /dev/null; done
for w in 1 2; do $ATLAS worker --worker-id "w$w" > "$LAKE/w$w.log" 2>&1 & W+=($!); done   # long-poll (--wait 20)
RUN=$($ATLAS submit engine_diamond --partition "$Y-01-01" --priority live --deadline-in 2 | sed -n 's/.*run=\([0-9a-f-]*\).*/\1/p')
$ATLAS submit engine_flaky --partition "$Y-01-01" --priority live > /dev/null
$ATLAS submit engine_assets --partition "$(date -u +%Y-%m-%d)" --priority live > /dev/null
sleep 12

echo "== atlas_* metrics"
curl -s "$ATLAS_URL/metrics" | grep -E '^atlas_(queue_depth|tasks_running|attempts_total|deadline_(missed|at_risk)_total|claim_duration_seconds_(count|sum)|asset_late_total|asset_freshness_lag|limit_key)' || true
echo
echo "Open in a browser (server stays up ${DEMO_HOLD:-60} s):"
echo "  $ATLAS_URL/ui/runs/$RUN"
echo "  $ATLAS_URL/ui/assets"
echo "  $ATLAS_URL/metrics"
sleep "${DEMO_HOLD:-60}"
