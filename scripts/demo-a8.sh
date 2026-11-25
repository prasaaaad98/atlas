#!/usr/bin/env bash
# Slice A8 demo: fenced materializations, check-gated current pointer, freshness.
#
#  1. engine_assets for day D: a worker writes a real file under an attempt
#     directory and reports it; Atlas records it and the pointer moves (fresh).
#  2. A re-run of D whose blocking check fails (ATLAS_DEMO_FAIL_PARTITIONS=D):
#     the new file is written and recorded, but the pointer stays on the first
#     attempt's file (status failed_check).
#  3. Freshness policy "every minute, tomorrow's partition must be fresh": the
#     minute's deadline has already passed when the demo starts, so D+1 is
#     late within a second (one asset.late event, not one per pass);
#     building D+1 makes it fresh.
# Needs: make up && make migrate. No external network. Who calls it: make demo-a8.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT" ATLAS_FRESHNESS_INTERVAL=1s
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-a8   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
BIN="$LAKE/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas
"$BIN" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

D=$(date -u +%Y-%m-%d)
TOMORROW=$(date -u -d tomorrow +%Y-%m-%d)
# Fresh state for this demo's asset (demo tables only).
STARTED=$($PSQL -Atc "SELECT now()")
$PSQL -qc "DELETE FROM asset_partitions WHERE asset_key = 'demo.engine_assets'"
$ATLAS register engine_assets
# Unique idempotency keys: the demo can be re-run (CLI submits use deterministic keys).
submit() {
  curl -s -X POST "$ATLAS_URL/runs" -d "{\"idempotency_key\":\"demo-a8-$1-$RANDOM$RANDOM\",\"pipeline\":\"engine_assets\",\"partition_key\":\"$1\",\"trigger\":\"manual\",\"priority_class\":\"adhoc\"}" > /dev/null
}
work() { "$@" $ATLAS worker --worker-id w1 --exit-when-idle | grep engine_assets || true; }
show() {
  curl -s "$ATLAS_URL/assets/demo.engine_assets/partitions?limit=5" | "$PY" -c '
import json, sys
for p in json.load(sys.stdin):
    cur = p.get("current") or {}
    print("  %s  status=%-12s current_attempt=%s  uri=%s" % (p["partition_key"], p["status"], (p.get("current_attempt_id") or "-")[:8], cur.get("uri", "-")))'
}

echo "== 1. build $D"
submit "$D"
work env
show

echo "== 2. re-run $D with a failing blocking check: pointer must not move"
submit "$D"
work env ATLAS_DEMO_FAIL_PARTITIONS="$D"
show
$PSQL -c "SELECT left(m.attempt_id::text, 8) AS attempt, m.uri, (p.current_materialization_id = m.id) AS is_current,
                 (SELECT bool_and(c.passed) FROM asset_checks c WHERE c.attempt_id = m.attempt_id AND c.severity='blocking') AS checks_passed
          FROM materializations m JOIN asset_partitions p ON p.asset_key = m.asset_key AND p.partition_key = m.partition_key
          WHERE m.asset_key='demo.engine_assets' AND m.partition_key='$D' ORDER BY m.created_at DESC LIMIT 2"
echo "  files on disk (both attempts wrote one; only the current one is published):"
find "$LAKE/demo/engine_assets" -name data.json | sed 's/^/    /'

echo "== 3. freshness: by every minute, tomorrow ($TOMORROW) must be fresh"
sleep $(( 61 - $(date +%S | sed 's/^0//') ))   # cross a minute boundary so the policy deadline passes
show
$PSQL -Atc "SELECT 'asset.late events for $TOMORROW: ' || count(*) FROM events WHERE type='asset.late'
            AND payload->>'asset_key'='demo.engine_assets' AND payload->>'partition_key'='$TOMORROW'
            AND created_at >= '$STARTED'"
submit "$TOMORROW"
work env
show
