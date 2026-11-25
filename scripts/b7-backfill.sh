#!/usr/bin/env bash
# OWNER-RUN (network, ENTSOE_TOKEN in .env): full ENTSO-E history through Atlas.
#
# Starts the control plane (unless ATLAS_URL already answers), syncs limit keys,
# registers prices_entsoe_daily, creates a backfill FROM..TO (oldest first, at
# most MAX_RUNS runs at once, priority backfill) and runs WORKERS workers until
# it finishes. With CHAOS=1 a random worker is killed (kill -9) every
# CHAOS_EVERY seconds and restarted: the acceptance test "backfill with worker
# kills, no corrupt current partitions".
# Requests are bounded by the `entsoe` limit key (config/limits/entsoe.yaml, 60/min),
# so 3 zones x ~1,350 days takes over an hour. Data use: development, testing and
# internal research only (docs/data/sources/entsoe.md).
# Afterwards: catalog from Atlas pointers + a pointer/file hash check.
set -euo pipefail

FROM=${FROM:-2022-04-01}
TO=${TO:-$(date -u -d yesterday +%Y-%m-%d)}
MAX_RUNS=${MAX_RUNS:-2}
WORKERS=${WORKERS:-2}
CHAOS=${CHAOS:-0}
CHAOS_EVERY=${CHAOS_EVERY:-300}
PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
export ATLAS_URL=${ATLAS_URL:-http://127.0.0.1:8080}
export PYTHONUNBUFFERED=1
LAKE=${LAKE_ROOT:-lake}
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
LOGS="$LAKE/_logs/b7-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$LOGS"

SERVER=
if ! curl -sf "$ATLAS_URL/healthz" > /dev/null; then
  go build -o "$LOGS/atlas$(go env GOEXE)" ./cmd/atlas
  ATLAS_ADDR="${ATLAS_URL#http://}" "$LOGS/atlas$(go env GOEXE)" > "$LOGS/atlas.log" 2>&1 &
  SERVER=$!
  for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done
fi
declare -A W
start_worker() { $ATLAS worker --worker-id "b7-w$1" > "$LOGS/w$1.log" 2>&1 & W[$1]=$!; }
cleanup() { kill "${W[@]}" $SERVER 2>/dev/null || true; }
trap cleanup EXIT

$ATLAS limits sync
$ATLAS register prices_entsoe_daily
BF=$($ATLAS backfill prices_entsoe_daily --from "$FROM" --to "$TO" --max-concurrent "$MAX_RUNS" | sed -n 's/.*id=\([0-9a-f-]*\).*/\1/p')
echo "backfill $BF: $FROM .. $TO, max $MAX_RUNS runs, $WORKERS workers, chaos=$CHAOS (logs: $LOGS)"
for i in $(seq "$WORKERS"); do start_worker "$i"; done

last_kill=$(date +%s)
while true; do
  status=$(curl -s "$ATLAS_URL/backfills/$BF" | "$PY" -c 'import json,sys; b=json.load(sys.stdin); r=b["runs"]; print(b["status"], r["Total"], r["Active"], r["Succeeded"], r["Failed"])')
  echo "$(date -u +%H:%M:%S) $status"
  [ "${status%% *}" != "RUNNING" ] && break
  if [ "$CHAOS" = "1" ] && [ $(( $(date +%s) - last_kill )) -ge "$CHAOS_EVERY" ]; then
    i=$(( RANDOM % WORKERS + 1 )); kill -9 "${W[$i]}" 2>/dev/null || true
    echo "chaos: killed worker $i"; sleep 2; start_worker "$i"; last_kill=$(date +%s)
  fi
  sleep 30
done

$ATLAS catalog --from-atlas
"$PY" - "$LAKE" <<'PYEOF'
import os, sys
from pathlib import Path
from atlas_pipelines.cli import atlas_pointers
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.worker import AtlasClient, urllib_send
lake = Path(sys.argv[1])
ps = [p for p in atlas_pointers(AtlasClient(urllib_send(os.environ["ATLAS_URL"]))) if p["asset_key"].startswith(("bronze.entsoe", "silver.power_prices__entsoe", "gold.daily_spreads__entsoe"))]
bad = [p for p in ps if not (lake / p["uri"]).exists() or sha256_file(lake / p["uri"]) != p["content_hash"]]
print(f"ENTSO-E current pointers: {len(ps)}; missing or corrupt files: {len(bad)}")
for p in bad[:20]:
    print("  ", p["asset_key"], p["partition_key"], p["uri"])
sys.exit(1 if bad else 0)
PYEOF
