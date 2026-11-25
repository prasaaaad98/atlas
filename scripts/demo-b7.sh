#!/usr/bin/env bash
# Slice B7 demo (offline): the first market through Atlas, end to end, on REAL data.
#
# A local fake ENTSO-E (scripts/fake_entsoe.py) serves the owner's saved real
# responses unchanged. Atlas (control plane + 3 Python workers) runs
# prices_entsoe_daily: ingest_<zone> → spreads_<zone> (gold) per zone.
#   1. Backfill 2025-09-29 .. 2025-10-02 (max 2 runs at once). Only 09-30 and
#      10-01 have saved files: the other two days get the real "no data" answer
#      and fail day_ahead_published. Missing stays missing, and their gold is canceled.
#   2. Four more real days (both DST days of each era, a normal day). One gets an
#      impossible deadline (1 s): the miss is detected and recorded.
#   3. A worker is killed with kill -9 mid-run: its tasks are re-run.
#   4. Catalog from Atlas's pointers; gold.daily_spreads queried with DuckDB;
#      every current pointer's file re-hashed (no corrupt current partition).
# Needs: make up && make migrate, and the local-only ENTSO-E fixtures. No network.
# Who calls it: make demo-b7.
set -euo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
PORT=${PORT:-18080}
FAKE=18099
export ATLAS_URL="http://127.0.0.1:$PORT" ATLAS_ADDR="127.0.0.1:$PORT"
export ATLAS_REAPER_INTERVAL=1s ATLAS_SCHEDULER_INTERVAL=1s ATLAS_DEADLINE_INTERVAL=1s
export ATLAS_ENTSOE_BASE_URL="http://127.0.0.1:$FAKE/api"
export ENTSOE_TOKEN="demo-not-a-real-token"   # the fake only checks that one is sent
export PYTHONUNBUFFERED=1
export DATABASE_URL=${DATABASE_URL:-postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable}
LAKE=bin/demo-b7   # repo-relative (bin/ is gitignored)
rm -rf "$LAKE" && mkdir -p "$LAKE"
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
PSQL="docker compose -f deploy/compose/docker-compose.yml exec -T postgres psql -U atlas -d atlas"
P=prices_entsoe_daily

"$PY" scripts/fake_entsoe.py $FAKE 2> "$LAKE/fake.log" &
FAKEPID=$!
BIN="$LAKE/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas
"$BIN" > "$LAKE/atlas.log" 2>&1 &
SERVER=$!
W=()
trap 'kill "${W[@]}" $SERVER $FAKEPID 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

# Earlier demo runs of this pipeline hold these partitions only while active; cancel leftovers.
for id in $($PSQL -Atc "SELECT id FROM workflow_runs WHERE pipeline_name='$P' AND status IN ('PENDING','RUNNING')"); do
  curl -s -X POST "$ATLAS_URL/runs/$id/cancel" > /dev/null
done
STARTED=$($PSQL -Atc "SELECT now()")
$ATLAS limits sync | sed 's/ {.*//'
$ATLAS register $P

echo "== 1-3. backfill + 4 single days, 3 workers (lease 10 s), one killed"
for w in 1 2 3; do $ATLAS worker --worker-id "w$w" --lease-seconds 10 > "$LAKE/w$w.log" 2>&1 & W+=($!); done
BF=$($ATLAS backfill $P --from 2025-09-29 --to 2025-10-02 --max-concurrent 2 | sed -n 's/.*id=\([0-9a-f-]*\).*/\1/p')
key() { echo "demo-b7-$1-$RANDOM$RANDOM"; }
for d in 2025-03-30 2025-10-26 2026-03-29; do
  curl -s -X POST "$ATLAS_URL/runs" -d "{\"idempotency_key\":\"$(key $d)\",\"pipeline\":\"$P\",\"partition_key\":\"$d\",\"trigger\":\"manual\",\"priority_class\":\"adhoc\"}" > /dev/null
done
DL=$(date -u -d '+1 second' +%Y-%m-%dT%H:%M:%SZ)
curl -s -X POST "$ATLAS_URL/runs" -d "{\"idempotency_key\":\"$(key late)\",\"pipeline\":\"$P\",\"partition_key\":\"2026-11-18\",\"trigger\":\"manual\",\"priority_class\":\"adhoc\",\"deadline_at\":\"$DL\"}" > /dev/null
sleep 3
kill -9 "${W[0]}"; echo "killed worker w1 (pid ${W[0]})"
for _ in $(seq 180); do
  active=$($PSQL -Atc "SELECT count(*) FROM workflow_runs WHERE pipeline_name='$P' AND status IN ('PENDING','RUNNING')")
  [ "$active" = "0" ] && break; sleep 1
done

$PSQL -c "SELECT r.partition_key, r.trigger, r.status,
                 count(t.*) FILTER (WHERE t.name LIKE 'ingest%' AND t.status='SUCCEEDED') AS zones_ingested,
                 count(t.*) FILTER (WHERE t.name LIKE 'spreads%' AND t.status='SUCCEEDED') AS gold_built,
                 string_agg(DISTINCT a.error_class, ',') AS attempt_errors,
                 (r.deadline_missed_at IS NOT NULL) AS deadline_missed
          FROM workflow_runs r JOIN tasks t ON t.run_id = r.id LEFT JOIN task_attempts a ON a.task_id = t.id
          WHERE r.pipeline_name='$P' AND r.created_at >= '$STARTED' GROUP BY r.id ORDER BY r.partition_key"
$PSQL -c "SELECT a.worker_id, a.status, a.outcome, count(*) FROM task_attempts a JOIN workflow_runs r ON r.id=a.run_id
          WHERE r.pipeline_name='$P' AND r.created_at >= '$STARTED' GROUP BY 1,2,3 ORDER BY 1,2"
$ATLAS backfill-status "$BF" | grep -E '"status"|"Total"|"Succeeded"|"Failed"' | tr -d ' ,' | paste -sd' '

echo "== 4. catalog from Atlas pointers; gold.daily_spreads; integrity of every current pointer"
$ATLAS catalog --from-atlas | head -3
"$PY" - "$LAKE" <<'PYEOF'
import sys, duckdb
from pathlib import Path
from atlas_pipelines.cli import atlas_pointers
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.worker import AtlasClient, urllib_send
import os
lake = Path(sys.argv[1])
con = duckdb.connect(str(lake / "catalog.duckdb"), read_only=True)
con.execute("SET TimeZone='UTC'")
for row in con.execute("""SELECT zone, local_date, intervals, round(max_min_spread, 2), round(best_2h_spread, 2),
       strftime(best_2h_buy_start_utc, '%H:%M'), strftime(best_2h_sell_start_utc, '%H:%M')
       FROM gold.daily_spreads ORDER BY local_date, zone""").fetchall():
    print("  %-5s %s intervals=%-3s max-min=%-8s best-2h=%-8s buy %s sell %s UTC" % row)
pointers = atlas_pointers(AtlasClient(urllib_send(os.environ["ATLAS_URL"])))
mine = [p for p in pointers if (lake / p["uri"]).exists()]
bad = [p for p in mine if sha256_file(lake / p["uri"]) != p["content_hash"]]
print(f"current pointers in this lake: {len(mine)}, files whose hash differs from the pointer: {len(bad)}")
PYEOF
echo "(spreads in EUR/MWh; ENTSO-E-derived: internal research only, not for publication)"
