#!/usr/bin/env bash
# OWNER-RUN (network, ENTSOE_TOKEN in .env): the 14-day schedule run on this machine.
#
# What it does:
#   - makes sure Postgres is up and migrated;
#   - starts the control plane on :8080 (unless one already answers) and one
#     long-polling worker, and restarts either if it exits;
#   - registers prices_entsoe_daily and gives it a schedule: every day at
#     12:00 Europe/Brussels, fetch the NEXT delivery day (offset +1), with
#     catchup from tomorrow, so days missed while the laptop was off or asleep
#     are fetched on wake-up instead of lost.
# 12:00 is our operating choice for when to start polling, not a claim about
# when ENTSO-E publishes: until a day is published, the fetch is retried every
# ≤5 min (up to ~3 h), and the schedule report brackets the publication time.
# The control plane writes an uptime row every minute; the report counts only
# days during which Atlas and a worker were up the whole time.
#
# Leave it running (Ctrl-C stops it; run it again to resume, nothing is lost).
# Check progress any time:  python -m atlas_pipelines schedule-report prices_entsoe_daily
# Data use: development, testing and internal research only (docs/data/sources/entsoe.md).
set -uo pipefail

PY=${PY:-pipelines/.venv/Scripts/python.exe}
[ -x "$PY" ] || PY=pipelines/.venv/bin/python
export ATLAS_URL=${ATLAS_URL:-http://127.0.0.1:8080}
export PYTHONUNBUFFERED=1
LAKE=${LAKE_ROOT:-lake}
ATLAS="$PY -m atlas_pipelines --lake-root $LAKE"
LOGS="$LAKE/_logs/schedule"
mkdir -p "$LOGS"
P=prices_entsoe_daily

make up > /dev/null && make migrate > /dev/null || { echo "Postgres not ready (is Docker Desktop running?)"; exit 1; }
BIN="$LOGS/atlas$(go env GOEXE)"
go build -o "$BIN" ./cmd/atlas || exit 1

SERVER= WORKER=
own_server=0
curl -sf "$ATLAS_URL/healthz" > /dev/null || own_server=1
start_server() { ATLAS_ADDR="${ATLAS_URL#http://}" "$BIN" >> "$LOGS/atlas.log" 2>&1 & SERVER=$!; sleep 2; }
start_worker() { $ATLAS worker --worker-id "schedule-$(hostname)" >> "$LOGS/worker.log" 2>&1 & WORKER=$!; }
trap 'kill $WORKER $SERVER 2>/dev/null; echo; echo "stopped (run again to resume)"; exit 0' INT TERM
[ "$own_server" = 1 ] && start_server
for _ in $(seq 50); do curl -sf "$ATLAS_URL/healthz" >/dev/null && break; sleep 0.2; done

$ATLAS limits sync > /dev/null && $ATLAS register $P || exit 1
if ! curl -sf "$ATLAS_URL/pipelines/$P/schedule" > /dev/null; then   # first time only: keep the cursor on re-runs
  TOMORROW=$(TZ=Europe/Brussels date -d tomorrow +%Y-%m-%d)
  $ATLAS schedule $P --cron "0 12 * * *" --tz Europe/Brussels --offset-days 1 --catchup --start "$TOMORROW" || exit 1
fi
start_worker
echo "running: control plane $([ "$own_server" = 1 ] && echo "pid $SERVER" || echo "(already running)"), worker pid $WORKER"
echo "logs: $LOGS   report: $ATLAS schedule-report $P"

while true; do
  sleep 60
  if [ "$own_server" = 1 ] && ! kill -0 "$SERVER" 2>/dev/null; then echo "$(date -u +%FT%TZ) control plane exited; restarting"; start_server; fi
  if ! kill -0 "$WORKER" 2>/dev/null; then echo "$(date -u +%FT%TZ) worker exited; restarting"; start_worker; fi
done
