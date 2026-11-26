#!/usr/bin/env bash
# OWNER-RUN (network, ENTSOE_TOKEN in .env): full ENTSO-E history through Atlas.
# 3 zones x ~1,350 days at 60 requests/min (config/limits/entsoe.yaml) takes over an hour.
# Data use: development, testing and internal research only (docs/data/sources/entsoe.md).
# FROM, TO, MAX_RUNS, WORKERS, CHAOS=1, BACKFILL=<id> (resume): see scripts/atlas-backfill.sh.
export PIPELINE=prices_entsoe_daily
export PREFIXES=bronze.entsoe,silver.power_prices__entsoe,gold.daily_spreads__entsoe
exec bash "$(dirname "$0")/atlas-backfill.sh"
