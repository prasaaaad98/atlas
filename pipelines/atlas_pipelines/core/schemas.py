"""Canonical silver schemas (PHASE_AB_PLAN.md §3.3, ADR-B02) and the schema check.

Why it exists: every market adapter writes the same columns with the same
types, so shared checks, gold builders and the simulator never special-case a
market. The schema check is blocking: a missing column, wrong type or an
unexpected null stops the pointer from moving.
Who calls it: adapters' silver tasks, before writing Parquet.

ADR-B02 decisions reflected here (proposed, 2026-11-25):
  - `zone` (not `location`) everywhere, matching the registry's `zones`.
  - Lineage column `source_uri`: the bronze file's lake URI. It is immutable
    and identical under every runner, unlike database materialization ids.
  - All timestamps are UTC (`Datetime("us", "UTC")`).
"""

from __future__ import annotations

import polars as pl

from atlas_pipelines.core.task import CheckResult

UTC = pl.Datetime("us", "UTC")

# column -> (dtype, nullable)
POWER_PRICES: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "interval_start_utc": (UTC, False),
    "interval_end_utc": (UTC, False),
    "interval_minutes": (pl.Int16, False),
    "local_date": (pl.Date, False),
    "local_block": (pl.Int16, False),
    "price": (pl.Float64, False),  # per MWh, local currency
    "currency": (pl.String, False),
    "source_uri": (pl.String, False),
}

WEATHER_HOURLY: dict[str, tuple[pl.DataType, bool]] = {
    "location_id": (pl.String, False),  # "<market>:<zone>", e.g. "entsoe:DE_LU"
    "ts_utc": (UTC, False),  # source timestamp; see per-variable semantics below
    "temperature_c": (pl.Float64, True),  # instant at ts_utc
    "ghi_w_m2": (pl.Float64, True),  # mean over [ts_utc - 1 h, ts_utc) (Open-Meteo "preceding hour mean")
    "cloud_cover_pct": (pl.Float64, True),  # instant
    "wind_speed_100m_m_s": (pl.Float64, True),  # instant
    "is_forecast": (pl.Boolean, False),
    "issued_at_utc": (UTC, True),  # null for archive/reanalysis rows (§3.4a)
    "source_uri": (pl.String, False),
}


# gold.daily_spreads (gold/spreads.py; definitions owner-confirmed 2026-11-25).
DAILY_SPREADS: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "local_date": (pl.Date, False),
    "interval_minutes": (pl.Int16, False),
    "intervals": (pl.Int16, False),
    "currency": (pl.String, False),
    "min_price": (pl.Float64, False),
    "max_price": (pl.Float64, False),
    "max_min_spread": (pl.Float64, False),
    "block_minutes": (pl.Int16, False),
    "best_2h_buy_start_utc": (UTC, True),   # null only if two blocks don't fit in the day
    "best_2h_buy_mean": (pl.Float64, True),
    "best_2h_sell_start_utc": (UTC, True),
    "best_2h_sell_mean": (pl.Float64, True),
    "best_2h_spread": (pl.Float64, True),
    "source_uri": (pl.String, False),          # the silver file the row was computed from
    "source_content_hash": (pl.String, False),
    "definition_version": (pl.String, False),
}


# silver.calendar (B4e; PHASE_AB_PLAN.md §3.3). Key (market, zone, local_date).
# special_day_source and provenance are lineage additions to the plan's columns.
CALENDAR: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "local_date": (pl.Date, False),
    "is_weekend": (pl.Boolean, False),
    "is_public_holiday": (pl.Boolean, False),
    "holiday_name": (pl.String, True),
    "is_special_day": (pl.Boolean, False),
    "special_day_name": (pl.String, True),
    "special_day_announced_on": (pl.Date, True),   # point-in-time (§3.4a): not usable before this date
    "special_day_source": (pl.String, True),       # the owner's citation
    "dst_transition": (pl.String, False),          # none | short | long, from the zone's local time zone
    "provenance": (pl.String, False),              # holidays version + special-day file hash
}


def empty_frame(schema: dict) -> pl.DataFrame:
    return pl.DataFrame(schema={c: t for c, (t, _) in schema.items()})


def schema_check(df: pl.DataFrame, schema: dict, name: str) -> CheckResult:
    problems = []
    missing = [c for c in schema if c not in df.columns]
    extra = [c for c in df.columns if c not in schema]
    if missing:
        problems.append(f"missing columns {missing}")
    if extra:
        problems.append(f"unexpected columns {extra}")
    for col, (dtype, nullable) in schema.items():
        if col not in df.columns:
            continue
        if df.schema[col] != dtype:
            problems.append(f"{col}: {df.schema[col]} != {dtype}")
        if not nullable and df[col].null_count():
            problems.append(f"{col}: {df[col].null_count()} nulls")
    return CheckResult(f"schema_{name}", "blocking", not problems, {"problems": problems, "rows": df.height})
