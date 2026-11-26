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


# silver.demand (PHASE_AB_PLAN.md §3.3, EIA plan §4). Actuals and forecast vintages.
# Additions to the plan: local_date/local_hour (derived from the zone's local time
# zone), fetched_at_utc + availability_assumption (EIA doesn't publish forecast issue
# times: issued_at_utc stays null, ADR-B06 addendum), source_uri lineage (ADR-B02).
DEMAND: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "interval_start_utc": (UTC, False),
    "interval_end_utc": (UTC, False),
    "interval_minutes": (pl.Int16, False),
    "local_date": (pl.Date, False),
    "local_hour": (pl.Int8, False),
    "demand_mw": (pl.Float64, True),          # average MW over the interval; null = source gap
    "kind": (pl.String, False),               # actual | forecast
    "issued_at_utc": (UTC, True),             # null when the source publishes no issue time (EIA-930)
    "horizon_minutes": (pl.Int32, True),      # interval_start − issued_at; null when issued_at is null
    "fetched_at_utc": (UTC, False),           # when we fetched it: the earliest a forecast is known to us
    "availability_assumption": (pl.String, True),  # registry rule for history, status assumed (EIA plan §4)
    "source_uri": (pl.String, False),
}

# silver.generation (PHASE_AB_PLAN.md §3.3, ADR-B04): demand's columns with fuel_type and
# generation_mw. fuel_type is canonical (config/mappings/fuel_types.yaml); source_type keeps
# the source's own code (B16, SUN) so mapping never loses detail. direction: generation |
# consumption (storage charging, a separate source series; value positive) | net (one signed
# series, EIA-930). Key: (kind, interval_start_utc, source_type, direction, fetched_at_utc).
GENERATION: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "interval_start_utc": (UTC, False),
    "interval_end_utc": (UTC, False),
    "interval_minutes": (pl.Int16, False),
    "local_date": (pl.Date, False),
    "local_hour": (pl.Int8, False),
    "fuel_type": (pl.String, False),
    "source_type": (pl.String, False),
    "direction": (pl.String, False),          # generation | consumption | net
    "generation_mw": (pl.Float64, True),      # average MW as sent; `net` rows may be negative (station use, charging)
    "kind": (pl.String, False),               # actual | forecast (EIA-930 has no fuel forecasts)
    "issued_at_utc": (UTC, True),
    "horizon_minutes": (pl.Int32, True),
    "fetched_at_utc": (UTC, False),
    "availability_assumption": (pl.String, True),
    "source_uri": (pl.String, False),
}

# silver.fuel_carbon_prices (PHASE_AB_PLAN.md §3.3). Key (commodity, hub, trade_date,
# delivery_period). Vocabulary in config/commodities/*.yaml. Additions: source_series,
# fetched_at_utc (point-in-time, §3.4a), source_uri lineage (ADR-B02).
FUEL_CARBON_PRICES: dict[str, tuple[pl.DataType, bool]] = {
    "commodity": (pl.String, False),
    "hub": (pl.String, False),
    "trade_date": (pl.Date, False),
    "delivery_period": (pl.String, False),
    "price": (pl.Float64, False),
    "currency": (pl.String, False),
    "unit": (pl.String, False),
    "source_series": (pl.String, False),
    "fetched_at_utc": (UTC, False),
    "source_uri": (pl.String, False),
}

# silver.storage_capacity (EIA plan §4): one row per storage generator per report month,
# as EIA's generator inventory lists it. zone = balancing authority (null when EIA gives
# none). energy_mwh and expected_online_month: the API route has neither (always null
# until a source that has them is added). Key (report_month, plant_id, generator_id).
STORAGE_CAPACITY: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, True),
    "state": (pl.String, True),
    "report_month": (pl.Date, False),
    "plant_id": (pl.String, False),
    "generator_id": (pl.String, False),
    "plant_name": (pl.String, True),
    "entity_name": (pl.String, True),
    "storage_type": (pl.String, False),
    "status": (pl.String, False),
    "source_status": (pl.String, False),
    "source_prime_mover": (pl.String, False),
    "capacity_mw": (pl.Float64, True),
    "net_summer_mw": (pl.Float64, True),
    "net_winter_mw": (pl.Float64, True),
    "energy_mwh": (pl.Float64, True),
    "operating_month": (pl.Date, True),
    "planned_retirement_month": (pl.Date, True),
    "expected_online_month": (pl.Date, True),
    "fetched_at_utc": (UTC, False),
    "source_uri": (pl.String, False),
}

# silver.interchange (EIA plan §4): net interchange per zone and interval.
INTERCHANGE: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "interval_start_utc": (UTC, False),
    "interval_end_utc": (UTC, False),
    "interval_minutes": (pl.Int16, False),
    "local_date": (pl.Date, False),
    "local_hour": (pl.Int8, False),
    "net_interchange_mw": (pl.Float64, True),  # positive = net exports (registry: interchange_sign)
    "fetched_at_utc": (UTC, False),
    "source_uri": (pl.String, False),
}


# gold.fundamentals_hourly (EIA plan E7): per BA and UTC hour, from silver demand,
# generation and interchange only. Definitions: docs/data/gold.md (proposed, owner to
# confirm) and config/gold/fundamentals.yaml. Null = not computable from what the
# source reported (never filled).
FUNDAMENTALS_HOURLY: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, False),
    "interval_start_utc": (UTC, False),
    "interval_end_utc": (UTC, False),
    "local_date": (pl.Date, False),
    "local_hour": (pl.Int8, False),
    "demand_mw": (pl.Float64, False),
    "solar_mw": (pl.Float64, True),            # null: the BA reported no solar that day
    "wind_mw": (pl.Float64, True),
    "net_load_mw": (pl.Float64, True),         # demand − solar − wind; null if either is null
    "generation_mw": (pl.Float64, False),      # total NET generation, every reported fuel
    "renewable_mw": (pl.Float64, False),       # reported fuels in the configured renewable set
    "renewable_share": (pl.Float64, True),     # renewable_mw / generation_mw; null if generation_mw <= 0
    "net_interchange_mw": (pl.Float64, True),  # positive = net exports; null = source gap
    "demand_source_hash": (pl.String, False),  # sha256 of each silver input file (lineage)
    "generation_source_hash": (pl.String, False),
    "interchange_source_hash": (pl.String, True),  # null when no current interchange partition
}

# gold.storage_buildout (EIA plan E7): per BA (zone null = EIA gave none) and report month.
STORAGE_BUILDOUT: dict[str, tuple[pl.DataType, bool]] = {
    "market": (pl.String, False),
    "zone": (pl.String, True),
    "report_month": (pl.Date, False),
    "operating_mw": (pl.Float64, False),       # nameplate, status operating
    "operating_generators": (pl.Int32, False),
    "added_mw": (pl.Float64, False),           # operating, with operating_month = report_month
    "standby_mw": (pl.Float64, False),
    "out_of_service_mw": (pl.Float64, False),  # both out-of-service statuses
    "source_hash": (pl.String, False),
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
