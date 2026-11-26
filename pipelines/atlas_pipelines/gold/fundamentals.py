"""Gold fundamentals from EIA silver (slice E7): hourly per BA, and monthly storage buildout.

Why it exists: Phase D forecasting and descriptive views (the net-load duck curve) need
one tidy row per BA and hour; long-term battery views need storage capacity per BA and
month. Gold is computed ONLY from silver (never from the source), so it can always be
rebuilt; the same silver bytes give byte-identical Parquet (tested).
Who calls it: defs/eia.py (fundamentals_<ba>, buildout_storage tasks); tests.

Definitions (config/gold/fundamentals.yaml, docs/data/gold.md; status proposed):
  net_load_mw      = demand − solar − wind; null if the BA reported no solar or no wind that day
  generation_mw    = sum of NET generation over every fuel the BA reported (storage included,
                     so charging hours lower it)
  renewable_mw     = sum over reported fuels in the renewable set
  renewable_share  = renewable_mw / generation_mw (null if generation_mw <= 0)
Invariants:
  - An incomplete demand or generation day produces no gold rows (blocking), never partial.
  - Missing interchange hours stay null and are counted (warn); nothing is filled.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.schemas import FUNDAMENTALS_HOURLY, STORAGE_BUILDOUT, schema_check
from atlas_pipelines.core.task import CheckResult

DEFINITION_VERSION = "fundamentals-1 (proposed 2026-11-26)"
BUILDOUT_VERSION = "storage_buildout-1 (proposed 2026-11-26)"
HOURS = 24


def load_definitions(path: Path | None = None) -> dict:
    path = path or config.REPO_ROOT / "config" / "gold" / "fundamentals.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    canonical = set((yaml.safe_load((config.REPO_ROOT / "config" / "fuel_types.yaml").read_text(encoding="utf-8"))
                     .get("canonical") or {}))
    for key in ("renewable_fuel_types", "net_load_subtracts"):
        unknown = set(raw.get(key) or []) - canonical
        if raw.get(key) is None or unknown:
            raise ValueError(f"{path}: {key} must list canonical fuel types; unknown {sorted(unknown)}")
    return raw


def _empty(schema: dict) -> pl.DataFrame:
    return pl.DataFrame(schema={c: t for c, (t, _) in schema.items()})


def hourly(demand: pl.DataFrame, generation: pl.DataFrame, interchange: pl.DataFrame | None, defs: dict,
           hashes: dict[str, str | None]) -> tuple[pl.DataFrame, list[CheckResult]]:
    """One BA's UTC day of silver → 24 gold rows, or none (with the blocking reason)."""
    actual = demand.filter(pl.col("kind") == "actual").sort("interval_start_utc")
    demand_ok = (actual.height == HOURS and actual["demand_mw"].null_count() == 0
                 and actual["interval_minutes"].unique().to_list() == [60])
    per_fuel = generation.group_by("fuel_type").agg(pl.col("generation_mw").drop_nulls().len().alias("hours"))
    short = sorted(per_fuel.filter(pl.col("hours") != HOURS)["fuel_type"].to_list())
    checks = [
        CheckResult("demand_day_complete", "blocking", demand_ok,
                    {"actual_hours": actual.height, "null_demand": actual["demand_mw"].null_count()}),
        CheckResult("generation_day_complete", "blocking", generation.height > 0 and not short,
                    {"fuels": sorted(per_fuel["fuel_type"].to_list()), "fuels_not_24h": short}),
    ]
    if not all(c.passed for c in checks):
        return _empty(FUNDAMENTALS_HOURLY), checks

    reported = set(per_fuel["fuel_type"].to_list())
    by_hour = generation.group_by("interval_start_utc").agg(
        pl.col("generation_mw").sum().alias("generation_mw"),
        pl.col("generation_mw").filter(pl.col("fuel_type").is_in(defs["renewable_fuel_types"])).sum().alias("renewable_mw"),
        *[pl.col("generation_mw").filter(pl.col("fuel_type") == f).sum().alias(f"{f}_mw") for f in ("solar", "wind")],
    )
    for f in ("solar", "wind"):
        if f not in reported:  # not reported that day: unknown, not zero
            by_hour = by_hour.with_columns(pl.lit(None, dtype=pl.Float64).alias(f"{f}_mw"))
    df = actual.select("market", "zone", "interval_start_utc", "interval_end_utc", "local_date", "local_hour",
                       "demand_mw").join(by_hour, on="interval_start_utc", how="left")
    inter = (interchange.select("interval_start_utc", "net_interchange_mw") if interchange is not None
             else pl.DataFrame(schema={"interval_start_utc": FUNDAMENTALS_HOURLY["interval_start_utc"][0],
                                       "net_interchange_mw": pl.Float64}))
    df = df.join(inter, on="interval_start_utc", how="left")
    df = df.with_columns(
        (pl.col("demand_mw") - pl.col("solar_mw") - pl.col("wind_mw")).alias("net_load_mw"),
        pl.when(pl.col("generation_mw") > 0).then(pl.col("renewable_mw") / pl.col("generation_mw"))
          .otherwise(None).alias("renewable_share"),
        pl.lit(hashes["demand"]).alias("demand_source_hash"),
        pl.lit(hashes["generation"]).alias("generation_source_hash"),
        pl.lit(hashes.get("interchange"), dtype=pl.String).alias("interchange_source_hash"),
    ).select(list(FUNDAMENTALS_HOURLY)).sort("interval_start_utc")
    missing_inter = df["net_interchange_mw"].null_count()
    absent = sorted(set(defs["renewable_fuel_types"]) - reported)
    checks += [
        schema_check(df, FUNDAMENTALS_HOURLY, "fundamentals_hourly"),
        CheckResult("interchange_hours_present", "warn", missing_inter == 0,
                    {"missing_hours": missing_inter, "interchange_partition": interchange is not None}),
        CheckResult("solar_and_wind_reported", "warn", {"solar", "wind"} <= reported,
                    {"reported_fuels": sorted(reported)}),
        CheckResult("renewable_types_reported", "warn", not absent, {"not_reported": absent}),
    ]
    return df, checks


def buildout(storage: pl.DataFrame, source_hash: str) -> tuple[pl.DataFrame, list[CheckResult]]:
    """One report month of silver.storage_capacity → one row per BA (zone null kept)."""
    mw = pl.col("capacity_mw")
    df = (storage.group_by("market", "zone", "report_month").agg(
        mw.filter(pl.col("status") == "operating").sum().alias("operating_mw"),
        (pl.col("status") == "operating").sum().cast(pl.Int32).alias("operating_generators"),
        mw.filter((pl.col("status") == "operating") & (pl.col("operating_month") == pl.col("report_month")))
          .sum().alias("added_mw"),
        mw.filter(pl.col("status") == "standby").sum().alias("standby_mw"),
        mw.filter(pl.col("status").str.starts_with("out_of_service")).sum().alias("out_of_service_mw"),
    ).with_columns(pl.lit(source_hash).alias("source_hash"))
          .select(list(STORAGE_BUILDOUT)).sort("zone", nulls_last=True))
    months = storage["report_month"].unique().to_list()
    return df, [
        CheckResult("input_one_month", "blocking", len(months) == 1, {"months": [str(m) for m in months]}),
        schema_check(df, STORAGE_BUILDOUT, "storage_buildout"),
        # Totals must equal the silver they came from (nothing lost in grouping, nulls included).
        CheckResult("totals_match_silver", "blocking",
                    abs(df["operating_mw"].sum() - storage.filter(pl.col("status") == "operating")["capacity_mw"].sum()) < 1e-6,
                    {"gold_operating_mw": df["operating_mw"].sum()}),
        CheckResult("nameplate_present", "warn", storage["capacity_mw"].null_count() == 0,
                    {"generators_without_nameplate": storage["capacity_mw"].null_count()}),
    ]
