"""EIA-930 fuel-type-data → silver.generation (slice E3).

Why it exists: hourly net generation by fuel per BA drives the net-load and
renewable-share analysis (E7) and Phase D features. Source codes (SUN, WND, NG, …)
are mapped to canonical fuel types through config/mappings/fuel_types.yaml; the EIA code is kept
as source_type, and every row is direction "net" (EIA-930 sends one signed net series).
Who calls it: defs/eia.py (eia930_fuel_daily); tests with real saved responses.

Same time rules as region.py: `period` is UTC and hour-ending; a UTC day D is
periods D T01 … D+1 T00; local date/hour derive from the BA's zone.

Checks:
  blocking: every code mapped (a new EIA code stops the partition until mapped),
            units, unique (interval, fuel), each fuel the BA reported that day has
            all 24 hours (gaps stay gaps), at least one fuel reported, schema.
  warn:     negative net generation for fuels where it isn't expected by definition
            (counted, never fixed; EIA-930 is NET generation); solar near zero at night.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core import completeness
from atlas_pipelines.core.schemas import GENERATION, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.eia import client
from atlas_pipelines.sources.eia.region import eia_section, interval_of, request_params as _region_params

ROUTE = "electricity/rto/fuel-type-data"
CODE_VERSION = "eia_fuel-1"
KEY_COLUMNS = ("respondent", "fueltype")
UNIT = "megawatthours"
NIGHT_LOCAL_HOURS = range(0, 4)      # 00:00-04:00 local: no sun anywhere in the covered BAs
NIGHT_SOLAR_SHARE = 0.02             # |solar| at night ≤ 2% of the day's solar peak (our tolerance)


def request_params(ba: str, day: date) -> list[tuple[str, str]]:
    return _region_params(ba, day)  # same window and respondent facet; the route differs


def parse(rows: list[dict], market: Market, ba: str, day: date | None, fetched_at: datetime, source_uri: str,
          mapping: dict[str, fuels.FuelCode]) -> tuple[pl.DataFrame, set[str]]:
    """Rows of one BA → silver.generation rows, and the set of codes with no mapping."""
    tz = ZoneInfo(eia_section(market)["bas"][ba]["tz"])
    lo = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) if day else None
    out, unknown = [], set()
    for r in rows:
        if r.get("respondent") != ba:
            continue
        start, end = interval_of(r["period"])
        if day and not (lo <= start < lo + timedelta(days=1)):
            continue
        code = r.get("fueltype")
        m = mapping.get(code)
        if m is None:
            unknown.add(code)
            continue
        loc = start.astimezone(tz)
        out.append({"market": market.id, "zone": ba, "interval_start_utc": start, "interval_end_utc": end,
                    "interval_minutes": 60, "local_date": loc.date(), "local_hour": loc.hour,
                    "fuel_type": m.fuel_type, "source_type": code, "direction": "net", "generation_mw": client.to_number(r.get("value")),
                    "kind": "actual", "issued_at_utc": None, "horizon_minutes": None, "fetched_at_utc": fetched_at,
                    "availability_assumption": None, "source_uri": source_uri})
    schema = {c: t for c, (t, _) in GENERATION.items() if c != "series_complete"}
    df = pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)
    if day:  # per series (ADR-B10); the task still blocks an incomplete EIA day (each_reported_fuel_complete)
        df = completeness.mark_complete(df, ["source_type"], lo, lo + timedelta(days=1), "generation_mw")
    else:    # multi-day parse (tests/backfill reads): one series per code and UTC day
        df = df.with_columns(pl.col("interval_start_utc").dt.date().alias("_d"))
        stats = df.group_by("source_type", "_d").agg(((pl.len() == 24) & (pl.col("generation_mw").null_count() == 0))
                                                    .alias("series_complete"))
        df = df.join(stats, on=["source_type", "_d"], how="left").drop("_d")
    return df.select(list(GENERATION)).sort("fuel_type", "interval_start_utc"), unknown


def checks(df: pl.DataFrame, rows: list[dict], unknown: set[str], day: date,
           mapping: dict[str, fuels.FuelCode]) -> list[CheckResult]:
    expected = {datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=h) for h in range(24)}
    units = sorted({r.get("value-units") for r in rows})
    dupes = df.height - df.select("interval_start_utc", "source_type").unique().height
    gaps = {}
    for code in sorted(set(df["source_type"].to_list())):
        have = {s for s, v in zip(*[df.filter(pl.col("source_type") == code)[c].to_list()
                                    for c in ("interval_start_utc", "generation_mw")]) if v is not None}
        if expected - have:
            gaps[code] = [iso_utc(s) for s in sorted(expected - have)[:5]] + ([f"+{len(expected - have) - 5} more"] if len(expected - have) > 5 else [])
    warn_codes = {c for c, m in mapping.items() if m.negative == "warn"}
    neg = df.filter(pl.col("source_type").is_in(list(warn_codes)) & (pl.col("generation_mw") < 0))
    neg_by = dict(neg.group_by("source_type").len().iter_rows()) if neg.height else {}
    solar = df.filter(pl.col("fuel_type") == "solar")
    peak = solar["generation_mw"].abs().max() if solar.height else None
    night = solar.filter(pl.col("local_hour").is_in(list(NIGHT_LOCAL_HOURS)))
    night_bad = night.filter(pl.col("generation_mw").abs() > NIGHT_SOLAR_SHARE * (peak or 0)).height if peak else 0
    return [
        schema_check(df, GENERATION, "generation"),
        CheckResult("fuel_codes_mapped", "blocking", not unknown,
                    {"unknown_codes": sorted(unknown), "action": "map in config/mappings/fuel_types.yaml or list as unmapped"}),
        CheckResult("units_megawatthours", "blocking", units == [UNIT] or (not rows), {"units": units}),
        CheckResult("primary_key_unique", "blocking", dupes == 0, {"duplicates": dupes}),
        CheckResult("fuels_reported", "blocking", df.height > 0, {"rows": df.height}),
        CheckResult("each_reported_fuel_complete", "blocking", not gaps, {"expected_hours": 24, "gaps": gaps}),
        CheckResult("negative_net_generation", "warn", not neg_by,
                    {"hours_by_code": neg_by, "note": "EIA-930 is net generation; storage/other negatives are expected and not counted"}),
        CheckResult("solar_near_zero_at_night", "warn", night_bad == 0,
                    {"night_local_hours": list(NIGHT_LOCAL_HOURS), "tolerance_share_of_peak": NIGHT_SOLAR_SHARE,
                     "peak_mw": peak, "night_hours_above": night_bad}),
    ]


def incomplete_only(results: list[CheckResult]) -> bool:
    failed = [c for c in results if c.severity == "blocking" and not c.passed]
    return bool(failed) and all(c.check_name in ("each_reported_fuel_complete", "fuels_reported") for c in failed)
