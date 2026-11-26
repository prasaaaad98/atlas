"""ENTSO-E generation per production type (actual 16.1.B&C) and wind/solar day-ahead forecast
(14.1.D) → silver.generation (slice B4b).

Why it exists: renewable output drives price spreads; Phase D needs actual generation by type
and the wind/solar forecast known before the day (point in time, §3.4a).
Who calls it: defs/entsoe_generation.py; tests on real saved documents (local-only fixtures).

Documents (GL_MarketDocument; parsed by load.parse_document): one TimeSeries per production
type (MktPSRType/psrType, mapped via config/fuel_types.yaml `entsoe`). Guide, 16.1.B&C: "Time
series with inBiddingZone_Domain attribute reflects Generation values while outBiddingZone_Domain
reflects Consumption values." Consumption (pumping B10, storage charging B25) is kept as its own
rows: generation_mw = −quantity, source_fuel_code "<code>/consumption", so a sum per hour is net
and nothing is netted silently.
Invariants: every series maps to a canonical type (unknown blocks, never defaults); every series
covers the whole local day at its resolution (else retried, then blocks); nothing is filled.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.schemas import GENERATION, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import load

CODE_VERSION = "entsoe_generation-1"
DOC = {"actual": ("A75", "A16"), "forecast": ("A69", "A01")}
CONSUMPTION = "/consumption"


def _direction(s: load.LoadSeries, eic: str) -> str | None:
    if s.in_eic == eic and s.zone_eic is None:
        return "generation"
    if s.zone_eic == eic and s.in_eic is None:
        return "consumption"
    return None


def build_silver(doc: load.LoadDocument, kind: str, market: Market, zone: str, local_date: date,
                 fetched_at: datetime, source_uri: str, mapping: dict[str, fuels.FuelCode],
                 availability: str | None) -> tuple[pl.DataFrame, set[str]]:
    """Rows of every mapped series for the zone in the local day; returns (frame, unknown codes)."""
    eic = da.series_rule(market, zone).eic
    start, end = market.day_bounds_utc(local_date)
    rows, unknown = [], set()
    for s in doc.series:
        direction = _direction(s, eic)
        if direction is None:
            continue
        code = s.psr_type or "?"
        if code not in mapping:
            unknown.add(code)
            continue
        sign = -1.0 if direction == "consumption" else 1.0
        for period in s.periods:
            for _, a, b, value in da.expand(period, s.curve_type):
                if not (start <= a < end):
                    continue
                loc = a.astimezone(market.tz)
                rows.append({"market": market.id, "zone": zone, "interval_start_utc": a, "interval_end_utc": b,
                             "interval_minutes": period.minutes, "local_date": loc.date(), "local_hour": loc.hour,
                             "fuel_type": mapping[code].fuel_type,
                             "source_fuel_code": code + (CONSUMPTION if direction == "consumption" else ""),
                             "generation_mw": sign * value, "kind": kind, "issued_at_utc": None,
                             "horizon_minutes": None, "fetched_at_utc": fetched_at,
                             "availability_assumption": availability if kind == "forecast" else None,
                             "source_uri": source_uri})
    schema = {c: t for c, (t, _) in GENERATION.items()}
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.sort("source_fuel_code", "interval_start_utc"), unknown


def checks(df: pl.DataFrame, doc: load.LoadDocument, kind: str, market: Market, zone: str, local_date: date,
           unknown: set[str], mapping: dict[str, fuels.FuelCode]) -> list[CheckResult]:
    eic = da.series_rule(market, zone).eic
    foreign = sorted({str(s.in_eic or s.zone_eic) for s in doc.series if _direction(s, eic) is None})
    units = sorted({str(s.unit) for s in doc.series})
    start, end = market.day_bounds_utc(local_date)
    span_min = (end - start).total_seconds() / 60
    per = df.group_by("source_fuel_code").agg(pl.len().alias("n"), pl.col("interval_minutes").first().alias("m"),
                                              pl.col("interval_minutes").n_unique().alias("res"),
                                              pl.col("generation_mw").null_count().alias("nulls"))
    short = sorted(r["source_fuel_code"] for r in per.iter_rows(named=True)
                   if r["res"] != 1 or r["n"] != span_min / r["m"] or r["nulls"])
    gen = df.filter(~pl.col("source_fuel_code").str.ends_with(CONSUMPTION))
    neg = gen.filter((pl.col("generation_mw") < 0) &
                     pl.col("source_fuel_code").map_elements(lambda c: mapping[c].negative == "warn", return_dtype=pl.Boolean))
    return [
        schema_check(df, GENERATION, "generation"),
        CheckResult("document_is_requested", "blocking", (doc.doc_type, doc.process_type) == DOC[kind],
                    {"type": doc.doc_type, "processType": doc.process_type, "expected": list(DOC[kind])}),
        CheckResult("series_for_zone", "blocking", not foreign and df.height > 0,
                    {"zone_eic": eic, "other_domains": foreign, "series": per.height}),
        CheckResult("unit_is_mw", "blocking", units == [load.UNIT], {"units": units}),
        CheckResult("production_type_mapped", "blocking", not unknown,
                    {"unknown_codes": sorted(unknown), "action": "map in config/fuel_types.yaml (entsoe)"}),
        CheckResult("series_day_complete", "blocking", not short and per.height > 0,
                    {"series": per.height, "incomplete": short}),
        CheckResult("primary_key_unique", "blocking",
                    df.height == df.select("interval_start_utc", "source_fuel_code", "fetched_at_utc").unique().height, {}),
        CheckResult("forecast_issue_time_policy", "blocking", df["issued_at_utc"].null_count() == df.height,
                    {"rule": "ENTSO-E publishes no issue time: issued_at_utc null, fetched_at_utc recorded"}),
        # Counted, never fixed (as E3): negative generation of a type that shouldn't be negative.
        CheckResult("negative_generation", "warn", neg.is_empty(),
                    {"rows": neg.height, "codes": sorted(set(neg["source_fuel_code"].to_list()))}),
    ]
