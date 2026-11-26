"""ENTSO-E generation per production type (actual 16.1.B&C) and wind/solar day-ahead forecast
(14.1.D) → silver.generation (slice B4b).

Why it exists: renewable output drives price spreads; Phase D needs actual generation by type
and the wind/solar forecast known before the day (point in time, §3.4a).
Who calls it: defs/entsoe_generation.py; tests on real saved documents (local-only fixtures).

Documents (GL_MarketDocument; parsed by load.parse_document): one TimeSeries per production
type (MktPSRType/psrType, mapped via config/mappings/fuel_types.yaml `entsoe`). Guide, 16.1.B&C: "Time
series with inBiddingZone_Domain attribute reflects Generation values while outBiddingZone_Domain
reflects Consumption values." Consumption (pumping B10, storage charging B25) is kept as its own
rows with direction "consumption" and the value as sent (positive); never netted (ADR-B09).
Invariants: every series maps to a canonical type (unknown blocks, never defaults). Completeness
is judged per series (ADR-B10): each series carries `series_complete` and gets its own check;
complete series publish even when another series has a gap; nothing is filled. Which series are
`required` (series-level severity blocking: consumers that need them block where they're
missing) comes from the registry (entsoe.generation.required); every other series is optional.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from atlas_pipelines.core import completeness
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.schemas import GENERATION, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import load

CODE_VERSION = "entsoe_generation-2"
DOC = {"actual": ("A75", "A16"), "forecast": ("A69", "A01")}


# A series is one source code in one direction: "B10" (generating) and "B10/consumption" (pumping).
SERIES = (pl.col("source_type") + pl.when(pl.col("direction") == "consumption").then(pl.lit("/consumption"))
          .otherwise(pl.lit(""))).alias("series")


def generation_section(market: Market) -> dict:
    """entsoe.generation in the registry, validated: required lists name canonical types and known zones."""
    sec = ((market.raw or {}).get("entsoe") or {}).get("generation") or {}
    req = sec.get("required")
    if not isinstance(req, dict) or set(req) != {"actual", "forecast"}:
        raise ValueError(f"{market.source_file}: entsoe.generation.required needs actual and forecast")
    canonical = fuels.load_canonical()
    for kind, zones in req.items():
        for z, types in (zones or {}).items():
            if z not in market.zones or set(types) - set(canonical):
                raise ValueError(f"{market.source_file}: entsoe.generation.required.{kind}.{z}: unknown zone or type {types}")
    return sec


def empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={c: t for c, (t, _) in GENERATION.items()})


def required_types(market: Market, kind: str, zone: str) -> list[str]:
    return list((generation_section(market)["required"].get(kind) or {}).get(zone) or [])


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
        for period in s.periods:
            for _, a, b, value in da.expand(period, s.curve_type):
                if not (start <= a < end):
                    continue
                loc = a.astimezone(market.tz)
                rows.append({"market": market.id, "zone": zone, "interval_start_utc": a, "interval_end_utc": b,
                             "interval_minutes": period.minutes, "local_date": loc.date(), "local_hour": loc.hour,
                             "fuel_type": mapping[code].fuel_type, "source_type": code, "direction": direction,
                             "generation_mw": value, "kind": kind, "issued_at_utc": None,
                             "horizon_minutes": None, "fetched_at_utc": fetched_at,
                             "availability_assumption": availability if kind == "forecast" else None,
                             "source_uri": source_uri})
    schema = {c: t for c, (t, _) in GENERATION.items()}
    schema.pop("series_complete")
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    df = completeness.mark_complete(df, ["source_type", "direction"], start, end, "generation_mw")
    return df.select(list(GENERATION)).sort("source_type", "direction", "interval_start_utc"), unknown


def series_checks(df: pl.DataFrame, kind: str, market: Market, zone: str, local_date: date) -> list[CheckResult]:
    """One check per (zone, day, series), plus one per required type that sent no series at all.

    Runner severity is `warn` on purpose: a gap must not stop the complete series from
    publishing (ADR-B10). The series-level severity (blocking for required series) is in the
    metrics and in the data (`series_complete`); gold assets that need a series block where
    it's missing."""
    start, end = market.day_bounds_utc(local_date)
    required = set(required_types(market, kind, zone))
    out = []
    for (code, direction), s in sorted(df.group_by(["source_type", "direction"]), key=lambda kv: kv[0]):
        name = code + ("/consumption" if direction == "consumption" else "")
        fuel_type = s["fuel_type"][0]
        is_req = fuel_type in required and direction == "generation"
        gap = completeness.missing_intervals(s, start, end)
        out.append(CheckResult(f"series_complete:{name}", "warn", bool(s["series_complete"][0]), {
            "fuel_type": fuel_type, "required": is_req, "series_severity": "blocking" if is_req else "warn",
            "missing_intervals": len(gap), "first_missing": [iso_utc(t) for t in gap[:4]],
            "null_values": s["generation_mw"].null_count()}))
    present = set(df.filter(pl.col("direction") == "generation")["fuel_type"].to_list())
    for fuel_type in sorted(required - present):
        out.append(CheckResult(f"series_complete:{fuel_type}", "warn", False, {
            "fuel_type": fuel_type, "required": True, "series_severity": "blocking", "absent": True}))
    return out


def checks(df: pl.DataFrame, doc: load.LoadDocument, kind: str, market: Market, zone: str, local_date: date,
           unknown: set[str], mapping: dict[str, fuels.FuelCode]) -> list[CheckResult]:
    eic = da.series_rule(market, zone).eic
    foreign = sorted({str(s.in_eic or s.zone_eic) for s in doc.series if _direction(s, eic) is None})
    units = sorted({str(s.unit) for s in doc.series})
    n_series = df.select("source_type", "direction").unique().height
    warn_codes = [c for c, m in mapping.items() if m.negative == "warn"]
    neg = df.filter((pl.col("generation_mw") < 0) & pl.col("source_type").is_in(warn_codes)).with_columns(SERIES)
    return [
        schema_check(df, GENERATION, "generation"),
        CheckResult("document_is_requested", "blocking", (doc.doc_type, doc.process_type) == DOC[kind],
                    {"type": doc.doc_type, "processType": doc.process_type, "expected": list(DOC[kind])}),
        CheckResult("series_for_zone", "blocking", not foreign and df.height > 0,
                    {"zone_eic": eic, "other_domains": foreign, "series": n_series}),
        CheckResult("unit_is_mw", "blocking", units == [load.UNIT], {"units": units}),
        CheckResult("production_type_mapped", "blocking", not unknown,
                    {"unknown_codes": sorted(unknown), "action": "map in config/mappings/fuel_types.yaml (entsoe)"}),
        CheckResult("primary_key_unique", "blocking",
                    df.height == df.select("interval_start_utc", "source_type", "direction", "fetched_at_utc").unique().height, {}),
        CheckResult("forecast_issue_time_policy", "blocking", df["issued_at_utc"].null_count() == df.height,
                    {"rule": "ENTSO-E publishes no issue time: issued_at_utc null, fetched_at_utc recorded"}),
        # Counted, never fixed (as E3): a negative value in a series that shouldn't have one.
        CheckResult("negative_generation", "warn", neg.is_empty(),
                    {"rows": neg.height, "series": sorted(set(neg["series"].to_list()))}),
        *series_checks(df, kind, market, zone, local_date),
    ]
