"""ENTSO-E total load (actual 6.1.A, day-ahead forecast 6.1.B) → silver.demand (slice B4a).

Why it exists: demand drives price shape; Phase D forecasts need both the actual and the
forecast that was known before the day (point-in-time, §3.4a).
Who calls it: defs/entsoe_load.py; tests on real saved documents (local-only fixtures).

Documents: GL_MarketDocument, type A65, processType A16 (actual) or A01 (day-ahead), one
TimeSeries for the bidding zone (outBiddingZone_Domain), unit MAW (= MW), curve A03 (a
value holds until the next listed position: same rule as prices, day_ahead.expand).
Invariants:
  - the document is the one requested: type A65, the requested processType, the zone's EIC;
  - every interval of the local delivery day is present (23/24/25 h × resolution), or the
    partition does not publish; nothing is filled;
  - ENTSO-E publishes no forecast issue time: issued_at_utc null, fetched_at_utc recorded
    (ADR-B06 addendum, as for EIA-930); vintages appended, never overwritten.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.schemas import DEMAND, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources.entsoe import day_ahead as da

CODE_VERSION = "entsoe_load-1"
PROCESS = {"actual": "A16", "forecast": "A01"}
UNIT = "MAW"  # megawatt (ENTSO-E unit code)


@dataclass(frozen=True)
class LoadSeries:
    """One TimeSeries of a GL_MarketDocument (load A65, generation A75, wind/solar forecast A69).
    zone_eic = outBiddingZone_Domain (load; for A75 the guide says it marks CONSUMPTION, e.g.
    pumping or charging), in_eic = inBiddingZone_Domain (A75/A69 generation)."""
    zone_eic: str | None
    unit: str | None
    curve_type: str | None
    business_type: str | None
    periods: tuple[da.Period, ...]
    in_eic: str | None = None
    psr_type: str | None = None


@dataclass(frozen=True)
class LoadDocument:
    doc_type: str | None
    process_type: str | None
    series: tuple[LoadSeries, ...]


def parse_document(raw: bytes) -> LoadDocument | da.Acknowledgement:
    if b"<!DOCTYPE" in raw[:4096] or b"<!ENTITY" in raw:
        raise da.ParseError("document contains a DTD/entity declaration")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise da.ParseError(f"not well-formed XML: {e}") from None
    kind = da._local(root.tag)
    if kind == "Acknowledgement_MarketDocument":
        reason = da._child(root, "Reason")
        return da.Acknowledgement(da._text(reason, "code"), da._text(reason, "text"))
    if kind != "GL_MarketDocument":
        raise da.ParseError(f"unexpected document type {kind!r}")
    series = []
    for ts in root:
        if da._local(ts.tag) != "TimeSeries":
            continue
        periods = []
        for p in ts:
            if da._local(p.tag) != "Period":
                continue
            m = da._RESOLUTION.match(da._text(p, "resolution") or "")
            if not m or int(m.group(1)) <= 0:
                raise da.ParseError(f"unsupported resolution {da._text(p, 'resolution')!r}")
            points = []
            for pt in p:
                if da._local(pt.tag) != "Point":
                    continue
                try:
                    points.append((int(da._text(pt, "position")), float(da._text(pt, "quantity"))))
                except (TypeError, ValueError):
                    raise da.ParseError("Point without a numeric position and quantity") from None
            periods.append(da.Period(da._ts(da._text(p, "timeInterval", "start"), "Period start"),
                                     da._ts(da._text(p, "timeInterval", "end"), "Period end"),
                                     int(m.group(1)), tuple(points)))
        series.append(LoadSeries(da._text(ts, "outBiddingZone_Domain.mRID"), da._text(ts, "quantity_Measure_Unit.name"),
                                 da._text(ts, "curveType"), da._text(ts, "businessType"), tuple(periods),
                                 in_eic=da._text(ts, "inBiddingZone_Domain.mRID"),
                                 psr_type=da._text(ts, "MktPSRType", "psrType")))
    return LoadDocument(da._text(root, "type"), da._text(root, "process.processType"), tuple(series))


def build_silver(doc: LoadDocument, kind: str, market: Market, zone: str, local_date: date, fetched_at: datetime,
                 source_uri: str, availability: str | None) -> pl.DataFrame:
    """All intervals of the zone's series that fall in the local delivery day."""
    eic = da.series_rule(market, zone).eic
    start, end = market.day_bounds_utc(local_date)
    rows = []
    for s in doc.series:
        if s.zone_eic != eic:
            continue
        for period in s.periods:
            for _, a, b, value in da.expand(period, s.curve_type):
                if not (start <= a < end):
                    continue
                loc = a.astimezone(market.tz)
                rows.append({"market": market.id, "zone": zone, "interval_start_utc": a, "interval_end_utc": b,
                             "interval_minutes": period.minutes, "local_date": loc.date(), "local_hour": loc.hour,
                             "demand_mw": value, "kind": kind, "issued_at_utc": None, "horizon_minutes": None,
                             "fetched_at_utc": fetched_at,
                             "availability_assumption": availability if kind == "forecast" else None,
                             "source_uri": source_uri})
    schema = {c: t for c, (t, _) in DEMAND.items()}
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.sort("interval_start_utc")


def checks(df: pl.DataFrame, doc: LoadDocument, kind: str, market: Market, zone: str, local_date: date) -> list[CheckResult]:
    eic = da.series_rule(market, zone).eic
    ours = [s for s in doc.series if s.zone_eic == eic]
    others = sorted({s.zone_eic for s in doc.series} - {eic})
    start, end = market.day_bounds_utc(local_date)
    minutes = df["interval_minutes"].unique().to_list()
    expected = int((end - start).total_seconds() // 60 // minutes[0]) if len(minutes) == 1 else None
    stamps = df["interval_start_utc"].to_list()
    gaps = [iso_utc(a) for a, b in zip(stamps, stamps[1:]) if (b - a).total_seconds() != minutes[0] * 60] if expected else []
    return [
        schema_check(df, DEMAND, "demand"),
        CheckResult("document_is_requested", "blocking", doc.doc_type == "A65" and doc.process_type == PROCESS[kind],
                    {"type": doc.doc_type, "processType": doc.process_type, "expected": ["A65", PROCESS[kind]]}),
        CheckResult("series_for_zone", "blocking", len(ours) == 1 and not others,
                    {"zone_eic": eic, "series_for_zone": len(ours), "other_domains": others}),
        CheckResult("unit_is_mw", "blocking", all(s.unit == UNIT for s in ours) and bool(ours),
                    {"units": sorted({str(s.unit) for s in ours})}),
        CheckResult("one_resolution", "blocking", len(minutes) == 1, {"interval_minutes": minutes}),
        CheckResult("day_complete", "blocking", expected is not None and df.height == expected and not gaps
                    and df["demand_mw"].null_count() == 0 and (not stamps or (stamps[0] == start)),
                    {"rows": df.height, "expected": expected, "gaps_first10": gaps[:10],
                     "null_values": df["demand_mw"].null_count()}),
        CheckResult("demand_positive", "blocking", df.filter(pl.col("demand_mw") <= 0).height == 0,
                    {"non_positive": df.filter(pl.col("demand_mw") <= 0).height}),
        CheckResult("primary_key_unique", "blocking",
                    df.height == df.select("interval_start_utc", "fetched_at_utc").unique().height, {}),
        CheckResult("forecast_issue_time_policy", "blocking", df["issued_at_utc"].null_count() == df.height,
                    {"rule": "ENTSO-E publishes no issue time: issued_at_utc null, fetched_at_utc recorded"}),
    ]
