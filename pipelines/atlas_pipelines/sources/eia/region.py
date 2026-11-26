"""EIA-930 region-data → silver.demand (actual + day-ahead forecast) and silver.interchange (slice E2).

Why it exists: US demand, its day-ahead forecast and net interchange per balancing
authority (PLAN_EIA_TRACK.md E2). One request per BA and UTC day gives all four
region-data types: D (demand), DF (day-ahead demand forecast), NG (net generation),
TI (total interchange).
Who calls it: defs/eia.py (eia930_region_daily); tests with real saved responses.

Time (docs/data/sources/eia.md, config/markets/us_ba.yaml):
  - `period` is UTC and HOUR-ENDING: "2026-11-18T06" covers 05:00–06:00 UTC. A UTC
    day D is the periods D T01 … D+1 T00.
  - local_date / local_hour derive from the BA's local zone (registry, status assumed
    until confirmed from local-hourly responses).
Values: "megawatthours" per hour = average MW over the hour.
Forecasts: EIA-930 publishes no issue time, so issued_at_utc = null and the row
carries fetched_at_utc plus the registry's availability_assumption (EIA plan §4,
ADR-B06 addendum). Each fetch adds a vintage; earlier vintages are carried forward.
Never fills: a missing hour has no record at EIA, stays missing here, and fails
completeness.

Demand and interchange are gated separately (demand_checks / interchange_checks):
real EIA-930 days often have complete demand but partial interchange (the owner's
saved samples: PJM 2025-11-02 has 12 of 37 TI hours, CISO's DST days 27-29), and a
gap in one must not keep the other from publishing.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.schemas import DEMAND, INTERCHANGE, schema_check
from atlas_pipelines.core import vintages
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources.eia import client

ROUTE = "electricity/rto/region-data"
CODE_VERSION = "eia_region-1"
TYPES = {"D": "actual", "DF": "forecast"}
UNIT = "megawatthours"
KEY_COLUMNS = ("respondent", "type")


def eia_section(market: Market) -> dict:
    sec = (market.raw or {}).get("eia") or {}
    if (sec.get("period_marks") or {}).get("value") != "interval_end":
        raise ValueError(f"{market.source_file}: eia.period_marks must be interval_end (EIA-930 is hour-ending)")
    return sec


def request_params(ba: str, day: date) -> list[tuple[str, str]]:
    """All region-data types for one BA and the UTC day `day` (hour-ending periods D T01 … D+1 T00)."""
    first = datetime.combine(day, datetime.min.time()) + timedelta(hours=1)
    last = datetime.combine(day + timedelta(days=1), datetime.min.time())
    return [("frequency", "hourly"), ("start", first.strftime("%Y-%m-%dT%H")), ("end", last.strftime("%Y-%m-%dT%H")),
            ("facets[respondent][]", ba), ("data[]", "value")]


def interval_of(period: str) -> tuple[datetime, datetime]:
    end = datetime.strptime(period, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    return end - timedelta(hours=1), end


def _local(start: datetime, tz: ZoneInfo) -> tuple[date, int]:
    loc = start.astimezone(tz)
    return loc.date(), loc.hour


def parse(rows: list[dict], market: Market, ba: str, day: date | None, fetched_at: datetime, source_uri: str):
    """Rows of one BA → (demand, interchange, net_generation_by_start). day=None keeps every hour (tests)."""
    sec = eia_section(market)
    tz = ZoneInfo(sec["bas"][ba]["tz"])
    avail = (sec.get("forecast_availability") or {}).get("rule")
    lo = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) if day else None
    hi = lo + timedelta(days=1) if day else None
    demand, inter, ng = [], [], {}
    for r in rows:
        if r.get("respondent") != ba:
            continue
        start, end = interval_of(r["period"])
        if day and not (lo <= start < hi):
            continue
        v = client.to_number(r.get("value"))
        ld, lh = _local(start, tz)
        base = {"market": market.id, "zone": ba, "interval_start_utc": start, "interval_end_utc": end,
                "interval_minutes": 60, "local_date": ld, "local_hour": lh, "fetched_at_utc": fetched_at,
                "source_uri": source_uri}
        t = r.get("type")
        if t in TYPES:
            demand.append({**base, "demand_mw": v, "kind": TYPES[t], "issued_at_utc": None, "horizon_minutes": None,
                           "availability_assumption": avail if t == "DF" else None})
        elif t == "TI":
            inter.append({**base, "net_interchange_mw": v})
        elif t == "NG":
            ng[start] = v
    d = pl.DataFrame(demand, schema={c: t for c, (t, _) in DEMAND.items()}) if demand else pl.DataFrame(schema={c: t for c, (t, _) in DEMAND.items()})
    i = pl.DataFrame(inter, schema={c: t for c, (t, _) in INTERCHANGE.items()}) if inter else pl.DataFrame(schema={c: t for c, (t, _) in INTERCHANGE.items()})
    return d.sort("kind", "interval_start_utc"), i.sort("interval_start_utc"), ng


# Moved to core/vintages.py (shared with ENTSO-E, B4a); kept here for existing callers.
carry_vintages = vintages.carry_vintages


def _expected(day: date) -> set[datetime]:
    return {datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=h) for h in range(24)}


def checks(demand: pl.DataFrame, inter: pl.DataFrame, ng: dict, rows: list[dict], market: Market, day: date,
           previous: pl.DataFrame | None) -> list[CheckResult]:
    """Both sets (tests, reports). Tasks use demand_checks and interchange_checks."""
    return demand_checks(demand, rows, day, previous) + interchange_checks(demand, inter, ng, market, day)


def demand_checks(demand: pl.DataFrame, rows: list[dict], day: date, previous: pl.DataFrame | None) -> list[CheckResult]:
    expected = _expected(day)
    latest = demand["fetched_at_utc"].max() if demand.height else None
    actual = demand.filter(pl.col("kind") == "actual")
    fc_now = demand.filter((pl.col("kind") == "forecast") & (pl.col("fetched_at_utc") == latest))

    act_missing = sorted(expected - {s for s, v in zip(actual["interval_start_utc"].to_list(), actual["demand_mw"].to_list()) if v is not None})
    fc_missing = sorted(expected - set(fc_now["interval_start_utc"].to_list()))
    neg = actual.filter(pl.col("demand_mw") < 0).height + fc_now.filter(pl.col("demand_mw") < 0).height
    units = sorted({r.get("value-units") for r in rows})
    keys = ["kind", "interval_start_utc", "fetched_at_utc"]
    dupes = demand.height - demand.select(keys).unique().height
    prev_vintages = set(previous.filter(pl.col("kind") == "forecast")["fetched_at_utc"].to_list()) if previous is not None and previous.height else set()
    now_vintages = set(demand.filter(pl.col("kind") == "forecast")["fetched_at_utc"].to_list())
    return [
        schema_check(demand, DEMAND, "demand"),
        CheckResult("units_megawatthours", "blocking", units == [UNIT], {"units": units}),
        CheckResult("demand_actual_complete", "blocking", not act_missing,
                    {"expected": 24, "missing_first10": [iso_utc(m) for m in act_missing[:10]]}),
        # The day-ahead forecast is often incomplete in EIA-930: recorded, not blocking.
        CheckResult("demand_forecast_complete", "warn", not fc_missing,
                    {"expected": 24, "missing_first10": [iso_utc(m) for m in fc_missing[:10]]}),
        CheckResult("demand_non_negative", "blocking", neg == 0, {"negative_rows": neg}),
        CheckResult("primary_key_unique", "blocking", dupes == 0, {"duplicates": dupes, "key": keys}),
        CheckResult("forecast_vintages_superset", "blocking", prev_vintages <= now_vintages,
                    {"previous": len(prev_vintages), "now": len(now_vintages),
                     "dropped": sorted(iso_utc(v) for v in prev_vintages - now_vintages)}),
        CheckResult("forecast_issue_time_policy", "blocking",
                    demand.filter((pl.col("kind") == "forecast") & pl.col("issued_at_utc").is_not_null()).height == 0,
                    {"rule": "EIA-930 publishes no issue time: issued_at_utc null, fetched_at_utc recorded"}),
    ]


def interchange_checks(demand: pl.DataFrame, inter: pl.DataFrame, ng: dict, market: Market, day: date) -> list[CheckResult]:
    sec = eia_section(market)
    expected = _expected(day)
    actual = demand.filter((pl.col("kind") == "actual") & (pl.col("fetched_at_utc") == demand["fetched_at_utc"].max())) \
        if demand.height else demand
    ti_missing = sorted(expected - {s for s, v in zip(inter["interval_start_utc"].to_list(), inter["net_interchange_mw"].to_list()) if v is not None})
    dupes = inter.height - inter.select("interval_start_utc").unique().height
    # Balance identity (warn): demand ≈ net generation − net interchange, per hour.
    tol = float(sec.get("balance_tolerance", 0.05))
    d_by = dict(zip(actual["interval_start_utc"].to_list(), actual["demand_mw"].to_list()))
    t_by = dict(zip(inter["interval_start_utc"].to_list(), inter["net_interchange_mw"].to_list()))
    comparable = [s for s in expected if d_by.get(s) and ng.get(s) is not None and t_by.get(s) is not None]
    outside = [s for s in comparable if abs(d_by[s] - (ng[s] - t_by[s])) > tol * abs(d_by[s])]
    return [
        schema_check(inter, INTERCHANGE, "interchange"),
        CheckResult("interchange_complete", "blocking", not ti_missing,
                    {"expected": 24, "missing": len(ti_missing), "missing_first10": [iso_utc(m) for m in ti_missing[:10]]}),
        CheckResult("interchange_key_unique", "blocking", dupes == 0, {"duplicates": dupes}),
        CheckResult("balance_identity", "warn", not outside,
                    {"tolerance_share_of_demand": tol, "hours_compared": len(comparable), "hours_outside": len(outside),
                     "share_outside": round(len(outside) / len(comparable), 4) if comparable else None,
                     "first10": [iso_utc(s) for s in sorted(outside)[:10]]}),
    ]


def incomplete_only(results: list[CheckResult]) -> bool:
    """True if the only blocking failures are missing hours (a day still being published)."""
    failed = [c for c in results if c.severity == "blocking" and not c.passed]
    return bool(failed) and all(c.check_name in ("demand_actual_complete", "interchange_complete") for c in failed)


def publication_deadline(market: Market, day: date) -> datetime:
    hours = float((eia_section(market).get("publication") or {}).get("late_after_hours_from_day_end", 48))
    return datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=hours)
