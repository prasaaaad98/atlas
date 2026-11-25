"""silver.calendar: one row per (market, zone, local date): weekend, public holiday,
special day (point-in-time), DST transition.

Why it exists: demand and price shape depend on weekends, holidays, festivals
and the 23/25-hour DST days. Phase C/D features need them per market and zone,
the same way for every market (PHASE_AB_PLAN.md §3.3, B4e).

Inputs, all versioned and offline (no network):
  - public holidays: the `holidays` library, pinned (pyproject.toml);
  - special days: config/calendar/<market>.yaml, owner-curated, each with a
    source and an `announced_on` date;
  - DST days: the IANA tz database (tzdata) for the zone's local time zone;
  - per-zone settings: the `calendar:` section of config/markets/<market>.yaml
    (country, subdivision, weekend days, local zone if not the market's).
Who calls it: defs/calendar.py (calendar_build pipeline); tests.

Invariants:
  - Exactly one row per (market, zone, local_date) for every day of the year.
  - A special day without a source or with announced_on after its date is
    rejected at load; `known_as_of` hides special days not yet announced, so a
    backtest can't use a holiday declared later (point-in-time rule §3.4a).
  - Nothing is invented: holidays come from the library, special days from
    the owner's file; a market without a `calendar:` section gets no calendar.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import holidays
import polars as pl
import yaml

from atlas_pipelines.core.hashing import sha256_bytes
from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.schemas import CALENDAR
from atlas_pipelines.core.task import CheckResult

SCHEMA_VERSION = "calendar-1"


class CalendarConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ZoneCalendar:
    zone: str
    country: str
    subdivision: str | None
    tz: str


@dataclass(frozen=True)
class SpecialDay:
    date: date
    name: str
    zones: tuple[str, ...]  # resolved zone codes
    source: str
    announced_on: date


def zone_calendars(market: Market) -> tuple[list[ZoneCalendar], tuple[int, ...]]:
    sec = (market.raw or {}).get("calendar")
    if not sec:
        return [], ()
    weekend = tuple(int(d) for d in sec.get("weekend", [6, 7]))
    if not weekend or any(d < 1 or d > 7 for d in weekend):
        raise CalendarConfigError(f"{market.id}: calendar.weekend must be ISO weekdays 1-7")
    out = []
    for zone, z in (sec.get("zones") or {}).items():
        if zone not in market.zones:
            raise CalendarConfigError(f"{market.id}: calendar zone {zone} is not a registry zone")
        tz = z.get("tz") or market.timezone
        ZoneInfo(tz)
        country = str(z["country"])
        if country not in holidays.list_supported_countries():
            raise CalendarConfigError(f"{market.id}/{zone}: country {country} not supported by holidays {holidays.__version__}")
        sub = z.get("subdivision")
        if sub and sub not in holidays.country_holidays(country).subdivisions:
            raise CalendarConfigError(f"{market.id}/{zone}: unknown subdivision {country}-{sub}")
        out.append(ZoneCalendar(zone, country, sub, tz))
    return out, weekend


def load_special_days(path: Path, zones: list[str]) -> tuple[list[SpecialDay], str]:
    """Owner-curated special days. Returns them and the file's hash (lineage)."""
    if not path.exists():
        return [], "none"
    raw_bytes = path.read_bytes()
    raw = yaml.safe_load(raw_bytes) or {}
    if raw.get("version") != 1:
        raise CalendarConfigError(f"{path}: version must be 1")
    out = []
    for i, e in enumerate(raw.get("special_days") or []):
        where = f"{path} entry {i + 1}"
        d, ann = e.get("date"), e.get("announced_on")
        if not isinstance(d, date) or not isinstance(ann, date):
            raise CalendarConfigError(f"{where}: date and announced_on must be YYYY-MM-DD")
        if not str(e.get("source") or "").strip() or not str(e.get("name") or "").strip():
            raise CalendarConfigError(f"{where}: name and source (a citation) are required")
        if ann > d:
            raise CalendarConfigError(f"{where}: announced_on {ann} is after the day itself ({d})")
        zs = zones if e.get("zones") == "all" else list(e.get("zones") or [])
        if not zs or any(z not in zones for z in zs):
            raise CalendarConfigError(f"{where}: zones must be 'all' or calendar zones {zones}")
        out.append(SpecialDay(d, str(e["name"]).strip(), tuple(zs), str(e["source"]).strip(), ann))
    keys = [(s.date, z) for s in out for z in s.zones]
    if len(keys) != len(set(keys)):
        raise CalendarConfigError(f"{path}: a zone has two special days on one date")
    return out, sha256_bytes(raw_bytes)


def _holidays_for(zc: ZoneCalendar, year: int):
    cls = holidays.country_holidays(zc.country)
    lang = "en_US" if "en_US" in getattr(cls, "supported_languages", ()) else None
    return holidays.country_holidays(zc.country, subdiv=zc.subdivision, years=year, language=lang)


def dst_transition(local: date, tz: str) -> str:
    zi = ZoneInfo(tz)
    start = datetime.combine(local, time(0), tzinfo=zi)
    end = datetime.combine(local + timedelta(days=1), time(0), tzinfo=zi)
    hours = (end.timestamp() - start.timestamp()) / 3600
    return "short" if hours < 24 else "long" if hours > 24 else "none"


def build(market: Market, year: int, special: list[SpecialDay], provenance: str) -> pl.DataFrame:
    zcs, weekend = zone_calendars(market)
    rows = []
    for zc in zcs:
        hol = _holidays_for(zc, year)
        specials = {s.date: s for s in special if zc.zone in s.zones}
        d = date(year, 1, 1)
        while d.year == year:
            names = hol.get_list(d)
            sp = specials.get(d)
            rows.append({
                "market": market.id, "zone": zc.zone, "local_date": d,
                "is_weekend": d.isoweekday() in weekend,
                "is_public_holiday": bool(names), "holiday_name": "; ".join(names) if names else None,
                "is_special_day": sp is not None, "special_day_name": sp.name if sp else None,
                "special_day_announced_on": sp.announced_on if sp else None,
                "special_day_source": sp.source if sp else None,
                "dst_transition": dst_transition(d, zc.tz),
                "provenance": provenance,
            })
            d += timedelta(days=1)
    return pl.DataFrame(rows, schema={c: t for c, (t, _) in CALENDAR.items()})


def known_as_of(df: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """Calendar as it was known on `as_of`: special days announced later are hidden.

    Point-in-time rule (§3.4a): a backtest for a day before an ad-hoc holiday
    was declared must not see it.
    """
    later = pl.col("special_day_announced_on") > pl.lit(as_of)
    return df.with_columns(
        pl.when(later).then(False).otherwise(pl.col("is_special_day")).alias("is_special_day"),
        *[pl.when(later).then(None).otherwise(pl.col(c)).alias(c)
          for c in ("special_day_name", "special_day_announced_on", "special_day_source")],
    )


def checks(df: pl.DataFrame, market: Market, year: int) -> list[CheckResult]:
    from atlas_pipelines.core.schemas import schema_check

    zcs, _ = zone_calendars(market)
    days = (date(year + 1, 1, 1) - date(year, 1, 1)).days
    dupes = df.height - df.select("zone", "local_date").unique().height
    dst = {zc.zone: (df.filter((pl.col("zone") == zc.zone) & (pl.col("dst_transition") != "none")).height,
                     _dst_days_in_year(zc.tz, year)) for zc in zcs}
    bad_specials = df.filter(pl.col("is_special_day") & (pl.col("special_day_source").is_null()
                             | (pl.col("special_day_announced_on") > pl.col("local_date")))).height
    return [
        schema_check(df, CALENDAR, "calendar"),
        CheckResult("one_row_per_zone_and_day", "blocking", df.height == days * len(zcs) and dupes == 0,
                    {"rows": df.height, "expected": days * len(zcs), "duplicates": dupes}),
        CheckResult("dst_days_match_tz_database", "blocking", all(a == b for a, b in dst.values()),
                    {z: {"found": a, "expected": b} for z, (a, b) in dst.items()}),
        CheckResult("special_days_point_in_time", "blocking", bad_specials == 0, {"violations": bad_specials}),
    ]


def _dst_days_in_year(tz: str, year: int) -> int:
    # Independent of build(): count UTC-offset changes between consecutive local noons.
    zi = ZoneInfo(tz)
    n, d = 0, date(year, 1, 1)
    prev = datetime.combine(d, time(12), tzinfo=zi).utcoffset()
    while d.year == year:
        d += timedelta(days=1)
        cur = datetime.combine(d, time(12), tzinfo=zi).utcoffset()
        n += cur != prev
        prev = cur
    return n
