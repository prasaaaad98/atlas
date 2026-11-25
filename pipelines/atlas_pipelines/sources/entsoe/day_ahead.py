"""ENTSO-E day-ahead prices (document type A44) → silver.power_prices.

Why it exists: first market of Atlas Grid (B1). Terms and limits:
docs/data/sources/entsoe.md (use = development, testing, internal research).
Who calls it: defs/prices.py tasks; golden tests call parse/build on real files.

How a document becomes rows:
  1. parse_document(): bytes → PriceDocument (all TimeSeries kept) or
     Acknowledgement (e.g. reason 999 "No matching data found"). Rejects DTDs
     and unknown document types; never guesses.
  2. select_series(): picks THE day-ahead series by (contract type, sequence)
     from the registry (config/markets/entsoe.yaml `entsoe.day_ahead_series`).
     Never by order in the file, resolution or coverage (ADR-B07).
     An unknown series (a new auction) or a duplicate blocks.
  3. expand(): turns each Period into one value per interval.
     curveType A03 ("Variable sized Block") lists only the positions where the
     value changes: ENTSO-E, "Introduction of different Timeseries possibilities
     (curvetypes) with ENTSO-E electronic documents" v1.4, §4.3: "only the
     position where a block change occurs is provided. Consequently all
     positions are not provided." So an omitted position carries the previous
     point's value until the next listed position or the end of the Period.
     This is decoding the format, not filling missing data: the Period's
     timeInterval states exactly which intervals the series covers.
     A01 (fixed blocks) must list every position. Other curve types are rejected.
  4. build_silver(): canonical rows; local_date/local_block derived from UTC.
  5. checks(): document covers the requested day, unit, the shared interval
     completeness/uniqueness and price checks, schema, series provenance.

Invariants protected:
  - Missing stays missing: a Period shorter than the delivery day yields fewer
    rows, and completeness fails. Nothing is interpolated.
  - Every row comes from exactly one listed point (traceable by position).
  - The security token never appears in bronze, manifests or errors
    (core/http.py redaction).
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import polars as pl

from atlas_pipelines.checks.intervals import interval_checks
from atlas_pipelines.checks.prices import price_checks
from atlas_pipelines.core.markets import STATUSES, Market, RegistryError, iso_utc
from atlas_pipelines.core.schemas import POWER_PRICES, schema_check
from atlas_pipelines.core.task import CheckResult

BASE_URL = "https://web-api.tp.entsoe.eu/api"
CODE_VERSION = "entsoe_day_ahead-1"
SCHEMA_VERSION = "power_prices-1"
PRODUCT = "DAM"
NO_SEQUENCE = "none"  # registry spelling for "no classificationSequence element"
_RESOLUTION = re.compile(r"^PT(\d+)M$")
SUPPORTED_CURVES = ("A01", "A03")


class ParseError(ValueError):
    """The document isn't one we understand. Never retried: the bytes won't change."""


# --- document model ----------------------------------------------------------


@dataclass(frozen=True)
class Period:
    start: datetime
    end: datetime
    minutes: int
    points: tuple[tuple[int, float], ...]  # (position, price) as listed, in file order


@dataclass(frozen=True)
class Series:
    contract: str | None  # contract_MarketAgreement.type, e.g. "A01"
    sequence: str  # classificationSequence position as text, or NO_SEQUENCE
    in_domain: str | None
    currency: str | None
    unit: str | None  # price_Measure_Unit.name, e.g. "MWH"
    curve_type: str | None
    periods: tuple[Period, ...]

    @property
    def key(self) -> tuple[str | None, str]:
        return (self.contract, self.sequence)


@dataclass(frozen=True)
class PriceDocument:
    start: datetime | None
    end: datetime | None
    series: tuple[Series, ...]


@dataclass(frozen=True)
class Acknowledgement:
    code: str | None
    text: str | None

    @property
    def no_data(self) -> bool:
        # Reason 999 with this text is what the real future-date fixture returns.
        return self.code == "999" and (self.text or "").startswith("No matching data found")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(el: ET.Element, name: str) -> ET.Element | None:
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _text(el: ET.Element | None, *path: str) -> str | None:
    for name in path:
        if el is None:
            return None
        el = _child(el, name)
    return el.text.strip() if el is not None and el.text is not None else None


def _ts(value: str | None, what: str) -> datetime:
    # ENTSO-E writes "2025-09-29T22:00Z" (minutes, Z). Only UTC is accepted.
    if not value or not value.endswith("Z"):
        raise ParseError(f"{what}: expected a UTC timestamp ending in Z, got {value!r}")
    try:
        dt = datetime.fromisoformat(value[:-1])
    except ValueError:
        raise ParseError(f"{what}: bad timestamp {value!r}") from None
    if dt.tzinfo is not None:
        raise ParseError(f"{what}: unexpected offset in {value!r}")
    return dt.replace(tzinfo=timezone.utc)


def parse_document(raw: bytes) -> PriceDocument | Acknowledgement:
    # Refuse DTDs outright: real responses never carry one, and a DTD is how
    # entity-expansion attacks work. The stdlib parser has no external-entity
    # fetching, and this closes the remaining expansion route.
    if b"<!DOCTYPE" in raw[:4096] or b"<!ENTITY" in raw:
        raise ParseError("document contains a DTD/entity declaration")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise ParseError(f"not well-formed XML: {e}") from None

    kind = _local(root.tag)
    if kind == "Acknowledgement_MarketDocument":
        reason = _child(root, "Reason")
        return Acknowledgement(_text(reason, "code"), _text(reason, "text"))
    if kind != "Publication_MarketDocument":
        raise ParseError(f"unexpected document type {kind!r}")
    if _text(root, "type") != "A44":
        raise ParseError(f"expected document type A44, got {_text(root, 'type')!r}")

    interval = _child(root, "period.timeInterval")
    series = []
    for ts in root:
        if _local(ts.tag) != "TimeSeries":
            continue
        periods = []
        for p in ts:
            if _local(p.tag) != "Period":
                continue
            m = _RESOLUTION.match(_text(p, "resolution") or "")
            if not m or int(m.group(1)) <= 0:
                raise ParseError(f"unsupported resolution {_text(p, 'resolution')!r}")
            points = []
            for pt in p:
                if _local(pt.tag) != "Point":
                    continue
                try:
                    points.append((int(_text(pt, "position")), float(_text(pt, "price.amount"))))
                except (TypeError, ValueError):
                    raise ParseError("Point without a numeric position and price.amount") from None
            periods.append(
                Period(
                    _ts(_text(p, "timeInterval", "start"), "Period start"),
                    _ts(_text(p, "timeInterval", "end"), "Period end"),
                    int(m.group(1)),
                    tuple(points),
                )
            )
        series.append(
            Series(
                contract=_text(ts, "contract_MarketAgreement.type"),
                sequence=_text(ts, "classificationSequence_AttributeInstanceComponent.position") or NO_SEQUENCE,
                in_domain=_text(ts, "in_Domain.mRID"),
                currency=_text(ts, "currency_Unit.name"),
                unit=_text(ts, "price_Measure_Unit.name"),
                curve_type=_text(ts, "curveType"),
                periods=tuple(periods),
            )
        )
    return PriceDocument(
        _ts(_text(interval, "start"), "document start") if interval is not None else None,
        _ts(_text(interval, "end"), "document end") if interval is not None else None,
        tuple(series),
    )


# --- curve expansion ------------------------------------------------------------


def expand(period: Period, curve_type: str | None) -> list[tuple[int, datetime, datetime, float]]:
    """One (position, start, end, price) per interval of the Period.

    A03: the value at a listed position holds until the next listed position
    (variable sized block, guide v1.4 §4.3). A01: every position must be listed.
    Raises ParseError on duplicates, out-of-order or out-of-range positions,
    and on any curve type we don't decode.
    """
    if curve_type not in SUPPORTED_CURVES:
        raise ParseError(f"curveType {curve_type!r} not supported (only {SUPPORTED_CURVES})")
    length = timedelta(minutes=period.minutes)
    span = period.end - period.start
    if span <= timedelta(0) or span % length:
        raise ParseError(f"Period {iso_utc(period.start)}..{iso_utc(period.end)} is not a whole number of {period.minutes} min")
    n = span // length
    positions = [pos for pos, _ in period.points]
    if len(set(positions)) != len(positions):
        dupes = sorted({p for p in positions if positions.count(p) > 1})
        raise ParseError(f"duplicate positions {dupes[:10]}")
    if positions != sorted(positions):
        raise ParseError("positions are not in increasing order")
    if not positions or positions[0] != 1:
        raise ParseError("first position must be 1 (nothing to carry before it)")
    if positions[-1] > n:
        raise ParseError(f"position {positions[-1]} beyond the Period's {n} intervals")
    if curve_type == "A01" and positions != list(range(1, n + 1)):
        raise ParseError("curveType A01 must list every position")

    out = []
    listed = dict(period.points)
    value = None
    for pos in range(1, n + 1):
        value = listed.get(pos, value)  # A03: carry the block's value
        start = period.start + (pos - 1) * length
        out.append((pos, start, start + length, value))
    return out


# --- registry section -------------------------------------------------------------


@dataclass(frozen=True)
class SeriesRule:
    zone: str
    eic: str
    select: tuple[str, str]  # (contract, sequence)
    status: str
    source: str
    known: frozenset


def series_rule(market: Market, zone: str) -> SeriesRule:
    sec = (market.raw or {}).get("entsoe") or {}
    eic = (sec.get("zone_eic") or {}).get(zone)
    rule = (sec.get("day_ahead_series") or {}).get(zone)
    where = f"{market.source_file}: entsoe.day_ahead_series.{zone}"
    if not eic:
        raise RegistryError(f"{market.source_file}: no EIC code for zone {zone}")
    if not isinstance(rule, dict) or not isinstance(rule.get("select"), dict):
        raise RegistryError(f"{where}: missing `select`")
    if rule.get("status") not in STATUSES or not rule.get("source"):
        raise RegistryError(f"{where}: needs status in {STATUSES} and a source")

    def key(d: dict) -> tuple[str, str]:
        seq = d.get("sequence")
        return (str(d["contract"]), NO_SEQUENCE if seq in (None, NO_SEQUENCE) else str(seq))

    known = frozenset(key(k) for k in rule.get("known") or [])
    select = key(rule["select"])
    if select not in known:
        raise RegistryError(f"{where}: selected series {select} is not in `known`")
    return SeriesRule(zone, str(eic), select, rule["status"], str(rule["source"]).strip(), known)


def adapter_config(market: Market) -> dict:
    sec = (market.raw or {}).get("entsoe") or {}
    for k in ("request", "publication", "revision_refetch_days"):
        if k not in sec:
            raise RegistryError(f"{market.source_file}: entsoe.{k} missing")
    return sec


def request_params(market: Market, zone: str, local_date: date) -> dict:
    """Query for one delivery day. The token is added by the fetcher, never here."""
    cfg = adapter_config(market)["request"]
    eic = series_rule(market, zone).eic
    start, end = market.day_bounds_utc(local_date)
    fmt = "%Y%m%d%H%M"
    return {
        "documentType": cfg["document_type"],
        "contract_MarketAgreement.type": cfg["contract_market_agreement_type"],
        "in_Domain": eic,
        "out_Domain": eic,
        "periodStart": start.strftime(fmt),
        "periodEnd": end.strftime(fmt),
    }


def publication_deadline(market: Market, local_date: date) -> datetime:
    hours = adapter_config(market)["publication"]["late_after_hours_from_delivery_start"]
    return market.day_bounds_utc(local_date)[0] + timedelta(hours=hours)


def revision_refetch_date(market: Market, local_date: date) -> date:
    """The local date on which this partition is fetched once more to catch silent corrections."""
    return local_date + timedelta(days=int(adapter_config(market)["revision_refetch_days"]))


# --- selection, rows, checks ----------------------------------------------------


def select_series(doc: PriceDocument, rule: SeriesRule) -> tuple[Series | None, list[CheckResult]]:
    keys = [s.key for s in doc.series]
    unknown = sorted({k for k in keys if k not in rule.known}, key=str)
    matches = [s for s in doc.series if s.key == rule.select]
    wrong_zone = sorted({s.in_domain for s in doc.series if s.in_domain != rule.eic}, key=str)
    checks = [
        CheckResult("series_zone_matches", "blocking", not wrong_zone,
                    {"expected_eic": rule.eic, "other_domains": wrong_zone}),
        # A new auction or product in the document: stop until a human adds it to `known`.
        CheckResult("known_series_only", "blocking", not unknown,
                    {"unknown": [list(k) for k in unknown], "known": sorted([list(k) for k in rule.known])}),
        CheckResult("day_ahead_series_present", "blocking", len(matches) == 1,
                    {"select": list(rule.select), "found": len(matches), "series_in_document": [list(k) for k in keys]}),
        # Flags every partition built on a selection we haven't verified ourselves.
        CheckResult("series_selection_verified", "warn", rule.status == "verified",
                    {"status": rule.status, "source": rule.source}),
    ]
    return (matches[0] if len(matches) == 1 else None), checks


def build_silver(series: Series, market: Market, zone: str, local_date: date, source_uri: str) -> pl.DataFrame:
    p = market.product(PRODUCT)
    day_start, _ = market.day_bounds_utc(local_date)
    rows = []
    for period in series.periods:
        for _, start, end, price in expand(period, series.curve_type):
            minutes = int((end - start).total_seconds() // 60)
            rows.append({
                "market": p.canonical_market,
                "zone": zone,
                "interval_start_utc": start,
                "interval_end_utc": end,
                "interval_minutes": minutes,
                # Derived columns: block counted from the local day's start in
                # UTC, so DST days have 92/100 quarter-hours (23/25 hours).
                "local_date": start.astimezone(market.tz).date(),
                "local_block": int((start - day_start) // timedelta(minutes=minutes)) + 1,
                "price": price,
                "currency": series.currency or "",
                "source_uri": source_uri,
            })
    schema = {c: t for c, (t, _) in POWER_PRICES.items()}
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema)


def checks(df: pl.DataFrame, series: Series, doc: PriceDocument, market: Market, local_date: date) -> list[CheckResult]:
    start, end = market.day_bounds_utc(local_date)
    covered = [(p.start, p.end) for p in series.periods]
    return [
        # Wrong day: the source answered for a different interval than asked.
        CheckResult("document_covers_requested_day", "blocking", covered == [(start, end)],
                    {"requested": [iso_utc(start), iso_utc(end)],
                     "periods": [[iso_utc(a), iso_utc(b)] for a, b in covered]}),
        CheckResult("price_unit_mwh", "blocking", series.unit == "MWH", {"unit": series.unit}),
        CheckResult("rows_in_delivery_day", "blocking",
                    df.height == 0 or bool(df["local_date"].eq(local_date).all()),
                    {"other_dates": sorted({str(d) for d in df["local_date"].to_list() if d != local_date})}),
        *interval_checks(df, market, PRODUCT, local_date),
        *price_checks(df, market, PRODUCT),
        schema_check(df, POWER_PRICES, "power_prices"),
    ]


def publication_pending(results: list[CheckResult]) -> bool:
    """True when the only blocking problems are "not (fully) published yet":
    the day-ahead series is absent, or present but short (missing intervals,
    none unexpected). Anything else (unknown series, wrong day, bad unit,
    duplicates) is a data problem and never waits for a retry."""
    failed = [c for c in results if c.severity == "blocking" and not c.passed]
    if not failed:
        return False
    for c in failed:
        if c.check_name == "day_ahead_series_present" and c.metrics["found"] == 0:
            continue
        if c.check_name == "interval_completeness" and not c.metrics["unexpected_first10"]:
            continue
        if c.check_name == "document_covers_requested_day" and _short_cover(c.metrics):
            continue
        return False
    return True


def _short_cover(m: dict) -> bool:
    # A Period that starts on time but ends early is a partial publication.
    req, periods = m["requested"], m["periods"]
    return len(periods) == 1 and periods[0][0] == req[0] and periods[0][1] < req[1]
