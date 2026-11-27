"""ENTSO-E balancing (B4c): 17.1.B&C contracted reserves and 17.1.G imbalance prices → silver.ancillary_prices.

Why it exists: the Phase C battery simulator needs what reserves pay (capacity, per product and
direction) and what imbalance costs (the risk side). Owner scope 2026-11-27: reserve capacity
prices (a) and imbalance prices (c).
Who calls it: defs/entsoe_balancing.py; tests on real saved answers (local-only fixtures).

Answers are ZIP files of Balancing_MarketDocuments (guide: "counted across all XML documents
contained in the ZIP response"), or an Acknowledgement XML ("No matching data"). Code meanings
are from ENTSO-E's official code list v96 (CodelistV96.zip, urn-entsoe-eu-wgedi-codelists.xsd,
read 2026-11-27), never guessed:
  processType   A52 FCR, A51 aFRR, A47 mFRR, A46 RR; A16 Realised (imbalance)
  flowDirection A01 Up, A02 Down, A03 Up and Down
  businessType  B95 Procured capacity; A19 Balance energy deviation (imbalance)
  price category A04 Excess balance, A05 Insufficient balance (imbalance);
                 A06 Average bid price, A07 Single marginal bid price (reserves)
  docStatus     A01 Intermediate, A02 Final
  unit          MAW megawatt, MWH megawatt hours
Invariants: every code must be one of the above (anything else blocks, never defaults); values are
kept as sent (A03 positions carry, nothing else is filled); a final document wins over an
intermediate one for the same interval and both are never mixed silently (doc_status column);
each reserve auction keeps its decision time (allocationDecision), the moment it was known.
"""

from __future__ import annotations

import io
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from atlas_pipelines.core import completeness
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.schemas import ANCILLARY_PRICES, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources.entsoe import day_ahead as da

CODE_VERSION = "entsoe_balancing-1"
RESERVE_PROCESS = {"A52": "fcr", "A51": "afrr", "A47": "mfrr", "A46": "rr"}
DIRECTION = {"A01": "up", "A02": "down", "A03": "up_down"}
RESERVE_PRICE = {"A06": "average_bid", "A07": "marginal"}
IMBALANCE = {"A04": "imbalance_excess", "A05": "imbalance_insufficient"}
STATUS = {"A01": "intermediate", "A02": "final", None: None}
MAX_SERIES = 100  # guide: at most 100 TimeSeries per answer (offset pages beyond)


@dataclass(frozen=True)
class BalSeries:
    business_type: str | None
    direction: str | None
    currency: str | None
    quantity_unit: str | None
    price_unit: str | None
    curve_type: str | None
    categories: tuple[str, ...]
    prices: tuple[da.Period, ...]        # (position, price) per Period
    quantities: tuple[da.Period, ...]    # (position, quantity) per Period; empty for imbalance


@dataclass(frozen=True)
class BalDocument:
    doc_type: str | None
    process_type: str | None
    status: str | None
    area: str | None
    decided_at: datetime | None
    series: tuple[BalSeries, ...]


def _periods(ts: ET.Element, value_tag: str) -> tuple[da.Period, ...]:
    out = []
    for p in ts:
        if da._local(p.tag) != "Period":
            continue
        start = da._ts(da._text(p, "timeInterval", "start"), "Period start")
        end = da._ts(da._text(p, "timeInterval", "end"), "Period end")
        res = da._text(p, "resolution") or ""
        minutes = {"PT15M": 15, "PT30M": 30, "PT60M": 60}.get(res)
        if minutes is None:
            raise da.ParseError(f"unsupported resolution {res!r}")
        pts = []
        for pt in p:
            if da._local(pt.tag) != "Point":
                continue
            v = da._text(pt, value_tag)
            if v is not None:
                pts.append((int(da._text(pt, "position")), float(v)))
        out.append(da.Period(start, end, minutes, tuple(pts)))
    return tuple(out)


def _doc(raw: bytes) -> BalDocument:
    root = ET.fromstring(raw)
    if da._local(root.tag) != "Balancing_MarketDocument":
        raise da.ParseError(f"unexpected document {da._local(root.tag)!r}")
    decided = da._text(root, "allocationDecision_DateAndOrTime.dateTime")
    series = []
    for ts in root:
        if da._local(ts.tag) != "TimeSeries":
            continue
        cats = tuple(sorted({c.text for c in ts.iter() if da._local(c.tag) == "imbalance_Price.category"}))
        price_tag = "imbalance_Price.amount" if any(da._local(e.tag) == "imbalance_Price.amount" for e in ts.iter()) \
            else "procurement_Price.amount"
        series.append(BalSeries(da._text(ts, "businessType"), da._text(ts, "flowDirection.direction"),
                                da._text(ts, "currency_Unit.name"), da._text(ts, "quantity_Measure_Unit.name"),
                                da._text(ts, "price_Measure_Unit.name"), da._text(ts, "curveType"), cats,
                                _periods(ts, price_tag), _periods(ts, "quantity")))
    return BalDocument(da._text(root, "type"), da._text(root, "process.processType"),
                       da._text(root, "docStatus", "value"), da._text(root, "area_Domain.mRID"),
                       da._ts(decided.replace(".000Z", "Z"), "allocationDecision") if decided else None, tuple(series))


def parse_answer(raw: bytes) -> list[BalDocument] | da.Acknowledgement:
    """A ZIP of documents, or an Acknowledgement (e.g. no matching data)."""
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            return [_doc(z.read(i)) for i in sorted(z.infolist(), key=lambda i: i.filename) if not i.is_dir()]
    doc = da.parse_document(raw)
    if isinstance(doc, da.Acknowledgement):
        return doc
    raise da.ParseError("expected a ZIP of Balancing_MarketDocuments or an Acknowledgement")


def _rows_for(series: BalSeries, doc: BalDocument, market: Market, zone: str, area: str, start, end, product: str,
              price_type: str, unit: str, fetched_at: datetime, uri: str) -> list[dict]:
    qty = {}
    for p in series.quantities:
        if not p.points:  # imbalance points carry no quantity
            continue
        for _, a, _, v in da.expand(p, series.curve_type):
            qty[a] = v
    rows = []
    for p in series.prices:
        for _, a, b, price in da.expand(p, series.curve_type):
            if not (start <= a < end):
                continue
            rows.append({"market": market.id, "zone": zone, "area_eic": area, "product": product,
                         "interval_start_utc": a, "interval_end_utc": b, "interval_minutes": p.minutes,
                         "local_date": a.astimezone(market.tz).date(), "price": price, "currency": series.currency,
                         "unit": unit, "quantity_mw": qty.get(a), "price_type": price_type,
                         "doc_status": STATUS.get(doc.status), "decided_at_utc": doc.decided_at,
                         "fetched_at_utc": fetched_at, "source_uri": uri})
    return rows


def build_silver(docs: list[BalDocument], kind: str, market: Market, zone: str, area: str, local_date: date,
                 fetched_at: datetime, uri: str, units: dict[str, str]) -> tuple[pl.DataFrame, list[str]]:
    """Rows for the local day; returns (frame, problems). kind: reserves | imbalance."""
    start, end = market.day_bounds_utc(local_date)
    rows, problems = [], []
    for doc in docs:
        for s in doc.series:
            if kind == "imbalance":
                if doc.doc_type != "A85" or s.business_type != "A19" or len(s.categories) != 1 or s.categories[0] not in IMBALANCE:
                    problems.append(f"imbalance series {doc.doc_type}/{s.business_type}/{s.categories}")
                    continue
                if s.price_unit != "MWH" or doc.status not in STATUS:
                    problems.append(f"imbalance unit/status {s.price_unit}/{doc.status}")
                    continue
                rows += _rows_for(s, doc, market, zone, area, start, end, IMBALANCE[s.categories[0]], "imbalance",
                                  units["imbalance"], fetched_at, uri)
            else:
                proc, direction = RESERVE_PROCESS.get(doc.process_type or ""), DIRECTION.get(s.direction or "")
                if doc.doc_type != "A81" or s.business_type != "B95" or proc is None or direction is None \
                        or len(s.categories) != 1 or s.categories[0] not in RESERVE_PRICE or s.quantity_unit != "MAW":
                    problems.append(f"reserve series {doc.doc_type}/{doc.process_type}/{s.business_type}/{s.direction}/"
                                    f"{s.categories}/{s.quantity_unit}")
                    continue
                rows += _rows_for(s, doc, market, zone, area, start, end, f"{proc}_capacity_{direction}",
                                  RESERVE_PRICE[s.categories[0]], units["reserves"], fetched_at, uri)
    schema = {c: t for c, (t, _) in ANCILLARY_PRICES.items() if c != "series_complete"}
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    if kind == "imbalance" and df.height:
        # A final value wins over an intermediate one for the same interval; never both.
        rank = pl.when(pl.col("doc_status") == "final").then(0).otherwise(1)
        df = (df.with_columns(rank.alias("_r")).sort("_r").unique(["product", "area_eic", "interval_start_utc"],
                                                                     keep="first", maintain_order=True).drop("_r"))
    by = ["product", "area_eic", "decided_at_utc"]
    # One series = (product, area, auction). Imbalance has no auction: a constant key stands in,
    # because a join never matches null keys.
    key = pl.col("decided_at_utc").fill_null(pl.lit(0).cast(df.schema["decided_at_utc"])).alias("_k")
    df = completeness.mark_complete(df.with_columns(key), ["product", "area_eic", "_k"], start, end, "price").drop("_k")
    return df.select(list(ANCILLARY_PRICES)).sort(*by, "interval_start_utc", nulls_last=True), problems


def checks(df: pl.DataFrame, docs: list[BalDocument], kind: str, market: Market, zone: str, area: str,
           local_date: date, problems: list[str], expected_currency: str) -> list[CheckResult]:
    start, end = market.day_bounds_utc(local_date)
    n_series = sum(len(d.series) for d in docs)
    series = []
    for key, s in df.group_by(["product", "area_eic", "decided_at_utc"]):
        gap = completeness.missing_intervals(s, start, end)
        series.append(CheckResult(f"series_complete:{key[0]}:{key[1]}" + (f"@{iso_utc(key[2])}" if key[2] else ""), "warn",
                                  bool(s["series_complete"][0]),
                                  {"missing_intervals": len(gap), "first_missing": [iso_utc(t) for t in gap[:4]],
                                   "intermediate_rows": int((s["doc_status"] == "intermediate").sum())}))
    return [
        schema_check(df, ANCILLARY_PRICES, "ancillary_prices"),
        CheckResult("codes_known", "blocking", not problems, {"unknown": problems[:10],
                    "action": "source the code in ENTSO-E's code list, then map it in sources/entsoe/balancing.py"}),
        CheckResult("documents_for_area", "blocking", all(d.area == area for d in docs),
                    {"area": area, "found": sorted({str(d.area) for d in docs})}),
        CheckResult("currency_matches_registry", "blocking", set(df["currency"].unique().to_list()) <= {expected_currency},
                    {"expected": expected_currency, "found": sorted(set(df["currency"].unique().to_list()))}),
        CheckResult("answer_not_truncated", "blocking", n_series < MAX_SERIES,
                    {"time_series": n_series, "limit": MAX_SERIES, "action": "page with offset if this ever fails"}),
        CheckResult("primary_key_unique", "blocking",
                    df.height == df.select("product", "area_eic", "decided_at_utc", "interval_start_utc").unique().height, {}),
        CheckResult("rows_present", "blocking", df.height > 0, {"rows": df.height, "kind": kind}),
        *sorted(series, key=lambda c: c.check_name),
    ]
