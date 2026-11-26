"""EIA natural gas spot prices (Henry Hub) → silver.fuel_carbon_prices (slice E4).

Why it exists: gas is often the marginal fuel, so its price explains power prices and
is a Phase D feature. EIA publishes the Henry Hub daily spot price as series RNGWHHD
(route natural-gas/pri/fut), found by name in EIA's own series facet.
Who calls it: defs/eia.py (eia_henry_hub_monthly); tests with the real saved response.

Partition = trade month (YYYY-MM-01). A daily partition would need a trading calendar
to tell a holiday from a gap; we don't invent one. Instead: no weekend rows (blocking),
and weekdays without a price are reported (warn), never filled.
Point-in-time: a price is usable only after publication; each row carries
fetched_at_utc, the earliest moment we know it (PHASE_AB_PLAN.md §3.4a).
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.schemas import FUEL_CARBON_PRICES, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources.eia import client

CODE_VERSION = "eia_gas-1"
KEY_COLUMNS = ("series",)


def load_series(path: Path | None = None) -> dict[str, dict]:
    path = path or config.REPO_ROOT / "config" / "commodities" / "eia.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise ValueError(f"{path}: version must be 1")
    vocab = raw["vocabulary"]
    for sid, s in (raw.get("series") or {}).items():
        for field, allowed in (("commodity", "commodities"), ("hub", "hubs"), ("delivery_period", "delivery_periods"),
                               ("unit", "units")):
            if s.get(field) not in vocab[allowed]:
                raise ValueError(f"{path}: series {sid} {field} {s.get(field)!r} not in vocabulary {allowed}")
    return raw["series"]


def month_bounds(partition: date) -> tuple[date, date]:
    if partition.day != 1:
        raise ValueError(f"partition {partition} must be the first day of a month")
    return partition, partition.replace(day=calendar.monthrange(partition.year, partition.month)[1])


def request_params(series_id: str, spec: dict, partition: date) -> list[tuple[str, str]]:
    first, last = month_bounds(partition)
    return [("frequency", spec["frequency"]), ("start", first.isoformat()), ("end", last.isoformat()),
            ("facets[series][]", series_id), ("data[]", "value")]


def parse(rows: list[dict], series_id: str, spec: dict, partition: date, fetched_at: datetime,
          source_uri: str) -> pl.DataFrame:
    first, last = month_bounds(partition)
    out = []
    for r in rows:
        if r.get("series") != series_id:
            continue
        d = date.fromisoformat(r["period"])
        if not first <= d <= last:
            continue
        out.append({"commodity": spec["commodity"], "hub": spec["hub"], "trade_date": d,
                    "delivery_period": spec["delivery_period"], "price": client.to_number(r.get("value")),
                    "currency": spec["currency"], "unit": spec["unit"], "source_series": series_id,
                    "fetched_at_utc": fetched_at, "source_uri": source_uri})
    schema = {c: t for c, (t, _) in FUEL_CARBON_PRICES.items()}
    return (pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)).sort("trade_date")


def checks(df: pl.DataFrame, rows: list[dict], spec: dict, partition: date, today: date) -> list[CheckResult]:
    first, last = month_bounds(partition)
    units = sorted({r.get("units") for r in rows})
    descriptions = sorted({r.get("series-description") for r in rows})
    days = df["trade_date"].to_list()
    weekend = [str(d) for d in days if d.weekday() >= 5]
    # Weekdays up to yesterday (a current month is partial by nature).
    end = min(last, today - timedelta(days=1))
    weekdays = [first + timedelta(days=i) for i in range((end - first).days + 1) if (first + timedelta(days=i)).weekday() < 5]
    missing = [str(d) for d in weekdays if d not in set(days)]
    dupes = df.height - df.select("trade_date").unique().height
    nulls = df["price"].null_count()
    return [
        schema_check(df, FUEL_CARBON_PRICES, "fuel_carbon_prices"),
        CheckResult("units_match", "blocking", units == [spec["source_units"]] or not rows,
                    {"expected": spec["source_units"], "found": units}),
        CheckResult("series_is_expected", "blocking",
                    all(spec["source_description_contains"] in (d or "") for d in descriptions) or not rows,
                    {"descriptions": descriptions}),
        CheckResult("no_weekend_rows", "blocking", not weekend, {"weekend_dates": weekend}),
        CheckResult("primary_key_unique", "blocking", dupes == 0, {"duplicates": dupes}),
        CheckResult("prices_present", "blocking", nulls == 0, {"null_prices": nulls}),
        # Exchange holidays and gaps look the same here: reported for review, never filled.
        CheckResult("weekdays_without_price", "warn", not missing,
                    {"checked_through": str(end), "weekdays": len(weekdays), "missing": missing}),
        CheckResult("price_positive", "warn", df.filter(pl.col("price") <= 0).height == 0,
                    {"non_positive": df.filter(pl.col("price") <= 0).height}),
    ]
