"""Interval completeness and uniqueness checks, driven by the market registry.

Why it exists: "IEX has 96 blocks" must never be hard-coded; the expected set
of intervals for a day comes from Market.intervals(), which knows the time
zone (DST: 23/25-hour days) and the interval length in force on that date.
These checks are shared by every market adapter.

Who calls it: market adapters' silver tasks, the demo pipeline.

Expects UTC datetime columns `interval_start_utc` and `interval_end_utc`.
Missing intervals are reported, never filled (CLAUDE.md data guardrails).
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.task import CheckResult


def _utc(dt: datetime) -> datetime:
    # Polars returns aware datetimes for tz-aware columns; treat naive as UTC
    # only because silver stores UTC exclusively.
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def interval_checks(
    df: pl.DataFrame,
    market: Market,
    product: str,
    local_date: date,
    key: tuple[str, ...] = ("interval_start_utc",),
) -> list[CheckResult]:
    expected = market.intervals(product, local_date)
    want = {(i.start_utc, i.end_utc) for i in expected}
    got = {
        (_utc(s), _utc(e))
        for s, e in zip(df["interval_start_utc"].to_list(), df["interval_end_utc"].to_list())
    }
    missing = sorted(want - got)
    unexpected = sorted(got - want)
    dupes = df.height - df.select(list(key)).unique().height

    return [
        CheckResult(
            "interval_completeness",
            "blocking",
            not missing and not unexpected,
            {
                "expected": len(expected),
                "found": len(got),
                "interval_minutes": market.product(product).interval_minutes_on(local_date),
                "missing_first10": [iso_utc(s) for s, _ in missing[:10]],
                "unexpected_first10": [iso_utc(s) for s, _ in unexpected[:10]],
            },
        ),
        CheckResult(
            "primary_key_unique", "blocking", dupes == 0, {"duplicate_rows": dupes, "key": list(key)}
        ),
    ]
