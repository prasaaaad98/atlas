"""Point-in-time selection and the `no_lookahead` check (B6, PHASE_AB_PLAN.md §3.4a / §3.6).

Why it exists: a backtest that uses a forecast published after the decision it informs is
too good to be true. Every gold feature table that uses forecasts must select, per target
interval, the latest vintage KNOWN at the decision time, and prove no row breaks that rule.
"Known at" = issued_at_utc when the source publishes it, otherwise fetched_at_utc (the
earliest moment we know it, e.g. EIA-930: ADR-B06 addendum).
Who calls it: gold feature builders that join forecasts (Phase C/D); tests on real EIA silver.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from atlas_pipelines.core.task import CheckResult

KNOWN_AT = "known_at_utc"


def with_known_at(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.coalesce("issued_at_utc", "fetched_at_utc").alias(KNOWN_AT))


def as_of(forecasts: pl.DataFrame, decision_at: datetime, keys: tuple[str, ...] = ("interval_start_utc",)) -> pl.DataFrame:
    """Per key, the latest forecast vintage known at or before `decision_at` (others dropped).
    A target with no vintage known by then has no row: missing stays missing."""
    known = with_known_at(forecasts).filter(pl.col(KNOWN_AT) <= decision_at)
    return (known.sort(KNOWN_AT).group_by(list(keys), maintain_order=True).last()
            .sort(list(keys)))


def no_lookahead_check(features: pl.DataFrame, decision_column: str = "decision_at_utc") -> CheckResult:
    """Blocking: every row's forecast was known by its decision time."""
    bad = features.filter(pl.col(KNOWN_AT) > pl.col(decision_column))
    return CheckResult("no_lookahead", "blocking", bad.is_empty(),
                       {"rows": features.height, "violations": bad.height,
                        "first5": [str(t) for t in bad[decision_column].head(5).to_list()]})
