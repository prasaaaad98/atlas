"""Forecast vintages are appended, never overwritten (PHASE_AB_PLAN.md §3.4a). Shared by every
adapter that ingests forecasts into silver.demand / silver.generation (EIA-930, ENTSO-E).

Who calls it: sources/eia/region.py (re-exported there), sources/entsoe/load.py.
Invariant: a new materialization contains every earlier forecast vintage (identified by
fetched_at_utc, or issued_at_utc when the source publishes one); actuals come from the
latest fetch only (a changed actual is a revision, warned on separately). A fetch that did not
ask for actuals (a forecast-only run before the delivery day) keeps the previous actuals.
"""

from __future__ import annotations

import polars as pl

from atlas_pipelines.core.markets import iso_utc
from atlas_pipelines.core.task import CheckResult


def carry_vintages(new: pl.DataFrame, previous: pl.DataFrame | None,
                   fetched_kinds: tuple[str, ...] = ("actual", "forecast")) -> pl.DataFrame:
    if previous is None or previous.is_empty():
        return new
    old = previous.filter((pl.col("kind") == "forecast") & (pl.col("fetched_at_utc") != new["fetched_at_utc"].max()))
    if "actual" not in fetched_kinds:  # this fetch didn't ask for actuals: they are not "gone"
        old = pl.concat([old, previous.filter(pl.col("kind") == "actual")])
    return pl.concat([new, old.select(new.columns)]).sort("kind", "interval_start_utc", "fetched_at_utc")


def vintages_superset_check(new: pl.DataFrame, previous: pl.DataFrame | None) -> CheckResult:
    def vintages(df):
        return set(df.filter(pl.col("kind") == "forecast")["fetched_at_utc"].to_list()) if df is not None else set()
    prev, now = vintages(previous), vintages(new)
    return CheckResult("forecast_vintages_superset", "blocking", prev <= now,
                       {"previous": len(prev), "now": len(now), "dropped": sorted(iso_utc(v) for v in prev - now)})
