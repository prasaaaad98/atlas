"""Per-series completeness (ADR-B10): judged per (zone, day, series), never per day.

Why it exists: one sparse series (an offshore-wind forecast, a minor fuel) must not hold back
the series that are complete. Each series gets its own `series_complete` flag; nothing is
filled. Downstream assets declare which series they need and block only where one is missing.
Who calls it: sources/entsoe/generation.py, sources/eia/fuel.py; gold reads the flag.
Invariant: a series is complete iff it has ONE resolution, every interval of the day at that
resolution, and no null values. Missing intervals stay missing (no rows are added).
"""

from __future__ import annotations

from datetime import datetime

import polars as pl


def mark_complete(df: pl.DataFrame, by: list[str], start: datetime, end: datetime, value: str) -> pl.DataFrame:
    """Adds a boolean `series_complete` column: the same value on every row of a series."""
    if df.is_empty():
        return df.with_columns(pl.lit(None, dtype=pl.Boolean).alias("series_complete"))
    span = (end - start).total_seconds() / 60
    stats = df.group_by(by).agg(
        pl.len().alias("_n"), pl.col("interval_minutes").first().alias("_m"),
        pl.col("interval_minutes").n_unique().alias("_res"), pl.col(value).null_count().alias("_nulls"),
        pl.col("interval_start_utc").n_unique().alias("_distinct"))
    stats = stats.with_columns(((pl.col("_res") == 1) & (pl.col("_nulls") == 0) & (pl.col("_n") == pl.col("_distinct"))
                                & (pl.col("_n").cast(pl.Float64) == span / pl.col("_m").cast(pl.Float64)))
                               .alias("series_complete")).select(*by, "series_complete")
    return df.join(stats, on=by, how="left")


def missing_intervals(df: pl.DataFrame, start: datetime, end: datetime) -> list[datetime]:
    """Interval starts of [start, end) absent from one series' rows, at its (first) resolution."""
    if df.is_empty():
        return []
    from datetime import timedelta
    m = int(df["interval_minutes"][0])
    have = set(df["interval_start_utc"].to_list())
    n = int((end - start).total_seconds() // 60 // m)
    return sorted({start + timedelta(minutes=m * i) for i in range(n)} - have)
