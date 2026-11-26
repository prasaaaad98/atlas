"""gold.block_profiles: per zone and local delivery day, the price for each local clock hour.

Why it exists: Phase C needs the shape of the day (when prices are low and high), not only its
spreads; averaging these rows across days by local hour gives the typical daily profile.
Definition confirmed by the owner 2026-11-27: the mean of the interval prices inside each local
clock hour of the delivery day (mean of 4 quarter-hours, or the one hourly price).

DST (design note, 2026-11-27): one row per local clock hour that ACTUALLY occurs that day, so the
rows always cover the day exactly once:
  - spring-forward (23 h): local hour 02 doesn't exist → no row (not a null row: nothing is missing);
  - fall-back (25 h): local hour 02 occurs twice → two rows, hour_occurrence 1 and 2 (in UTC order),
    never averaged together (they are different hours with different prices).
Who calls it: defs/prices.py (profiles_<zone> tasks); tests.
Invariants:
  - Computed only from a complete silver day (same rule as daily_spreads); otherwise no rows.
  - Rows cover the day exactly once: Σ intervals = the day's interval count; hours = 23/24/25.
  - Deterministic: the same silver bytes give byte-identical Parquet; every row names its input.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from atlas_pipelines.core.schemas import BLOCK_PROFILES, schema_check
from atlas_pipelines.core.task import CheckResult

DEFINITION_VERSION = "block_profiles-1 (owner-confirmed 2026-11-27)"


def compute(silver: pl.DataFrame, local_date: date, expected_intervals: int, tz, source_uri: str,
            source_hash: str) -> tuple[pl.DataFrame, list[CheckResult]]:
    df = silver.sort("interval_start_utc")
    minutes = df["interval_minutes"].unique().to_list()
    complete = (df.height == expected_intervals and len(minutes) == 1 and df["local_date"].eq(local_date).all()
                and df["price"].null_count() == 0)
    checks = [CheckResult("input_complete", "blocking", bool(complete),
                          {"rows": df.height, "expected": expected_intervals, "interval_minutes": minutes})]
    schema = {c: t for c, (t, _) in BLOCK_PROFILES.items()}
    if not complete or 60 % minutes[0]:
        return pl.DataFrame(schema=schema), checks
    # Group consecutive intervals by their local clock hour; a repeated hour (fall-back) is a new
    # group because its UTC offset differs.
    groups: list[dict] = []
    for start, end, price in zip(df["interval_start_utc"], df["interval_end_utc"], df["price"]):
        loc = start.astimezone(tz)
        key = (loc.hour, loc.utcoffset())
        if not groups or groups[-1]["key"] != key:
            seen = sum(1 for g in groups if g["key"][0] == loc.hour)
            groups.append({"key": key, "hour": loc.hour, "occurrence": seen + 1, "start": start, "end": end, "prices": []})
        groups[-1]["prices"].append(price)
        groups[-1]["end"] = end
    rows = [{"market": df["market"][0], "zone": df["zone"][0], "local_date": local_date, "local_hour": g["hour"],
             "hour_occurrence": g["occurrence"], "hour_start_utc": g["start"], "hour_end_utc": g["end"],
             "interval_minutes": minutes[0], "intervals": len(g["prices"]),
             "mean_price": sum(g["prices"]) / len(g["prices"]), "min_price": min(g["prices"]),
             "max_price": max(g["prices"]), "currency": df["currency"][0], "source_uri": source_uri,
             "source_content_hash": source_hash, "definition_version": DEFINITION_VERSION} for g in groups]
    out = pl.DataFrame(rows, schema=schema)
    hours_expected = round(expected_intervals * minutes[0] / 60)
    checks += [
        schema_check(out, BLOCK_PROFILES, "block_profiles"),
        # Every interval in exactly one full clock hour; as many hours as the day really has.
        CheckResult("profile_covers_day", "blocking",
                    out["intervals"].sum() == df.height and out.height == hours_expected
                    and (out["intervals"] == 60 // minutes[0]).all(),
                    {"hours": out.height, "expected_hours": hours_expected, "intervals": int(out["intervals"].sum())}),
    ]
    return out, checks
