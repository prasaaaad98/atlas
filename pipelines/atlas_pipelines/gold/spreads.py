"""gold.daily_spreads: per zone and local delivery day, the price spreads a battery could capture.

Why it exists: Phase C (battery simulation, the index) starts from daily
spreads. The definitions have business meaning and were confirmed by the
owner on 2026-11-25 (B7):

  max_min_spread  = highest interval price − lowest interval price of the
                    local delivery day.
  best_2h_spread  = mean price of the best 2-hour block to sell − mean price of
                    the best 2-hour block to buy, where the buy block ends at or
                    before the sell block starts (charge, then discharge), same
                    day. A "2-hour block" is consecutive intervals covering
                    2 hours (2 hourly or 8 quarter-hourly intervals). Ties go to
                    the earliest blocks. It can be negative on a day whose
                    prices only fall: then no buy-then-sell pair gains.

Who calls it: defs/prices.py (spreads_<zone> tasks); tests.
Invariants:
  - Computed only from a complete silver day (every interval present); an
    incomplete day produces no gold row (blocking check), never a partial one.
  - Deterministic: the same silver bytes give byte-identical Parquet.
  - Every row names the exact silver input (source_uri + source_content_hash).
"""

from __future__ import annotations

from datetime import date

import polars as pl

from atlas_pipelines.core.schemas import DAILY_SPREADS, schema_check
from atlas_pipelines.core.task import CheckResult

DEFINITION_VERSION = "daily_spreads-1 (owner-confirmed 2026-11-25)"
BLOCK_MINUTES = 120


def best_block_spread(prices: list[float], k: int) -> tuple[float, int, int] | None:
    """Max over (buy j, sell i) with i >= j + k of mean(sell block) − mean(buy block).

    Returns (spread, buy_index, sell_index) or None if two blocks don't fit.
    O(n): keep the cheapest buy block that ends before each sell block starts.
    """
    n = len(prices)
    if k < 1 or n < 2 * k:
        return None
    window = sum(prices[:k])
    means = [window / k]
    for i in range(k, n):
        window += prices[i] - prices[i - k]
        means.append(window / k)
    best = None
    cheapest = 0  # index of the cheapest buy block among those ending by the current sell start
    for sell in range(k, len(means)):
        buy_candidate = sell - k
        if means[buy_candidate] < means[cheapest]:
            cheapest = buy_candidate
        spread = means[sell] - means[cheapest]
        if best is None or spread > best[0]:
            best = (spread, cheapest, sell)
    return best


def compute(silver: pl.DataFrame, local_date: date, expected_intervals: int, source_uri: str,
            source_hash: str) -> tuple[pl.DataFrame, list[CheckResult]]:
    """One gold row from one zone's silver day, plus checks. Empty frame if the input is unusable."""
    df = silver.sort("interval_start_utc")
    minutes = df["interval_minutes"].unique().to_list()
    complete = df.height == expected_intervals and len(minutes) == 1 and df["local_date"].eq(local_date).all()
    checks = [CheckResult("input_complete", "blocking", bool(complete),
                          {"rows": df.height, "expected": expected_intervals, "interval_minutes": minutes})]
    schema = {c: t for c, (t, _) in DAILY_SPREADS.items()}
    if not complete or BLOCK_MINUTES % minutes[0]:
        return pl.DataFrame(schema=schema), checks
    prices = df["price"].to_list()
    k = BLOCK_MINUTES // minutes[0]
    best = best_block_spread(prices, k)
    starts = df["interval_start_utc"].to_list()
    row = {
        "market": df["market"][0], "zone": df["zone"][0], "local_date": local_date,
        "interval_minutes": minutes[0], "intervals": df.height, "currency": df["currency"][0],
        "min_price": min(prices), "max_price": max(prices), "max_min_spread": max(prices) - min(prices),
        "block_minutes": BLOCK_MINUTES,
        "best_2h_buy_start_utc": starts[best[1]] if best else None,
        "best_2h_buy_mean": sum(prices[best[1]:best[1] + k]) / k if best else None,
        "best_2h_sell_start_utc": starts[best[2]] if best else None,
        "best_2h_sell_mean": sum(prices[best[2]:best[2] + k]) / k if best else None,
        "best_2h_spread": best[0] if best else None,
        "source_uri": source_uri, "source_content_hash": source_hash, "definition_version": DEFINITION_VERSION,
    }
    out = pl.DataFrame([row], schema=schema)
    checks += [
        schema_check(out, DAILY_SPREADS, "daily_spreads"),
        # A 2-hour mean can't spread wider than the extreme single intervals.
        CheckResult("spreads_consistent", "blocking",
                    row["max_min_spread"] >= 0 and (best is None or best[0] <= row["max_min_spread"] + 1e-9),
                    {"max_min_spread": row["max_min_spread"], "best_2h_spread": row["best_2h_spread"]}),
    ]
    return out, checks
