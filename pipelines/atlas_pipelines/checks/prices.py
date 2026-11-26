"""Price range and currency checks, driven by the market registry.

Why it exists: price limits differ by market and product, change over time, and
in Europe prices can be negative, so "price >= 0" is wrong as a shared rule.
The limits come only from the registry, with provenance (core/markets.py).

Severity follows the provenance of the limit:
  - both limits `sourced`/`verified` -> blocking
  - any limit `assumed` or unset  -> warn (recorded, never silently passed)
  - `price_limits: none` (owner policy) -> passes; min/max and the decision are recorded
A non-finite price (NaN, inf) is always blocking. A currency that doesn't match
the registry is always blocking.
Who calls it: every market adapter's silver task.
"""

from __future__ import annotations

import polars as pl

from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.task import CheckResult


def price_checks(df: pl.DataFrame, market: Market, product: str) -> list[CheckResult]:
    p = market.product(product)
    prices = df["price"]
    non_finite = int((~prices.is_finite()).sum()) + prices.null_count()

    lo, hi = p.price_floor, p.price_cap
    below = int((prices < lo).sum()) if lo is not None else 0
    above = int((prices > hi).sum()) if hi is not None else 0
    sourced = {p.price_floor_status, p.price_cap_status} <= {"sourced", "verified"}
    unset = [name for name, v in (("price_floor", lo), ("price_cap", hi)) if v is None]

    currencies = sorted(set(df["currency"].unique().to_list()))
    if p.limits_policy == "none":
        # Owner policy: take prices as sent (negative included). Range still recorded so a
        # strange value is visible; B6 price_outliers flags it against the zone's own history.
        limits = CheckResult("price_within_limits", "warn", True,
                             {"policy": "none", **(p.limits_policy_meta or {}),
                              "min": prices.min(), "max": prices.max()})
    else:
        limits = CheckResult(
            "price_within_limits",
            "blocking" if sourced else "warn",
            below == 0 and above == 0 and not unset,
            {
                "floor": lo,
                "cap": hi,
                "floor_status": p.price_floor_status,
                "cap_status": p.price_cap_status,
                "unset_limits": unset,
                "below_floor": below,
                "above_cap": above,
                "min": prices.min(),
                "max": prices.max(),
            },
        )
    return [
        CheckResult("price_finite", "blocking", non_finite == 0, {"non_finite": non_finite}),
        limits,
        CheckResult(
            "currency_matches_registry",
            "blocking",
            currencies == [market.currency],
            {"expected": market.currency, "found": currencies},
        ),
    ]
