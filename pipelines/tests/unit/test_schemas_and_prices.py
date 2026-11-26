"""Canonical schema check and registry-driven price checks.

Values below are derived from the registry limits themselves (exactly at the
cap, cap + 1, ...) to test check logic. They are not market data; golden tests
on real fetched files cover real prices.
"""

import math
from datetime import date, datetime, timezone

import polars as pl
import pytest

from atlas_pipelines.checks.prices import price_checks
from atlas_pipelines.core.markets import load_market
from atlas_pipelines.core.schemas import POWER_PRICES, UTC, WEATHER_HOURLY, empty_frame, schema_check

MARKET = """registry_version: 1
id: t
name: Test
timezone: UTC
currency: EUR
products:
  DAM:
    canonical_market: T_DAM
    intervals: [{effective_from: 2024-01-01, minutes: 60}]
@LIMITS@
"""


def market(tmp_path, limits=""):
    p = tmp_path / "t.yaml"
    p.write_text(MARKET.replace("@LIMITS@", limits))
    return load_market(p)


SOURCED = (
    "    price_floor: -10\n    price_floor_meta: {status: sourced, source: doc}\n"
    "    price_cap: 100\n    price_cap_meta: {status: sourced, source: doc}"
)


def frame(prices, currency="EUR"):
    n = len(prices)
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return pl.DataFrame(
        {
            "market": ["T_DAM"] * n,
            "zone": ["Z"] * n,
            "interval_start_utc": [t] * n,
            "interval_end_utc": [t] * n,
            "interval_minutes": [60] * n,
            "local_date": [date(2026, 1, 1)] * n,
            "local_block": list(range(1, n + 1)),
            "price": prices,
            "currency": [currency] * n,
            "source_uri": ["bronze/x"] * n,
        },
        schema={c: dt for c, (dt, _) in POWER_PRICES.items()},
    )


def by_name(checks):
    return {c.check_name: c for c in checks}


def test_prices_at_limits_pass_and_block_when_sourced(tmp_path):
    c = by_name(price_checks(frame([-10.0, 100.0]), market(tmp_path, SOURCED), "DAM"))
    assert c["price_within_limits"].passed and c["price_within_limits"].severity == "blocking"
    assert c["price_finite"].passed and c["currency_matches_registry"].passed


def test_price_above_sourced_cap_is_blocking_failure(tmp_path):
    c = by_name(price_checks(frame([100.0 + 1]), market(tmp_path, SOURCED), "DAM"))["price_within_limits"]
    assert (c.passed, c.severity, c.metrics["above_cap"]) == (False, "blocking", 1)


def test_negative_prices_allowed_when_floor_is_negative(tmp_path):
    assert by_name(price_checks(frame([-5.0]), market(tmp_path, SOURCED), "DAM"))["price_within_limits"].passed


def test_assumed_limit_only_warns(tmp_path):
    limits = SOURCED.replace("price_cap_meta: {status: sourced, source: doc}", "price_cap_meta: {status: assumed}")
    c = by_name(price_checks(frame([100.0 + 1]), market(tmp_path, limits), "DAM"))["price_within_limits"]
    assert (c.passed, c.severity) == (False, "warn")


def test_unset_limits_warn_and_say_so(tmp_path):
    c = by_name(price_checks(frame([1.0]), market(tmp_path), "DAM"))["price_within_limits"]
    assert (c.passed, c.severity, c.metrics["unset_limits"]) == (False, "warn", ["price_floor", "price_cap"])


@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_non_finite_price_is_blocking(tmp_path, bad):
    c = by_name(price_checks(frame([1.0, bad]), market(tmp_path, SOURCED), "DAM"))["price_finite"]
    assert (c.passed, c.severity) == (False, "blocking")


def test_wrong_currency_is_blocking(tmp_path):
    c = by_name(price_checks(frame([1.0], currency="USD"), market(tmp_path, SOURCED), "DAM"))
    assert not c["currency_matches_registry"].passed


def test_schema_check_passes_canonical_and_catches_drift():
    assert schema_check(empty_frame(POWER_PRICES), POWER_PRICES, "power_prices").passed
    assert schema_check(empty_frame(WEATHER_HOURLY), WEATHER_HOURLY, "weather_hourly").passed
    df = frame([1.0])
    bad_type = df.with_columns(pl.col("price").cast(pl.Float32))
    assert not schema_check(bad_type, POWER_PRICES, "p").passed
    missing = df.drop("currency")
    assert "missing columns" in schema_check(missing, POWER_PRICES, "p").metrics["problems"][0]
    null_price = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("price"))
    assert not schema_check(null_price, POWER_PRICES, "p").passed
    naive_ts = df.with_columns(pl.col("interval_start_utc").dt.replace_time_zone(None))
    assert not schema_check(naive_ts, POWER_PRICES, "p").passed  # UTC-only rule
    assert df.schema["interval_start_utc"] == UTC


NO_LIMITS = "    price_limits: none\n    price_limits_meta: {decided_by: owner, date: 2026-11-26, reason: take data as sent}"


def test_no_limits_policy_passes_any_finite_price_and_records_range(tmp_path):
    """Owner policy (ENTSO-E, 2026-11-26): no range limits; prices are taken as the source sends them."""
    c = by_name(price_checks(frame([-500.0, 0.0, 9999.0]), market(tmp_path, NO_LIMITS), "DAM"))
    lim = c["price_within_limits"]
    assert lim.passed and lim.metrics["policy"] == "none" and (lim.metrics["min"], lim.metrics["max"]) == (-500.0, 9999.0)
    assert lim.metrics["decided_by"] == "owner"


def test_no_limits_policy_still_blocks_non_finite(tmp_path):
    c = by_name(price_checks(frame([1.0, math.nan]), market(tmp_path, NO_LIMITS), "DAM"))
    assert (c["price_finite"].passed, c["price_finite"].severity) == (False, "blocking")


@pytest.mark.parametrize("limits", [
    NO_LIMITS + "\n    price_floor: 0\n    price_floor_meta: {status: assumed}",   # policy contradicts a limit
    "    price_limits: none",                                                     # decision without who/when
    "    price_limits: loose\n    price_limits_meta: {decided_by: owner, date: 2026-11-26}",
])
def test_no_limits_policy_is_validated(tmp_path, limits):
    from atlas_pipelines.core.markets import RegistryError
    with pytest.raises(RegistryError):
        market(tmp_path, limits)
