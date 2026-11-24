"""Market registry: loading, validation, and registry-driven interval counts.

B0 acceptance: two dummy markets with different time zones and interval
lengths load from the registry. Data failure tests: DST days, a day before
registry coverage, malformed registry files.
"""

from datetime import date, datetime, timezone

import pytest

from atlas_pipelines.core.markets import RegistryError, load_market, load_registry, parse_partition_date
from conftest import REPO


def test_two_dummy_markets_load_with_different_tz_and_intervals(registry):
    ist, cet = registry["dummy_ist"], registry["dummy_cet"]
    assert (ist.timezone, ist.currency) == ("Asia/Kolkata", "INR")
    assert (cet.timezone, cet.currency) == ("Europe/Berlin", "EUR")
    assert ist.product("DAM").interval_minutes_on(date(2026, 11, 24)) == 15
    assert cet.product("DAM").interval_minutes_on(date(2026, 9, 30)) == 60
    assert cet.product("DAM").interval_minutes_on(date(2026, 10, 1)) == 15  # effective date inclusive


def test_real_registry_loads():
    reg = load_registry(REPO / "config" / "markets")
    assert reg["iex"].timezone == "Asia/Kolkata"
    assert set(reg["iex"].products) == {"DAM", "RTM"}


@pytest.mark.parametrize("day", ["2026-03-29", "2026-10-25", "2026-11-24", "2024-02-29"])
def test_ist_day_always_96_intervals(registry, day):
    # IST has no DST: the count must not change on European/US DST dates.
    assert len(registry["dummy_ist"].intervals("DAM", parse_partition_date(day))) == 96


def test_ist_day_bounds_are_1830_utc(registry):
    start, end = registry["dummy_ist"].day_bounds_utc(date(2026, 11, 24))
    assert start == datetime(2026, 11, 23, 18, 30, tzinfo=timezone.utc)
    assert end == datetime(2026, 11, 24, 18, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "day,expected",
    [
        ("2026-03-29", 23),  # spring forward, hourly era: 23 hours
        ("2025-10-26", 25),  # fall back, hourly era: 25 hours
        ("2026-09-30", 24),  # last hourly day
        ("2026-10-01", 96),  # first 15-min day
        ("2026-10-25", 100),  # fall back, 15-min era: 25 h × 4
        ("2027-03-28", 92),  # spring forward, 15-min era: 23 h × 4
    ],
)
def test_cet_dst_and_resolution_change(registry, day, expected):
    ivs = registry["dummy_cet"].intervals("DAM", parse_partition_date(day))
    assert len(ivs) == expected
    # Contiguous, no gaps or overlaps, blocks numbered 1..N.
    assert all(a.end_utc == b.start_utc for a, b in zip(ivs, ivs[1:]))
    assert [i.block for i in ivs] == list(range(1, expected + 1))


def test_date_before_registry_coverage_is_an_error(registry):
    with pytest.raises(RegistryError, match="no interval rule"):
        registry["dummy_cet"].intervals("DAM", date(2023, 12, 31))


def test_unknown_product_is_an_error(registry):
    with pytest.raises(RegistryError):
        registry["dummy_ist"].product("RTM")


@pytest.mark.parametrize("bad", ["20261124", "2026-11-24T00:00", "2026-13-01", "../2026-11-24"])
def test_partition_key_is_strict(bad):
    with pytest.raises(ValueError):
        parse_partition_date(bad)


GOOD = """registry_version: 1
id: {id}
name: Test
timezone: {tz}
currency: {cur}
products:
  DAM:
    canonical_market: T_DAM
    intervals:
{intervals}
    price_floor: {floor}
    price_cap: {cap}
"""


def _write(tmp_path, id="t", tz="UTC", cur="EUR", floor="null", cap="null", intervals=None):
    intervals = intervals or "      - {effective_from: 2024-01-01, minutes: 60}"
    p = tmp_path / "t.yaml"
    p.write_text(GOOD.format(id=id, tz=tz, cur=cur, floor=floor, cap=cap, intervals=intervals))
    return p


def test_valid_minimal_file_loads(tmp_path):
    assert load_market(_write(tmp_path)).id == "t"


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"tz": "Mars/Olympus"}, "unknown time zone"),
        ({"cur": "euro"}, "ISO 4217"),
        ({"id": "other"}, "match the file name"),
        ({"floor": "100", "cap": "10"}, "price_floor > price_cap"),
        ({"intervals": "      - {effective_from: 2024-01-01, minutes: 7}"}, "divisor of 1440"),
        (
            {
                "intervals": "      - {effective_from: 2025-01-01, minutes: 60}\n"
                "      - {effective_from: 2024-01-01, minutes: 15}"
            },
            "strictly increasing",
        ),
    ],
)
def test_malformed_registry_rejected(tmp_path, kwargs, match):
    with pytest.raises(RegistryError, match=match):
        load_market(_write(tmp_path, **kwargs))


def test_missing_required_field_rejected(tmp_path):
    p = _write(tmp_path)
    p.write_text(p.read_text().replace("currency: EUR\n", ""))
    with pytest.raises(RegistryError, match="currency"):
        load_market(p)


def test_duplicate_canonical_market_rejected(tmp_path):
    from conftest import FIXTURES

    for name in ("dummy_ist", "dummy_cet"):
        text = (FIXTURES / "markets" / f"{name}.yaml").read_text()
        (tmp_path / f"{name}.yaml").write_text(text.replace("DUMMY_CET_DAM", "DUMMY_IST_DAM"))
    with pytest.raises(RegistryError, match="canonical_market"):
        load_registry(tmp_path)
