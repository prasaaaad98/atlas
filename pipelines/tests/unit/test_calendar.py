"""silver.calendar (B4e). Holiday expectations are statutory dates (e.g. German Unity
Day, 3 October), checked against the pinned `holidays` version; no network.
Special-day entries in these tests are test configuration, not market data."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.calendar import calendar_pipelines
from atlas_pipelines.sources.calendar import build as cal
from conftest import REPO

REG = load_registry(REPO / "config" / "markets")


def frame(market_id, year, special=()):
    return cal.build(REG[market_id], year, list(special), "test")


def row(df, zone, d):
    r = df.filter((pl.col("zone") == zone) & (pl.col("local_date") == date.fromisoformat(d)))
    assert r.height == 1
    return r.row(0, named=True)


@pytest.mark.parametrize("market,zone,day,name", [
    ("entsoe", "DE_LU", "2025-10-03", "German Unity Day"),
    ("entsoe", "ES", "2026-10-12", "National Day"),
    ("entsoe", "SE3", "2025-06-06", "National Day"),
    ("us_ba", "PJM", "2025-07-04", "Independence Day"),
    ("us_ba", "ERCO", "2026-03-02", "Texas Independence Day"),
    ("us_ba", "CISO", "2026-11-26", "Thanksgiving Day"),
    ("iex", "SYSTEM", "2026-01-26", "Republic Day"),
])
def test_public_holidays(market, zone, day, name):
    r = row(frame(market, int(day[:4])), zone, day)
    assert r["is_public_holiday"] and name in r["holiday_name"]


def test_state_holiday_only_in_the_single_state_ba():
    df = frame("us_ba", 2026)
    assert row(df, "ERCO", "2026-03-02")["is_public_holiday"]
    assert not row(df, "PJM", "2026-03-02")["is_public_holiday"]  # federal-only calendar


def test_weekends_and_plain_days():
    df = frame("entsoe", 2026)
    assert row(df, "DE_LU", "2026-11-28")["is_weekend"]           # Saturday
    plain = row(df, "DE_LU", "2026-11-25")                         # Wednesday
    assert not plain["is_weekend"] and not plain["is_public_holiday"] and plain["holiday_name"] is None


@pytest.mark.parametrize("market,zone,short,long", [
    ("entsoe", "DE_LU", "2025-03-30", "2025-10-26"),
    ("entsoe", "SE3", "2025-03-30", "2025-10-26"),
    ("us_ba", "CISO", "2025-03-09", "2025-11-02"),   # Pacific
    ("us_ba", "ERCO", "2025-03-09", "2025-11-02"),   # Central
    ("us_ba", "PJM", "2025-03-09", "2025-11-02"),    # Eastern
])
def test_dst_days(market, zone, short, long):
    df = frame(market, 2025)
    z = df.filter(pl.col("zone") == zone)
    assert z.filter(pl.col("dst_transition") == "short")["local_date"].to_list() == [date.fromisoformat(short)]
    assert z.filter(pl.col("dst_transition") == "long")["local_date"].to_list() == [date.fromisoformat(long)]


def test_no_dst_in_india():
    assert frame("iex", 2025)["dst_transition"].unique().to_list() == ["none"]


def test_one_row_per_zone_and_day_including_leap_years():
    df = frame("entsoe", 2024)
    assert df.height == 366 * 3 and df.select("zone", "local_date").unique().height == df.height


def test_calendar_tz_matches_the_eia_section():
    raw = REG["us_ba"].raw
    for zone, z in raw["calendar"]["zones"].items():
        assert z["tz"] == raw["eia"]["bas"][zone]["tz"], zone


def write_special(tmp_path, body):
    p = tmp_path / "x.yaml"
    p.write_text("version: 1\nspecial_days:\n" + body, encoding="utf-8")
    return p


@pytest.mark.parametrize("entry,match", [
    ("  - {date: 2026-12-24, name: Eve, zones: all, announced_on: 2026-01-01}\n", "source"),
    ("  - {date: 2026-12-24, name: Eve, zones: all, source: s, announced_on: 2026-12-25}\n", "after the day"),
    ("  - {date: 2026-12-24, name: Eve, zones: [XX], source: s, announced_on: 2026-01-01}\n", "zones"),
    ("  - {date: 2026-12-24, name: A, zones: all, source: s, announced_on: 2026-01-01}\n"
     "  - {date: 2026-12-24, name: B, zones: [DE_LU], source: s, announced_on: 2026-01-01}\n", "two special days"),
])
def test_special_days_are_validated(tmp_path, entry, match):
    with pytest.raises(cal.CalendarConfigError, match=match):
        cal.load_special_days(write_special(tmp_path, entry), ["DE_LU", "ES", "SE3"])


def test_special_day_point_in_time(tmp_path):
    p = write_special(tmp_path, "  - {date: 2026-12-24, name: Test bridge day, zones: [ES], source: test-config, announced_on: 2026-12-01}\n")
    special, h = cal.load_special_days(p, ["DE_LU", "ES", "SE3"])
    df = frame("entsoe", 2026, special)
    r = row(df, "ES", "2026-12-24")
    assert r["is_special_day"] and r["special_day_announced_on"] == date(2026, 12, 1) and h.startswith("sha256:")
    assert not row(df, "DE_LU", "2026-12-24")["is_special_day"]
    before = cal.known_as_of(df, date(2026, 11, 30))   # a backtest on 30 Nov can't know it yet
    assert not row(before, "ES", "2026-12-24")["is_special_day"]
    assert row(cal.known_as_of(df, date(2026, 12, 1)), "ES", "2026-12-24")["is_special_day"]
    assert all(c.passed for c in cal.checks(df, REG["entsoe"], 2026))


def test_bad_registry_calendar_rejected(tmp_path):
    text = (REPO / "config" / "markets" / "entsoe.yaml").read_text(encoding="utf-8")
    bad = tmp_path / "entsoe.yaml"
    bad.write_text(text.replace("DE_LU: {country: DE, subdivision: null", "DE_LU: {country: DE, subdivision: ZZ"), encoding="utf-8")
    from atlas_pipelines.core.markets import load_market
    with pytest.raises(cal.CalendarConfigError, match="subdivision"):
        cal.zone_calendars(load_market(bad))


@pytest.mark.parametrize("year", range(2022, date.today().year + 2))
def test_every_market_builds_and_passes_checks_2022_through_next_year(year):
    for m in ("entsoe", "us_ba", "iex"):
        df = frame(m, year)
        failed = [c for c in cal.checks(df, REG[m], year) if not c.passed]
        assert failed == [], (m, year, failed)


def test_pipeline_runs_rebuilds_identically_and_rejects_non_year_partitions(lake, tmp_path):
    [p] = calendar_pipelines(REG)
    assert {t.name for t in p.tasks} == {"build_entsoe", "build_iex", "build_us_ba"}
    assert run_pipeline(p, "2026-01-01", lake).status == "SUCCEEDED"
    files = sorted(Path(lake).rglob("calendar__entsoe/**/part-0.parquet"))
    assert run_pipeline(p, "2026-01-01", lake).status == "SUCCEEDED"
    files2 = sorted(Path(lake).rglob("calendar__entsoe/**/part-0.parquet"))
    assert len(files2) == 2 and sha256_file(files2[0]) == sha256_file(files2[1]) and files[0] in files2
    bad = run_pipeline(p, "2026-06-01", lake)
    assert bad.status == "FAILED" and all(t.error_class == "data_check_failed" for t in bad.tasks)
