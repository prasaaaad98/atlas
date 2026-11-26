"""gold.block_profiles (owner-confirmed 2026-11-27) on REAL ENTSO-E day-ahead documents (local-only).

Every real day: one row per local clock hour that occurs (24; 23 on spring-forward; 25 on
fall-back with hour 02 twice), each row's mean equals the mean of its raw interval prices read
independently from silver, rows cover the day exactly once, rebuild is byte-identical.
Failure cases change one thing in a real day. No network.
"""

import re
from datetime import date

import pytest

from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.gold import block_profiles
from atlas_pipelines.sources.entsoe import day_ahead as da
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
NAME = re.compile(r"^(DE_LU|ES|SE3)_(\d{4}-\d{2}-\d{2})_\w+$")
FUNDAMENTALS = ("_load_actual", "_load_forecast_da", "_generation_actual", "_wind_solar_forecast_da")
CASES = sorted(p for p in FX.glob("*.xml") if NAME.match(p.stem) and not p.stem.endswith(("_A01",) + FUNDAMENTALS)) if FX.exists() else []
pytestmark = pytest.mark.skipif(not CASES, reason="ENTSO-E price fixtures are local-only")
HASH = "sha256:" + "a" * 64


def silver_of(path):
    zone, day_s = NAME.match(path.stem).groups()
    day = date.fromisoformat(day_s)
    s, _ = da.select_series(da.parse_document(path.read_bytes()), da.series_rule(M, zone))
    return da.build_silver(s, M, zone, day, "fixture"), day


def build(silver, day):
    return block_profiles.compute(silver, day, len(M.intervals("DAM", day)), M.tz, "fixture", HASH)


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_day_profile(path, tmp_path):
    silver, day = silver_of(path)
    df, checks = build(silver, day)
    assert all(c.passed for c in checks), [c for c in checks if not c.passed]
    hours = round(silver.height * silver["interval_minutes"][0] / 60)
    assert df.height == hours and df["intervals"].sum() == silver.height
    for r in df.iter_rows(named=True):  # each hour = mean of exactly the intervals inside it
        inside = silver.filter((silver["interval_start_utc"] >= r["hour_start_utc"]) & (silver["interval_start_utc"] < r["hour_end_utc"]))
        assert inside.height == r["intervals"] and abs(r["mean_price"] - inside["price"].mean()) < 1e-9
        assert (r["min_price"], r["max_price"]) == (inside["price"].min(), inside["price"].max())
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    df.write_parquet(a)
    build(silver, day)[0].write_parquet(b)
    assert a.read_bytes() == b.read_bytes()


def by(zone, day):
    return next(p for p in CASES if p.stem.startswith(f"{zone}_{day}_"))


def test_spring_forward_has_no_hour_two_and_fall_back_has_it_twice():
    spring, _ = build(*silver_of(by("DE_LU", "2026-03-29")))
    assert spring.height == 23 and 2 not in spring["local_hour"].to_list()
    fall, _ = build(*silver_of(by("DE_LU", "2025-10-26")))
    twos = fall.filter(fall["local_hour"] == 2)
    assert fall.height == 25 and twos["hour_occurrence"].to_list() == [1, 2]  # never averaged together
    assert twos["hour_start_utc"].to_list()[0] < twos["hour_start_utc"].to_list()[1]


def test_incomplete_day_gives_no_rows():
    silver, day = silver_of(by("ES", "2026-11-18"))
    df, checks = build(silver.head(silver.height - 1), day)  # one real interval removed
    assert df.is_empty() and not next(c for c in checks if c.check_name == "input_complete").passed
