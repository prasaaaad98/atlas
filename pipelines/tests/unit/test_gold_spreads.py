"""gold.daily_spreads (B7): definitions, algorithm, determinism, input integrity.

The property test uses seeded synthetic number sequences to check the O(n)
algorithm against a brute-force reference; they are algorithm inputs, not
market data. Market-shaped checks use the real local-only ENTSO-E fixtures
(skipped in CI where they are absent).
"""

import random
import re
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.core.schemas import POWER_PRICES
from atlas_pipelines.core.task import TaskContext
from atlas_pipelines.defs.prices import price_pipelines
from atlas_pipelines.gold import spreads
from atlas_pipelines.sources.entsoe import day_ahead as da
from conftest import FIXTURES, REPO

REGISTRY = load_registry(REPO / "config" / "markets")
M = REGISTRY["entsoe"]
FX = FIXTURES / "entsoe"
NAME = re.compile(r"^(DE_LU|ES|SE3)_(\d{4}-\d{2}-\d{2})_\w+$")
FUNDAMENTALS = ("_load_actual", "_load_forecast_da", "_generation_actual", "_wind_solar_forecast_da",
                "_reserves_daily", "_imbalance_prices")  # B4a/B4b/B4c files, not A44 price days
CASES = sorted(p for p in FX.glob("*.xml") if NAME.match(p.stem) and not p.stem.endswith(("_A01",) + FUNDAMENTALS)) if FX.exists() else []


def brute(prices, k):
    best = None
    means = [sum(prices[i:i + k]) / k for i in range(len(prices) - k + 1)]
    for j in range(len(means)):
        for i in range(j + k, len(means)):
            s = means[i] - means[j]
            if best is None or s > best[0] + 1e-12:
                best = (s, j, i)
    return best


def silver_frame(prices, minutes=60, day=date(2026, 11, 18)):
    start = datetime(2026, 11, 17, 23, tzinfo=timezone.utc)
    rows = [{"market": "ENTSOE_DAM", "zone": "SE3", "interval_start_utc": start + timedelta(minutes=minutes * i),
             "interval_end_utc": start + timedelta(minutes=minutes * (i + 1)), "interval_minutes": minutes,
             "local_date": day, "local_block": i + 1, "price": float(p), "currency": "EUR", "source_uri": "x"}
            for i, p in enumerate(prices)]
    return pl.DataFrame(rows, schema={c: t for c, (t, _) in POWER_PRICES.items()})


def test_hand_worked_example():
    # 24 hourly prices: cheapest 2 h = hours 3-4 (mean 1), dearest later 2 h = hours 18-19 (mean 95).
    prices = [40, 30, 20, 1, 1, 25, 50, 60, 55, 45, 35, 30, 30, 35, 45, 60, 80, 90, 95, 95, 70, 60, 50, 45]
    df, checks = spreads.compute(silver_frame(prices), date(2026, 11, 18), 24, "silver/x.parquet", "sha256:" + "a" * 64)
    row = df.row(0, named=True)
    assert row["max_min_spread"] == 94 and row["min_price"] == 1 and row["max_price"] == 95
    assert row["best_2h_buy_mean"] == 1 and row["best_2h_sell_mean"] == 95 and row["best_2h_spread"] == 94
    assert row["best_2h_buy_start_utc"].hour == 2  # 4th hour of the Brussels day = 02:00 UTC
    assert row["block_minutes"] == 120 and row["definition_version"].startswith("daily_spreads-1")
    assert all(c.passed for c in checks)


def test_buy_must_come_before_sell():
    # Dearest block first, cheapest last: the late cheap block can't be bought before selling.
    prices = list(range(230, -10, -10))  # strictly falling: 230, 220, ..., 0
    df, _ = spreads.compute(silver_frame(prices), date(2026, 11, 18), 24, "x", "sha256:" + "a" * 64)
    row = df.row(0, named=True)
    # Best is buying hours 1-2 (mean 225) and selling hours 3-4 (mean 205): -20.
    assert row["best_2h_spread"] == -20 and row["max_min_spread"] == 230


@pytest.mark.parametrize("k", [2, 8])
def test_linear_algorithm_matches_brute_force(k):
    rng = random.Random(20261125 + k)
    for _ in range(250):
        n = rng.choice([23, 24, 25, 92, 96, 100])
        prices = [round(rng.uniform(-50, 300), 2) for _ in range(n)]
        fast, slow = spreads.best_block_spread(prices, k), brute(prices, k)
        assert abs(fast[0] - slow[0]) < 1e-9 and (fast[1], fast[2]) == (slow[1], slow[2])
    assert spreads.best_block_spread([1.0, 2.0, 3.0], 2) is None


def test_incomplete_day_produces_no_row():
    df, checks = spreads.compute(silver_frame(list(range(23))), date(2026, 11, 18), 24, "x", "sha256:" + "a" * 64)
    assert df.is_empty() and [(c.check_name, c.passed) for c in checks] == [("input_complete", False)]


def test_tampered_input_is_refused(lake):
    [p] = price_pipelines(REGISTRY)
    task = p.task("spreads_se3")
    bad = {"uri": "silver/x/part-0.parquet", "content_hash": "sha256:" + "0" * 64}
    target = lake + "/silver/x/part-0.parquet"
    import pathlib
    pathlib.Path(target).parent.mkdir(parents=True)
    silver_frame(list(range(24))).write_parquet(target)
    ctx = TaskContext("prices_entsoe_daily", "spreads_se3", "2026-11-18", "a", "b", "att", None, lake,
                      lambda d: True, lambda asset, part: bad)
    mats, checks = task.fn(ctx)
    assert mats == [] and checks[0].check_name == "input_hash_matches_pointer" and not checks[0].passed


needs_fixtures = pytest.mark.skipif(not CASES, reason="ENTSO-E fixtures are local-only and not present")


@needs_fixtures
@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_days(path, tmp_path):
    zone, day_s = NAME.match(path.stem).groups()
    day = date.fromisoformat(day_s)
    doc = da.parse_document(path.read_bytes())
    s, _ = da.select_series(doc, da.series_rule(M, zone))
    silver = da.build_silver(s, M, zone, day, "fixture")
    expected = len(M.intervals("DAM", day))
    df, checks = spreads.compute(silver, day, expected, "fixture", "sha256:" + "a" * 64)
    assert all(c.passed for c in checks), [c for c in checks if not c.passed]
    row = df.row(0, named=True)
    prices = silver.sort("interval_start_utc")["price"].to_list()
    k = 120 // silver["interval_minutes"][0]
    ref = brute(prices, k)
    assert abs(row["best_2h_spread"] - ref[0]) < 1e-9
    assert row["max_min_spread"] == max(prices) - min(prices)
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    df.write_parquet(a, compression="zstd", statistics=True)
    spreads.compute(silver, day, expected, "fixture", "sha256:" + "a" * 64)[0].write_parquet(b, compression="zstd", statistics=True)
    assert sha256_file(a) == sha256_file(b)  # gold reproduces byte-for-byte from the same silver


@needs_fixtures
def test_pipeline_builds_gold_from_the_current_silver(lake, monkeypatch):
    from test_entsoe import AFTER, NOLIMIT, normal_day, serving, TOKEN
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    [p] = price_pipelines(REGISTRY, opener=serving(normal_day()), now=AFTER, limiter=NOLIMIT)
    r = run_pipeline(p, "2026-11-18", lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    for z in ("de_lu", "es", "se3"):
        silver = read_pointer(lake, f"silver.power_prices__entsoe_{z}", "2026-11-18")
        gold = read_pointer(lake, f"gold.daily_spreads__entsoe_{z}", "2026-11-18")
        g = pl.read_parquet(lake + "/" + gold["uri"])
        assert g.height == 1 and g["source_content_hash"][0] == silver["content_hash"]
        assert g["source_uri"][0] == silver["uri"] and g["intervals"][0] == 96
