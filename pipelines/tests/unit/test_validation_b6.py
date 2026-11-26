"""B6 Layer 3 statistical checks and the point-in-time rule, on REAL EIA-930 silver.

Failure cases change one thing in real data (one spiked hour). Mechanism cases (flat
history, too little history) say so. No network.
"""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.eia import make_region_task
from atlas_pipelines.sources.eia import region
from atlas_pipelines.validation import lookahead, stats
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["us_ba"]
FX = FIXTURES / "eia"
UTC = timezone.utc
CFG = {**stats.load_config(), "trailing": {"window_days": 28, "min_history_days": 3}}  # 4 saved CISO days exist


def ok(name):
    meta = FX / name.replace(".json", ".meta.json")
    return meta.exists() and json.loads(meta.read_text())["http_status"] == 200


CISO = sorted(p.name for p in FX.glob("data__region-data__hourly__CISO__*.json")
              if not p.name.endswith(".meta.json") and ok(p.name))
pytestmark = pytest.mark.skipif(len(CISO) < 4, reason="needs 4 saved CISO region-data days")


def actual(name):
    day = date.fromisoformat(name[:-5].split("__")[-1])
    rows = json.loads((FX / name).read_text())["response"]["data"]
    d, _, _ = region.parse(rows, M, "CISO", day, datetime(2026, 11, 26, tzinfo=UTC), "x")
    return d.filter(pl.col("kind") == "actual")


def test_real_day_against_real_history_flags_nothing():
    days = [actual(n) for n in CISO]
    c = stats.outlier_check("demand_outliers", days[-1], "demand_mw", days[:-1], CFG)
    assert c.passed and c.metrics["history_days"] == len(days) - 1 and c.metrics["mad"] > 0


def test_one_spiked_hour_is_flagged_and_left_unchanged():
    days = [actual(n) for n in CISO]
    today = days[-1]
    spike_at = today["interval_start_utc"][7]
    spiked = today.with_columns(pl.when(pl.col("interval_start_utc") == spike_at).then(pl.col("demand_mw") * 10)
                                .otherwise(pl.col("demand_mw")).alias("demand_mw"))
    c = stats.outlier_check("demand_outliers", spiked, "demand_mw", days[:-1], CFG)
    assert not c.passed and c.severity == "warn" and c.metrics["flagged"] == 1
    assert c.metrics["first10"][0]["at"] == str(spike_at)
    assert spiked["demand_mw"][7] == today["demand_mw"][7] * 10  # flagged, never fixed


def test_spread_far_outside_the_trailing_spreads_is_flagged():
    days = [actual(n) for n in CISO]
    assert stats.day_structure_check("s", days[-1], days[:-1], CFG, column="demand_mw").passed
    wide = days[-1].with_columns((pl.col("demand_mw") * pl.when(pl.int_range(pl.len()) == 0).then(20).otherwise(1))
                                 .alias("demand_mw"))
    c = stats.day_structure_check("s", wide, days[:-1], CFG, column="demand_mw")
    assert not c.passed and c.metrics["spread_z"] > CFG["robust_z_threshold"]


def test_too_little_or_flat_history_passes_and_says_why():
    days = [actual(n) for n in CISO]
    c = stats.outlier_check("x", days[-1], "demand_mw", days[:1], CFG)
    assert c.passed and "insufficient history" in c.metrics["skipped"]
    flat = [d.with_columns(pl.lit(1000.0).alias("demand_mw")) for d in days[:-1]]  # mechanism case: constant history
    c = stats.outlier_check("x", days[-1], "demand_mw", flat, CFG)
    assert c.passed and "MAD = 0" in c.metrics["skipped"]


def test_config_is_validated(tmp_path):
    bad = tmp_path / "v.yaml"
    bad.write_text("version: 1\ntrailing: {window_days: 3, min_history_days: 7}\nrobust_z_threshold: 8\n", encoding="utf-8")
    with pytest.raises(ValueError):
        stats.load_config(bad)


class Ctx:
    """The one TaskContext method trailing_frames uses, over real parquet files."""
    def __init__(self, lake, pointers):
        self.lake_root, self._p = str(lake), pointers

    def resolve_input(self, asset, partition):
        return self._p.get((asset, partition))


def test_trailing_frames_reads_current_pointers_and_skips_gaps(tmp_path):
    days = [actual(n) for n in CISO[:3]]
    pointers = {}
    for k, df in zip((1, 2, 5), days):  # partitions 1, 2 and 5 days back; 3 and 4 missing
        f = tmp_path / f"d{k}.parquet"
        df.write_parquet(f)
        pointers[("silver.demand__us_ba_ciso", (date(2026, 11, 18) - timedelta(days=k)).isoformat())] = {"uri": f.name}
    got = stats.trailing_frames(Ctx(tmp_path, pointers), "silver.demand__us_ba_ciso", date(2026, 11, 18), 4)
    assert len(got) == 2  # 5 days back is outside the 4-day window; gaps skipped, nothing invented


CISO_DAY = "data__region-data__hourly__CISO__2026-11-18.json"


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.mark.skipif(not ok(CISO_DAY), reason="CISO 2026-11-18 sample missing")
def test_as_of_never_uses_a_vintage_from_the_future(lake, monkeypatch):
    """Two real forecast vintages (fetched t1, t2) of one day; decisions before, between, after."""
    monkeypatch.setenv("EIA_API_KEY", "test-eia-key-not-real-0123456789")
    data = (FX / CISO_DAY).read_bytes()
    t1, t2 = datetime(2026, 11, 26, 1, tzinfo=UTC), datetime(2026, 11, 27, 1, tzinfo=UTC)
    for t in (t1, t2):
        task = make_region_task(M, "CISO", opener=lambda req, timeout: Resp(data), now=lambda t=t: t,
                                limiter=RateLimiter(rpm=60_000, sleep=lambda s: None))
        assert run_pipeline(Pipeline("eia930_region_daily", "UTC", (task,)), "2026-11-18", lake).status == "SUCCEEDED"
    ptr = read_pointer(lake, "silver.demand__us_ba_ciso", "2026-11-18")
    fc = pl.read_parquet(Path(lake) / ptr["uri"]).filter(pl.col("kind") == "forecast")
    assert lookahead.as_of(fc, t1 - timedelta(seconds=1)).is_empty()  # nothing known yet: no row, no guess
    between = lookahead.as_of(fc, t1 + timedelta(hours=1))
    after = lookahead.as_of(fc, t2)
    assert between.height == 24 and set(between[lookahead.KNOWN_AT]) == {t1}
    assert after.height == 24 and set(after[lookahead.KNOWN_AT]) == {t2}
    feats = between.with_columns(pl.lit(t1 + timedelta(hours=1)).cast(pl.Datetime("us", "UTC")).alias("decision_at_utc"))
    assert lookahead.no_lookahead_check(feats).passed
    # Lookahead bug: the latest vintage regardless of decision time → caught.
    leaky = after.with_columns(pl.lit(t1 + timedelta(hours=1)).cast(pl.Datetime("us", "UTC")).alias("decision_at_utc"))
    c = lookahead.no_lookahead_check(leaky)
    assert not c.passed and c.severity == "blocking" and c.metrics["violations"] == 24
