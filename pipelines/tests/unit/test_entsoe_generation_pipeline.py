"""`generation_entsoe_daily` end to end on REAL ENTSO-E documents (local-only fixtures; skips in CI).

The fake opener serves the saved A75 (actual) or A69 (forecast) document by documentType.
The real ES 2025-10-26 document (a quarter-hour missing in every series) is retried while
recent and blocks after: the day never publishes with a hole in it. No network.
"""

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.entsoe_generation import make_generation_task
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
TOKEN = "test-entsoe-token-not-real-0123"
UTC = timezone.utc
NEED = ["SE3_2026-11-18_normal_generation_actual.xml", "SE3_2026-11-18_normal_wind_solar_forecast_da.xml",
        "ES_2025-10-26_fall_back_generation_actual.xml", "ES_2025-10-26_fall_back_wind_solar_forecast_da.xml"]
pytestmark = pytest.mark.skipif(not all((FX / n).exists() for n in NEED), reason="ENTSO-E generation fixtures are local-only")


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run(lake, zone, day, stem, now):
    def opener(req, timeout):
        item = "generation_actual" if "documentType=A75" in req.full_url else "wind_solar_forecast_da"
        return Resp((FX / f"{stem}_{item}.xml").read_bytes())
    task = make_generation_task(M, zone, opener=opener, now=lambda: now,
                                limiter=RateLimiter(rpm=60_000, sleep=lambda s: None))
    return run_pipeline(Pipeline("generation_entsoe_daily", "Europe/Brussels", (task,)), day, lake)


def test_day_publishes_by_type_and_forecast_vintages(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    t1, t2 = datetime(2026, 11, 26, 15, tzinfo=UTC), datetime(2026, 11, 27, 15, tzinfo=UTC)
    for t in (t1, t2):
        r = run(lake, "SE3", "2026-11-18", "SE3_2026-11-18_normal", t)
        assert r.status == "SUCCEEDED", [(x.task, x.error) for x in r.tasks]
    df = pl.read_parquet(Path(lake) / read_pointer(lake, "silver.generation__entsoe_se3", "2026-11-18")["uri"])
    actual = df.filter(pl.col("kind") == "actual")
    assert set(actual["source_fuel_code"]) == {"B04", "B12", "B14", "B16", "B19", "B20"} and actual.height == 6 * 96
    fc = df.filter(pl.col("kind") == "forecast")
    assert set(fc["fuel_type"]) == {"solar", "wind"} and sorted(fc["fetched_at_utc"].unique().to_list()) == [t1, t2]
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert TOKEN.encode() not in f.read_bytes(), f


def test_real_gap_is_retried_while_recent_then_blocks(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    t = run(lake, "ES", "2025-10-26", "ES_2025-10-26_fall_back", datetime(2025, 10, 27, 6, tzinfo=UTC)).tasks[0]
    assert t.retryable and t.error_class == "source_not_ready"
    r = run(lake, "ES", "2025-10-26", "ES_2025-10-26_fall_back", datetime(2026, 11, 26, tzinfo=UTC))
    assert r.status == "FAILED" and "generation_published" in r.tasks[0].error
    assert read_pointer(lake, "silver.generation__entsoe_es", "2025-10-26") is None
