"""`load_entsoe_daily` end to end on REAL ENTSO-E documents (local-only fixtures; skips in CI).

A fake opener serves the saved actual (A16) or forecast (A01) document by the request's
processType; the "not published yet" case serves ENTSO-E's real "No matching data found"
acknowledgement (DE_LU_future_no_data.xml) and the real gateway timeout page (599). No network.
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
from atlas_pipelines.defs.entsoe_load import make_load_task
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
TOKEN = "test-entsoe-token-not-real-0123"
UTC = timezone.utc
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
NEED = ["DE_LU_2026-11-18_normal_load_actual.xml", "DE_LU_2026-11-18_normal_load_forecast_da.xml",
        "ES_2026-03-29_spring_forward_load_actual.xml", "ES_2026-03-29_spring_forward_load_forecast_da.xml",
        "DE_LU_future_no_data.xml", "errors/ES_2026-03-29_load_forecast_da_gateway_599.json"]
pytestmark = pytest.mark.skipif(not all((FX / n).exists() for n in NEED), reason="ENTSO-E load fixtures are local-only")


class Resp:
    def __init__(self, data, status=200):
        self.status, self._d, self.headers = status, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def serve(actual: str, forecast: str):
    def opener(req, timeout):
        name = actual if "processType=A16" in req.full_url else forecast
        assert TOKEN not in name
        return Resp((FX / name).read_bytes())
    return opener


def run(lake, zone, day, actual, forecast, now):
    task = make_load_task(M, zone, opener=serve(actual, forecast), now=lambda: now, limiter=NOLIMIT)
    return run_pipeline(Pipeline("load_entsoe_daily", "Europe/Brussels", (task,)), day, lake)


def test_day_publishes_actual_and_forecast_and_keeps_vintages(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    a, f = "DE_LU_2026-11-18_normal_load_actual.xml", "DE_LU_2026-11-18_normal_load_forecast_da.xml"
    t1, t2 = datetime(2026, 11, 26, 15, tzinfo=UTC), datetime(2026, 11, 27, 15, tzinfo=UTC)
    r = run(lake, "DE_LU", "2026-11-18", a, f, t1)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    assert run(lake, "DE_LU", "2026-11-18", a, f, t2).status == "SUCCEEDED"
    ptr = read_pointer(lake, "silver.demand__entsoe_de_lu", "2026-11-18")
    df = pl.read_parquet(Path(lake) / ptr["uri"])
    assert df.filter(pl.col("kind") == "actual").height == 96                       # one actual, latest fetch
    fc = df.filter(pl.col("kind") == "forecast")
    assert fc.height == 192 and sorted(fc["fetched_at_utc"].unique().to_list()) == [t1, t2]  # both vintages kept
    for f_ in Path(lake).rglob("*"):
        if f_.is_file():
            assert TOKEN.encode() not in f_.read_bytes(), f_


def test_spring_forward_day_has_92_quarter_hours(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    r = run(lake, "ES", "2026-03-29", "ES_2026-03-29_spring_forward_load_actual.xml",
            "ES_2026-03-29_spring_forward_load_forecast_da.xml", datetime(2026, 11, 26, 15, tzinfo=UTC))
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    df = pl.read_parquet(Path(lake) / read_pointer(lake, "silver.demand__entsoe_es", "2026-03-29")["uri"])
    assert df.filter(pl.col("kind") == "actual").height == 92 == df.filter(pl.col("kind") == "forecast").height


def test_not_published_is_retried_while_recent_then_blocks(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    a, nodata = "DE_LU_2026-11-18_normal_load_actual.xml", "DE_LU_future_no_data.xml"
    soon = datetime(2026, 11, 19, 6, tzinfo=UTC)   # within 48 h of the day's end
    t = run(lake, "DE_LU", "2026-11-18", a, nodata, soon).tasks[0]
    assert t.retryable and t.error_class == "source_not_ready"
    late = datetime(2026, 11, 26, tzinfo=UTC)
    r = run(lake, "DE_LU", "2026-11-18", a, nodata, late)
    assert r.status == "FAILED" and "load_published" in r.tasks[0].error
    assert read_pointer(lake, "silver.demand__entsoe_de_lu", "2026-11-18") is None


def test_gateway_timeout_is_retryable(lake, monkeypatch):
    """ENTSO-E's gateway answered HTTP 599 ("Unable to access service within time limit") on the
    owner's run; the page is kept in errors/. A 5xx is retried, never published."""
    import urllib.error

    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    page = (FX / "errors/ES_2026-03-29_load_forecast_da_gateway_599.json").read_bytes()
    assert b"Unable to access service within time limit" in page

    def opener(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 599, "gateway timeout", {}, None)
    task = make_load_task(M, "ES", opener=opener, now=lambda: datetime(2026, 11, 26, tzinfo=UTC), limiter=NOLIMIT)
    t = run_pipeline(Pipeline("load_entsoe_daily", "Europe/Brussels", (task,)), "2026-03-29", lake).tasks[0]
    assert t.retryable and t.error_class == "server_error"
    assert read_pointer(lake, "silver.demand__entsoe_es", "2026-03-29") is None
