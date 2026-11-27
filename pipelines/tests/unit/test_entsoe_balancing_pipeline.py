"""`balancing_entsoe_daily` end to end on REAL ENTSO-E answers (local-only fixtures; skips in CI).

The fake opener serves the saved answer for the requested documentType and area. DE_LU asks for
reserves in its 4 TSO areas and never for imbalance (deferred, registry); ES publishes reserves and
imbalance; an area that answers "no matching data" is retried while recent. No network.
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
from atlas_pipelines.defs.entsoe_balancing import make_balancing_task
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
TOKEN = "test-entsoe-token-not-real-0123"
UTC = timezone.utc
TSO = {"10YDE-VE-------2": "50hertz", "10YDE-RWENET---I": "amprion", "10YDE-EON------1": "tennet", "10YDE-ENBW-----N": "transnetbw"}
NEED = ["ES_2026-11-18_normal_reserves_daily.zip", "ES_2026-11-18_normal_imbalance_prices.zip"] + \
       [f"DE_LU_2026-11-18_probe_{t}_reserves_daily.zip" for t in TSO.values()]
pytestmark = pytest.mark.skipif(not all((FX / n).exists() for n in NEED), reason="ENTSO-E balancing fixtures are local-only")


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def run(lake, zone, now, seen, serve=None):
    def opener(req, timeout):
        url = req.full_url
        seen.append(url)
        if serve:
            return Resp(serve(url))
        item = "reserves_daily" if "documentType=A81" in url else "imbalance_prices"
        if zone == "DE_LU":
            tso = next(t for eic, t in TSO.items() if eic in url)
            return Resp((FX / f"DE_LU_2026-11-18_probe_{tso}_{item}.zip").read_bytes())
        return Resp((FX / f"{zone}_2026-11-18_normal_{item}.zip").read_bytes())
    task = make_balancing_task(M, zone, opener=opener, now=lambda: now, limiter=RateLimiter(rpm=60_000, sleep=lambda s: None))
    return run_pipeline(Pipeline("balancing_entsoe_daily", "Europe/Brussels", (task,)), "2026-11-18", lake)


def silver(lake, zone):
    return pl.read_parquet(Path(lake) / read_pointer(lake, f"silver.ancillary_prices__entsoe_{zone.lower()}", "2026-11-18")["uri"])


def test_es_publishes_reserves_and_imbalance(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    seen = []
    r = run(lake, "ES", datetime(2026, 11, 27, tzinfo=UTC), seen)
    assert r.status == "SUCCEEDED", r.tasks[0].error
    df = silver(lake, "ES")
    assert set(df["product"]) == {"afrr_capacity_up", "afrr_capacity_down", "imbalance_excess", "imbalance_insufficient"}
    assert df["series_complete"].all() and len(seen) == 2
    checks = {c["check_name"]: c for c in r.tasks[0].checks}
    assert not checks["unit_confirmed"]["passed"] and checks["unit_confirmed"]["severity"] == "warn"  # reserves unit flagged
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert TOKEN.encode() not in f.read_bytes(), f


def test_de_lu_asks_each_tso_area_for_reserves_and_never_for_imbalance(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    seen = []
    r = run(lake, "DE_LU", datetime(2026, 11, 27, tzinfo=UTC), seen)
    assert r.status == "SUCCEEDED", r.tasks[0].error
    assert len(seen) == 4 and all("documentType=A81" in u for u in seen)  # imbalance deferred (registry)
    df = silver(lake, "DE_LU")
    assert set(df["area_eic"]) == set(TSO) and df.height == 4 * 480


def test_no_data_is_retried_while_recent_then_the_rest_publishes(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    nodata = (FX / "DE_LU_2026-11-18_normal_imbalance_prices.xml").read_bytes()

    def serve(url):  # reserves answer; imbalance not published (a real 'no matching data' answer)
        return (FX / "ES_2026-11-18_normal_reserves_daily.zip").read_bytes() if "documentType=A81" in url else nodata
    t = run(lake, "ES", datetime(2026, 11, 19, 6, tzinfo=UTC), [], serve).tasks[0]
    assert t.retryable and t.error_class == "source_not_ready"
    r = run(lake, "ES", datetime(2026, 11, 27, tzinfo=UTC), [], serve)
    assert r.status == "SUCCEEDED", r.tasks[0].error
    assert set(silver(lake, "ES")["product"]) == {"afrr_capacity_up", "afrr_capacity_down"}
    c = {c["check_name"]: c for c in r.tasks[0].checks}["balancing_published"]
    assert (c["severity"], c["passed"]) == ("warn", False)
