"""B6 Layer 4 reconciliation mechanics and the daily data-quality report. No network.

Reconciliation: the comparison is tested against a reference DERIVED from real silver
(the mechanism, not a market figure); real references come only from the owner's config.
Report: built from manifests of real local runs (one succeeding, one blocked)."""

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.eia import make_gas_task
from atlas_pipelines.sources.eia import gas, region
from atlas_pipelines.validation import reconcile as rec
from atlas_pipelines.validation import report
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["us_ba"]
FX = FIXTURES / "eia"
DAY = "data__region-data__hourly__CISO__2026-11-18.json"
pytestmark = pytest.mark.skipif(not (FX / DAY).exists(), reason="CISO sample missing")


def actual():
    rows = json.loads((FX / DAY).read_text())["response"]["data"]
    d, _, _ = region.parse(rows, M, "CISO", date(2026, 11, 18), datetime(2026, 11, 26, tzinfo=timezone.utc), "x")
    return d


def test_metrics_weigh_intervals_by_length():
    d = actual().filter(pl.col("kind") == "actual")
    assert rec.metric_value(d, "demand_mw", "energy_sum_mwh") == pytest.approx(d["demand_mw"].sum())  # 60-min hours
    assert rec.metric_value(d, "demand_mw", "time_weighted_mean") == pytest.approx(d["demand_mw"].mean())


def ref(value, tol=0.5):
    return {"asset": "silver.demand__us_ba_ciso", "column": "demand_mw", "kind": "actual", "metric": "energy_sum_mwh",
            "month": "2026-11", "value": value, "unit": "MWh", "tolerance_pct": tol,
            "source": {"title": "test: derived from the real silver", "url": "n/a", "retrieved_on": "2026-11-26"}}


def month_of_frames():
    d = actual()
    return {day: d for day in rec.month_days("2026-11")}  # mechanism: the same real day for every date


def test_within_tolerance_passes_outside_fails():
    frames = month_of_frames()
    ours = rec.metric_value(pl.concat(list(frames.values())).filter(pl.col("kind") == "actual"), "demand_mw", "energy_sum_mwh")
    assert rec.reconcile(ref(ours), frames).passed
    c = rec.reconcile(ref(ours * 1.02), frames)  # 2% off, tolerance 0.5%
    assert not c.passed and c.metrics["diff_pct"] == pytest.approx(-1.961, abs=1e-3)


def test_incomplete_month_is_reported_not_compared():
    frames = month_of_frames()
    del frames[date(2026, 11, 7)]
    c = rec.reconcile(ref(1.0), frames)
    assert not c.passed and c.metrics["month_complete"] is False and c.metrics["missing_days"] == ["2026-11-07"]
    assert "ours" not in c.metrics


def test_references_need_a_cited_source(tmp_path):
    f = tmp_path / "x.yaml"
    bad = {k: v for k, v in ref(1.0).items()}
    bad["source"] = {"title": "no url or date"}
    f.write_text(json.dumps({"version": 1, "references": [bad]}), encoding="utf-8")
    with pytest.raises(ValueError, match="source"):
        rec.load_references("x", f)
    assert rec.load_references("no_such_market") == []


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_daily_report_counts_real_runs(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "test-eia-key-not-real-0123456789")
    name = FX / "data__natural-gas_pri_fut__RNGWHHD__daily.json"
    spec = gas.load_series()["RNGWHHD"]
    good = name.read_bytes()
    doc = json.loads(good)
    doc["response"]["data"][0]["units"] = "$/Mcf"  # one change: a unit drift → blocking
    bad = json.dumps(doc).encode()
    for body, t in ((good, datetime(2026, 11, 26, 1, tzinfo=timezone.utc)), (bad, datetime(2026, 11, 26, 2, tzinfo=timezone.utc))):
        task = make_gas_task("RNGWHHD", spec, opener=lambda req, timeout, b=body: Resp(b), now=lambda t=t: t,
                             limiter=RateLimiter(rpm=60_000, sleep=lambda s: None))
        run_pipeline(Pipeline("eia_henry_hub_monthly", "UTC", (task,)), "2026-10-01", lake)
    today = datetime.now(timezone.utc).date().isoformat()  # manifests carry the real finish time
    s = report.collect(lake, today)
    assert s["runs"] == 2 and s["status"]["eia_henry_hub_monthly"] == {"SUCCEEDED": 1, "FAILED": 1}
    assert s["blocking"][("eia_henry_hub_monthly", "units_match")] == 1
    md = report.markdown(s)
    assert "units_match" in md and "eia_henry_hub_monthly" in md
    assert report.collect(lake, "1999-01-01")["runs"] == 0
