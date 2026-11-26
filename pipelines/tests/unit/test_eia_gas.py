"""EIA Henry Hub daily spot → silver.fuel_carbon_prices (E4), on the REAL saved response.
Failure cases change one thing in it. No network."""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.eia import make_gas_task
from atlas_pipelines.sources.eia import gas
from conftest import FIXTURES

FX = FIXTURES / "eia"
NAME = "data__natural-gas_pri_fut__RNGWHHD__daily.json"
SPEC = gas.load_series()["RNGWHHD"]
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
KEY = "test-eia-key-not-real-0123456789"
FETCHED = datetime(2026, 11, 26, tzinfo=timezone.utc)
pytestmark = pytest.mark.skipif(not (FX / NAME).exists(), reason="Henry Hub sample missing")


def rows():
    return json.loads((FX / NAME).read_text())["response"]["data"]


def blocking(cs):
    return sorted(c.check_name for c in cs if c.severity == "blocking" and not c.passed)


def run(rs, month="2026-10-01"):
    part = date.fromisoformat(month)
    df = gas.parse(rs, "RNGWHHD", SPEC, part, FETCHED, "x")
    return df, gas.checks(df, rs, SPEC, part, FETCHED.date())


def test_october_2026_every_weekday_priced_and_equal_to_the_source():
    df, cs = run(rows())
    assert df.height == 22 and blocking(cs) == [] and all(c.passed for c in cs)
    raw = {r["period"]: float(r["value"]) for r in rows()}
    for r in df.iter_rows(named=True):
        assert r["price"] == raw[str(r["trade_date"])] and (r["commodity"], r["hub"], r["unit"]) == ("natural_gas", "HENRY_HUB", "USD/MMBtu")


def test_current_month_is_partial_and_checked_only_to_yesterday():
    df, cs = run(rows(), "2026-11-01")
    assert df["trade_date"].max() == date(2026, 11, 24) and blocking(cs) == []
    gaps = next(c for c in cs if c.check_name == "weekdays_without_price")
    assert gaps.metrics["checked_through"] == "2026-11-25"  # the day before the fetch


def test_missing_weekday_is_reported_not_filled():
    rs = [r for r in rows() if r["period"] != "2026-10-14"]
    df, cs = run(rs)
    assert df.height == 21 and blocking(cs) == []
    assert next(c for c in cs if c.check_name == "weekdays_without_price").metrics["missing"] == ["2026-10-14"]


@pytest.mark.parametrize("mutate,check", [
    (lambda r: r.update(period="2026-10-03"), "no_weekend_rows"),                    # a Saturday
    (lambda r: r.update(units="$/Mcf"), "units_match"),
    (lambda r: r.update(**{"series-description": "Some other gas price"}), "series_is_expected"),
])
def test_unexpected_content_blocks(mutate, check):
    rs = [dict(r) for r in rows()]
    target = next(r for r in rs if r["period"] == "2026-10-15")
    mutate(target)
    assert check in blocking(run(rs)[1])


def test_vocabulary_is_enforced(tmp_path):
    from conftest import REPO
    text = (REPO / "config" / "commodities" / "eia.yaml").read_text(encoding="utf-8")
    bad = tmp_path / "eia.yaml"
    bad.write_text(text.replace("hub: HENRY_HUB", "hub: SOMEWHERE"), encoding="utf-8")
    with pytest.raises(ValueError, match="hub"):
        gas.load_series(bad)


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_pipeline_month_partition_and_no_key(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    data = (FX / NAME).read_bytes()
    p = Pipeline("eia_henry_hub_monthly", "UTC", (make_gas_task("RNGWHHD", SPEC, opener=lambda req, timeout: Resp(data),
                                                                now=lambda: FETCHED, limiter=NOLIMIT),))
    r = run_pipeline(p, "2026-10-01", lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    ptr = read_pointer(lake, "silver.fuel_carbon_prices__henry_hub_spot", "2026-10-01")
    assert ptr["row_count"] == 22 and pl.read_parquet(Path(lake) / ptr["uri"])["trade_date"].min() == date(2026, 10, 1)
    bad = run_pipeline(p, "2026-10-15", lake)
    assert bad.status == "FAILED" and "partition_is_a_month" in bad.tasks[0].error
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert KEY.encode() not in f.read_bytes(), f
