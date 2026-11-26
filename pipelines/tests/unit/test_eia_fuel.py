"""EIA-930 fuel-type-data → silver.generation (E3), on REAL saved EIA responses.
Failure cases change one thing in a real response. No network."""

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
from atlas_pipelines.defs.eia import make_fuel_task
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.eia import fuel
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["us_ba"]
MAP, _ = fuels.load_mapping("eia930")
FX = FIXTURES / "eia"
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
KEY = "test-eia-key-not-real-0123456789"
UTC = timezone.utc
FETCHED = datetime(2026, 11, 26, tzinfo=UTC)


def ok(name):
    meta = FX / name.replace(".json", ".meta.json")
    return meta.exists() and json.loads(meta.read_text())["http_status"] == 200


SAMPLES = sorted(p.name for p in FX.glob("data__fuel-type-data__hourly__*.json")
                 if not p.name.endswith(".meta.json") and ok(p.name)) if FX.exists() else []


def parts(name):
    _, _, _, ba, day = name[:-5].split("__")
    return ba, date.fromisoformat(day)


def rows_of(name):
    return json.loads((FX / name).read_text())["response"]["data"]


def blocking(results):
    return sorted(c.check_name for c in results if c.severity == "blocking" and not c.passed)


@pytest.mark.skipif(not SAMPLES, reason="no real fuel-type samples")
@pytest.mark.parametrize("name", SAMPLES)
def test_real_day_maps_every_fuel_and_hour(name):
    ba, day = parts(name)
    rows = rows_of(name)
    df, unknown = fuel.parse(rows, M, ba, day, FETCHED, "x", MAP)
    assert unknown == set() and blocking(fuel.checks(df, rows, unknown, day, MAP)) == []
    codes = sorted(set(df["source_fuel_code"].to_list()))
    assert df.height == 24 * len(codes)
    raw = {(r["fueltype"], r["period"]): r["value"] for r in rows}
    for r in df.iter_rows(named=True):
        assert MAP[r["source_fuel_code"]].fuel_type == r["fuel_type"]
        assert r["generation_mw"] == float(raw[(r["source_fuel_code"], r["interval_end_utc"].strftime("%Y-%m-%dT%H"))])


def test_every_code_in_the_saved_data_is_mapped():
    seen = {r["fueltype"] for n in SAMPLES for r in rows_of(n)}
    assert seen and seen <= set(MAP), sorted(seen - set(MAP))


def test_full_eia_code_list_is_mapped_or_explained():
    facet = FX / "facet__electricity_rto_fuel-type-data__fueltype.json"
    if not ok(facet.name):
        pytest.skip("EIA's fueltype facet not saved yet (next make fixture-eia)")
    codes = {v["id"] for v in json.loads(facet.read_text())["response"]["facets"]}
    _, unmapped = fuels.load_mapping("eia930")
    assert codes <= set(MAP) | set(unmapped), sorted(codes - set(MAP) - set(unmapped))


CISO = "data__fuel-type-data__hourly__CISO__2026-11-18.json"
needs_ciso = pytest.mark.skipif(not ok(CISO), reason="CISO fuel sample missing")


@needs_ciso
def test_new_fuel_code_blocks():
    rows = [dict(r) for r in rows_of(CISO)]
    for r in rows:
        if r["fueltype"] == "GEO":
            r["fueltype"] = "XYZ"       # a code EIA might add
    df, unknown = fuel.parse(rows, M, "CISO", date(2026, 11, 18), FETCHED, "x", MAP)
    cs = fuel.checks(df, rows, unknown, date(2026, 11, 18), MAP)
    assert blocking(cs) == ["fuel_codes_mapped"] and "XYZ" in cs[1].metrics["unknown_codes"]


@needs_ciso
def test_negative_net_generation_is_counted_not_fixed():
    rows = rows_of(CISO)
    df, unknown = fuel.parse(rows, M, "CISO", date(2026, 11, 18), FETCHED, "x", MAP)
    c = next(c for c in fuel.checks(df, rows, unknown, date(2026, 11, 18), MAP) if c.check_name == "negative_net_generation")
    raw_neg = sum(1 for r in rows if r["fueltype"] == "SUN" and float(r["value"]) < 0
                  and datetime(2026, 11, 18, 1) <= datetime.strptime(r["period"], "%Y-%m-%dT%H") <= datetime(2026, 11, 19))
    assert c.severity == "warn" and not c.passed and c.metrics["hours_by_code"]["SUN"] == raw_neg
    assert df.filter((pl.col("fuel_type") == "solar") & (pl.col("generation_mw") < 0)).height == raw_neg  # kept as is


def test_mapping_config_is_validated(tmp_path):
    text = (REPO / "config" / "fuel_types.yaml").read_text(encoding="utf-8")
    bad = tmp_path / "f.yaml"
    bad.write_text(text.replace("SUN: {fuel_type: solar", "SUN: {fuel_type: sunshine"), encoding="utf-8")
    with pytest.raises(fuels.FuelConfigError, match="unknown fuel type"):
        fuels.load_mapping("eia930", bad)


@pytest.mark.parametrize("ba,day,hours", [("CISO", "2025-11-02", 25), ("ERCO", "2026-03-08", 23), ("PJM", "2025-03-09", 23)])
def test_local_dst_day_per_fuel(ba, day, hours):
    name = f"data__fuel-type-data__hourly__{ba}__{day}.json"
    if not ok(name):
        pytest.skip(f"{name} missing")
    df, _ = fuel.parse(rows_of(name), M, ba, None, FETCHED, "x", MAP)
    local = df.filter((pl.col("local_date") == date.fromisoformat(day)) & (pl.col("fuel_type") == "gas"))
    assert local.height == hours


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def pipe(data, now):
    return Pipeline("eia930_fuel_daily", "UTC",
                    (make_fuel_task(M, "CISO", opener=lambda req, timeout: Resp(data), now=now, limiter=NOLIMIT),))


@needs_ciso
def test_pipeline_publishes_generation_and_stores_no_key(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    r = run_pipeline(pipe((FX / CISO).read_bytes(), lambda: FETCHED), "2026-11-18", lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    ptr = read_pointer(lake, "silver.generation__us_ba_ciso", "2026-11-18")
    df = pl.read_parquet(Path(lake) / ptr["uri"])
    assert df.height == ptr["row_count"] and set(df["fuel_type"]) >= {"solar", "wind", "gas", "nuclear", "hydro"}
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert KEY.encode() not in f.read_bytes(), f


@needs_ciso
def test_missing_fuel_hour_retried_then_blocks(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    doc = json.loads((FX / CISO).read_bytes())
    doc["response"]["data"] = [r for r in doc["response"]["data"] if not (r["fueltype"] == "WND" and r["period"] == "2026-11-18T12")]
    doc["response"]["total"] = str(len(doc["response"]["data"]))
    data = json.dumps(doc).encode()
    t = run_pipeline(pipe(data, lambda: datetime(2026, 11, 19, 6, tzinfo=UTC)), "2026-11-18", lake).tasks[0]
    assert (t.error_class, t.retryable) == ("source_not_ready", True)
    t = run_pipeline(pipe(data, lambda: datetime(2026, 11, 25, tzinfo=UTC)), "2026-11-18", lake).tasks[0]
    assert t.error_class == "data_check_failed" and "each_reported_fuel_complete" in t.error
