"""EIA-930 region-data → silver.demand / silver.interchange (E2), on REAL saved EIA responses.

Failure cases change one thing in a real response (a removed hour, a negative value).
No network: a fake opener serves the saved body.
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
from atlas_pipelines.defs.eia import make_interchange_task, make_region_task
from atlas_pipelines.sources.eia import region
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["us_ba"]
FX = FIXTURES / "eia"
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
KEY = "test-eia-key-not-real-0123456789"
UTC = timezone.utc


def ok(name):
    meta = FX / name.replace(".json", ".meta.json")
    return meta.exists() and json.loads(meta.read_text())["http_status"] == 200


SAMPLES = sorted(p.name for p in FX.glob("data__region-data__hourly__*.json")
                 if not p.name.endswith(".meta.json") and ok(p.name)) if FX.exists() else []


def parts(name):
    _, _, _, ba, day = name[:-5].split("__")
    return ba, date.fromisoformat(day)


def rows_of(name):
    return json.loads((FX / name).read_text())["response"]["data"]


def test_hour_ending_mapping_and_request_window():
    assert region.interval_of("2026-11-18T06") == (datetime(2026, 11, 18, 5, tzinfo=UTC), datetime(2026, 11, 18, 6, tzinfo=UTC))
    p = dict(region.request_params("CISO", date(2026, 11, 18)))
    assert (p["start"], p["end"], p["facets[respondent][]"]) == ("2026-11-18T01", "2026-11-19T00", "CISO")


@pytest.mark.skipif(not SAMPLES, reason="no real region-data samples")
@pytest.mark.parametrize("name", SAMPLES)
def test_real_day_parses_to_24_hours_equal_to_the_source(name):
    ba, day = parts(name)
    rows = rows_of(name)
    fetched = datetime(2026, 11, 26, tzinfo=UTC)
    demand, inter, ng = region.parse(rows, M, ba, day, fetched, "bronze/x")
    actual = demand.filter(pl.col("kind") == "actual")
    assert actual.height == 24
    raw_ti = {r["period"] for r in rows if r["type"] == "TI"}
    in_day = {(datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=h + 1)).strftime("%Y-%m-%dT%H")
              for h in range(24)}
    assert inter.height == len(raw_ti & in_day)   # interchange gaps in the source stay gaps
    # Every silver value equals the raw value of the hour that ENDS one hour later.
    raw = {(r["type"], r["period"]): r["value"] for r in rows}
    for r in actual.iter_rows(named=True):
        period = (r["interval_start_utc"] + timedelta(hours=1)).strftime("%Y-%m-%dT%H")
        assert r["demand_mw"] == float(raw[("D", period)])
    for r in inter.iter_rows(named=True):
        assert r["net_interchange_mw"] == float(raw[("TI", (r["interval_end_utc"]).strftime("%Y-%m-%dT%H"))])
    blocking = [c.check_name for c in region.demand_checks(demand, rows, day, None) if c.severity == "blocking" and not c.passed]
    assert blocking == [], blocking
    ic = {c.check_name: c for c in region.interchange_checks(demand, inter, ng, M, day)}
    assert ic["interchange_complete"].passed == (inter.height == 24)
    if inter.height < 24:
        assert ic["interchange_complete"].metrics["missing"] == 24 - inter.height
    fc = demand.filter(pl.col("kind") == "forecast")
    assert fc["issued_at_utc"].null_count() == fc.height and (fc["fetched_at_utc"] == fetched).all()


DST = [("CISO", "2025-11-02", 25), ("CISO", "2025-03-09", 23), ("CISO", "2026-03-08", 23),
       ("ERCO", "2025-11-02", 25), ("ERCO", "2025-03-09", 23), ("ERCO", "2026-03-08", 23),
       ("PJM", "2025-11-02", 25), ("PJM", "2025-03-09", 23), ("PJM", "2026-03-08", 23)]


@pytest.mark.parametrize("ba,day,hours", DST)
def test_local_dst_day_has_23_or_25_hours(ba, day, hours):
    name = f"data__region-data__hourly__{ba}__{day}.json"
    if not ok(name):
        pytest.skip(f"{name} not saved yet (EIA error in the first run)")
    demand, _, _ = region.parse(rows_of(name), M, ba, None, datetime(2026, 11, 26, tzinfo=UTC), "x")
    local = demand.filter((pl.col("kind") == "actual") & (pl.col("local_date") == date.fromisoformat(day)))
    assert local.height == hours and local["interval_start_utc"].n_unique() == hours


def balance(name):
    ba, day = parts(name)
    rows = rows_of(name)
    d, i, ng = region.parse(rows, M, ba, day, datetime(2026, 11, 26, tzinfo=UTC), "x")
    return next(c for c in region.checks(d, i, ng, rows, M, day, None) if c.check_name == "balance_identity")


def test_balance_identity_is_reported():
    erco = "data__region-data__hourly__ERCO__2025-11-02.json"
    ciso = "data__region-data__hourly__CISO__2026-11-18.json"
    if not (ok(erco) and ok(ciso)):
        pytest.skip("samples missing")
    assert balance(erco).passed and balance(erco).metrics["hours_compared"] == 24
    c = balance(ciso)  # CISO's self-reported numbers don't balance every hour: warned, never fixed
    assert c.severity == "warn" and c.metrics["hours_compared"] == 24 and c.metrics["share_outside"] is not None


CISO_DAY = "data__region-data__hourly__CISO__2026-11-18.json"
needs_ciso = pytest.mark.skipif(not ok(CISO_DAY), reason="CISO sample missing")


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def serve(data):
    return lambda req, timeout: Resp(data)


def pipe(data, now, ba="CISO"):
    ingest = make_region_task(M, ba, opener=serve(data), now=now, limiter=NOLIMIT)
    return Pipeline("eia930_region_daily", "UTC", (ingest, make_interchange_task(M, ba, ingest, now=now)))


@needs_ciso
def test_pipeline_ingests_and_keeps_forecast_vintages(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    data = (FX / CISO_DAY).read_bytes()
    t1, t2 = datetime(2026, 11, 26, 1, tzinfo=UTC), datetime(2026, 11, 27, 1, tzinfo=UTC)
    r = run_pipeline(pipe(data, lambda: t1), "2026-11-18", lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    r = run_pipeline(pipe(data, lambda: t2), "2026-11-18", lake)
    assert r.status == "SUCCEEDED"
    checks = {c["check_name"]: c for c in r.tasks[0].checks}
    assert checks["forecast_vintages_superset"]["passed"] and checks["source_revision"]["passed"]
    ptr = read_pointer(lake, "silver.demand__us_ba_ciso", "2026-11-18")
    df = pl.read_parquet(Path(lake) / ptr["uri"])
    fc = df.filter(pl.col("kind") == "forecast")
    assert sorted(fc["fetched_at_utc"].unique().to_list()) == [t1, t2] and fc.height == 48   # two vintages kept
    assert df.filter(pl.col("kind") == "actual").height == 24
    assert read_pointer(lake, "silver.interchange__us_ba_ciso", "2026-11-18")["row_count"] == 24
    inter_checks = {c["check_name"]: c for c in r.tasks[1].checks}
    assert inter_checks["balance_identity"]["metrics"]["hours_compared"] == 24
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert KEY.encode() not in f.read_bytes(), f


def drop_hour(data: bytes, period: str) -> bytes:
    doc = json.loads(data)
    keep = [r for r in doc["response"]["data"] if not (r["type"] == "D" and r["period"] == period)]
    doc["response"]["data"] = keep
    doc["response"]["total"] = str(len(keep))
    return json.dumps(doc).encode()


@needs_ciso
def test_missing_hour_is_retried_while_recent_and_blocks_after(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    data = drop_hour((FX / CISO_DAY).read_bytes(), "2026-11-18T12")
    soon = lambda: datetime(2026, 11, 19, 6, tzinfo=UTC)     # 6 h after the UTC day ended
    t = run_pipeline(pipe(data, soon), "2026-11-18", lake).tasks[0]
    assert (t.error_class, t.retryable) == ("source_not_ready", True)
    assert read_pointer(lake, "silver.demand__us_ba_ciso", "2026-11-18") is None
    late = lambda: datetime(2026, 11, 25, tzinfo=UTC)         # past the 48 h policy
    t = run_pipeline(pipe(data, late), "2026-11-18", lake).tasks[0]
    assert (t.error_class, t.retryable) == ("data_check_failed", False) and "demand_actual_complete" in t.error


@needs_ciso
def test_negative_demand_blocks(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    doc = json.loads((FX / CISO_DAY).read_bytes())
    r0 = next(r for r in doc["response"]["data"] if r["type"] == "D" and r["period"] == "2026-11-18T12")
    r0["value"] = "-5"
    t = run_pipeline(pipe(json.dumps(doc).encode(), lambda: datetime(2026, 11, 26, tzinfo=UTC)), "2026-11-18", lake).tasks[0]
    assert t.error_class == "data_check_failed" and "demand_non_negative" in t.error


def test_dropped_vintage_is_caught():
    fetched = [datetime(2026, 11, 26, tzinfo=UTC), datetime(2026, 11, 27, tzinfo=UTC)]
    if not ok(CISO_DAY):
        pytest.skip("sample missing")
    rows = rows_of(CISO_DAY)
    day = date(2026, 11, 18)
    first, i, ng = region.parse(rows, M, "CISO", day, fetched[0], "x")
    second, _, _ = region.parse(rows, M, "CISO", day, fetched[1], "x")
    # Replacing instead of carrying forward drops the first vintage: blocking.
    c = next(c for c in region.checks(second, i, ng, rows, M, day, first) if c.check_name == "forecast_vintages_superset")
    assert not c.passed and c.metrics["dropped"]
    kept = region.carry_vintages(second, first)
    c = next(c for c in region.checks(kept, i, ng, rows, M, day, first) if c.check_name == "forecast_vintages_superset")
    assert c.passed


PJM_GAPPY = "data__region-data__hourly__PJM__2025-11-02.json"


@pytest.mark.skipif(not ok(PJM_GAPPY), reason="PJM sample missing")
def test_interchange_gaps_never_hold_back_complete_demand(lake, monkeypatch):
    # Real EIA-930 day: demand complete, interchange reported for only part of the day.
    monkeypatch.setenv("EIA_API_KEY", KEY)
    late = lambda: datetime(2026, 11, 26, tzinfo=UTC)
    r = run_pipeline(pipe((FX / PJM_GAPPY).read_bytes(), late, "PJM"), "2025-11-02", lake)
    ingest, inter = r.tasks
    assert ingest.status == "SUCCEEDED" and read_pointer(lake, "silver.demand__us_ba_pjm", "2025-11-02") is not None
    assert (inter.status, inter.error_class) == ("FAILED_TERMINAL", "data_check_failed") and "interchange_complete" in inter.error
    assert read_pointer(lake, "silver.interchange__us_ba_pjm", "2025-11-02") is None   # missing stays missing


LOCAL = sorted(p.name for p in FX.glob("data__region-data__local-hourly__*.json")
               if not p.name.endswith(".meta.json") and ok(p.name)) if FX.exists() else []


@pytest.mark.skipif(not LOCAL, reason="no local-hourly samples saved")
@pytest.mark.parametrize("name", LOCAL)
def test_local_hourly_offsets_match_the_registry_zone(name):
    # EIA's local-hourly frequency labels each hour with the BA's own UTC offset
    # (taken at the hour's END). It must agree with the registry zone, and every value
    # must equal the UTC-hourly value of the same hour (both hour-ending).
    from zoneinfo import ZoneInfo
    ba, day = parts(name)
    tz = ZoneInfo(M.raw["eia"]["bas"][ba]["tz"])
    local = [r for r in rows_of(name) if r["type"] == "D"]
    utc_name = name.replace("local-hourly", "hourly")
    utc = {r["period"]: r["value"] for r in rows_of(utc_name) if r["type"] == "D"} if ok(utc_name) else {}
    for r in local:
        label, offset = datetime.strptime(r["period"][:13], "%Y-%m-%dT%H"), int(r["period"][13:])
        end_utc = (label - timedelta(hours=offset)).replace(tzinfo=UTC)
        assert int(end_utc.astimezone(tz).utcoffset().total_seconds() // 3600) == offset, r["period"]
        if utc:
            assert utc[end_utc.strftime("%Y-%m-%dT%H")] == r["value"], r["period"]
    assert M.raw["eia"]["bas"][ba]["tz_status"] == "verified"
