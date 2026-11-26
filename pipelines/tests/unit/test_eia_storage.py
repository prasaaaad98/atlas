"""EIA generator inventory (storage) → silver.storage_capacity (E5), on REAL saved responses.

The saved sample is the first 500 of EIA's 4,522 storage rows for 2026-09 (the E0 script
asked for length=500). Parsing and checks run on those 500 rows; the complete-month golden
test needs the full-month file (`make fixture-eia` step 5) and skips until it exists.
Failure cases change one thing in the real rows. No network.
"""

import copy
import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.eia import make_storage_task
from atlas_pipelines.sources.eia import storage
from conftest import FIXTURES, REPO

FX = FIXTURES / "eia"
SAMPLE = FX / "data__electricity_operating-generator-capacity__energy_source_code_MWH__monthly.json"
FULL = FX / "data__electricity_operating-generator-capacity__storage__2026-09__full.json"
CFG = storage.load_config()
SEP = date(2026, 9, 1)
FETCHED = datetime(2026, 11, 26, tzinfo=timezone.utc)
KEY = "test-eia-key-not-real-0123456789"
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
pytestmark = pytest.mark.skipif(not SAMPLE.exists(), reason="storage sample missing")


def rows(path=SAMPLE):
    return json.loads(path.read_text(encoding="utf-8"))["response"]["data"]


def run(rs):
    df, problems = storage.parse(rs, CFG, "us_ba", SEP, FETCHED, "x")
    return df, storage.checks(df, rs, CFG, problems, SEP)


def failed(cs, severity="blocking"):
    return sorted(c.check_name for c in cs if c.severity == severity and not c.passed)


def test_config_codes_match_eias_own_facets():
    status = json.loads((FX / "facet__electricity_operating-generator-capacity__status.json").read_text())
    assert {f["id"] for f in status["response"]["facets"]} == set(CFG["statuses"])
    movers = json.loads((FX / "facet__electricity_operating-generator-capacity__prime_mover_code.json").read_text())
    assert set(CFG["prime_movers"]) <= {f["id"] for f in movers["response"]["facets"]}


def test_real_rows_map_one_to_one_with_values_unchanged():
    rs = rows()
    df, cs = run(rs)
    assert df.height == len(rs) == 500 and failed(cs) == []
    raw = {(r["plantid"], r["generatorid"]): r for r in rs}
    for r in df.iter_rows(named=True):
        src = raw[(r["plant_id"], r["generator_id"])]
        assert r["capacity_mw"] == float(src["nameplate-capacity-mw"]) and r["zone"] == src["balancing_authority_code"]
        assert r["source_status"] == src["status"] and r["energy_mwh"] is None and r["expected_online_month"] is None
    assert set(df["storage_type"]) == {"battery", "flywheel"} and set(df["report_month"]) == {SEP}


def test_rows_without_a_balancing_authority_are_kept_and_warned():
    df, cs = run(rows())
    no_ba = sum(r["balancing_authority_code"] is None for r in rows())
    assert no_ba > 0 and df.filter(pl.col("zone").is_null()).height == no_ba
    assert "balancing_authority_present" in failed(cs, "warn")


def change(field, value, n=1):
    rs = copy.deepcopy(rows())
    for r in rs[:n]:
        r[field] = value
    return rs


@pytest.mark.parametrize("rs,check", [
    (lambda: change("status", "P"), "status_mapped"),                       # a planned code the route never listed
    (lambda: change("prime_mover_code", "PS"), "prime_mover_mapped"),       # pumped hydro is out of scope
    (lambda: change("period", "2026-08"), "report_month_matches"),
    (lambda: change("nameplate-capacity-mw-units", "kW"), "units_are_mw"),
    (lambda: change("nameplate-capacity-mw", "n/a"), "capacity_numeric"),
    (lambda: change("energy_source_code", "NG"), "energy_source_is_storage"),
    (lambda: change("net-summer-capacity-mw", "-5"), "capacity_non_negative"),
    (lambda: rows() + rows()[:1], "primary_key_unique"),
])
def test_unexpected_content_blocks(rs, check):
    assert check in failed(run(rs())[1])


def test_unknown_codes_are_never_silently_dropped():
    rs = change("status", "P", n=3)
    df, cs = run(rs)
    assert df.height == 497 and "status_mapped" in failed(cs)  # left out of the frame AND blocking


FULL_OK = FULL.exists() and json.loads(FULL.with_name(FULL.stem + ".meta.json").read_text())["http_status"] == 200


@pytest.mark.skipif(not FULL_OK, reason="complete 2026-09 month not saved yet (make fixture-eia, step 5; a 504 page may be there)")
def test_complete_month_golden():
    body = json.loads(FULL.read_text(encoding="utf-8"))["response"]
    rs = body["data"]
    assert len(rs) == int(body["total"])
    df, cs = run(rs)
    assert failed(cs) == [] and df.height == len(rs)


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def pipeline(opener):
    market = load_registry(REPO / "config" / "markets")["us_ba"]
    return Pipeline("eia_storage_monthly", "UTC", (make_storage_task(market, opener=opener, now=lambda: FETCHED,
                                                                     limiter=NOLIMIT),))


def test_partial_month_never_publishes(lake, monkeypatch):
    """The real response says total 4,522 but holds 500 rows; the next page comes back empty."""
    monkeypatch.setenv("EIA_API_KEY", KEY)
    first = SAMPLE.read_bytes()
    empty = json.dumps({"response": {"total": "4522", "data": []}}).encode()
    p = pipeline(lambda req, timeout: Resp(first if "offset=0" in req.full_url else empty))
    r = run_pipeline(p, "2026-09-01", lake)
    assert r.status == "FAILED" and "rows_match_total" in r.tasks[0].error
    assert read_pointer(lake, "silver.storage_capacity__us_ba", "2026-09-01") is None


def test_pipeline_publishes_month_and_stores_no_key(lake, monkeypatch):
    """Same real rows, served as if they were the whole month (total set to 500: the one change)."""
    monkeypatch.setenv("EIA_API_KEY", KEY)
    doc = json.loads(SAMPLE.read_text(encoding="utf-8"))
    doc["response"]["total"] = "500"
    data = json.dumps(doc).encode()
    p = pipeline(lambda req, timeout: Resp(data))
    r = run_pipeline(p, "2026-09-01", lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    ptr = read_pointer(lake, "silver.storage_capacity__us_ba", "2026-09-01")
    assert ptr["row_count"] == 500
    assert pl.read_parquet(Path(lake) / ptr["uri"])["capacity_mw"].sum() == sum(
        float(x["nameplate-capacity-mw"]) for x in doc["response"]["data"])
    bad = run_pipeline(p, "2026-09-15", lake)
    assert bad.status == "FAILED" and "partition_is_a_month" in bad.tasks[0].error
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert KEY.encode() not in f.read_bytes(), f
