"""E7 gold fundamentals, built from REAL silver: the saved EIA responses go through the E2/E3
pipelines into a test lake, then gold is built from that silver only. No network.

Proves: 24 rows per BA and UTC day with values derived exactly from silver; identical
hashes on rebuild; incomplete inputs block; missing interchange stays null; tampered
silver is refused; storage buildout totals equal silver.
"""

import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.eia import (make_buildout_task, make_fuel_task, make_fundamentals_task, make_interchange_task,
                                      make_region_task, make_storage_task)
from atlas_pipelines.gold import fundamentals
from conftest import FIXTURES, REPO

FX = FIXTURES / "eia"
M = load_registry(REPO / "config" / "markets")["us_ba"]
LATE = datetime(2026, 11, 26, tzinfo=timezone.utc)  # after every saved day's publication window
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
KEY = "test-eia-key-not-real-0123456789"
DEFS = fundamentals.load_definitions()


def ok(name):
    meta = FX / name.replace(".json", ".meta.json")
    return (FX / name).exists() and meta.exists() and json.loads(meta.read_text())["http_status"] == 200


def pairs():
    out = []
    for f in sorted(FX.glob("data__region-data__hourly__*.json")):
        if f.name.endswith(".meta.json"):
            continue
        _, _, _, ba, day = f.stem.split("__")
        fuel = f"data__fuel-type-data__hourly__{ba}__{day}.json"
        if ok(f.name) and ok(fuel):
            out.append((ba, day))
    return out


PAIRS = pairs()
pytestmark = pytest.mark.skipif(not PAIRS, reason="EIA hourly samples missing")


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def build_silver(lake, ba, day, now=LATE):
    region = (FX / f"data__region-data__hourly__{ba}__{day}.json").read_bytes()
    fuel = (FX / f"data__fuel-type-data__hourly__{ba}__{day}.json").read_bytes()
    ingest = make_region_task(M, ba, opener=lambda req, timeout: Resp(region), now=lambda: now, limiter=NOLIMIT)
    run_pipeline(Pipeline("eia930_region_daily", "UTC", (ingest, make_interchange_task(M, ba, ingest, now=lambda: now))),
                 day, lake)
    run_pipeline(Pipeline("eia930_fuel_daily", "UTC", (make_fuel_task(M, ba, opener=lambda req, timeout: Resp(fuel),
                                                                      now=lambda: now, limiter=NOLIMIT),)), day, lake)


def gold_run(lake, ba, day, now=LATE):
    return run_pipeline(Pipeline("eia930_gold_daily", "UTC", (make_fundamentals_task(M, ba, now=lambda: now),)), day, lake)


def frame(lake, asset, day):
    ptr = read_pointer(lake, asset, day)
    return pl.read_parquet(Path(lake) / ptr["uri"]) if ptr else None


@pytest.mark.parametrize("ba,day", PAIRS)
def test_gold_hour_by_hour_equals_its_definition_on_silver(lake, monkeypatch, ba, day):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    build_silver(lake, ba, day)
    r = gold_run(lake, ba, day)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    slug = ba.lower()
    g = frame(lake, f"gold.fundamentals_hourly__us_ba_{slug}", day)
    demand = frame(lake, f"silver.demand__us_ba_{slug}", day).filter(pl.col("kind") == "actual")
    gen = frame(lake, f"silver.generation__us_ba_{slug}", day)
    assert g.height == 24 and g["interval_start_utc"].to_list() == sorted(demand["interval_start_utc"].to_list())
    for row in g.iter_rows(named=True):
        t = row["interval_start_utc"]
        hour = gen.filter(pl.col("interval_start_utc") == t)
        fuel = lambda f: hour.filter(pl.col("fuel_type") == f)["generation_mw"].sum()  # noqa: E731
        assert row["demand_mw"] == demand.filter(pl.col("interval_start_utc") == t)["demand_mw"][0]
        assert row["generation_mw"] == pytest.approx(hour["generation_mw"].sum())
        assert row["renewable_mw"] == pytest.approx(sum(fuel(f) for f in DEFS["renewable_fuel_types"]))
        if row["solar_mw"] is not None and row["wind_mw"] is not None:
            assert row["net_load_mw"] == pytest.approx(row["demand_mw"] - fuel("solar") - fuel("wind"))
    ptr = read_pointer(lake, f"silver.demand__us_ba_{slug}", day)
    assert set(g["demand_source_hash"]) == {ptr["content_hash"]}


def test_rebuild_gives_identical_hashes(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    ba, day = PAIRS[0]
    build_silver(lake, ba, day)
    asset = f"gold.fundamentals_hourly__us_ba_{ba.lower()}"
    gold_run(lake, ba, day)
    first = read_pointer(lake, asset, day)
    gold_run(lake, ba, day)
    second = read_pointer(lake, asset, day)
    assert first["uri"] != second["uri"] and first["content_hash"] == second["content_hash"]


def silver_frames(lake, ba, day):
    slug = ba.lower()
    return (frame(lake, f"silver.demand__us_ba_{slug}", day), frame(lake, f"silver.generation__us_ba_{slug}", day),
            frame(lake, f"silver.interchange__us_ba_{slug}", day))


HASHES = {"demand": "d", "generation": "g", "interchange": "i"}


def failed(cs, severity="blocking"):
    return sorted(c.check_name for c in cs if c.severity == severity and not c.passed)


def test_incomplete_inputs_produce_no_rows(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    ba, day = PAIRS[0]
    build_silver(lake, ba, day)
    demand, gen, inter = silver_frames(lake, ba, day)
    first_hour = demand["interval_start_utc"].min()
    df, cs = fundamentals.hourly(demand.filter(pl.col("interval_start_utc") != first_hour), gen, inter, DEFS, HASHES)
    assert df.is_empty() and failed(cs) == ["demand_day_complete"]
    some_fuel = gen["fuel_type"][0]
    gen_short = gen.filter(~((pl.col("fuel_type") == some_fuel) & (pl.col("interval_start_utc") == first_hour)))
    df, cs = fundamentals.hourly(demand, gen_short, inter, DEFS, HASHES)
    assert df.is_empty() and failed(cs) == ["generation_day_complete"]


def test_gaps_stay_null_and_are_reported(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    for ba, day in PAIRS:  # a day whose interchange was complete enough to be published
        build_silver(lake, ba, day)
        demand, gen, inter = silver_frames(lake, ba, day)
        if inter is not None and inter["net_interchange_mw"].null_count() == 0 and inter.height == 24:
            break
    else:
        pytest.skip("no saved day with complete interchange")
    df, cs = fundamentals.hourly(demand, gen, inter.head(20), DEFS, HASHES)  # 4 interchange hours gone
    assert df.height == 24 and df["net_interchange_mw"].null_count() == 4
    assert "interchange_hours_present" in failed(cs, "warn")
    df, cs = fundamentals.hourly(demand, gen.filter(pl.col("fuel_type") != "solar"), inter, DEFS, HASHES)
    assert df["solar_mw"].null_count() == 24 and df["net_load_mw"].null_count() == 24  # unknown, not zero
    assert "solar_and_wind_reported" in failed(cs, "warn") and failed(cs) == []


def test_missing_inputs_wait_while_recent_then_block(lake):
    ba, day = PAIRS[0]
    soon = datetime.fromisoformat(day + "T12:00:00+00:00")
    r = gold_run(lake, ba, day, now=soon)
    assert r.tasks[0].retryable and "waiting for" in r.tasks[0].error
    r = gold_run(lake, ba, day)
    assert r.status == "FAILED" and "inputs_current" in r.tasks[0].error


def test_tampered_silver_is_refused(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    ba, day = PAIRS[0]
    build_silver(lake, ba, day)
    ptr = read_pointer(lake, f"silver.generation__us_ba_{ba.lower()}", day)
    path = Path(lake) / ptr["uri"]
    path.write_bytes(path.read_bytes() + b"x")
    r = gold_run(lake, ba, day)
    assert r.status == "FAILED" and "input_hash_matches_pointer" in r.tasks[0].error


def test_definitions_are_validated(tmp_path):
    bad = tmp_path / "f.yaml"
    bad.write_text("version: 1\nrenewable_fuel_types: [wind, biomass]\nnet_load_subtracts: [solar, wind]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="biomass"):
        fundamentals.load_definitions(bad)


STORAGE = FX / "data__electricity_operating-generator-capacity__energy_source_code_MWH__monthly.json"


@pytest.mark.skipif(not STORAGE.exists(), reason="storage sample missing")
def test_storage_buildout_totals_equal_silver_and_rebuild_identically(lake, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", KEY)
    doc = json.loads(STORAGE.read_text(encoding="utf-8"))
    doc["response"]["total"] = "500"  # serve the 500 real rows as a whole month (as in test_eia_storage)
    data = json.dumps(doc).encode()
    def pipe(at):  # each run is a new fetch (bronze is immutable per fetch time)
        ingest = make_storage_task(M, opener=lambda req, timeout: Resp(data), now=lambda: at, limiter=NOLIMIT)
        return Pipeline("eia_storage_monthly", "UTC", (ingest, make_buildout_task(M, ingest)))
    assert run_pipeline(pipe(LATE), "2026-09-01", lake).status == "SUCCEEDED"
    first = read_pointer(lake, "gold.storage_buildout__us_ba", "2026-09-01")
    g = pl.read_parquet(Path(lake) / first["uri"])
    rows = doc["response"]["data"]
    ciso = sum(float(r["nameplate-capacity-mw"]) for r in rows if r["balancing_authority_code"] == "CISO" and r["status"] == "OP")
    assert g.filter(pl.col("zone") == "CISO")["operating_mw"][0] == pytest.approx(ciso)
    assert g.filter(pl.col("zone").is_null()).height == 1  # generators without a BA, kept as their own row
    assert g["operating_mw"].sum() == pytest.approx(sum(float(r["nameplate-capacity-mw"]) for r in rows if r["status"] == "OP"))
    # Rebuild gold alone from the SAME silver (a re-fetch would change silver, and gold records its hash).
    gold_only = dataclasses.replace(make_buildout_task(M, make_storage_task(M)), deps=())
    assert run_pipeline(Pipeline("eia_storage_monthly", "UTC", (gold_only,)), "2026-09-01", lake).status == "SUCCEEDED"
    second = read_pointer(lake, "gold.storage_buildout__us_ba", "2026-09-01")
    assert second["uri"] != first["uri"]
    assert read_pointer(lake, "gold.storage_buildout__us_ba", "2026-09-01")["content_hash"] == first["content_hash"]
