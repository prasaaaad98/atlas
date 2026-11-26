"""Per-series completeness end to end (ADR-B10): silver → gold net load → re-fetch, on REAL documents.

Owner's scenario (2026-11-26): one series has a gap; the other series publish; the gold net-load
asset blocks for exactly those intervals; a later re-fetch completes everything.

The gap is made from the real DE_LU 2026-11-18 day-ahead wind/solar document with one change:
the offshore-wind (B18) series' Period ends 4 h early (its last 16 points withheld, as if
published late). No value is invented: the re-fetch serves the real, complete document.
Load and actual generation are the real documents for the same day. Local-only fixtures; no network.
"""

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs.entsoe_generation import make_generation_task
from atlas_pipelines.defs.entsoe_gold import make_net_load_task
from atlas_pipelines.defs.entsoe_load import make_load_task
from atlas_pipelines.validation import report
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
TOKEN = "test-entsoe-token-not-real-0123"
UTC = timezone.utc
DAY = "2026-11-18"
STEM = f"DE_LU_{DAY}_normal"
NEED = [f"{STEM}_{x}.xml" for x in ("load_actual", "load_forecast_da", "generation_actual", "wind_solar_forecast_da")]
pytestmark = pytest.mark.skipif(not all((FX / n).exists() for n in NEED), reason="ENTSO-E fixtures are local-only")
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
T1, T2 = datetime(2026, 11, 19, 6, tzinfo=UTC), datetime(2026, 11, 21, 6, tzinfo=UTC)  # first fetch, +2 d re-fetch
CUT = datetime(2026, 11, 18, 19, tzinfo=UTC)  # B18 Period cut to end here: 19:00Z-23:00Z withheld


def late_offshore(raw: bytes) -> bytes:
    """The real forecast document with B18's last 16 points withheld (the one change)."""
    text = raw.decode()

    def cut(m):
        ts = m.group(0)
        if "<psrType>B18<" not in ts:
            return ts
        ts = ts.replace("<end>2026-11-18T23:00Z</end>", "<end>2026-11-18T19:00Z</end>")
        return re.sub(r"<Point>\s*<position>(\d+)</position>.*?</Point>",
                      lambda p: "" if int(p.group(1)) > 80 else p.group(0), ts, flags=re.S)
    out = re.sub(r"<TimeSeries>.*?</TimeSeries>", cut, text, flags=re.S)
    assert out != text and out.count("<end>2026-11-18T19:00Z</end>") == 1
    return out.encode()


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def ingest(lake, now, forecast: bytes):
    def load_opener(req, timeout):
        return Resp((FX / f"{STEM}_{'load_actual' if 'processType=A16' in req.full_url else 'load_forecast_da'}.xml").read_bytes())

    def gen_opener(req, timeout):
        return Resp((FX / f"{STEM}_generation_actual.xml").read_bytes() if "documentType=A75" in req.full_url else forecast)
    for task in (make_load_task(M, "DE_LU", opener=load_opener, now=lambda: now, limiter=NOLIMIT),
                 make_generation_task(M, "DE_LU", opener=gen_opener, now=lambda: now, limiter=NOLIMIT)):
        r = run_pipeline(Pipeline("p", "Europe/Brussels", (task,)), DAY, lake)
        assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]


def gold(lake, now):
    r = run_pipeline(Pipeline("entsoe_gold_daily", "Europe/Brussels", (make_net_load_task(M, "DE_LU", now=lambda: now),)), DAY, lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    frame = {k: pl.read_parquet(Path(lake) / read_pointer(lake, f"gold.{k}__entsoe_de_lu", DAY)["uri"])
             for k in ("net_load", "generation_mix")}
    return frame["net_load"], frame["generation_mix"], {c["check_name"]: c for c in r.tasks[0].checks}


def silver_generation(lake):
    return pl.read_parquet(Path(lake) / read_pointer(lake, "silver.generation__entsoe_de_lu", DAY)["uri"])


def test_net_load_blocks_only_the_gap_intervals_and_a_refetch_completes_everything(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    real = (FX / f"{STEM}_wind_solar_forecast_da.xml").read_bytes()
    gap = [CUT + timedelta(minutes=15 * i) for i in range(16)]

    # 1. First fetch: B18 forecast late. Every other series publishes; B18 is flagged, not filled.
    ingest(lake, T1, late_offshore(real))
    g = silver_generation(lake)
    fc = g.filter(pl.col("kind") == "forecast")
    assert fc.filter(pl.col("source_type") == "B18").height == 80 and not fc.filter(pl.col("source_type") == "B18")["series_complete"].any()
    assert fc.filter(pl.col("source_type") != "B18")["series_complete"].all()
    assert g.filter(pl.col("kind") == "actual")["series_complete"].all() and g.filter(pl.col("kind") == "actual").height > 0

    # 2. Gold: forecast net load is null for exactly the 16 withheld quarter-hours, naming B18.
    nl, mix, checks = gold(lake, T1)
    f = nl.filter(pl.col("kind") == "forecast")
    blocked = f.filter(pl.col("net_load_mw").is_null())
    assert blocked["interval_start_utc"].to_list() == gap and set(blocked["missing_inputs"]) == {"B18"}
    assert f.height == 96 and f.filter(pl.col("net_load_mw").is_not_null()).height == 80
    ok = f.filter(pl.col("net_load_mw").is_not_null())
    assert (ok["net_load_mw"] - (ok["demand_mw"] - ok["solar_mw"] - ok["wind_mw"])).abs().max() < 1e-9
    a = nl.filter(pl.col("kind") == "actual")
    assert a.height == 96 and a["net_load_mw"].null_count() == 0  # the actual side is not held back
    assert checks["net_load_intervals_complete"]["metrics"]["blocked_intervals"] == {"actual": 0, "forecast": 16}
    # Generation mix: the renewable group is null in the gap, fossil is not (it doesn't depend on B18).
    fmix = mix.filter((pl.col("kind") == "forecast") & (pl.col("direction") == "generation"))
    assert fmix.filter(pl.col("generation_mw").is_null())["interval_start_utc"].to_list() == gap
    amix = mix.filter((pl.col("kind") == "actual") & (pl.col("fuel_group") == "fossil"))
    assert amix.height == 96 and amix["generation_mw"].null_count() == 0
    # The quality record is open.
    assert [(x["kind"], x["series"]) for x in report.open_series_gaps(lake)] == [("forecast", "B18")]

    # 3. Scheduled re-fetch (+2 d): the real, complete document arrives as a new vintage.
    ingest(lake, T2, real)
    g = silver_generation(lake)
    b18 = g.filter((pl.col("kind") == "forecast") & (pl.col("source_type") == "B18"))
    assert sorted(b18["fetched_at_utc"].unique().to_list()) == [T1, T2]  # both vintages kept
    assert b18.filter(pl.col("fetched_at_utc") == T2)["series_complete"].all()

    # 4. Gold rebuilt: every interval complete; the quality record closed.
    nl, _, checks = gold(lake, T2)
    assert nl.height == 192 and nl["net_load_mw"].null_count() == 0 and nl["missing_inputs"].null_count() == 192
    assert set(nl.filter(pl.col("kind") == "forecast")["generation_fetched_at_utc"]) == {T2}
    assert checks["net_load_intervals_complete"]["passed"]
    assert report.open_series_gaps(lake) == []


def test_same_silver_gives_byte_identical_gold(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    ingest(lake, T1, (FX / f"{STEM}_wind_solar_forecast_da.xml").read_bytes())
    gold(lake, T1)
    first = read_pointer(lake, "gold.net_load__entsoe_de_lu", DAY)["content_hash"]
    gold(lake, T1)
    assert read_pointer(lake, "gold.net_load__entsoe_de_lu", DAY)["content_hash"] == first


def test_missing_silver_waits_then_blocks(lake):
    task = make_net_load_task(M, "DE_LU", now=lambda: T1)
    t = run_pipeline(Pipeline("g", "Europe/Brussels", (task,)), DAY, lake).tasks[0]
    assert t.retryable and t.error_class == "source_not_ready"
    task = make_net_load_task(M, "DE_LU", now=lambda: datetime(2026, 12, 1, tzinfo=UTC))
    r = run_pipeline(Pipeline("g", "Europe/Brussels", (task,)), DAY, lake)
    assert r.status == "FAILED" and "inputs_current" in r.tasks[0].error


def test_gold_uses_only_the_latest_vintage_never_a_mix(lake, monkeypatch):
    """A complete fetch, then a re-fetch where B18 is shorter: gold follows the current vintage
    (blocks the 16 intervals) instead of stitching the older vintage's values into the gap."""
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    real = (FX / f"{STEM}_wind_solar_forecast_da.xml").read_bytes()
    ingest(lake, T1, real)
    ingest(lake, T2, late_offshore(real))
    nl, _, _ = gold(lake, T2)
    f = nl.filter(pl.col("kind") == "forecast")
    assert f.filter(pl.col("net_load_mw").is_null())["interval_start_utc"].to_list() == \
        [CUT + timedelta(minutes=15 * i) for i in range(16)]
    assert [(x["kind"], x["series"]) for x in report.open_series_gaps(lake)] == [("forecast", "B18")]
