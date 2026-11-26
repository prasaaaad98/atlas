"""Per-series completeness and point in time, end to end, on REAL documents (ADR-B10, §3.4a).

Owner's scenarios (2026-11-26/27):
- one series has a gap; the other series publish; gold net load blocks exactly those intervals;
  a later re-fetch completes everything;
- the forecast side of gold uses only vintages known by the start of the local day; a vintage
  fetched later is stored but never used (no_lookahead, blocking).

Timeline (delivery day 2026-11-18 local = 2026-11-17T23:00Z .. 2026-11-18T23:00Z):
  T0  2026-11-17 19:00Z  scheduled run the evening before: forecast only
  T0b 2026-11-17 21:00Z  retry before the day
  TA  2026-11-20 19:00Z  +1 re-fetch after the day: actuals + a later forecast vintage

The gap is made from the real DE_LU 2026-11-18 day-ahead wind/solar document with one change:
the offshore-wind (B18) series' Period ends 4 h early (its last 16 points withheld, as if
published late). No value is invented: re-fetches serve the real, complete document.
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
T0, T0B = datetime(2026, 11, 17, 19, tzinfo=UTC), datetime(2026, 11, 17, 21, tzinfo=UTC)
TA = datetime(2026, 11, 20, 19, tzinfo=UTC)
DECISION = datetime(2026, 11, 17, 23, tzinfo=UTC)  # start of the local delivery day
CUT = datetime(2026, 11, 18, 19, tzinfo=UTC)  # B18 Period cut to end here: 19:00Z-23:00Z withheld
GAP = [CUT + timedelta(minutes=15 * i) for i in range(16)]


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


REAL = (FX / f"{STEM}_wind_solar_forecast_da.xml").read_bytes() if (FX / f"{STEM}_wind_solar_forecast_da.xml").exists() else b""


class Resp:
    def __init__(self, data):
        self.status, self._d, self.headers = 200, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def ingest(lake, now, forecast: bytes, seen: list | None = None):
    def load_opener(req, timeout):
        (seen if seen is not None else []).append(req.full_url)
        return Resp((FX / f"{STEM}_{'load_actual' if 'processType=A16' in req.full_url else 'load_forecast_da'}.xml").read_bytes())

    def gen_opener(req, timeout):
        (seen if seen is not None else []).append(req.full_url)
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


def silver(lake, name):
    return pl.read_parquet(Path(lake) / read_pointer(lake, f"silver.{name}__entsoe_de_lu", DAY)["uri"])


def forecast_blocked(nl):
    f = nl.filter(pl.col("kind") == "forecast")
    return f.filter(pl.col("net_load_mw").is_null())


def test_net_load_blocks_only_the_gap_intervals_and_a_refetch_completes_everything(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    # 1. Evening before: B18 forecast late. Every other series publishes; B18 is flagged, not filled.
    ingest(lake, T0, late_offshore(REAL))
    fc = silver(lake, "generation").filter(pl.col("kind") == "forecast")
    b18 = fc.filter(pl.col("source_type") == "B18")
    assert b18.height == 80 and not b18["series_complete"].any()
    assert fc.filter(pl.col("source_type") != "B18")["series_complete"].all()

    # 2. Gold: forecast net load null for exactly the 16 withheld quarter-hours, naming B18.
    nl, mix, checks = gold(lake, T0)
    blocked = forecast_blocked(nl)
    assert blocked["interval_start_utc"].to_list() == GAP and set(blocked["missing_inputs"]) == {"B18"}
    ok = nl.filter((pl.col("kind") == "forecast") & pl.col("net_load_mw").is_not_null())
    assert ok.height == 80 and (ok["net_load_mw"] - (ok["demand_mw"] - ok["solar_mw"] - ok["wind_mw"])).abs().max() < 1e-9
    assert set(nl["decision_at_utc"]) == {DECISION} and checks["no_lookahead"]["passed"]
    fmix = mix.filter((pl.col("kind") == "forecast") & (pl.col("direction") == "generation"))
    assert fmix.filter(pl.col("generation_mw").is_null())["interval_start_utc"].to_list() == GAP  # renewable group only
    assert [(x["kind"], x["series"]) for x in report.open_series_gaps(lake)] == [("forecast", "B18")]

    # 3. Retry before the day: the real, complete document arrives. 4. After the day: actuals.
    ingest(lake, T0B, REAL)
    ingest(lake, TA, REAL)
    nl, mix, checks = gold(lake, TA)
    assert nl.height == 192 and nl["net_load_mw"].null_count() == 0 and nl["missing_inputs"].null_count() == 192
    f = nl.filter(pl.col("kind") == "forecast")
    assert set(f["generation_fetched_at_utc"]) == {T0B} and set(f["demand_fetched_at_utc"]) == {T0B}  # never TA
    assert checks["no_lookahead"]["passed"] and checks["net_load_intervals_complete"]["passed"]
    a = mix.filter((pl.col("kind") == "actual") & (pl.col("fuel_group") == "fossil"))
    assert a.height == 96 and a["generation_mw"].null_count() == 0
    assert report.open_series_gaps(lake) == []


def test_a_forecast_completed_only_after_the_day_is_never_used(lake, monkeypatch):
    """Point in time: the complete B18 arrives only with the +1 re-fetch (after delivery). Silver keeps it as
    a vintage, but the forecast view stays blocked for the 16 intervals; the actual side is complete."""
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    ingest(lake, T0, late_offshore(REAL))
    ingest(lake, TA, REAL)
    b18 = silver(lake, "generation").filter((pl.col("kind") == "forecast") & (pl.col("source_type") == "B18"))
    assert sorted(b18["fetched_at_utc"].unique().to_list()) == [T0, TA]  # both vintages stored
    nl, _, checks = gold(lake, TA)
    assert forecast_blocked(nl)["interval_start_utc"].to_list() == GAP
    assert nl.filter(pl.col("kind") == "actual")["net_load_mw"].null_count() == 0
    assert checks["no_lookahead"]["passed"]


def test_no_forecast_known_before_the_day_leaves_the_forecast_view_empty(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    ingest(lake, TA, REAL)  # first fetch only after the day
    nl, _, _ = gold(lake, TA)
    f = nl.filter(pl.col("kind") == "forecast")
    assert f.height == 96 and f["net_load_mw"].null_count() == 96
    tag = " (not known by 2026-11-17T23:00:00Z)"
    assert set(f["missing_inputs"]) == {f"demand{tag},B16{tag},B18{tag},B19{tag}"}
    assert nl.filter(pl.col("kind") == "actual")["net_load_mw"].null_count() == 0


def test_gold_uses_only_the_latest_known_vintage_never_a_mix(lake, monkeypatch):
    """Two vintages before the day, the later one shorter: gold follows the latest known vintage
    (blocks the 16 intervals) instead of stitching the older vintage's values into the gap."""
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    ingest(lake, T0, REAL)
    ingest(lake, T0B, late_offshore(REAL))
    nl, _, _ = gold(lake, T0B)
    assert forecast_blocked(nl)["interval_start_utc"].to_list() == GAP
    assert [(x["kind"], x["series"]) for x in report.open_series_gaps(lake)] == [("forecast", "B18")]


def test_run_before_the_day_fetches_only_forecasts_and_keeps_stored_actuals(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    seen = []
    ingest(lake, T0, REAL, seen)
    assert seen and not any("processType=A16" in u or "documentType=A75" in u for u in seen)  # no actual requested
    assert set(silver(lake, "demand")["kind"]) == {"forecast"} and set(silver(lake, "generation")["kind"]) == {"forecast"}
    ingest(lake, TA, REAL)
    ingest(lake, T0B, REAL)  # a forecast-only run after actuals exist (e.g. a backfill): actuals are kept
    for name in ("demand", "generation"):
        df = silver(lake, name)
        assert df.filter(pl.col("kind") == "actual").height > 0, name
        assert sorted(df.filter(pl.col("kind") == "forecast")["fetched_at_utc"].unique().to_list()) == [T0, T0B, TA], name


def test_same_silver_gives_byte_identical_gold(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    ingest(lake, T0, REAL)
    ingest(lake, TA, REAL)
    gold(lake, TA)
    first = read_pointer(lake, "gold.net_load__entsoe_de_lu", DAY)["content_hash"]
    gold(lake, TA)
    assert read_pointer(lake, "gold.net_load__entsoe_de_lu", DAY)["content_hash"] == first


def test_missing_silver_waits_then_blocks(lake):
    task = make_net_load_task(M, "DE_LU", now=lambda: T0)
    t = run_pipeline(Pipeline("g", "Europe/Brussels", (task,)), DAY, lake).tasks[0]
    assert t.retryable and t.error_class == "source_not_ready"
    task = make_net_load_task(M, "DE_LU", now=lambda: datetime(2026, 12, 1, tzinfo=UTC))
    r = run_pipeline(Pipeline("g", "Europe/Brussels", (task,)), DAY, lake)
    assert r.status == "FAILED" and "inputs_current" in r.tasks[0].error
