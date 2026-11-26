"""Golden tests: real ENTSO-E load documents (A65 actual A16 / day-ahead A01), saved unchanged
by the owner's `make fixture-entsoe-fundamentals` (2026-11-26). Local-only fixtures (gitignored):
the tests skip without them (CI).

For each document: every silver value equals the raw point value of its interval (A03 blocks
carried), rows = intervals of the local delivery day (23/24/25 h at the document's resolution),
all checks pass. Failure cases change one thing in a real document.
"""

import json
import re
from datetime import date, datetime, timedelta, timezone

import pytest

from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources.entsoe import load
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
FETCHED = datetime(2026, 11, 26, 15, tzinfo=timezone.utc)
CASES = sorted(p for p in FX.glob("*_load_*.xml")) if FX.exists() else []
CASES = [p for p in CASES if json.loads(p.with_suffix(".meta.json").read_text())["http_status"] == 200]
pytestmark = pytest.mark.skipif(not CASES, reason="no ENTSO-E load fixtures (make fixture-entsoe-fundamentals)")


def meta(path):
    m = json.loads(path.with_suffix(".meta.json").read_text())
    return m["zone"], date.fromisoformat(m["date"]), "actual" if m["data"] == "load_actual" else "forecast"


def run(raw, zone, day, kind):
    doc = load.parse_document(raw)
    df = load.build_silver(doc, kind, M, zone, day, FETCHED, "fixture", None)
    return df, load.checks(df, doc, kind, M, zone, day)


def failed(cs):
    return sorted(c.check_name for c in cs if c.severity == "blocking" and not c.passed)


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_document_parses_completely_and_exactly(path):
    zone, day, kind = meta(path)
    raw = path.read_bytes()
    df, cs = run(raw, zone, day, kind)
    assert failed(cs) == [], [c for c in cs if not c.passed]
    start, end = M.day_bounds_utc(day)
    minutes = df["interval_minutes"][0]
    assert df.height == (end - start) // timedelta(minutes=minutes)
    # Independent re-read of the raw points: position p of a Period covers start + (p-1)·resolution.
    text = raw.decode()
    raw_points = {}
    for per in re.findall(r"<Period>(.*?)</Period>", text, re.S):
        p_start = datetime.fromisoformat(re.search(r"<start>([^<]+)Z</start>", per).group(1)).replace(tzinfo=timezone.utc)
        res = int(re.search(r"<resolution>PT(\d+)M", per).group(1))
        for pos, q in re.findall(r"<position>(\d+)</position>\s*<quantity>([^<]+)</quantity>", per):
            raw_points[p_start + (int(pos) - 1) * timedelta(minutes=res)] = float(q)
    last = None
    for r in df.iter_rows(named=True):
        last = raw_points.get(r["interval_start_utc"], last)  # A03: an omitted position repeats the block value
        assert r["demand_mw"] == last, r["interval_start_utc"]
    assert set(df["kind"]) == {kind} and df["issued_at_utc"].null_count() == df.height


def first(kind):
    return next((p for p in CASES if meta(p)[2] == kind and meta(p)[1] == date(2026, 11, 18)), None)


def test_wrong_process_type_blocks():
    path = first("actual")
    zone, day, _ = meta(path)
    _, cs = run(path.read_bytes(), zone, day, "forecast")  # an actual document read as a forecast
    assert "document_is_requested" in failed(cs)


def test_missing_interval_blocks():
    path = first("actual")
    zone, day, kind = meta(path)
    raw = path.read_text()
    # Dropping a point alone isn't a gap under A03 (the previous value carries), so shorten the
    # Period by one interval and drop its last point: the day is then one interval short.
    res = int(re.search(r"<resolution>PT(\d+)M", raw).group(1))
    end = re.search(r"<timeInterval>\s*<start>[^<]+</start>\s*<end>([^<]+)Z</end>", raw).group(1)
    shorter = (datetime.fromisoformat(end) - timedelta(minutes=res)).strftime("%Y-%m-%dT%H:%M")
    raw2 = re.sub(r"(<Period>\s*<timeInterval>\s*<start>[^<]+</start>\s*<end>)[^<]+(</end>)", rf"\g<1>{shorter}Z\g<2>", raw, 1)
    raw2 = re.sub(r"<Point>\s*<position>%d</position>.*?</Point>" % (((24 * 60) // res)), "", raw2, flags=re.S)
    _, cs = run(raw2.encode(), zone, day, kind)
    assert "day_complete" in failed(cs)


def test_other_zone_document_blocks():
    path = first("actual")
    zone, day, kind = meta(path)
    other = next(z for z in M.zones if z != zone)
    _, cs = run(path.read_bytes(), other, day, kind)
    assert {"series_for_zone", "day_complete"} <= set(failed(cs))


def test_unit_change_blocks():
    path = first("forecast")
    zone, day, kind = meta(path)
    raw = path.read_bytes().replace(b"<quantity_Measure_Unit.name>MAW<", b"<quantity_Measure_Unit.name>KWT<")
    _, cs = run(raw, zone, day, kind)
    assert "unit_is_mw" in failed(cs)
