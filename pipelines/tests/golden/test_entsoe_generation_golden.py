"""Golden tests: real ENTSO-E generation documents (A75 actual per type, A69 wind/solar day-ahead
forecast) from the owner's `make fixture-entsoe-fundamentals` (2026-11-26; local-only, skip in CI).

Per document: every row equals its raw point (as sent; consumption series kept with direction
"consumption", positive), each series
covers the local day at its resolution, every production type maps, all blocking checks pass.
Failure cases change one thing in a real document.
"""

import json
import re
from datetime import date, datetime, timedelta, timezone

import pytest

from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.entsoe import generation as gen
from atlas_pipelines.sources.entsoe import load
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
MAP, _ = fuels.load_mapping("entsoe")
FETCHED = datetime(2026, 11, 26, 15, tzinfo=timezone.utc)
CASES = sorted(list(FX.glob("*_generation_actual.xml")) + list(FX.glob("*_wind_solar_forecast_da.xml"))) if FX.exists() else []
pytestmark = pytest.mark.skipif(not CASES, reason="no ENTSO-E generation fixtures (make fixture-entsoe-fundamentals)")


def meta(path):
    m = json.loads(path.with_suffix(".meta.json").read_text())
    return m["zone"], date.fromisoformat(m["date"]), "actual" if m["data"] == "generation_actual" else "forecast"


def run(raw, zone, day, kind):
    doc = load.parse_document(raw)
    df, unknown = gen.build_silver(doc, kind, M, zone, day, FETCHED, "fixture", MAP, None)
    return df.with_columns(gen.SERIES), gen.checks(df, doc, kind, M, zone, day, unknown, MAP)


def failed(cs):
    return sorted(c.check_name for c in cs if c.severity == "blocking" and not c.passed)


def raw_points(text):
    """(code with /consumption, interval start) -> value, re-read independently with regexes."""
    out = {}
    for ts in re.findall(r"<TimeSeries>(.*?)</TimeSeries>", text, re.S):
        code = re.search(r"<psrType>([^<]+)", ts).group(1) + ("/consumption" if "outBiddingZone_Domain" in ts else "")
        for per in re.findall(r"<Period>(.*?)</Period>", ts, re.S):
            p0 = datetime.fromisoformat(re.search(r"<start>([^<]+)Z</start>", per).group(1)).replace(tzinfo=timezone.utc)
            res = int(re.search(r"<resolution>PT(\d+)M", per).group(1))
            for pos, q in re.findall(r"<position>(\d+)</position>\s*<quantity>([^<]+)</quantity>", per):
                out[(code, p0 + (int(pos) - 1) * timedelta(minutes=res))] = float(q)
    return out


# Real gaps in ENTSO-E's own documents, both on the 2025-10-26 fall-back day (found 2026-11-26).
# Per-series completeness (ADR-B10): the gappy series are flagged incomplete (missing stays
# missing), every other series is complete, and no blocking check fails. Pinned exactly.
REAL_GAPS = {
    # every series: two Periods, 22:00Z-00:30Z and 00:45Z-23:00Z → the 00:30Z quarter-hour is absent
    "ES_2025-10-26_fall_back_generation_actual": ("*", [datetime(2025, 10, 26, 0, 30, tzinfo=timezone.utc)]),
    # solar (B16) and onshore wind (B19) forecasts start at 23:00Z, not at local midnight (22:00Z):
    # their first hour is absent; offshore wind (B18) covers all 100 quarter-hours
    "DE_LU_2025-10-26_fall_back_wind_solar_forecast_da": (
        ("B16", "B19"), [datetime(2025, 10, 25, 22, tzinfo=timezone.utc) + timedelta(minutes=15 * i) for i in range(4)]),
}


def missing(df, code, zone, day):
    start, end = M.day_bounds_utc(day)
    have = set(df.filter(df["series"] == code)["interval_start_utc"].to_list())
    m = df.filter(df["series"] == code)["interval_minutes"][0]
    return sorted({start + timedelta(minutes=m * i) for i in range(int((end - start).total_seconds() // 60 // m))} - have)


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_document_parses_completely_and_exactly(path):
    zone, day, kind = meta(path)
    raw = path.read_bytes()
    df, cs = run(raw, zone, day, kind)
    if path.stem in REAL_GAPS:
        codes, gap = REAL_GAPS[path.stem]
        assert failed(cs) == []
        gappy = set(df["series"]) if codes == "*" else set(codes)
        flagged = {c.check_name.split(":", 1)[1] for c in cs if c.check_name.startswith("series_complete:") and not c.passed}
        assert flagged == gappy
        for code in gappy:
            assert missing(df, code, zone, day) == gap, code
            assert not df.filter(df["series"] == code)["series_complete"].any(), code
        others = set(df["series"]) - gappy
        assert all(missing(df, c, zone, day) == [] for c in others)
        assert df.filter(df["series"].is_in(list(others)))["series_complete"].all()
    else:
        assert failed(cs) == [], [c for c in cs if not c.passed and c.severity == "blocking"]
        assert df["series_complete"].all() and all(c.passed for c in cs if c.check_name.startswith("series_complete:"))
    pts = raw_points(raw.decode())
    last = {}
    for r in df.iter_rows(named=True):
        code = r["series"]
        last[code] = pts.get((code, r["interval_start_utc"]), last.get(code))  # A03 carry within a series
        assert r["generation_mw"] == last[code], (code, r["interval_start_utc"])  # as sent, no sign change
        assert r["fuel_type"] == MAP[r["source_type"]].fuel_type
    assert set(df["series"]) == {c for c, _ in pts}


def by(zone, day, kind):
    return next(p for p in CASES if meta(p) == (zone, date.fromisoformat(day), kind))


def test_storage_is_kept_in_both_directions_positive_not_netted():
    df, _ = run(by("ES", "2026-11-18", "actual").read_bytes(), "ES", date(2026, 11, 18), "actual")
    for code, fuel_type in (("B10", "hydro_pumped_storage"), ("B25", "battery_storage")):
        s = df.filter(df["source_type"] == code)
        pumping, producing = (s.filter(s["direction"] == d) for d in ("consumption", "generation"))
        assert pumping.height > 0 and pumping.height == producing.height, code
        assert (pumping["generation_mw"] >= 0).all() and set(s["fuel_type"]) == {fuel_type}, code
    # ES pumped hydro really pumped that day (the load is there, not netted away); ES B25
    # charging is 0 MW in every quarter-hour of this real document, kept as sent.
    pumping = df.filter((df["source_type"] == "B10") & (df["direction"] == "consumption"))
    assert pumping["generation_mw"].sum() > 0


def test_new_production_type_blocks():
    raw = by("SE3", "2026-11-18", "actual").read_bytes().replace(b"<psrType>B14<", b"<psrType>B99<")
    df, cs = run(raw, "SE3", date(2026, 11, 18), "actual")
    assert "production_type_mapped" in failed(cs) and "B99" not in set(df["source_type"])  # never "other"


def test_forecast_read_as_actual_blocks():
    _, cs = run(by("DE_LU", "2026-11-18", "forecast").read_bytes(), "DE_LU", date(2026, 11, 18), "actual")
    assert "document_is_requested" in failed(cs)


def test_another_zones_document_blocks():
    _, cs = run(by("DE_LU", "2026-11-18", "actual").read_bytes(), "ES", date(2026, 11, 18), "actual")
    assert "series_for_zone" in failed(cs)


def test_dst_days_follow_each_series_resolution():
    for day, quarter_hours in (("2026-03-29", 92), ("2025-10-26", 100)):
        df, _ = run(by("DE_LU", day, "actual").read_bytes(), "DE_LU", date.fromisoformat(day), "actual")
        assert set(df.group_by("series").len()["len"]) == {quarter_hours}
    df, _ = run(by("SE3", "2025-10-26", "actual").read_bytes(), "SE3", date(2025, 10, 26), "actual")
    assert set(df["interval_minutes"]) == {60} and set(df.group_by("series").len()["len"]) == {25}


def test_real_required_forecast_gap_is_series_level_blocking():
    """DE_LU 2025-10-26: solar and onshore-wind forecasts (required, registry) start an hour late;
    offshore wind (also required) is complete. Only the two gappy series carry blocking severity."""
    df, cs = run(by("DE_LU", "2025-10-26", "forecast").read_bytes(), "DE_LU", date(2025, 10, 26), "forecast")
    s = {c.check_name.split(":", 1)[1]: c for c in cs if c.check_name.startswith("series_complete:")}
    assert set(s) == {"B16", "B18", "B19"} and all(c.severity == "warn" for c in s.values())  # never stops publishing
    assert [k for k, c in sorted(s.items()) if not c.passed] == ["B16", "B19"]
    assert all(c.metrics["required"] and c.metrics["series_severity"] == "blocking" for c in s.values())
    assert s["B16"].metrics["missing_intervals"] == 4 and s["B16"].metrics["first_missing"][0] == "2025-10-25T22:00:00Z"
