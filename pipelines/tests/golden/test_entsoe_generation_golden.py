"""Golden tests: real ENTSO-E generation documents (A75 actual per type, A69 wind/solar day-ahead
forecast) from the owner's `make fixture-entsoe-fundamentals` (2026-11-26; local-only, skip in CI).

Per document: every row equals its raw point (sign: consumption series negative), each series
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
    return df, gen.checks(df, doc, kind, M, zone, day, unknown, MAP)


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


# Real gaps in ENTSO-E's own documents, both on the 2025-10-26 fall-back day (found 2026-11-26):
# the day can't publish (missing stays missing); the test pins exactly what is missing.
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
    have = set(df.filter(df["source_fuel_code"] == code)["interval_start_utc"].to_list())
    m = df.filter(df["source_fuel_code"] == code)["interval_minutes"][0]
    return sorted({start + timedelta(minutes=m * i) for i in range(int((end - start).total_seconds() // 60 // m))} - have)


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_document_parses_completely_and_exactly(path):
    zone, day, kind = meta(path)
    raw = path.read_bytes()
    df, cs = run(raw, zone, day, kind)
    if path.stem in REAL_GAPS:
        codes, gap = REAL_GAPS[path.stem]
        assert failed(cs) == ["series_day_complete"]
        gappy = set(df["source_fuel_code"]) if codes == "*" else set(codes)
        for code in gappy:
            assert missing(df, code, zone, day) == gap, code
        others = set(df["source_fuel_code"]) - gappy
        assert all(missing(df, c, zone, day) == [] for c in others)
    else:
        assert failed(cs) == [], [c for c in cs if not c.passed and c.severity == "blocking"]
    pts = raw_points(raw.decode())
    last = {}
    for r in df.iter_rows(named=True):
        code = r["source_fuel_code"]
        last[code] = pts.get((code, r["interval_start_utc"]), last.get(code))  # A03 carry within a series
        expected = -last[code] if code.endswith("/consumption") else last[code]
        assert r["generation_mw"] == expected, (code, r["interval_start_utc"])
    assert set(df["source_fuel_code"].str.replace("/consumption", "")) == {c.split("/")[0] for c, _ in pts}


def by(zone, day, kind):
    return next(p for p in CASES if meta(p) == (zone, date.fromisoformat(day), kind))


def test_pumping_is_kept_as_negative_consumption_not_netted():
    df, _ = run(by("ES", "2026-11-18", "actual").read_bytes(), "ES", date(2026, 11, 18), "actual")
    pumped = df.filter(df["source_fuel_code"] == "B10/consumption")
    assert pumped.height > 0 and (pumped["generation_mw"] <= 0).all() and set(pumped["fuel_type"]) == {"pumped_storage"}
    assert df.filter(df["source_fuel_code"] == "B10").height == pumped.height  # generation side kept separately


def test_new_production_type_blocks():
    raw = by("SE3", "2026-11-18", "actual").read_bytes().replace(b"<psrType>B14<", b"<psrType>B99<")
    df, cs = run(raw, "SE3", date(2026, 11, 18), "actual")
    assert "production_type_mapped" in failed(cs) and "B99" not in set(df["source_fuel_code"])


def test_forecast_read_as_actual_blocks():
    _, cs = run(by("DE_LU", "2026-11-18", "forecast").read_bytes(), "DE_LU", date(2026, 11, 18), "actual")
    assert "document_is_requested" in failed(cs)


def test_another_zones_document_blocks():
    _, cs = run(by("DE_LU", "2026-11-18", "actual").read_bytes(), "ES", date(2026, 11, 18), "actual")
    assert "series_for_zone" in failed(cs)


def test_dst_days_follow_each_series_resolution():
    for day, quarter_hours in (("2026-03-29", 92), ("2025-10-26", 100)):
        df, _ = run(by("DE_LU", day, "actual").read_bytes(), "DE_LU", date.fromisoformat(day), "actual")
        assert set(df.group_by("source_fuel_code").len()["len"]) == {quarter_hours}
    df, _ = run(by("SE3", "2025-10-26", "actual").read_bytes(), "SE3", date(2025, 10, 26), "actual")
    assert set(df["interval_minutes"]) == {60} and set(df.group_by("source_fuel_code").len()["len"]) == {25}
