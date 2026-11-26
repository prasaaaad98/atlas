"""Golden tests: real ENTSO-E A44 documents saved unchanged by the owner (2026-11-25).

Fixtures: tests/fixtures/entsoe/<ZONE>_<YYYY-MM-DD>_<label>.xml for DE_LU, ES and
SE3 on a normal day, the last hourly day, the first 15-minute day, both DST
days of the 15-minute era and the hourly-era spring DST day, plus a no-data
acknowledgement. No network.

For each price fixture this checks, against the raw XML read independently here:
  - the row count is the local day's interval count from the registry
    (24/23 hours in the hourly era; 96/92/100 quarter-hours after 2025-10-01);
  - every LISTED point appears unchanged at its position, and every omitted
    position (A03 compression) carries the value of the last listed position
    before it, never anything else;
  - the selected series is the configured one, never the others in the file;
  - every blocking check passes; the only failures are the two expected warns;
  - parsing twice gives byte-identical Parquet.
"""

import re
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import pytest

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources.entsoe import day_ahead as da
from conftest import FIXTURES, REPO

REGISTRY = load_registry(REPO / "config" / "markets")
MARKET = REGISTRY["entsoe"]
NAME = re.compile(r"^(DE_LU|ES|SE3)_(\d{4}-\d{2}-\d{2})_\w+$")
FUNDAMENTALS = ("_load_actual", "_load_forecast_da", "_generation_actual", "_wind_solar_forecast_da")  # B4a/B4b files
CASES = sorted(p for p in (FIXTURES / "entsoe").glob("*.xml") if NAME.match(p.stem) and not p.stem.endswith(FUNDAMENTALS))
# The fixtures are local-only (exchange-owned prices, see docs/data/sources/entsoe.md),
# so CI has none: skip, saying why, instead of passing vacuously.
pytestmark = pytest.mark.skipif(not CASES, reason="ENTSO-E fixtures are local-only and not present")
EXPECTED_WARNS = ["price_within_limits", "series_selection_verified"]  # limits unset; selection not verified


def test_all_expected_fixtures_present():
    # 3 zones x 6 days; a lost file must fail loudly, not shrink the suite.
    assert len([p for p in CASES if not p.stem.endswith("_A01")]) == 18


def _reference_points(raw: bytes, contract: str, sequence: str):
    """Independent reading of the selected series: [(n_intervals, {position: price})]."""
    root = ET.fromstring(raw)
    local = lambda el: el.tag.rsplit("}", 1)[-1]
    out = []
    for ts in (c for c in root if local(c) == "TimeSeries"):
        fields = {local(c): (c.text or "").strip() for c in ts}
        if fields.get("contract_MarketAgreement.type") != contract:
            continue
        if fields.get("classificationSequence_AttributeInstanceComponent.position", da.NO_SEQUENCE) != sequence:
            continue
        for period in (c for c in ts if local(c) == "Period"):
            pts = {}
            for pt in (c for c in period if local(c) == "Point"):
                f = {local(c): c.text for c in pt}
                pts[int(f["position"])] = float(f["price.amount"])
            out.append(pts)
    return out


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_entsoe_fixture(path: Path, tmp_path):
    zone, day_s = NAME.match(path.stem).groups()
    day = date.fromisoformat(day_s)
    raw = path.read_bytes()
    rule = da.series_rule(MARKET, zone)

    doc = da.parse_document(raw)
    series, sel = da.select_series(doc, rule)
    assert series is not None and series.key == rule.select
    df = da.build_silver(series, MARKET, zone, day, source_uri="fixture")

    assert df.height == len(MARKET.intervals("DAM", day))
    assert df["interval_minutes"].unique().to_list() == [MARKET.product("DAM").interval_minutes_on(day)]
    assert df["local_block"].to_list() == list(range(1, df.height + 1))

    [listed] = _reference_points(raw, *rule.select)
    prices = df["price"].to_list()
    carried = None
    for pos, price in enumerate(prices, start=1):
        if pos in listed:
            carried = listed[pos]
        assert price == carried, (pos, price, carried)

    failed = sorted(c.check_name for c in sel + da.checks(df, series, doc, MARKET, day) if not c.passed)
    assert failed == EXPECTED_WARNS

    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    df.write_parquet(a, compression="zstd", statistics=True)
    da.build_silver(da.select_series(da.parse_document(raw), rule)[0], MARKET, zone, day, "fixture").write_parquet(
        b, compression="zstd", statistics=True)
    assert sha256_file(a) == sha256_file(b)


def test_compressed_series_carries_values_forward():
    # ES 2025-03-30 (real): the day-ahead series lists 22 of 23 hourly positions;
    # position 13 is omitted, so hour 13 must equal hour 12.
    raw = (FIXTURES / "entsoe" / "ES_2025-03-30_spring_hourly.xml").read_bytes()
    [listed] = _reference_points(raw, "A01", da.NO_SEQUENCE)
    assert 13 not in listed and 12 in listed
    s, _ = da.select_series(da.parse_document(raw), da.series_rule(MARKET, "ES"))
    df = da.build_silver(s, MARKET, "ES", date(2025, 3, 30), "fixture")
    assert df["price"][12] == df["price"][11] == listed[12]


def test_selection_ignores_file_order_and_resolution():
    # DE_LU (real): series 1 comes first on 2025-10-01 and second on 2026-11-18,
    # and in the hourly era sequence 2 was quarter-hourly. Selection follows the
    # sequence number only.
    for name, first in (("DE_LU_2025-10-01_first_15min.xml", "1"), ("DE_LU_2026-11-18_normal.xml", "2")):
        doc = da.parse_document((FIXTURES / "entsoe" / name).read_bytes())
        assert doc.series[0].sequence == first
        s, _ = da.select_series(doc, da.series_rule(MARKET, "DE_LU"))
        assert s.sequence == "1"
    doc = da.parse_document((FIXTURES / "entsoe" / "DE_LU_2025-09-30_last_hourly.xml").read_bytes())
    s, _ = da.select_series(doc, da.series_rule(MARKET, "DE_LU"))
    assert (s.sequence, s.periods[0].minutes) == ("1", 60)
    assert {x.periods[0].minutes for x in doc.series} == {15, 60}


def test_no_data_acknowledgement():
    doc = da.parse_document((FIXTURES / "entsoe" / "DE_LU_future_no_data.xml").read_bytes())
    assert isinstance(doc, da.Acknowledgement) and doc.code == "999" and doc.no_data
