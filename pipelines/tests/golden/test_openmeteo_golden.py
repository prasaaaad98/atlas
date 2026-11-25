"""Golden tests: real Open-Meteo responses saved unchanged by the owner.

Fixtures: tests/fixtures/openmeteo/<market>_<zone>_<date>.json plus the
.meta.json written by `python -m atlas_pipelines fetch-fixture openmeteo ...`.
For each fixture this checks, against the raw bytes themselves:
  - every silver row equals the source value at that UTC hour (no transformation errors);
  - the row count is the local day's hours (23/24/25 on DST days);
  - all checks pass (unless the meta lists expected failures);
  - parsing twice gives byte-identical Parquet (idempotency).
Skips, saying so, when no fixtures exist yet.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources.openmeteo import archive
from conftest import FIXTURES, REPO

REGISTRY = load_registry(REPO / "config" / "markets")
LOCATIONS = {l.id: l for l in archive.load_locations(REPO / "config" / "locations" / "openmeteo.yaml", REGISTRY)}
CASES = sorted((FIXTURES / "openmeteo").glob("*.json")) if (FIXTURES / "openmeteo").exists() else []
CASES = [p for p in CASES if not p.name.endswith(".meta.json")]


@pytest.mark.skipif(not CASES, reason="no real Open-Meteo fixtures saved yet (make fixture-openmeteo)")
@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_openmeteo_fixture(path: Path, tmp_path):
    meta = json.loads(path.with_suffix(".meta.json").read_text(encoding="utf-8"))
    raw = path.read_bytes()
    assert sha256_file(path) == meta["sha256"], "fixture changed since it was fetched"
    loc = LOCATIONS[meta["location_id"]]
    market = REGISTRY[loc.market]
    day = date.fromisoformat(meta["date"])

    df = archive.parse(raw, loc, market, day, source_uri="fixture")
    start, end = market.day_bounds_utc(day)
    assert df.height == int((end - start).total_seconds() // 3600)

    doc = json.loads(raw)
    index = {datetime.fromisoformat(t).replace(tzinfo=timezone.utc): i for i, t in enumerate(doc["hourly"]["time"])}
    for row in df.iter_rows(named=True):
        i = index[row["ts_utc"]]
        for var, (col, _) in archive.VARIABLES.items():
            src = doc["hourly"][var][i]
            assert row[col] == (None if src is None else float(src)), (var, row["ts_utc"])

    failed = sorted(c.check_name for c in archive.checks(df, raw, market, day) if not c.passed)
    assert failed == sorted(meta.get("expected_failed_checks", []))

    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    df.write_parquet(a, compression="zstd", statistics=True)
    archive.parse(raw, loc, market, day, source_uri="fixture").write_parquet(b, compression="zstd", statistics=True)
    assert sha256_file(a) == sha256_file(b)
