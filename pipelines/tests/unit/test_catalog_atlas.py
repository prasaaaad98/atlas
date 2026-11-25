"""Catalog from explicit pointers (B7: `catalog --from-atlas`): only the files named
by current pointers appear, whichever runner owns the pointers."""

import duckdb
import polars as pl

from atlas_pipelines.core.catalog import build_catalog


def test_catalog_uses_only_the_given_pointers(lake):
    import pathlib
    root = pathlib.Path(lake)
    for name, value in (("current", 1.0), ("stale", 99.0)):
        f = root / "gold" / "t__a" / f"date=2026-11-24/attempt={name}/part-0.parquet"
        f.parent.mkdir(parents=True)
        pl.DataFrame({"v": [value]}).write_parquet(f)
    pointers = [{"asset_key": "gold.t__a", "partition_key": "2026-11-24",
                 "uri": "gold/t__a/date=2026-11-24/attempt=current/part-0.parquet"}]
    path, counts = build_catalog(lake, pointers)
    assert counts == {"gold.t__a": 1}
    con = duckdb.connect(str(path), read_only=True)
    try:
        assert con.execute('SELECT v FROM "gold"."t"').fetchall() == [(1.0,)]  # the stale attempt is invisible
    finally:
        con.close()
