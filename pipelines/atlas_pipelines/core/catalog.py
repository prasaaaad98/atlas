"""DuckDB catalog: SQL views over the *current* partitions of every asset.

Why it exists: analysts and gold builders query `SELECT * FROM silver.power_prices`
without knowing about attempt directories. Each view lists exactly the files
named by current pointers, so stale-attempt or failed-check files are invisible.

Who calls it: `make lake-catalog` / `python -m atlas_pipelines catalog`, tests.

Invariants:
  - The catalog is derived data: it is rebuilt from scratch from the pointers
    every time and can be deleted safely. It never holds data itself.
  - The new catalog file replaces the old one only when fully built.
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import duckdb

from atlas_pipelines.core.lake import absolute_path, check_asset_key, resolve_lake_root
from atlas_pipelines.core.manifest import list_pointers

CATALOG_NAME = "catalog.duckdb"


def build_catalog(lake_root: str) -> tuple[Path, dict[str, int]]:
    """Rebuild <lake>/catalog.duckdb. Returns its path and {asset_key: partitions}."""
    root = resolve_lake_root(lake_root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / CATALOG_NAME
    tmp = root / f".{CATALOG_NAME}.tmp-{os.getpid()}"
    if tmp.exists():
        tmp.unlink()

    files: dict[str, list[str]] = defaultdict(list)
    for p in list_pointers(lake_root):
        # Bronze raw files (HTML/CSV/JSON) are not tabular; only Parquet gets a view.
        if not p["uri"].endswith(".parquet"):
            continue
        files[check_asset_key(p["asset_key"])].append(absolute_path(lake_root, p["uri"]).as_posix())

    con = duckdb.connect(str(tmp))
    try:
        for asset_key in sorted(files):
            layer, name = asset_key.split(".", 1)  # both validated by check_asset_key
            paths = sorted(files[asset_key])
            # Paths are passed as a literal list; quotes in paths are escaped.
            literal = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in paths) + "]"
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{layer}"')
            # hive_partitioning=false: the "attempt=..." directory must not
            # leak into the data as a column; partition columns live in the files.
            con.execute(
                f'CREATE VIEW "{layer}"."{name}" AS '
                f"SELECT * FROM read_parquet({literal}, hive_partitioning=false, union_by_name=true)"
            )
    finally:
        con.close()
    os.replace(tmp, target)
    return target, {k: len(v) for k, v in files.items()}
