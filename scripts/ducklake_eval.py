"""ADR-B01 hands-on evaluation: plain Parquet + pointer catalog vs DuckLake.

Why it exists: ADR-B01 chose plain Parquet with Atlas pointers on paper; this
measures the alternative on this machine before the decision is final.

Data: one REAL silver day per ENTSO-E zone (parsed from the owner's local-only
fixtures, 2026-11-18, 96 quarter-hours), copied and re-dated to N days per
zone. The copies are synthetic and used for timing only; no market numbers are
reported from them.

Compared, each timed REPEATS times (median reported):
  A  current: DuckDB views over the files named by pointers (core/catalog.py)
  B  DuckLake, local DuckDB catalog file, files registered in place (ducklake_add_data_files)
  C  DuckLake, catalog in PostgreSQL (the compose database, a throwaway database), same registration
Operations: register/build, three queries, replacing one partition (a re-run),
time travel (DuckLake only).

Usage: python scripts/ducklake_eval.py [days_per_zone=1700] [repeats=3]
Writes JSON to stdout. Needs network once (DuckDB downloads the ducklake and
postgres extensions) and `make up` for C.
"""

from __future__ import annotations

import json
import shutil
import statistics
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import polars as pl

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "pipelines"))
from atlas_pipelines.core.catalog import build_catalog  # noqa: E402
from atlas_pipelines.core.markets import load_registry  # noqa: E402
from atlas_pipelines.sources.entsoe import day_ahead as da  # noqa: E402

PG = "dbname=ducklake_eval host=localhost port=5432 user=atlas password=atlas"
ZONES = ("DE_LU", "ES", "SE3")


def timed(fn, repeats):
    out = []
    for _ in range(repeats):
        t = time.perf_counter()
        fn()
        out.append(time.perf_counter() - t)
    return round(statistics.median(out), 4)


def make_data(root: Path, days: int) -> list[dict]:
    market = load_registry(REPO / "config" / "markets")["entsoe"]
    base = date(2026, 11, 18)
    pointers = []
    for zone in ZONES:
        raw = (REPO / "pipelines/tests/fixtures/entsoe" / f"{zone}_2026-11-18_normal.xml").read_bytes()
        s, _ = da.select_series(da.parse_document(raw), da.series_rule(market, zone))
        real = da.build_silver(s, market, zone, base, "eval")
        for i in range(days):
            d = date(2022, 4, 1) + timedelta(days=i)
            shift = d - base
            df = real.with_columns(
                pl.col("interval_start_utc") + shift, pl.col("interval_end_utc") + shift,
                pl.lit(d).alias("local_date"), pl.lit(f"synthetic-copy:{zone}:{d}").alias("source_uri"))
            uri = f"silver/power_prices__entsoe_{zone.lower()}/date={d}/attempt=eval/part-0.parquet"
            f = root / uri
            f.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(f, compression="zstd", statistics=True)
            pointers.append({"asset_key": f"silver.power_prices__entsoe_{zone.lower()}", "partition_key": str(d), "uri": uri})
    return pointers


QUERIES = {
    "count_all": "SELECT count(*) FROM {t}",
    "avg_by_zone": "SELECT zone, avg(price) FROM {t} GROUP BY zone",
    "one_partition": "SELECT * FROM {t} WHERE zone = 'ES' AND local_date = DATE '2024-06-01'",
}


def eval_views(root, pointers, repeats):
    res = {"build_seconds": timed(lambda: build_catalog(str(root), pointers), repeats)}
    con = duckdb.connect(str(root / "catalog.duckdb"), read_only=True)
    for name, q in QUERIES.items():
        res[f"{name}_seconds"] = timed(lambda q=q: con.execute(q.format(t='"silver"."power_prices"')).fetchall(), repeats)
    con.close()
    # A re-run of one partition: write the new file, move the pointer, rebuild the views.
    new = dict(pointers[-1], uri=pointers[-1]["uri"].replace("attempt=eval", "attempt=rerun"))
    shutil.copy(root / pointers[-1]["uri"], (root / new["uri"]).parent.mkdir(parents=True, exist_ok=True) or root / new["uri"])
    swapped = pointers[:-1] + [new]
    res["replace_one_partition_seconds"] = timed(lambda: build_catalog(str(root), swapped), repeats)
    res["time_travel"] = "no (only the current pointers; history is in Atlas's materializations table)"
    return res


def eval_ducklake(root, pointers, repeats, catalog: str, label: str):
    files = [str((root / p["uri"]).as_posix()) for p in pointers]
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    if catalog.startswith("postgres:"):
        con.execute("INSTALL postgres; LOAD postgres")
    res = {}

    def register():
        con.execute("DETACH DATABASE IF EXISTS lk")
        if catalog.startswith("postgres:"):
            subprocess.run(["docker", "compose", "-f", str(REPO / "deploy/compose/docker-compose.yml"), "exec", "-T",
                            "postgres", "psql", "-U", "atlas", "-d", "atlas", "-q",
                            "-c", "DROP DATABASE IF EXISTS ducklake_eval WITH (FORCE)", "-c", "CREATE DATABASE ducklake_eval"],
                           check=True, capture_output=True)
        else:
            for f in root.glob("lake_meta.ducklake*"):
                f.unlink()
        con.execute(f"ATTACH 'ducklake:{catalog}' AS lk (DATA_PATH '{(root / 'ducklake_data').as_posix()}')")
        con.execute(f"CREATE TABLE lk.power_prices AS SELECT * FROM read_parquet('{files[0]}') LIMIT 0")
        con.execute("BEGIN")
        for f in files:
            con.execute(f"CALL ducklake_add_data_files('lk', 'power_prices', '{f}')")
        con.execute("COMMIT")

    res["register_seconds"] = timed(register, repeats)
    for name, q in QUERIES.items():
        res[f"{name}_seconds"] = timed(lambda q=q: con.execute(q.format(t="lk.power_prices")).fetchall(), repeats)
    rerun = files[-1].replace("attempt=eval", "attempt=rerun")
    d = pointers[-1]["partition_key"]

    def replace():
        con.execute("BEGIN")
        con.execute(f"DELETE FROM lk.power_prices WHERE zone = 'SE3' AND local_date = DATE '{d}'")
        con.execute(f"CALL ducklake_add_data_files('lk', 'power_prices', '{rerun}')")
        con.execute("COMMIT")

    res["replace_one_partition_seconds"] = timed(replace, repeats)
    snaps = con.execute("SELECT snapshot_id FROM lk.snapshots() ORDER BY snapshot_id").fetchall()
    first_full = snaps[2][0] if len(snaps) > 2 else snaps[-1][0]
    res["snapshots"] = len(snaps)
    res["time_travel"] = con.execute(f"SELECT count(*) FROM lk.power_prices AT (VERSION => {first_full})").fetchone()[0] > 0
    res["rows_now"] = con.execute("SELECT count(*) FROM lk.power_prices").fetchone()[0]
    con.close()
    return {label: res}


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 1700
    repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    root = REPO / "bin" / "ducklake-eval"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    t = time.perf_counter()
    pointers = make_data(root, days)
    out = {"files": len(pointers), "rows": len(pointers) * 96, "data_prep_seconds": round(time.perf_counter() - t, 1),
           "duckdb": duckdb.__version__, "repeats": repeats,
           "A_views_over_pointers": eval_views(root, pointers, repeats)}
    out.update(eval_ducklake(root, pointers, repeats, (root / "lake_meta.ducklake").as_posix(), "B_ducklake_duckdb_catalog"))
    try:
        out.update(eval_ducklake(root, pointers, repeats, f"postgres:{PG}", "C_ducklake_postgres_catalog"))
    except Exception as e:  # recorded, not hidden
        out["C_ducklake_postgres_catalog"] = {"error": f"{type(e).__name__}: {e}"}
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
