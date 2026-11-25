"""Command line for the local runner and lake catalog.

  python -m atlas_pipelines markets                      list registry markets and products
  python -m atlas_pipelines list                         list pipelines
  python -m atlas_pipelines run <pipeline> --partition YYYY-MM-DD
  python -m atlas_pipelines catalog                      rebuild <lake>/catalog.duckdb
  python -m atlas_pipelines show <asset_key>             current partitions + row counts via DuckDB

Locations come from LAKE_ROOT and ATLAS_MARKETS_DIR (core/config.py), or
--lake-root. Exit code is non-zero when a run does not succeed.
Who calls it: Makefile targets (pipelines-run, lake-catalog, demo-b0).
"""

from __future__ import annotations

import argparse
import json

import duckdb

from atlas_pipelines.core import config
from atlas_pipelines.core.catalog import build_catalog
from atlas_pipelines.core.lake import check_asset_key, resolve_lake_root
from atlas_pipelines.core.manifest import list_pointers
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.defs import build_pipelines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="atlas_pipelines")
    ap.add_argument("--lake-root", default=None, help="path or file:// URI (default: $LAKE_ROOT or ./lake)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("markets")
    sub.add_parser("list")
    r = sub.add_parser("run")
    r.add_argument("pipeline")
    r.add_argument("--partition", required=True)
    sub.add_parser("catalog")
    s = sub.add_parser("show")
    s.add_argument("asset_key")
    args = ap.parse_args(argv)

    lake = args.lake_root or config.lake_root()
    registry = load_registry(config.markets_dir())

    if args.cmd == "markets":
        for m in registry.values():
            for code, p in m.products.items():
                rules = ", ".join(f"{r.minutes} min from {r.effective_from}" for r in p.intervals)
                print(f"{m.id:<12} {code:<5} {p.canonical_market:<14} {m.timezone:<18} {m.currency}  {rules or 'intervals not confirmed'}")
        return 0

    pipelines = build_pipelines(registry)
    if args.cmd == "list":
        for name, p in sorted(pipelines.items()):
            print(f"{name:<32} {p.description}")
        return 0

    if args.cmd == "run":
        if args.pipeline not in pipelines:
            print(f"unknown pipeline {args.pipeline!r}; try `list`")
            return 2
        result = run_pipeline(pipelines[args.pipeline], args.partition, lake)
        print(f"run {result.run_id}: {result.status}")
        for t in result.tasks:
            print(f"  {t.task:<16} {t.status:<16} {t.error_class or ''}")
            for m in t.materializations:
                print(f"    {m['asset_key']}  rows={m['row_count']}  {m['content_hash'][:23]}...")
                print(f"      {m['uri']}")
            for c in t.checks:
                print(f"    check {c['check_name']:<24} {c['severity']:<8} {'PASS' if c['passed'] else 'FAIL'}")
        print(f"manifest: {result.manifest_path}")
        return 0 if result.status == "SUCCEEDED" else 1

    if args.cmd == "catalog":
        path, counts = build_catalog(lake)
        print(f"catalog: {path}")
        for k, n in sorted(counts.items()):
            print(f"  {k:<36} {n} current partition(s)")
        return 0

    if args.cmd == "show":
        key = check_asset_key(args.asset_key)
        pointers = [p for p in list_pointers(lake) if p["asset_key"] == key]
        if not pointers:
            print(f"{key}: no current partitions")
            return 1
        for p in pointers:
            print(json.dumps({k: p[k] for k in ("partition_key", "uri", "row_count", "content_hash")}))
        db = resolve_lake_root(lake) / "catalog.duckdb"
        layer, name = key.split(".", 1)
        con = duckdb.connect(str(db), read_only=True)
        try:
            # Show timestamps in UTC, not the PC's local zone (silver/gold are UTC-only).
            con.execute("SET TimeZone = 'UTC'")
            total = con.execute(f'SELECT count(*) FROM "{layer}"."{name}"').fetchone()[0]
            head = con.execute(f'SELECT * FROM "{layer}"."{name}" LIMIT 3')
            cols = [d[0] for d in head.description]
            sample = head.fetchall()
        except duckdb.Error as e:
            print(f"query failed ({e}); run `catalog` first")
            return 1
        finally:
            con.close()
        print(f'via DuckDB: SELECT count(*) FROM {layer}.{name} -> {total}')
        print("  " + " | ".join(cols))
        for row in sample:
            print("  " + " | ".join(str(v) for v in row))
        return 0
    return 2
