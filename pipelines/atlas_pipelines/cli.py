"""Command line for the local runner and lake catalog.

  python -m atlas_pipelines markets                      list registry markets and products
  python -m atlas_pipelines list                         list pipelines
  python -m atlas_pipelines run <pipeline> --partition YYYY-MM-DD [--to YYYY-MM-DD]
  python -m atlas_pipelines catalog                      rebuild <lake>/catalog.duckdb
  python -m atlas_pipelines show <asset_key>             current partitions + row counts via DuckDB
  python -m atlas_pipelines fetch-fixture openmeteo --location entsoe:DE_LU --date YYYY-MM-DD
                                                         owner-run: save one real response as a test fixture
  python -m atlas_pipelines fetch-fixture entsoe --zone ES --date YYYY-MM-DD --label normal_A01
                                                         owner-run: same for ENTSO-E (needs ENTSOE_TOKEN)

Secrets: ENTSOE_TOKEN is read from the environment, or from the repo's .env
(KEY=VALUE lines) when unset. It is never printed; URLs are shown redacted.

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
    r.add_argument("--to", help="last partition (inclusive): run each day from --partition to --to")
    sub.add_parser("catalog")
    s = sub.add_parser("show")
    s.add_argument("asset_key")
    f = sub.add_parser("fetch-fixture")
    f.add_argument("source", choices=["openmeteo", "entsoe"])
    f.add_argument("--location", help="openmeteo: <market>:<zone>")
    f.add_argument("--zone", help="entsoe: zone code, e.g. ES")
    f.add_argument("--label", help="entsoe: file label, e.g. normal_A01")
    f.add_argument("--date", required=True)
    f.add_argument("--out", default=str(config.REPO_ROOT / "pipelines" / "tests" / "fixtures"))
    args = ap.parse_args(argv)
    _load_dotenv(config.REPO_ROOT / ".env", ("ENTSOE_TOKEN", "EIA_API_KEY"))

    lake = args.lake_root or config.lake_root()
    registry = load_registry(config.markets_dir())

    if args.cmd == "markets":
        for m in registry.values():
            for code, p in m.products.items():
                rules = ", ".join(f"{r.minutes} min from {r.effective_from}" for r in p.intervals)
                print(f"{m.id:<12} {code:<5} {p.canonical_market:<14} {m.timezone:<18} {m.currency}  {rules or 'intervals not confirmed'}")
        return 0

    if args.cmd == "fetch-fixture":
        if args.source == "entsoe":
            return fetch_fixture_entsoe(args, registry)
        if not args.location:
            print("--location is required for openmeteo")
            return 2
        return fetch_fixture(args, registry)

    pipelines = build_pipelines(registry)
    if args.cmd == "list":
        for name, p in sorted(pipelines.items()):
            print(f"{name:<32} {p.description}")
        return 0

    if args.cmd == "run":
        if args.pipeline not in pipelines:
            print(f"unknown pipeline {args.pipeline!r}; try `list`")
            return 2
        from datetime import timedelta

        from atlas_pipelines.core.markets import parse_partition_date

        first = parse_partition_date(args.partition)
        last = parse_partition_date(args.to) if args.to else first
        if last < first or (last - first).days > 366:
            print("--to must be on or after --partition, within 366 days")
            return 2
        code, day = 0, first
        while day <= last:
            code |= _run_one(pipelines[args.pipeline], day.isoformat(), lake)
            day += timedelta(days=1)
        return code

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


def _run_one(pipeline, partition: str, lake: str) -> int:
    result = run_pipeline(pipeline, partition, lake)
    print(f"run {result.run_id} {partition}: {result.status}")
    for t in result.tasks:
        retry = f" retryable (after {t.retry_after_s}s)" if t.retryable else ""
        print(f"  {t.task:<16} {t.status:<16} {t.error_class or ''}{retry}")
        for m in t.materializations:
            print(f"    {m['asset_key']}  rows={m['row_count']}  {m['content_hash'][:23]}...")
            print(f"      {m['uri']}")
        for c in t.checks:
            print(f"    check {c['check_name']:<30} {c['severity']:<8} {'PASS' if c['passed'] else 'FAIL'}")
    print(f"manifest: {result.manifest_path}")
    return 0 if result.status == "SUCCEEDED" else 1


def _load_dotenv(path, names) -> None:
    """Set the named variables from a KEY=VALUE .env file when they are not set already.

    Only the listed names are read; values are never printed or logged.
    """
    import os

    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.strip().partition("=")
        key = key.removeprefix("export ").strip()
        if sep and key in names and not os.environ.get(key):
            os.environ[key] = value.strip().strip('"').strip("'")


def fetch_fixture_entsoe(args, registry) -> int:
    """One real A44 request (with the registry's request parameters) saved unchanged.

    Run by the owner (network, ENTSOE_TOKEN). Never overwrites; writes a
    .meta.json with the redacted URL, status, fetch time and SHA-256.
    """
    import os
    from datetime import datetime, timezone
    from pathlib import Path

    from atlas_pipelines.core.hashing import sha256_bytes
    from atlas_pipelines.core.http import RateLimiter, http_get
    from atlas_pipelines.core.limits import load_limit
    from atlas_pipelines.core.markets import iso_utc, parse_partition_date
    from atlas_pipelines.sources.entsoe import day_ahead as da

    market = registry["entsoe"]
    if args.zone not in market.zones or not args.label:
        print(f"--zone must be one of {market.zones} and --label is required")
        return 2
    token = os.environ.get("ENTSOE_TOKEN")
    if not token:
        print("ENTSOE_TOKEN is not set (environment or .env)")
        return 2
    day = parse_partition_date(args.date)
    out_dir = Path(args.out) / "entsoe"
    stem = f"{args.zone}_{day.isoformat()}_{args.label}"
    body_path, meta_path = out_dir / f"{stem}.xml", out_dir / f"{stem}.meta.json"
    if body_path.exists() or meta_path.exists():
        print(f"refusing to overwrite {body_path}")
        return 1
    limiter = RateLimiter(load_limit("entsoe", config.REPO_ROOT / "config" / "limits").rpm)
    params = {**da.request_params(market, args.zone, day), "securityToken": token}
    res = http_get(da.BASE_URL, params, limiter=limiter, secret_params=("securityToken",))
    print(f"{res.url_redacted} -> {res.status} {res.outcome} {res.error_class or ''}")
    if res.outcome == "unknown":
        print(f"not saved: {res.detail}")
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)
    body_path.write_bytes(res.body)
    meta = {
        "source": "entsoe", "zone": args.zone, "date": day.isoformat(), "url": res.url_redacted,
        "http_status": res.status, "outcome": res.outcome, "fetched_at": iso_utc(datetime.now(timezone.utc)),
        "sha256": sha256_bytes(res.body), "bytes": len(res.body),
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"saved {body_path} ({len(res.body)} bytes)")
    return 0 if res.outcome == "succeeded" else 1


def fetch_fixture(args, registry) -> int:
    """One real request through the source's limit key; body saved unchanged.

    Run by the owner (network). Never overwrites an existing fixture.
    """
    from datetime import date, datetime, timezone
    from pathlib import Path

    from atlas_pipelines.core.hashing import sha256_bytes
    from atlas_pipelines.core.http import RateLimiter, http_get
    from atlas_pipelines.core.limits import load_limit
    from atlas_pipelines.core.markets import iso_utc, parse_partition_date
    from atlas_pipelines.sources.openmeteo import archive

    locations = {l.id: l for l in archive.load_locations(config.REPO_ROOT / "config" / "locations" / "openmeteo.yaml", registry)}
    if args.location not in locations:
        print(f"unknown location {args.location!r}; known: {sorted(locations)}")
        return 2
    loc = locations[args.location]
    day: date = parse_partition_date(args.date)
    out_dir = Path(args.out) / "openmeteo"
    stem = f"{loc.market}_{loc.zone.lower()}_{day.isoformat()}"
    body_path, meta_path = out_dir / f"{stem}.json", out_dir / f"{stem}.meta.json"
    if body_path.exists() or meta_path.exists():
        print(f"refusing to overwrite {body_path}")
        return 1
    limiter = RateLimiter(load_limit("openmeteo", config.REPO_ROOT / "config" / "limits").rpm)
    res = http_get(archive.BASE_URL, archive.request_params(loc, registry[loc.market], day), limiter=limiter)
    print(f"{res.url_redacted} -> {res.status} {res.outcome} {res.error_class or ''}")
    if res.outcome == "unknown":
        print(f"not saved: {res.detail}")
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)
    body_path.write_bytes(res.body)
    meta = {
        "source": "openmeteo", "location_id": loc.id, "date": day.isoformat(),
        "url": res.url_redacted, "http_status": res.status, "outcome": res.outcome,
        "fetched_at": iso_utc(datetime.now(timezone.utc)), "sha256": sha256_bytes(res.body),
        "bytes": len(res.body), "expected_failed_checks": [],
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"saved {body_path} ({len(res.body)} bytes)")
    return 0 if res.outcome == "succeeded" else 1
