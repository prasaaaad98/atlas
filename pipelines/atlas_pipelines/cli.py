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

With Atlas (control plane URL from --atlas or ATLAS_URL, default http://127.0.0.1:8080):
  python -m atlas_pipelines register <pipeline>           register the pipeline definition (POST /pipelines)
  python -m atlas_pipelines submit <pipeline> --partition YYYY-MM-DD [--to ..] [--priority live|backfill|adhoc]
  python -m atlas_pipelines limits sync                   PUT every config/limits/*.yaml as an Atlas limit key
  python -m atlas_pipelines schedule <pipeline> --cron "0 14 * * *" --tz Europe/Brussels [--offset-days 1] [--catchup --start D] [--disable]
  python -m atlas_pipelines backfill <pipeline> --from D --to D [--max-concurrent 4]   then: backfill-status <id>
  python -m atlas_pipelines cancel <run_id>               cancel a run (running workers stop at their next heartbeat)
  python -m atlas_pipelines worker [--max-tasks N] [--exit-when-idle]
                                                         claim and run tasks (same task code as `run`)

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
    ap.add_argument("--atlas", default=None, help="control plane URL (default: $ATLAS_URL or http://127.0.0.1:8080)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("markets")
    sub.add_parser("list")
    r = sub.add_parser("run")
    r.add_argument("pipeline")
    r.add_argument("--partition", required=True)
    r.add_argument("--to", help="last partition (inclusive): run each day from --partition to --to")
    cat = sub.add_parser("catalog")
    cat.add_argument("--from-atlas", action="store_true",
                     help="use Atlas's current pointers (GET /assets) instead of the local runner's")
    reg = sub.add_parser("register")
    reg.add_argument("pipeline")
    sm = sub.add_parser("submit")
    sm.add_argument("pipeline")
    sm.add_argument("--partition", required=True)
    sm.add_argument("--to")
    sm.add_argument("--priority", default="adhoc", choices=["live", "backfill", "adhoc"])
    sm.add_argument("--deadline-in", type=float, help="explicit deadline: this many seconds from now (else the pipeline's rule)")
    sm.add_argument("--deadline-at", type=float, help="explicit deadline as a Unix timestamp (else the pipeline's rule)")
    lm = sub.add_parser("limits")
    lm.add_argument("action", choices=["sync"])
    sc = sub.add_parser("schedule")
    sc.add_argument("pipeline")
    sc.add_argument("--cron", required=True)
    sc.add_argument("--tz", required=True)
    sc.add_argument("--offset-days", type=int, default=0)
    sc.add_argument("--catchup", action="store_true")
    sc.add_argument("--start")
    sc.add_argument("--disable", action="store_true")
    sc.add_argument("--grain", choices=["day", "month"], default="day",
                    help="month: partition = first day of a month (E6)")
    sc.add_argument("--offset-months", type=int, default=0, help="month grain: partition month = fire month + this")
    sc.add_argument("--refetch-after", default="",
                    help="comma-separated days, e.g. 3,30: re-fetch each partition once, that long after it ended")
    sa = sub.add_parser("schedules", help="apply every schedule in a config/schedules/*.yaml file")
    sa.add_argument("action", choices=["apply"])
    sa.add_argument("file")
    bf = sub.add_parser("backfill")
    bf.add_argument("pipeline")
    bf.add_argument("--from", dest="from_", required=True)
    bf.add_argument("--to", required=True)
    bf.add_argument("--max-concurrent", type=int, default=4)
    bf.add_argument("--grain", choices=["day", "month"], default="day")
    sr = sub.add_parser("schedule-report")
    sr.add_argument("pipeline")
    sr.add_argument("--since", help="YYYY-MM-DD (default: 30 days ago)")
    bs = sub.add_parser("backfill-status")
    bs.add_argument("backfill_id")
    cn = sub.add_parser("cancel")
    cn.add_argument("run_id")
    wk = sub.add_parser("worker")
    wk.add_argument("--worker-id", default=None)
    wk.add_argument("--max-tasks", type=int, default=None)
    wk.add_argument("--exit-when-idle", action="store_true")
    wk.add_argument("--lease-seconds", type=int, default=30, help="lease per claim/heartbeat (ADR-004 default 30)")
    wk.add_argument("--wait", type=float, default=20,
                    help="long-poll seconds per claim, woken by LISTEN/NOTIFY (ADR-007); 0 = poll every 2 s")
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
        code, day, down = 0, first, 0
        while day <= last:
            rc, result = _run_one(pipelines[args.pipeline], day.isoformat(), lake)
            code |= rc
            reason = source_unreachable(result)
            down = down + 1 if reason else 0
            # A source that is down (or throttling us) fails every task the same way: stop instead
            # of spending a full timeout per task per day. 429: at once (respect the source).
            if reason == "rate_limited" or down >= UNREACHABLE_DAYS_TO_STOP:
                print(f"stopped at {day}: the source is unreachable ({reason}) on {down} day(s) in a row; "
                      f"nothing was published for them. Re-run later: --partition {day} --to {last}")
                return 1
            day += timedelta(days=1)
        return code

    if args.cmd in ("register", "submit", "worker", "limits", "cancel", "schedule", "backfill", "backfill-status",
                    "schedule-report", "schedules"):
        return atlas_command(args, pipelines, lake)

    if args.cmd == "catalog":
        pointers = None
        if args.from_atlas:
            import os

            from atlas_pipelines.worker import AtlasClient, AtlasError, urllib_send

            client = AtlasClient(urllib_send(args.atlas or os.environ.get("ATLAS_URL", "http://127.0.0.1:8080")))
            try:
                pointers = atlas_pointers(client)
            except AtlasError as e:
                print(f"atlas: {e}")
                return 1
        path, counts = build_catalog(lake, pointers)
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


def schedule_body(spec: dict) -> dict:
    """One schedule spec (CLI flags or a config/schedules entry) -> the PUT /pipelines/{name}/schedule body."""
    body = {"cron": spec["cron"], "tz": spec["tz"], "partition_offset_days": spec.get("offset_days", 0),
            "catchup": bool(spec.get("catchup", False)), "enabled": spec.get("enabled", True),
            "partition_grain": spec.get("grain", "day"), "partition_offset_months": spec.get("offset_months", 0),
            "refetch_after_days": list(spec.get("refetch_after_days") or [])}
    if spec.get("start"):
        body["start_partition"] = str(spec["start"])
    return body


def load_schedules(path) -> dict[str, dict]:
    """config/schedules/*.yaml: {version: 1, schedules: {<pipeline>: {cron, tz, ...}}}, checked before any call."""
    from pathlib import Path

    import yaml

    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw.get("version") != 1 or not isinstance(raw.get("schedules"), dict):
        raise ValueError(f"{path}: needs version: 1 and a schedules mapping")
    allowed = {"cron", "tz", "offset_days", "catchup", "start", "enabled", "grain", "offset_months",
               "refetch_after_days", "why"}
    for name, spec in raw["schedules"].items():
        unknown = set(spec) - allowed
        if unknown or "cron" not in spec or "tz" not in spec:
            raise ValueError(f"{path}: schedule {name}: needs cron and tz; unknown keys {sorted(unknown)}")
    return raw["schedules"]


def atlas_command(args, pipelines, lake: str) -> int:
    """register / submit / worker against a running control plane."""
    import os
    import socket
    from datetime import timedelta

    from atlas_pipelines.core.markets import parse_partition_date
    from atlas_pipelines.worker import AtlasClient, AtlasError, pipeline_definition, urllib_send, work

    url = args.atlas or os.environ.get("ATLAS_URL", "http://127.0.0.1:8080")
    client = AtlasClient(urllib_send(url))
    try:
        if args.cmd == "limits":
            from atlas_pipelines.core.limits import load_limit

            limits_dir = config.REPO_ROOT / "config" / "limits"
            code = 0
            for f in sorted(limits_dir.glob("*.yaml")):
                lim = load_limit(f.stem, limits_dir)
                status, out = client.call("PUT", f"/limit-keys/{lim.key}",
                                          {"max_concurrency": lim.max_concurrency, "rpm": lim.rpm})
                print(f"limit key {lim.key}: HTTP {status} {out}")
                code |= 0 if status == 200 else 1
            return code
        if args.cmd == "schedule":
            body = schedule_body({"cron": args.cron, "tz": args.tz, "offset_days": args.offset_days,
                                  "catchup": args.catchup, "start": args.start, "enabled": not args.disable,
                                  "grain": args.grain, "offset_months": args.offset_months,
                                  "refetch_after_days": [int(x) for x in args.refetch_after.split(",") if x.strip()]})
            status, out = client.call("PUT", f"/pipelines/{args.pipeline}/schedule", body)
            print(f"schedule {args.pipeline}: HTTP {status} {out}")
            return 0 if status == 200 else 1
        if args.cmd == "schedules":
            code = 0
            for name, spec in load_schedules(args.file).items():
                if name not in pipelines:
                    print(f"unknown pipeline {name!r} in {args.file}")
                    return 2
                status, out = client.call("POST", "/pipelines", pipeline_definition(pipelines[name]))
                if status not in (200, 201):
                    print(f"register {name}: HTTP {status} {out}")
                    return 1
                status, out = client.call("PUT", f"/pipelines/{name}/schedule", schedule_body(spec))
                print(f"schedule {name}: HTTP {status} {out if status != 200 else ''}".rstrip())
                code |= 0 if status == 200 else 1
            return code
        if args.cmd == "backfill":
            status, out = client.call("POST", "/backfills", {"pipeline": args.pipeline, "from_partition": args.from_,
                                                            "to_partition": args.to, "max_concurrent_runs": args.max_concurrent,
                                                            "partition_grain": args.grain})
            detail = "" if status == 201 else out
            print(f"backfill {args.pipeline}: HTTP {status} id={(out or {}).get('id')} {detail}")
            return 0 if status == 201 else 1
        if args.cmd == "schedule-report":
            q = f"?pipeline={args.pipeline}" + (f"&since={args.since}" if args.since else "")
            status, rows = client.call("GET", "/reports/schedule" + q)
            if status != 200:
                print(f"report: HTTP {status} {rows}")
                return 1
            print(schedule_report_text(rows))
            return 0
        if args.cmd == "backfill-status":
            status, out = client.call("GET", f"/backfills/{args.backfill_id}")
            print(json.dumps(out, indent=2))
            return 0 if status == 200 else 1
        if args.cmd == "cancel":
            status, out = client.call("POST", f"/runs/{args.run_id}/cancel")
            print(f"cancel {args.run_id}: HTTP {status} status={(out or {}).get('status')}")
            return 0 if status == 200 else 1
        if args.cmd == "register":
            if args.pipeline not in pipelines:
                print(f"unknown pipeline {args.pipeline!r}; try `list`")
                return 2
            status, out = client.call("POST", "/pipelines", pipeline_definition(pipelines[args.pipeline]))
            print(f"register {args.pipeline}: HTTP {status} version={out.get('version') if out else None}")
            return 0 if status in (200, 201) else 1
        if args.cmd == "submit":
            first = parse_partition_date(args.partition)
            last = parse_partition_date(args.to) if args.to else first
            trigger = {"live": "schedule", "backfill": "backfill", "adhoc": "manual"}[args.priority]
            code, day = 0, first
            while day <= last:
                key = f"cli:{args.pipeline}:{day.isoformat()}:{args.priority}"
                body = {"idempotency_key": key, "pipeline": args.pipeline, "partition_key": day.isoformat(),
                        "trigger": trigger, "priority_class": args.priority}
                if args.deadline_in is not None or args.deadline_at is not None:
                    from datetime import datetime, timezone
                    dl = (datetime.fromtimestamp(args.deadline_at, timezone.utc) if args.deadline_at is not None
                          else datetime.now(timezone.utc) + timedelta(seconds=args.deadline_in))
                    body["deadline_at"] = dl.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    body["idempotency_key"] = f"{key}:{dl.timestamp():.0f}"
                status, out = client.call("POST", "/runs", body)
                print(f"submit {args.pipeline} {day}: HTTP {status} run={(out or {}).get('id') or out}")
                code |= 0 if status in (200, 201) else 1
                day += timedelta(days=1)
            return code
        worker_id = args.worker_id or f"{socket.gethostname()}-{os.getpid()}"

        def show(o):
            print(f"{o.pipeline}.{o.task} {o.partition_key} attempt={o.attempt_id[:8]} -> "
                  f"{o.reported} task={o.task_status} run={o.run_status} {o.detail[:120]}")
        n = work(client, pipelines, lake, worker_id, max_tasks=args.max_tasks,
                 exit_when_idle=args.exit_when_idle, lease_seconds=args.lease_seconds,
                 wait_seconds=0 if args.exit_when_idle else args.wait, on_outcome=show)
        print(f"worker {worker_id}: {n} task(s)")
        return 0
    except AtlasError as e:
        print(f"atlas: {e}")
        return 1


def schedule_report_text(rows: list[dict]) -> str:
    """Human table for GET /reports/schedule; lag counted only for fully-up days."""
    def hhmm(ts):
        return ts[11:16] if ts else "-"

    lines = [f"{'partition':<11} {'status':<10} {'fired':>5} {'done':>5} {'lag_min':>7} {'waits':>5} "
             f"{'published between (UTC)':<24} {'uptime':>7}  counted"]
    counted = []
    for r in rows:
        lag = r.get("lag_seconds")
        between = f"{hhmm(r.get('last_not_ready_at'))}-{hhmm(r.get('first_success_at'))}" if r.get("last_not_ready_at") else "-"
        ok = r["fully_up"] and r["status"] == "SUCCEEDED"
        if ok:
            counted.append(lag / 60)
        lines.append(f"{r['partition_key']:<11} {r['status']:<10} {hhmm(r['fired_at']):>5} {hhmm(r.get('finished_at')):>5} "
                     f"{(f'{lag / 60:.1f}' if lag is not None else '-'):>7} {r['not_ready_attempts']:>5} {between:<24} "
                     f"{r['uptime_coverage'] * 100:>6.0f}%  {'yes' if ok else 'no (downtime or not finished)'}")
    if counted:
        counted.sort()
        lines.append(f"{len(counted)} of {len(rows)} days counted (Atlas up the whole time); lag minutes: "
                     f"median {counted[len(counted) // 2]:.1f}, max {counted[-1]:.1f}")
    else:
        lines.append(f"0 of {len(rows)} days counted yet")
    return "\n".join(lines)


def atlas_pointers(client) -> list[dict]:
    """Every current pointer Atlas holds: only fenced, check-passing materializations (ADR-013)."""
    from atlas_pipelines.worker import AtlasError

    status, assets = client.call("GET", "/assets")
    if status != 200:
        raise AtlasError(status, assets, "GET /assets")
    out = []
    for a in assets or []:
        status, parts = client.call("GET", f"/assets/{a['key']}/partitions?limit=20000")
        if status != 200:
            raise AtlasError(status, parts, f"GET /assets/{a['key']}/partitions")
        for p in parts or []:
            if p.get("current"):
                out.append({"asset_key": a["key"], "partition_key": p["partition_key"], "uri": p["current"]["uri"],
                            "content_hash": p["current"]["content_hash"]})
    return out


UNREACHABLE_DAYS_TO_STOP = 2
UNREACHABLE_CLASSES = ("network", "server_error", "rate_limited")


def source_unreachable(result) -> str | None:
    """The error class if NOTHING in the run succeeded and every task that ran failed
    retryably because the source couldn't be reached (network, 5xx, 429); else None."""
    ran = [t for t in result.tasks if t.status != "CANCELED"]
    if not ran or any(t.status == "SUCCEEDED" for t in ran):
        return None
    classes = {t.error_class for t in ran}
    if all(t.retryable for t in ran) and classes <= set(UNREACHABLE_CLASSES):
        return "rate_limited" if "rate_limited" in classes else sorted(classes)[0]
    return None


def _run_one(pipeline, partition: str, lake: str):
    result = run_pipeline(pipeline, partition, lake)
    print(f"run {result.run_id} {partition}: {result.status}")
    for t in result.tasks:
        after = f" (after {t.retry_after_s:g}s)" if t.retry_after_s else ""
        retry = f" retryable{after}" if t.retryable else ""
        print(f"  {t.task:<16} {t.status:<16} {t.error_class or ''}{retry}")
        for m in t.materializations:
            print(f"    {m['asset_key']}  rows={m['row_count']}  {m['content_hash'][:23]}...")
            print(f"      {m['uri']}")
        for c in t.checks:
            print(f"    check {c['check_name']:<30} {c['severity']:<8} {'PASS' if c['passed'] else 'FAIL'}")
    print(f"manifest: {result.manifest_path}")
    return (0 if result.status == "SUCCEEDED" else 1), result


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
