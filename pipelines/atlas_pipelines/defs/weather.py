"""Weather pipelines: `weather_<market>_daily`, one ingest task per location (fan-out).

Why it exists: turns config/locations/openmeteo.yaml into pipelines the local
runner (and later Atlas) can run per delivery day. The partition is the local
day in the market's time zone, so weather rows line up with that market's prices.
Each task fetches one location (through the `openmeteo` limit key), stores the
raw response in bronze, parses it to silver.weather_hourly and runs the checks.
Assets are per location (`silver.weather_hourly__<market>_<zone>`), so one
location failing a check never blocks another; the catalog unions them into
one `silver.weather_hourly` view.
Who calls it: defs/__init__.py.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from atlas_pipelines.core import config
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.lake import absolute_path, attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.limits import load_limit
from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import Materialization, TaskContext
from atlas_pipelines.sources.openmeteo import archive

_limiters: dict[str, RateLimiter] = {}


def _limiter(key: str) -> RateLimiter:
    # One limiter per key per process, shared by all tasks in a run.
    if key not in _limiters:
        _limiters[key] = RateLimiter(load_limit(key, config.REPO_ROOT / "config" / "limits").rpm)
    return _limiters[key]


def make_task(loc: archive.Location, market: Market):
    silver_dataset = f"weather_hourly__{loc.slug}"

    def ingest(ctx: TaskContext):
        bronze, res = archive.fetch(ctx, loc, market, limiter=_limiter("openmeteo"))
        checks = archive.bronze_checks(bronze, res.body)
        mats = [bronze.materialization]
        if not all(c.passed for c in checks if c.severity == "blocking"):
            return mats, checks  # raw kept for diagnosis; runner blocks the pointer
        local_date = date.fromisoformat(ctx.partition_key)
        df = archive.parse(res.body, loc, market, local_date, bronze.materialization.uri)
        out = attempt_dir(ctx.lake_root, "silver", silver_dataset, "date", ctx.partition_key, ctx.attempt_id)
        path = out / "part-0.parquet"
        write_parquet_atomic(df, path)
        raw = absolute_path(ctx.lake_root, bronze.materialization.uri).read_bytes()
        checks += archive.checks(df, raw, market, local_date)
        mats.append(
            Materialization(
                asset_key=f"silver.{silver_dataset}",
                partition_key=ctx.partition_key,
                uri=relative_uri(ctx.lake_root, path),
                row_count=df.height,
                content_hash=sha256_file(path),
                schema_version=archive.SCHEMA_VERSION,
                code_version=archive.CODE_VERSION,
            )
        )
        return mats, checks

    outputs = (f"bronze.openmeteo_archive_hourly__{loc.slug}", f"silver.{silver_dataset}")
    return TaskDef(f"ingest_{loc.zone.lower()}", ingest, asset_outputs=outputs)


def weather_pipelines(registry: dict[str, Market], locations_file: Path | None = None) -> list[Pipeline]:
    path = locations_file or config.REPO_ROOT / "config" / "locations" / "openmeteo.yaml"
    if not path.exists():
        return []
    locations = archive.load_locations(path, registry, only_loaded_markets=True)
    out = []
    for market_id in sorted({loc.market for loc in locations}):
        market = registry[market_id]
        tasks = tuple(make_task(loc, market) for loc in locations if loc.market == market_id)
        out.append(
            Pipeline(
                name=f"weather_{market_id}_daily",
                partition_tz=market.timezone,
                tasks=tasks,
                description=f"Open-Meteo archive weather for {market.name} zones (non-commercial testing only)",
            )
        )
    return out
