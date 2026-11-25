"""Calendar pipeline: `calendar_build`, one `build_<market>` task per market with a calendar.

Why it exists: silver.calendar for every market and zone, per year (PHASE_AB_PLAN.md
§3.5: yearly, and on any change to config/calendar/* or the `holidays` version).
The partition key is the year's first day (YYYY-01-01); the engine's partitions are
daily keys, and a calendar partition covers that whole year. No network.
Assets per market (`silver.calendar__<market>`), unioned by the catalog.
Who calls it: defs/__init__.py.
"""

from __future__ import annotations

from datetime import date

import holidays

from atlas_pipelines.core import config
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.lake import attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, TaskContext
from atlas_pipelines.sources.calendar import build as cal


def make_task(market: Market) -> TaskDef:
    dataset = f"calendar__{market.id}"

    def run(ctx: TaskContext):
        d = date.fromisoformat(ctx.partition_key)
        if (d.month, d.day) != (1, 1):
            return [], [CheckResult("partition_is_a_year", "blocking", False,
                                    {"partition": ctx.partition_key, "expected": f"{d.year}-01-01"})]
        zcs, _ = cal.zone_calendars(market)
        special, file_hash = cal.load_special_days(config.REPO_ROOT / "config" / "calendar" / f"{market.id}.yaml",
                                                   [z.zone for z in zcs])
        provenance = f"holidays=={holidays.__version__}; config/calendar/{market.id}.yaml {file_hash}"
        df = cal.build(market, d.year, special, provenance)
        out = attempt_dir(ctx.lake_root, "silver", dataset, "year", ctx.partition_key, ctx.attempt_id)
        path = out / "part-0.parquet"
        write_parquet_atomic(df, path)
        mat = Materialization(f"silver.{dataset}", ctx.partition_key, relative_uri(ctx.lake_root, path), df.height,
                              sha256_file(path), cal.SCHEMA_VERSION, provenance)
        return [mat], cal.checks(df, market, d.year)

    return TaskDef(f"build_{market.id}", run, asset_outputs=(f"silver.{dataset}",))


def calendar_pipelines(registry: dict[str, Market]) -> list[Pipeline]:
    markets = [m for m in registry.values() if (m.raw or {}).get("calendar")]
    if not markets:
        return []
    return [Pipeline("calendar_build", "UTC", tuple(make_task(m) for m in sorted(markets, key=lambda m: m.id)),
                     description="Per-market calendar: holidays, special days, DST days (partition = YYYY-01-01)")]
