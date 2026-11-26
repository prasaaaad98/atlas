"""`entsoe_gold_daily`: one `net_load_<zone>` task per ENTSO-E zone and local day (ADR-B10).

Why it exists: builds gold.net_load__entsoe_<zone> and gold.generation_mix__entsoe_<zone> from the
CURRENT silver demand and generation only (no source request, no limit key). Inputs are read only
if their bytes match the pointer's hash. A missing silver partition is retried while the day is
recent (silver may still be ingesting), then blocks. Re-run after a silver re-fetch to rebuild.
Who calls it: defs/__init__.py; the schedule in config/schedules/entsoe.yaml.
Data use: development, testing and internal research only (docs/data/sources/entsoe.md).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, RetryableTaskError, TaskContext
from atlas_pipelines.defs.eia import _verified_input, _write_gold
from atlas_pipelines.defs.entsoe_load import load_section
from atlas_pipelines.gold import net_load
from atlas_pipelines.sources.entsoe import generation as gen

CODE_VERSION = "entsoe_gold-1"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_net_load_task(market: Market, zone: str, *, now: Callable[[], datetime] = _utcnow) -> TaskDef:
    defs = net_load.load_definitions()
    sec = load_section(market)
    required = gen.required_types(market, "forecast", zone)  # the zone's wind/solar (config/gold/net_load.yaml)
    slug = zone.lower()
    assets = {k: f"silver.{k}__{market.id}_{slug}" for k in ("demand", "generation")}
    out = {k: f"{k}__{market.id}_{slug}" for k in ("net_load", "generation_mix")}

    def build(ctx: TaskContext):
        frames, hashes = {}, {}
        for k, asset in assets.items():
            frames[k], hashes[k], bad = _verified_input(ctx, asset)
            if bad:
                return [], [bad]
        day = date.fromisoformat(ctx.partition_key)
        missing = [assets[k] for k in assets if frames[k] is None]
        if missing:
            deadline = market.day_bounds_utc(day)[1] + timedelta(hours=float(sec["publication"]["late_after_hours_from_day_end"]))
            if now() < deadline:
                raise RetryableTaskError("source_not_ready", f"gold {zone} {ctx.partition_key}: waiting for {missing}",
                                         retry_after_s=1800)
            return [], [CheckResult("inputs_current", "blocking", False, {"missing": missing})]
        nl, mix, results = net_load.build(frames["demand"], frames["generation"], market, zone, day, required, defs, hashes)
        if any(c.severity == "blocking" and not c.passed for c in results):
            return [], results
        return [_write_gold(ctx, nl, out["net_load"], "net_load-1", CODE_VERSION),
                _write_gold(ctx, mix, out["generation_mix"], "generation_mix-1", CODE_VERSION)], results

    return TaskDef(f"net_load_{slug}", build, asset_outputs=tuple(f"gold.{v}" for v in out.values()),
                   max_attempts=8, retry={"initial_interval_seconds": 300, "backoff_coefficient": 2, "max_interval_seconds": 3600})


def gold_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "generation" not in ((market.raw or {}).get("entsoe") or {}):
        return []
    return [Pipeline("entsoe_gold_daily", market.timezone,
                     tuple(make_net_load_task(market, z, **task_kwargs) for z in market.zones),
                     description="ENTSO-E gold: net load (actual + day-ahead) and generation by fuel group (internal research only)")]
