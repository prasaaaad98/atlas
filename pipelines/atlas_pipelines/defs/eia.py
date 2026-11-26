"""EIA pipelines. `eia930_region_daily`: one `ingest_<ba>` task per US balancing authority (E2).

Why it exists: turns config/markets/us_ba.yaml into a pipeline the local runner and
Atlas run per UTC day. Per BA, two tasks:
  ingest_<ba>       fetches EIA-930 region-data (limit key `eia`, key from EIA_API_KEY,
                    sent as a header), stores every page in bronze, writes
                    silver.demand__us_ba_<ba> (actuals + forecast vintages);
  interchange_<ba>  (after ingest_<ba>) re-parses that bronze file (no second request)
                    into silver.interchange__us_ba_<ba>, with the balance-identity report.
Demand and interchange are gated separately: EIA-930 interchange is often incomplete
on days whose demand is complete. One BA failing never blocks another.

Retry rules (outcome classes, CLAUDE.md; EIA's gateway is often slow, see eia.md):
  - network failure / 5xx / 429 -> RetryableTaskError (backoff by Atlas, key paused on 429);
  - 401/403/400, bad responses, truncation, duplicates, non-numeric values -> blocking;
  - missing hours only, before the publication deadline (registry policy) ->
    RetryableTaskError("source_not_ready"); after it, blocking.
Revisions: actual values that changed against the current silver -> `source_revision` (warn).
Who calls it: defs/__init__.py; tests inject `opener` and `now`.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import date, datetime, timezone

import polars as pl

from atlas_pipelines.core import config
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.lake import absolute_path, attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.limits import load_limit
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, RetryableTaskError, TaskContext
from atlas_pipelines.gold import fundamentals
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.eia import client, fuel, gas, region, storage

KEY_ENV = "EIA_API_KEY"
RETRY = {"initial_interval_seconds": 60, "backoff_coefficient": 2, "max_interval_seconds": 1800}
_limiters: dict[str, RateLimiter] = {}


def _limiter() -> RateLimiter:
    if "eia" not in _limiters:  # one per process, shared by all BAs
        _limiters["eia"] = RateLimiter(load_limit("eia", config.REPO_ROOT / "config" / "limits").rpm)
    return _limiters["eia"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _current_frame(ctx: TaskContext, asset: str) -> pl.DataFrame | None:
    cur = ctx.resolve_input(asset, ctx.partition_key)
    return pl.read_parquet(absolute_path(ctx.lake_root, cur["uri"])) if cur else None


def _write(ctx: TaskContext, df: pl.DataFrame, dataset: str, schema_version: str) -> Materialization:
    out = attempt_dir(ctx.lake_root, "silver", dataset, "date", ctx.partition_key, ctx.attempt_id)
    path = out / "part-0.parquet"
    write_parquet_atomic(df, path)
    return Materialization(f"silver.{dataset}", ctx.partition_key, relative_uri(ctx.lake_root, path), df.height,
                           sha256_file(path), schema_version, region.CODE_VERSION)


def revision_check(new: pl.DataFrame, previous: pl.DataFrame | None) -> CheckResult:
    """Warn when an actual value changed since the current silver (EIA-930 is revised)."""
    if previous is None or previous.is_empty():
        return CheckResult("source_revision", "warn", True, {"previous": None})
    key = ["interval_start_utc"]
    a = new.filter(pl.col("kind") == "actual").select(key + ["demand_mw"])
    b = previous.filter(pl.col("kind") == "actual").select(key + [pl.col("demand_mw").alias("previous_mw")])
    changed = a.join(b, on=key).filter(pl.col("demand_mw").ne_missing(pl.col("previous_mw")))
    return CheckResult("source_revision", "warn", changed.is_empty(),
                       {"changed_hours": changed.height,
                        "first10": [iso_utc(t) for t in changed["interval_start_utc"].to_list()[:10]]})


def make_region_task(market: Market, ba: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
                     limiter: RateLimiter | None = None) -> TaskDef:
    region.eia_section(market)  # fail at definition time on a bad registry
    slug = ba.lower()
    bronze_asset = f"bronze.eia_region_data__{slug}"
    demand_ds, inter_ds = f"demand__{market.id}_{slug}", f"interchange__{market.id}_{slug}"

    def ingest(ctx: TaskContext):
        key = os.environ.get(KEY_ENV)
        if not key:
            raise RuntimeError(f"{KEY_ENV} is not set (put it in .env; never commit it)")
        day = date.fromisoformat(ctx.partition_key)
        f = client.fetch(region.ROUTE, region.request_params(ba, day), key=key, limiter=limiter or _limiter(),
                         sort=region.KEY_COLUMNS, opener=opener)
        fetched_at = now()
        b = client.write_fetch(ctx, f, f"region_data__{slug}", fetched_at, region.CODE_VERSION)
        mats = [b.materialization]
        results = list(b.checks) + client.checks(f, region.KEY_COLUMNS)
        if f.outcome == "unknown" or f.error_class in ("rate_limited", "server_error"):
            raise RetryableTaskError(f.error_class or "network", f"eia {ba} {ctx.partition_key}: {f.stopped}",
                                     retry_after_s=next((p.retry_after_s for p in f.pages if p.retry_after_s), None),
                                     materializations=mats, checks=results)
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results  # auth, bad request, truncation, duplicates, non-numeric: won't fix itself

        demand, _, _ = region.parse(f.rows, market, ba, day, fetched_at, b.materialization.uri)
        previous = _current_frame(ctx, f"silver.{demand_ds}")
        demand = region.carry_vintages(demand, previous)
        data_checks = region.demand_checks(demand, f.rows, day, previous) + [revision_check(demand, previous)]
        if region.incomplete_only(data_checks) and now() < region.publication_deadline(market, day):
            raise RetryableTaskError("source_not_ready", f"eia {ba} {ctx.partition_key}: hours still missing",
                                     retry_after_s=1800, materializations=mats, checks=results + data_checks)
        mats.append(_write(ctx, demand, demand_ds, "demand-1"))
        return mats, results + data_checks

    return TaskDef(f"ingest_{slug}", ingest, asset_outputs=(bronze_asset, f"silver.{demand_ds}"),
                   max_attempts=8, limit_key="eia", retry=RETRY)


def make_interchange_task(market: Market, ba: str, ingest: TaskDef, *, now: Callable[[], datetime] = _utcnow) -> TaskDef:
    slug = ba.lower()
    bronze_asset = f"bronze.eia_region_data__{slug}"
    inter_ds = f"interchange__{market.id}_{slug}"

    def build(ctx: TaskContext):
        day = date.fromisoformat(ctx.partition_key)
        cur = ctx.resolve_input(bronze_asset, ctx.partition_key)
        if cur is None:
            return [], [CheckResult("input_current", "blocking", False, {"asset": bronze_asset})]
        pages = client.read_bronze(absolute_path(ctx.lake_root, cur["uri"]).read_bytes())
        rows = [r for p in pages for r in json.loads(p["body"])["response"]["data"]]
        # The bronze path records the fetch time (core/lake.py: fetched_at=20261123T113005Z).
        seg = cur["uri"].split("fetched_at=")[1].split("/")[0] if "fetched_at=" in cur["uri"] else None
        fetched_at = datetime.strptime(seg, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc) if seg else now()
        demand, inter, ng = region.parse(rows, market, ba, day, fetched_at, cur["uri"])
        results = region.interchange_checks(demand, inter, ng, market, day)
        if region.incomplete_only(results) and now() < region.publication_deadline(market, day):
            raise RetryableTaskError("source_not_ready", f"eia {ba} {ctx.partition_key}: interchange hours missing",
                                     retry_after_s=1800, checks=results)
        return [_write(ctx, inter, inter_ds, "interchange-1")], results

    return TaskDef(f"interchange_{slug}", build, deps=(ingest.name,), asset_outputs=(f"silver.{inter_ds}",),
                   max_attempts=8, retry=RETRY)


def generation_revision_check(new: pl.DataFrame, previous: pl.DataFrame | None) -> CheckResult:
    """Warn when a value changed since the current silver (EIA-930 is revised)."""
    if previous is None or previous.is_empty():
        return CheckResult("source_revision", "warn", True, {"previous": None})
    key = ["interval_start_utc", "source_fuel_code"]
    changed = new.select(key + ["generation_mw"]).join(
        previous.select(key + [pl.col("generation_mw").alias("previous_mw")]), on=key
    ).filter(pl.col("generation_mw").ne_missing(pl.col("previous_mw")))
    return CheckResult("source_revision", "warn", changed.is_empty(), {"changed_values": changed.height})


def make_fuel_task(market: Market, ba: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
                   limiter: RateLimiter | None = None) -> TaskDef:
    """E3: EIA-930 fuel-type-data for one BA and UTC day -> silver.generation__us_ba_<ba>."""
    region.eia_section(market)
    mapping, _ = fuels.load_mapping("eia930")  # fail at definition time on a bad mapping
    slug = ba.lower()
    bronze_asset, gen_ds = f"bronze.eia_fuel_type_data__{slug}", f"generation__{market.id}_{slug}"

    def ingest(ctx: TaskContext):
        key = os.environ.get(KEY_ENV)
        if not key:
            raise RuntimeError(f"{KEY_ENV} is not set (put it in .env; never commit it)")
        day = date.fromisoformat(ctx.partition_key)
        f = client.fetch(fuel.ROUTE, fuel.request_params(ba, day), key=key, limiter=limiter or _limiter(),
                         sort=fuel.KEY_COLUMNS, opener=opener)
        fetched_at = now()
        b = client.write_fetch(ctx, f, f"fuel_type_data__{slug}", fetched_at, fuel.CODE_VERSION)
        mats = [b.materialization]
        results = list(b.checks) + client.checks(f, fuel.KEY_COLUMNS)
        if f.outcome == "unknown" or f.error_class in ("rate_limited", "server_error"):
            raise RetryableTaskError(f.error_class or "network", f"eia fuel {ba} {ctx.partition_key}: {f.stopped}",
                                     materializations=mats, checks=results)
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        gen, unknown = fuel.parse(f.rows, market, ba, day, fetched_at, b.materialization.uri, mapping)
        previous = _current_frame(ctx, f"silver.{gen_ds}")
        data_checks = fuel.checks(gen, f.rows, unknown, day, mapping) + [generation_revision_check(gen, previous)]
        if fuel.incomplete_only(data_checks) and now() < region.publication_deadline(market, day):
            raise RetryableTaskError("source_not_ready", f"eia fuel {ba} {ctx.partition_key}: hours still missing",
                                     retry_after_s=1800, materializations=mats, checks=results + data_checks)
        mats.append(_write(ctx, gen, gen_ds, "generation-1"))
        return mats, results + data_checks

    return TaskDef(f"ingest_{slug}", ingest, asset_outputs=(bronze_asset, f"silver.{gen_ds}"),
                   max_attempts=8, limit_key="eia", retry=RETRY)


def make_gas_task(series_id: str, spec: dict, *, opener=None, now: Callable[[], datetime] = _utcnow,
                  limiter: RateLimiter | None = None) -> TaskDef:
    """E4: one EIA gas price series for one trade month -> silver.fuel_carbon_prices__<hub>_<period>."""
    slug = series_id.lower()
    bronze_asset = f"bronze.eia_gas__{slug}"
    ds = f"fuel_carbon_prices__{spec['hub'].lower()}_{spec['delivery_period']}"

    def ingest(ctx: TaskContext):
        key = os.environ.get(KEY_ENV)
        if not key:
            raise RuntimeError(f"{KEY_ENV} is not set (put it in .env; never commit it)")
        part = date.fromisoformat(ctx.partition_key)
        try:
            params = gas.request_params(series_id, spec, part)
        except ValueError as e:
            return [], [CheckResult("partition_is_a_month", "blocking", False, {"error": str(e)})]
        f = client.fetch(spec["route"], params, key=key, limiter=limiter or _limiter(), sort=gas.KEY_COLUMNS,
                         opener=opener)
        fetched_at = now()
        b = client.write_fetch(ctx, f, f"gas__{slug}", fetched_at, gas.CODE_VERSION)
        mats = [b.materialization]
        results = list(b.checks) + client.checks(f, gas.KEY_COLUMNS)
        if f.outcome == "unknown" or f.error_class in ("rate_limited", "server_error"):
            raise RetryableTaskError(f.error_class or "network", f"eia {series_id} {ctx.partition_key}: {f.stopped}",
                                     materializations=mats, checks=results)
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        df = gas.parse(f.rows, series_id, spec, part, fetched_at, b.materialization.uri)
        data_checks = gas.checks(df, f.rows, spec, part, fetched_at.date())
        mats.append(_write(ctx, df, ds, "fuel_carbon_prices-1"))
        return mats, results + data_checks

    return TaskDef(f"ingest_{slug}", ingest, asset_outputs=(bronze_asset, f"silver.{ds}"),
                   max_attempts=8, limit_key="eia", retry=RETRY)


def make_storage_task(market: Market, *, opener=None, now: Callable[[], datetime] = _utcnow,
                      limiter: RateLimiter | None = None) -> TaskDef:
    """E5: EIA generator inventory, storage only, one report month -> silver.storage_capacity__<market>."""
    cfg = storage.load_config()  # fail at definition time on a bad config
    ds = f"storage_capacity__{market.id}"

    def ingest(ctx: TaskContext):
        key = os.environ.get(KEY_ENV)
        if not key:
            raise RuntimeError(f"{KEY_ENV} is not set (put it in .env; never commit it)")
        part = date.fromisoformat(ctx.partition_key)
        try:
            params = storage.request_params(cfg, part)
        except ValueError as e:
            return [], [CheckResult("partition_is_a_month", "blocking", False, {"error": str(e)})]
        f = client.fetch(cfg["route"], params, key=key, limiter=limiter or _limiter(), sort=storage.KEY_COLUMNS,
                         opener=opener)
        fetched_at = now()
        b = client.write_fetch(ctx, f, "generator_capacity__storage", fetched_at, storage.CODE_VERSION)
        mats = [b.materialization]
        # The route has no `value` column, so only the fetch-level checks apply (not values_*).
        results = list(b.checks) + [c for c in client.checks(f, storage.KEY_COLUMNS)
                                    if not c.check_name.startswith("values_")]
        if f.outcome == "unknown" or f.error_class in ("rate_limited", "server_error"):
            raise RetryableTaskError(f.error_class or "network", f"eia storage {ctx.partition_key}: {f.stopped}",
                                     materializations=mats, checks=results)
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        df, problems = storage.parse(f.rows, cfg, market.id, part, fetched_at, b.materialization.uri)
        data_checks = storage.checks(df, f.rows, cfg, problems, part)
        mats.append(_write(ctx, df, ds, "storage_capacity-1"))
        return mats, results + data_checks

    return TaskDef("ingest_storage", ingest, asset_outputs=("bronze.eia_generator_capacity__storage", f"silver.{ds}"),
                   max_attempts=8, limit_key="eia", retry=RETRY)


def _verified_input(ctx: TaskContext, asset: str) -> tuple[pl.DataFrame | None, str | None, CheckResult | None]:
    """The current silver partition of `asset`, read only if its bytes match the pointer's hash."""
    cur = ctx.resolve_input(asset, ctx.partition_key)
    if cur is None:
        return None, None, None
    path = absolute_path(ctx.lake_root, cur["uri"])
    actual = sha256_file(path)
    if actual != cur["content_hash"]:
        return None, None, CheckResult("input_hash_matches_pointer", "blocking", False,
                                       {"asset": asset, "uri": cur["uri"], "pointer": cur["content_hash"], "file": actual})
    return pl.read_parquet(path), actual, None


def _write_gold(ctx: TaskContext, df: pl.DataFrame, dataset: str, schema_version: str, code_version: str) -> Materialization:
    out = attempt_dir(ctx.lake_root, "gold", dataset, "date", ctx.partition_key, ctx.attempt_id)
    path = out / "part-0.parquet"
    write_parquet_atomic(df, path)
    return Materialization(f"gold.{dataset}", ctx.partition_key, relative_uri(ctx.lake_root, path), df.height,
                           sha256_file(path), schema_version, code_version)


def make_fundamentals_task(market: Market, ba: str, *, now: Callable[[], datetime] = _utcnow) -> TaskDef:
    """E7: silver demand + generation (+ interchange) for one BA and UTC day -> gold.fundamentals_hourly__us_ba_<ba>.
    Reads only silver (no source request, no limit key). Missing inputs are retried while the
    day is recent (they may still be ingesting), then block."""
    defs = fundamentals.load_definitions()
    slug = ba.lower()
    assets = {k: f"silver.{k}__{market.id}_{slug}" for k in ("demand", "generation", "interchange")}
    ds = f"fundamentals_hourly__{market.id}_{slug}"

    def build(ctx: TaskContext):
        frames, hashes, results = {}, {}, []
        for k, asset in assets.items():
            frames[k], hashes[k], bad = _verified_input(ctx, asset)
            if bad:
                return [], [bad]
        missing = [assets[k] for k in ("demand", "generation") if frames[k] is None]
        if missing:
            if now() < region.publication_deadline(market, date.fromisoformat(ctx.partition_key)):
                raise RetryableTaskError("source_not_ready", f"gold {ba} {ctx.partition_key}: waiting for {missing}",
                                         retry_after_s=1800)
            return [], [CheckResult("inputs_current", "blocking", False, {"missing": missing})]
        df, results = fundamentals.hourly(frames["demand"], frames["generation"], frames["interchange"], defs, hashes)
        if df.is_empty():
            return [], results
        return [_write_gold(ctx, df, ds, "fundamentals_hourly-1", fundamentals.DEFINITION_VERSION)], results

    return TaskDef(f"fundamentals_{slug}", build, asset_outputs=(f"gold.{ds}",), max_attempts=8, retry=RETRY)


def make_buildout_task(market: Market, ingest: TaskDef) -> TaskDef:
    """E7: one report month of silver.storage_capacity -> gold.storage_buildout__<market> (per BA)."""
    silver_asset, ds = f"silver.storage_capacity__{market.id}", f"storage_buildout__{market.id}"

    def build(ctx: TaskContext):
        df, digest, bad = _verified_input(ctx, silver_asset)
        if bad or df is None:
            return [], [bad or CheckResult("input_current", "blocking", False, {"asset": silver_asset})]
        gold, results = fundamentals.buildout(df, digest)
        return [_write_gold(ctx, gold, ds, "storage_buildout-1", fundamentals.BUILDOUT_VERSION)], results

    return TaskDef("buildout_storage", build, deps=(ingest.name,), asset_outputs=(f"gold.{ds}",))


def eia_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("us_ba")
    if market is None or "eia" not in (market.raw or {}):
        return []
    ingests = [make_region_task(market, ba, **task_kwargs) for ba in market.zones]
    inters = [make_interchange_task(market, ba, t, **({"now": task_kwargs["now"]} if "now" in task_kwargs else {}))
              for ba, t in zip(market.zones, ingests)]
    fuel_tasks = [make_fuel_task(market, ba, **task_kwargs) for ba in market.zones]
    storage_ingest = make_storage_task(market, **task_kwargs)
    return [Pipeline("eia930_region_daily", "UTC", tuple(ingests + inters),
                     description="EIA-930 demand, day-ahead forecast and net interchange per US BA (UTC days)"),
            Pipeline("eia930_fuel_daily", "UTC", tuple(fuel_tasks),
                     description="EIA-930 net generation by fuel per US BA (UTC days)"),
            Pipeline("eia_henry_hub_monthly", "UTC",
                     tuple(make_gas_task(sid, spec, **task_kwargs) for sid, spec in gas.load_series().items()),
                     description="EIA Henry Hub daily spot price per trade month (partition = YYYY-MM-01)"),
            Pipeline("eia_storage_monthly", "UTC", (storage_ingest, make_buildout_task(market, storage_ingest)),
                     description="EIA generator inventory, energy storage, per report month (partition = YYYY-MM-01)"),
            Pipeline("eia930_gold_daily", "UTC",
                     tuple(make_fundamentals_task(market, ba, **({"now": task_kwargs["now"]} if "now" in task_kwargs else {}))
                           for ba in market.zones),
                     description="Gold fundamentals per US BA and UTC hour, from EIA silver only (E7)")]
