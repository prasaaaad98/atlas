"""`load_entsoe_daily`: one `load_<zone>` task per ENTSO-E zone per local delivery day (B4a).

Why it exists: turns config/markets/entsoe.yaml into a pipeline that fetches the actual total
load (6.1.A) and the day-ahead load forecast (6.1.B) for a zone and day, stores both raw
documents in bronze, and writes silver.demand__entsoe_<zone> (actual + forecast vintages).
Who calls it: defs/__init__.py; tests inject `opener` and `now`.
Retry rules (outcome classes, CLAUDE.md): network / 5xx (ENTSO-E's gateway answers 599 when it
times out) / 429 → RetryableTaskError; "no matching data" or an incomplete day before the
registry's late_after_hours_from_day_end → RetryableTaskError("source_not_ready"); after it,
blocking. Everything else that fails a check is blocking (won't fix itself).
Data use: development, testing and internal research only (docs/data/sources/entsoe.md).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

import polars as pl

from atlas_pipelines.core import vintages
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter, http_get
from atlas_pipelines.core.lake import absolute_path, attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, RetryableTaskError, TaskContext
from atlas_pipelines.defs.prices import TOKEN_ENV, _limiter, base_url
from atlas_pipelines.sources.adapter import write_bronze
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import load
from atlas_pipelines.sources.entsoe import requests as er
from atlas_pipelines.validation import stats

ITEM = {"actual": "load_actual", "forecast": "load_forecast_da"}
COMPLETENESS = ("day_complete", "series_for_zone")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def load_section(market: Market) -> dict:
    sec = ((market.raw or {}).get("entsoe") or {}).get("load") or {}
    if "late_after_hours_from_day_end" not in (sec.get("publication") or {}):
        raise ValueError(f"{market.source_file}: entsoe.load.publication.late_after_hours_from_day_end missing")
    return sec


def kinds_due(market: Market, day: date, now: datetime) -> tuple[str, ...]:
    """Point in time (§3.4a): before the delivery day starts only its day-ahead forecast can
    exist, so a run then fetches the forecast alone (the vintage a decision can use); actuals
    come with a run after the day (the schedule's re-fetches)."""
    return ("forecast",) if now < market.day_bounds_utc(day)[0] else ("actual", "forecast")


def make_load_task(market: Market, zone: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
                   limiter: RateLimiter | None = None) -> TaskDef:
    sec = load_section(market)
    eic = da.series_rule(market, zone).eic
    slug = zone.lower()
    ds = f"demand__{market.id}_{slug}"
    availability = (sec.get("forecast_availability") or {}).get("rule")

    def ingest(ctx: TaskContext):
        token = os.environ.get(TOKEN_ENV)
        if not token:
            raise RuntimeError(f"{TOKEN_ENV} is not set (put it in .env; never commit it)")
        day = date.fromisoformat(ctx.partition_key)
        deadline = market.day_bounds_utc(day)[1] + timedelta(hours=float(sec["publication"]["late_after_hours_from_day_end"]))
        due = kinds_due(market, day, now())
        mats, results, frames, waiting = [], [], [], []
        results.append(CheckResult("kinds_fetched", "warn", True, {"fetched": list(due), "rule": "before the day: forecast only"}))
        for kind, item in ITEM.items():
            if kind not in due:
                continue
            params = {**er.request_params(item, market, eic, day), "securityToken": token}
            res = http_get(base_url(), params, limiter=limiter or _limiter("entsoe"), secret_params=("securityToken",),
                           **({"opener": opener} if opener else {}))
            if res.outcome == "unknown":
                raise RetryableTaskError("network", f"entsoe load {zone} {kind}: {res.detail}", materializations=mats,
                                         checks=results)
            fetched_at = now()
            b = write_bronze(ctx, source="entsoe", dataset=f"{item}__{slug}", partition_field="date", filename="raw.xml",
                             data=res.body, fetched_at=fetched_at, source_url=res.url_redacted, http_status=res.status,
                             code_version=load.CODE_VERSION)
            mats.append(b.materialization)
            results += [CheckResult(f"{c.check_name}_{kind}", c.severity, c.passed, c.metrics) for c in b.checks]
            if res.error_class in ("rate_limited", "server_error"):
                raise RetryableTaskError(res.error_class, f"entsoe load {zone} {kind}: HTTP {res.status}",
                                         retry_after_s=res.retry_after_s, materializations=mats, checks=results)
            if res.status != 200:
                return mats, results
            try:
                doc = load.parse_document(res.body)
            except da.ParseError as e:
                return mats, results + [CheckResult(f"document_parses_{kind}", "blocking", False, {"error": str(e)})]
            if isinstance(doc, da.Acknowledgement):
                if doc.no_data:
                    waiting.append(f"{kind}: {doc.text}")
                    continue
                return mats, results + [CheckResult(f"document_parses_{kind}", "blocking", False,
                                                    {"acknowledgement": doc.code, "text": doc.text})]
            df = load.build_silver(doc, kind, market, zone, day, fetched_at, b.materialization.uri, availability)
            cs = load.checks(df, doc, kind, market, zone, day)
            bad = [c.check_name for c in cs if c.severity == "blocking" and not c.passed]
            if bad and set(bad) <= set(COMPLETENESS):
                waiting.append(f"{kind}: incomplete ({df.height} intervals)")
            results += [CheckResult(f"{c.check_name}_{kind}", c.severity, c.passed, c.metrics) for c in cs]
            frames.append(df)
        if waiting:
            if now() < deadline:
                raise RetryableTaskError("source_not_ready", f"entsoe load {zone} {ctx.partition_key}: {waiting}",
                                         retry_after_s=1800, materializations=mats, checks=results)
            return mats, results + [CheckResult("load_published", "blocking", False,
                                                {"waiting": waiting, "deadline_utc": iso_utc(deadline)})]
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        cur = ctx.resolve_input(f"silver.{ds}", ctx.partition_key)
        previous = pl.read_parquet(absolute_path(ctx.lake_root, cur["uri"])) if cur else None
        demand = vintages.carry_vintages(pl.concat(frames), previous, due)
        vcfg = stats.load_config()
        history = [h.filter(pl.col("kind") == "actual")
                   for h in stats.trailing_frames(ctx, f"silver.{ds}", day, vcfg["trailing"]["window_days"])]
        results += [vintages.vintages_superset_check(demand, previous),
                    stats.outlier_check("demand_outliers", demand.filter(pl.col("kind") == "actual"), "demand_mw",
                                        history, vcfg)]
        out = attempt_dir(ctx.lake_root, "silver", ds, "date", ctx.partition_key, ctx.attempt_id) / "part-0.parquet"
        write_parquet_atomic(demand, out)
        mats.append(Materialization(f"silver.{ds}", ctx.partition_key, relative_uri(ctx.lake_root, out), demand.height,
                                    sha256_file(out), "demand-1", load.CODE_VERSION))
        return mats, results

    return TaskDef(f"load_{slug}", ingest, asset_outputs=(f"bronze.entsoe_load_actual__{slug}",
                                                          f"bronze.entsoe_load_forecast_da__{slug}", f"silver.{ds}"),
                   max_attempts=8, limit_key="entsoe",
                   retry={"initial_interval_seconds": 300, "backoff_coefficient": 2, "max_interval_seconds": 3600})


def load_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "load" not in ((market.raw or {}).get("entsoe") or {}):
        return []
    return [Pipeline("load_entsoe_daily", market.timezone,
                     tuple(make_load_task(market, z, **task_kwargs) for z in market.zones),
                     description="ENTSO-E actual total load + day-ahead load forecast per zone (internal research only)")]
