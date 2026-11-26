"""`generation_entsoe_daily`: one `generation_<zone>` task per ENTSO-E zone and local day (B4b).

Why it exists: fetches actual generation per production type (16.1.B&C, A75/A16) and the
wind/solar day-ahead forecast (14.1.D, A69/A01), stores both raw documents in bronze and writes
silver.generation__entsoe_<zone> (actuals + forecast vintages). Network/5xx/429 are retried.
"No matching data" (a whole document absent) is retried until the registry's
late_after_hours_from_day_end; after that, whatever was sent publishes and the absent required
series are recorded. A series with a gap does NOT hold the day back (ADR-B10): it publishes
with series_complete = false, missing intervals stay missing, and the scheduled re-fetch
(config/schedules/entsoe.yaml) brings late intervals in as a new vintage. Real gaps exist
(2025-10-26 fall-back day, see the golden tests).
Who calls it: defs/__init__.py; tests inject `opener` and `now`.
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
from atlas_pipelines.defs.entsoe_load import load_section
from atlas_pipelines.defs.prices import TOKEN_ENV, _limiter, base_url
from atlas_pipelines.sources import fuels
from atlas_pipelines.sources.adapter import write_bronze
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import generation as gen
from atlas_pipelines.sources.entsoe import load
from atlas_pipelines.sources.entsoe import requests as er

ITEM = {"actual": "generation_actual", "forecast": "wind_solar_forecast_da"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_generation_task(market: Market, zone: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
                         limiter: RateLimiter | None = None) -> TaskDef:
    sec = load_section(market)  # same publication policy and availability rule as load
    gen.generation_section(market)  # fail at definition time on a bad required list
    mapping, _ = fuels.load_mapping("entsoe")  # fail at definition time on a bad mapping
    eic = da.series_rule(market, zone).eic
    slug = zone.lower()
    ds = f"generation__{market.id}_{slug}"
    availability = (sec.get("forecast_availability") or {}).get("rule")

    def ingest(ctx: TaskContext):
        token = os.environ.get(TOKEN_ENV)
        if not token:
            raise RuntimeError(f"{TOKEN_ENV} is not set (put it in .env; never commit it)")
        day = date.fromisoformat(ctx.partition_key)
        deadline = market.day_bounds_utc(day)[1] + timedelta(hours=float(sec["publication"]["late_after_hours_from_day_end"]))
        mats, results, frames, waiting = [], [], [], []
        for kind, item in ITEM.items():
            params = {**er.request_params(item, market, eic, day), "securityToken": token}
            res = http_get(base_url(), params, limiter=limiter or _limiter("entsoe"), secret_params=("securityToken",),
                           **({"opener": opener} if opener else {}))
            if res.outcome == "unknown":
                raise RetryableTaskError("network", f"entsoe generation {zone} {kind}: {res.detail}",
                                         materializations=mats, checks=results)
            fetched_at = now()
            b = write_bronze(ctx, source="entsoe", dataset=f"{item}__{slug}", partition_field="date", filename="raw.xml",
                             data=res.body, fetched_at=fetched_at, source_url=res.url_redacted, http_status=res.status,
                             code_version=gen.CODE_VERSION)
            mats.append(b.materialization)
            results += [CheckResult(f"{c.check_name}_{kind}", c.severity, c.passed, c.metrics) for c in b.checks]
            if res.error_class in ("rate_limited", "server_error"):
                raise RetryableTaskError(res.error_class, f"entsoe generation {zone} {kind}: HTTP {res.status}",
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
            df, unknown = gen.build_silver(doc, kind, market, zone, day, fetched_at, b.materialization.uri, mapping,
                                           availability)
            cs = gen.checks(df, doc, kind, market, zone, day, unknown, mapping)
            results += [CheckResult(f"{c.check_name}_{kind}", c.severity, c.passed, c.metrics) for c in cs]
            frames.append(df)
        if waiting:
            if now() < deadline:
                raise RetryableTaskError("source_not_ready", f"entsoe generation {zone} {ctx.partition_key}: {waiting}",
                                         retry_after_s=1800, materializations=mats, checks=results)
            # Past the deadline: publish what was sent; a document that never came is recorded,
            # and its required series appear as absent (series-level blocking, ADR-B10).
            sent = {k for k in ITEM if not any(w.startswith(f"{k}:") for w in waiting)}
            results.append(CheckResult("generation_published", "blocking" if not sent else "warn", False,
                                       {"waiting": waiting, "deadline_utc": iso_utc(deadline)}))
            for kind in sorted(set(ITEM) - sent):
                results += [CheckResult(f"{c.check_name}_{kind}", c.severity, c.passed, c.metrics)
                            for c in gen.series_checks(gen.empty_frame(), kind, market, zone, day)]
            if not sent:
                return mats, results
        if any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        cur = ctx.resolve_input(f"silver.{ds}", ctx.partition_key)
        previous = pl.read_parquet(absolute_path(ctx.lake_root, cur["uri"])) if cur else None
        out_df = vintages.carry_vintages(pl.concat(frames), previous)
        results.append(vintages.vintages_superset_check(out_df, previous))
        out = attempt_dir(ctx.lake_root, "silver", ds, "date", ctx.partition_key, ctx.attempt_id) / "part-0.parquet"
        write_parquet_atomic(out_df, out)
        mats.append(Materialization(f"silver.{ds}", ctx.partition_key, relative_uri(ctx.lake_root, out), out_df.height,
                                    sha256_file(out), "generation-3", gen.CODE_VERSION))
        return mats, results

    return TaskDef(f"generation_{slug}", ingest,
                   asset_outputs=(f"bronze.entsoe_generation_actual__{slug}",
                                  f"bronze.entsoe_wind_solar_forecast_da__{slug}", f"silver.{ds}"),
                   max_attempts=8, limit_key="entsoe",
                   retry={"initial_interval_seconds": 300, "backoff_coefficient": 2, "max_interval_seconds": 3600})


def generation_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "load" not in ((market.raw or {}).get("entsoe") or {}):
        return []
    return [Pipeline("generation_entsoe_daily", market.timezone,
                     tuple(make_generation_task(market, z, **task_kwargs) for z in market.zones),
                     description="ENTSO-E generation per type + wind/solar day-ahead forecast per zone (internal research only)")]
