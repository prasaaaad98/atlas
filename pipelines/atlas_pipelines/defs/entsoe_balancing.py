"""`balancing_entsoe_daily`: one `balancing_<zone>` task per ENTSO-E zone and local day (B4c).

Why it exists: fetches reserve capacity (17.1.B&C) per area and imbalance prices (17.1.G) for the
zone, keeps every raw answer in bronze (ZIP or acknowledgement, unchanged), and writes
silver.ancillary_prices__entsoe_<zone> from the latest fetch. Areas, units and deferrals come from
the registry (config/markets/entsoe.yaml, entsoe.balancing).
Retry rules: network/5xx/429 retried; "no matching data" for an area retried until the registry's
late_after_hours_from_day_end, then the rest publishes and the missing item is recorded (per-series
principle, ADR-B10); unknown codes, other areas' documents, a currency mismatch or a truncated
answer block.
Who calls it: defs/__init__.py; tests inject `opener` and `now`.
Data use: development, testing and internal research only (docs/data/sources/entsoe.md).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

import polars as pl

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter, http_get
from atlas_pipelines.core.lake import attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, RetryableTaskError, TaskContext
from atlas_pipelines.defs.prices import TOKEN_ENV, _limiter, base_url
from atlas_pipelines.sources.adapter import write_bronze
from atlas_pipelines.sources.entsoe import balancing as bal
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import requests as er

ITEM = {"reserves": "reserves_daily", "imbalance": "imbalance_prices"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def balancing_section(market: Market) -> dict:
    sec = ((market.raw or {}).get("entsoe") or {}).get("balancing") or {}
    areas, units = sec.get("areas") or {}, sec.get("units") or {}
    if set(areas) != set(ITEM) or set(units) != set(ITEM):
        raise ValueError(f"{market.source_file}: entsoe.balancing needs areas and units for {sorted(ITEM)}")
    for kind, zones in areas.items():
        if set(zones) - set(market.zones):
            raise ValueError(f"{market.source_file}: entsoe.balancing.areas.{kind}: unknown zone")
    if "late_after_hours_from_day_end" not in (sec.get("publication") or {}):
        raise ValueError(f"{market.source_file}: entsoe.balancing.publication.late_after_hours_from_day_end missing")
    return sec


def _slug(eic: str) -> str:
    return "".join(c for c in eic.lower() if c.isalnum())


def make_balancing_task(market: Market, zone: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
                        limiter: RateLimiter | None = None) -> TaskDef:
    sec = balancing_section(market)
    slug = zone.lower()
    ds = f"ancillary_prices__{market.id}_{slug}"
    plan = [(kind, area) for kind in ITEM for area in (sec["areas"][kind].get(zone) or [])]
    units = {k: v["unit"] for k, v in sec["units"].items()}

    def ingest(ctx: TaskContext):
        token = os.environ.get(TOKEN_ENV)
        if not token:
            raise RuntimeError(f"{TOKEN_ENV} is not set (put it in .env; never commit it)")
        day = date.fromisoformat(ctx.partition_key)
        deadline = market.day_bounds_utc(day)[1] + timedelta(hours=float(sec["publication"]["late_after_hours_from_day_end"]))
        mats, results, frames, waiting = [], [], [], []
        for kind, area in plan:
            params = {**er.request_params(ITEM[kind], market, area, day), "securityToken": token}
            res = http_get(base_url(), params, limiter=limiter or _limiter("entsoe"), secret_params=("securityToken",),
                           **({"opener": opener} if opener else {}))
            tag = f"{kind}_{_slug(area)}"
            if res.outcome == "unknown":
                raise RetryableTaskError("network", f"entsoe balancing {zone} {tag}: {res.detail}", materializations=mats,
                                         checks=results)
            fetched_at = now()
            b = write_bronze(ctx, source="entsoe", dataset=f"{ITEM[kind]}__{slug}_{_slug(area)}", partition_field="date",
                             filename="raw.zip" if res.body[:2] == b"PK" else "raw.xml", data=res.body,
                             fetched_at=fetched_at, source_url=res.url_redacted, http_status=res.status,
                             code_version=bal.CODE_VERSION)
            mats.append(b.materialization)
            results += [CheckResult(f"{c.check_name}_{tag}", c.severity, c.passed, c.metrics) for c in b.checks]
            if res.error_class in ("rate_limited", "server_error"):
                raise RetryableTaskError(res.error_class, f"entsoe balancing {zone} {tag}: HTTP {res.status}",
                                         retry_after_s=res.retry_after_s, materializations=mats, checks=results)
            if res.status != 200:
                return mats, results
            try:
                answer = bal.parse_answer(res.body)
            except (da.ParseError, ValueError) as e:
                return mats, results + [CheckResult(f"answer_parses_{tag}", "blocking", False, {"error": str(e)})]
            if isinstance(answer, da.Acknowledgement):
                if answer.no_data:
                    waiting.append(f"{tag}: {answer.text}")
                    continue
                return mats, results + [CheckResult(f"answer_parses_{tag}", "blocking", False,
                                                    {"acknowledgement": answer.code, "text": answer.text})]
            df, problems = bal.build_silver(answer, kind, market, zone, area, day, fetched_at, b.materialization.uri, units)
            results += [CheckResult(f"{c.check_name}_{tag}", c.severity, c.passed, c.metrics)
                        for c in bal.checks(df, answer, kind, market, zone, area, day, problems, market.currency)]
            frames.append(df)
        if waiting:
            if now() < deadline:
                raise RetryableTaskError("source_not_ready", f"entsoe balancing {zone} {ctx.partition_key}: {waiting}",
                                         retry_after_s=1800, materializations=mats, checks=results)
            results.append(CheckResult("balancing_published", "blocking" if not frames else "warn", False,
                                       {"waiting": waiting, "deadline_utc": iso_utc(deadline)}))
        results.append(CheckResult("unit_confirmed", "warn", all(v["status"] != "unconfirmed" for v in sec["units"].values()),
                                   {k: v for k, v in sec["units"].items()}))
        if not frames or any(c.severity == "blocking" and not c.passed for c in results):
            return mats, results
        out_df = pl.concat(frames)
        out = attempt_dir(ctx.lake_root, "silver", ds, "date", ctx.partition_key, ctx.attempt_id) / "part-0.parquet"
        write_parquet_atomic(out_df, out)
        mats.append(Materialization(f"silver.{ds}", ctx.partition_key, relative_uri(ctx.lake_root, out), out_df.height,
                                    sha256_file(out), "ancillary_prices-1", bal.CODE_VERSION))
        return mats, results

    bronze = tuple(f"bronze.entsoe_{ITEM[k]}__{slug}_{_slug(a)}" for k, a in plan)
    return TaskDef(f"balancing_{slug}", ingest, asset_outputs=bronze + (f"silver.{ds}",), max_attempts=8,
                   limit_key="entsoe", retry={"initial_interval_seconds": 300, "backoff_coefficient": 2,
                                              "max_interval_seconds": 3600})


def balancing_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "balancing" not in ((market.raw or {}).get("entsoe") or {}):
        return []
    return [Pipeline("balancing_entsoe_daily", market.timezone,
                     tuple(make_balancing_task(market, z, **task_kwargs) for z in market.zones),
                     description="ENTSO-E reserve capacity (17.1.B&C) + imbalance prices (17.1.G) per zone (internal research only)")]
