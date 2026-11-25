"""Price pipelines: `prices_entsoe_daily`, one ingest task per zone (fan-out).

Why it exists: turns the ENTSO-E registry entry into a pipeline the local
runner (and later Atlas) runs per delivery day (CET/CEST). Each task:
  1. fetches A44 for its zone through the `entsoe` limit key (token from the
     ENTSOE_TOKEN environment variable, redacted everywhere);
  2. stores the response unchanged in bronze (error bodies too);
  3. parses, selects the day-ahead series, writes silver to an attempt
     directory and runs the checks;
  4. compares the bronze hash with the current one (source revision, warn).
Assets are per zone (`silver.power_prices__entsoe_<zone>`), so one zone failing
never blocks another; the catalog unions them into `silver.power_prices`.

Retry rules (outcome classes, CLAUDE.md):
  - network failure → `unknown` → RetryableTaskError("network").
  - HTTP 429 / 5xx → retryable (rate_limited / server_error), raw kept.
  - HTTP 401/403/400 → blocking check failure (won't fix itself).
  - "No matching data" acknowledgement, or a missing/short day-ahead series:
    before the publication deadline → RetryableTaskError("source_not_ready");
    after it → blocking failure `day_ahead_published`. A 200 OK alone never
    means the day is complete.
Who calls it: defs/__init__.py; tests inject `opener` and `now`.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, timezone

from atlas_pipelines.core import config
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter, http_get
from atlas_pipelines.core.lake import attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.limits import load_limit
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, RetryableTaskError, TaskContext
from atlas_pipelines.sources.adapter import revision_check, write_bronze
from atlas_pipelines.sources.entsoe import day_ahead as da

TOKEN_ENV = "ENTSOE_TOKEN"
_limiters: dict[str, RateLimiter] = {}


def _limiter(key: str) -> RateLimiter:
    if key not in _limiters:  # one per key per process, shared by all zones
        _limiters[key] = RateLimiter(load_limit(key, config.REPO_ROOT / "config" / "limits").rpm)
    return _limiters[key]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_task(market: Market, zone: str, *, opener=None, now: Callable[[], datetime] = _utcnow,
              limiter: RateLimiter | None = None):
    slug = zone.lower()
    bronze_asset = f"bronze.entsoe_day_ahead__{slug}"
    silver_dataset = f"power_prices__{market.id}_{slug}"
    rule = da.series_rule(market, zone)  # fail at definition time on a bad registry
    retry = da.adapter_config(market)["publication"]["retry"]

    def ingest(ctx: TaskContext):
        token = os.environ.get(TOKEN_ENV)
        if not token:
            raise RuntimeError(f"{TOKEN_ENV} is not set (put it in .env; never commit it)")
        local_date = date.fromisoformat(ctx.partition_key)
        params = {**da.request_params(market, zone, local_date), "securityToken": token}
        kwargs = {"opener": opener} if opener else {}
        res = http_get(da.BASE_URL, params, limiter=limiter or _limiter("entsoe"),
                       secret_params=("securityToken",), **kwargs)
        if res.outcome == "unknown":
            raise RetryableTaskError("network", f"entsoe {zone}: outcome unknown: {res.detail}")

        previous = read_pointer(ctx.lake_root, bronze_asset, ctx.partition_key)
        bronze = write_bronze(
            ctx, source="entsoe", dataset=f"day_ahead__{slug}", partition_field="date",
            filename="raw.xml", data=res.body, fetched_at=now(), source_url=res.url_redacted,
            http_status=res.status, code_version=da.CODE_VERSION,
        )
        mats = [bronze.materialization]
        results = list(bronze.checks)
        results.append(revision_check(bronze.materialization.content_hash,
                                      previous.get("content_hash") if previous else None))
        if res.error_class in ("rate_limited", "server_error"):
            raise RetryableTaskError(res.error_class, f"entsoe {zone}: HTTP {res.status}",
                                     retry_after_s=res.retry_after_s, materializations=mats, checks=results)
        if res.status != 200:
            return mats, results  # http_200 failed (blocking): auth/bad request won't fix itself

        try:
            doc = da.parse_document(res.body)
        except da.ParseError as e:
            return mats, results + [CheckResult("document_parses", "blocking", False, {"error": str(e)})]

        deadline = da.publication_deadline(market, local_date)
        late = now() >= deadline

        def not_ready(why: str, extra: list[CheckResult]):
            published = CheckResult("day_ahead_published", "warn" if not late else "blocking", False,
                                    {"why": why, "deadline_utc": iso_utc(deadline), "checked_at_utc": iso_utc(now())})
            if late:
                return mats, results + extra + [published]
            raise RetryableTaskError("source_not_ready", f"entsoe {zone} {ctx.partition_key}: {why}",
                                     retry_after_s=retry["first_after_minutes"] * 60,
                                     materializations=mats, checks=results + extra + [published])

        if isinstance(doc, da.Acknowledgement):
            if doc.no_data:
                return not_ready(f"no data yet: {doc.text}", [])
            return mats, results + [CheckResult("document_parses", "blocking", False,
                                                {"acknowledgement": doc.code, "text": doc.text})]

        series, sel = da.select_series(doc, rule)
        results += sel
        if series is None:
            if da.publication_pending(sel):
                return not_ready("day-ahead series absent", [])
            return mats, results

        try:
            df = da.build_silver(series, market, zone, local_date, bronze.materialization.uri)
        except da.ParseError as e:
            return mats, results + [CheckResult("document_parses", "blocking", False, {"error": str(e)})]
        data_checks = da.checks(df, series, doc, market, local_date)
        if da.publication_pending(data_checks):
            return not_ready("day-ahead series incomplete", data_checks)

        out = attempt_dir(ctx.lake_root, "silver", silver_dataset, "date", ctx.partition_key, ctx.attempt_id)
        path = out / "part-0.parquet"
        write_parquet_atomic(df, path)
        mats.append(Materialization(
            asset_key=f"silver.{silver_dataset}", partition_key=ctx.partition_key,
            uri=relative_uri(ctx.lake_root, path), row_count=df.height, content_hash=sha256_file(path),
            schema_version=da.SCHEMA_VERSION, code_version=da.CODE_VERSION,
        ))
        return mats, results + data_checks

    # Retries: 429/5xx/network and "not published yet" are retryable (raised as
    # RetryableTaskError); Atlas backs off with jitter (ADR-005) and pauses the
    # `entsoe` key on 429 (ADR-008).
    return TaskDef(f"ingest_{slug}", ingest, asset_outputs=(bronze_asset, f"silver.{silver_dataset}"),
                   max_attempts=8, limit_key="entsoe",
                   retry={"initial_interval_seconds": 60, "backoff_coefficient": 2, "max_interval_seconds": 3600})


def price_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "entsoe" not in (market.raw or {}):
        return []
    return [Pipeline(
        name="prices_entsoe_daily",
        partition_tz=market.timezone,
        tasks=tuple(make_task(market, z, **task_kwargs) for z in market.zones),
        description="ENTSO-E A44 day-ahead prices per zone (development/testing/internal research only)",
    )]
