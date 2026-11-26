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

Gold (B7): `spreads_<zone>` depends on `ingest_<zone>`, so it runs only after
that zone's silver passed its blocking checks and became current. It reads the
silver partition through ctx.resolve_input (the runner's current pointer),
verifies the file's hash against the pointer, and writes
`gold.daily_spreads__entsoe_<zone>` (gold/spreads.py). Derived from ENTSO-E
prices: internal research only, never published (docs/data/sources/entsoe.md).

Source URL: ATLAS_ENTSOE_BASE_URL may point the fetcher at a local fake that
serves saved real files (scripts/fake_entsoe.py, offline demos). Only
http://127.0.0.1 / localhost is accepted, so the token can't be sent elsewhere.

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
from urllib.parse import urlsplit

import polars as pl

from atlas_pipelines.core import config
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.http import RateLimiter, http_get
from atlas_pipelines.core.lake import absolute_path, attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.limits import load_limit
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, RetryableTaskError, TaskContext
from atlas_pipelines.sources.adapter import revision_check, write_bronze
from atlas_pipelines.gold import spreads
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.validation import stats

TOKEN_ENV = "ENTSOE_TOKEN"
_limiters: dict[str, RateLimiter] = {}


def _limiter(key: str) -> RateLimiter:
    if key not in _limiters:  # one per key per process, shared by all zones
        _limiters[key] = RateLimiter(load_limit(key, config.REPO_ROOT / "config" / "limits").rpm)
    return _limiters[key]


def base_url() -> str:
    override = os.environ.get("ATLAS_ENTSOE_BASE_URL")
    if not override:
        return da.BASE_URL
    host = urlsplit(override).hostname
    if host not in ("127.0.0.1", "localhost"):
        raise RuntimeError(f"ATLAS_ENTSOE_BASE_URL must be a local fake (127.0.0.1/localhost), got host {host!r}")
    return override


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
        res = http_get(base_url(), params, limiter=limiter or _limiter("entsoe"),
                       secret_params=("securityToken",), **kwargs)
        if res.outcome == "unknown":
            raise RetryableTaskError("network", f"entsoe {zone}: outcome unknown: {res.detail}")

        # Previous raw response for this day, from whichever runner owns pointers.
        previous = ctx.resolve_input(bronze_asset, ctx.partition_key)
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
        # Layer 3 (B6, warn): against the current silver of the trailing days.
        vcfg = stats.load_config()
        history = stats.trailing_frames(ctx, f"silver.{silver_dataset}", local_date, vcfg["trailing"]["window_days"])
        data_checks += [stats.outlier_check("price_outliers", df, "price", history, vcfg),
                        stats.day_structure_check("price_day_structure", df, history, vcfg)]

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
    # `entsoe` key on 429 (ADR-008). Constant ≤5 min (full jitter over 300 s)
    # for up to 36 attempts (~3 h): while a day isn't published yet, the gap
    # between the last "not ready" and the first success brackets ENTSO-E's
    # publication time to within one retry (the 14-day schedule measures it).
    # Past days with no data fail at once (deadline passed), so a backfill
    # doesn't spend these retries.
    return TaskDef(f"ingest_{slug}", ingest, asset_outputs=(bronze_asset, f"silver.{silver_dataset}"),
                   max_attempts=36, limit_key="entsoe",
                   retry={"initial_interval_seconds": 300, "backoff_coefficient": 1, "max_interval_seconds": 300})


def make_spreads_task(market: Market, zone: str, ingest: TaskDef) -> TaskDef:
    slug = zone.lower()
    silver_asset = f"silver.power_prices__{market.id}_{slug}"
    gold_dataset = f"daily_spreads__{market.id}_{slug}"

    def build(ctx: TaskContext):
        local_date = date.fromisoformat(ctx.partition_key)
        current = ctx.resolve_input(silver_asset, ctx.partition_key)
        if current is None:
            # Can't happen after a successful ingest_<zone> in the same run; if it
            # does, the pointer store is inconsistent: fail loudly, don't guess.
            return [], [CheckResult("input_current", "blocking", False, {"asset": silver_asset})]
        path = absolute_path(ctx.lake_root, current["uri"])
        actual = sha256_file(path)
        if actual != current["content_hash"]:
            return [], [CheckResult("input_hash_matches_pointer", "blocking", False,
                                    {"uri": current["uri"], "pointer": current["content_hash"], "file": actual})]
        silver = pl.read_parquet(path)
        df, checks = spreads.compute(silver, local_date, len(market.intervals(da.PRODUCT, local_date)),
                                     current["uri"], current["content_hash"])
        if df.is_empty():
            return [], checks
        out = attempt_dir(ctx.lake_root, "gold", gold_dataset, "date", ctx.partition_key, ctx.attempt_id)
        file = out / "part-0.parquet"
        write_parquet_atomic(df, file)
        mat = Materialization(asset_key=f"gold.{gold_dataset}", partition_key=ctx.partition_key,
                              uri=relative_uri(ctx.lake_root, file), row_count=df.height,
                              content_hash=sha256_file(file), schema_version="daily_spreads-1",
                              code_version=spreads.DEFINITION_VERSION)
        return [mat], checks

    return TaskDef(f"spreads_{slug}", build, deps=(ingest.name,), asset_outputs=(f"gold.{gold_dataset}",))


def price_pipelines(registry: dict[str, Market], **task_kwargs) -> list[Pipeline]:
    market = registry.get("entsoe")
    if market is None or "entsoe" not in (market.raw or {}):
        return []
    ingests = [make_task(market, z, **task_kwargs) for z in market.zones]
    golds = [make_spreads_task(market, z, t) for z, t in zip(market.zones, ingests)]
    return [Pipeline(
        name="prices_entsoe_daily",
        partition_tz=market.timezone,
        tasks=tuple(ingests + golds),
        description="ENTSO-E A44 day-ahead prices per zone (development/testing/internal research only)",
    )]
