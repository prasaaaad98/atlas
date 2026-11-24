"""Market-adapter interface: the only place market-specific logic may live.

Why it exists: "a new market should need a registry entry plus an adapter,
nothing else" (CLAUDE.md). Shared code (runner, checks, gold, simulator) talks
to markets only through this interface plus the registry (core/markets.py).

Who implements it: sources/<market>/ (IEX in B1, ERCOT and ENTSO-E in B3).
Who calls it: pipeline tasks in defs/.

Contract for an adapter:
  fetch(ctx, market, product, zone) -> BronzeFetch
      Network happens only here, through the source's limit key. Writes the
      response bytes, unmodified, with write_bronze(). Never called in tests.
  parse(raw: bytes, market, product, zone, local_date) -> polars.DataFrame
      Pure: bytes in, canonical silver rows out. Golden-file tested on real
      saved fetches. Never fills or interpolates missing intervals.
  checks(df, market, product, zone, local_date) -> list[CheckResult]
      Market-specific checks on top of the shared ones.

Outcome classes (CLAUDE.md): every fetch attempt is `succeeded`,
`failed_definite` (e.g. HTTP 4xx, wrong delivery date in the body) or `unknown`
(e.g. timeout: we can't tell whether the source served it).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import polars as pl

from atlas_pipelines.core.hashing import sha256_bytes
from atlas_pipelines.core.lake import bronze_dir, check_segment, relative_uri, write_bytes_atomic
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.task import CheckResult, Materialization, TaskContext

OUTCOMES = ("succeeded", "failed_definite", "unknown")


@dataclass(frozen=True)
class BronzeFetch:
    materialization: Materialization
    checks: list[CheckResult]
    outcome: str  # one of OUTCOMES
    source_url: str
    http_status: int | None
    fetched_at: str  # ISO UTC


class MarketAdapter(Protocol):
    market_id: str  # registry id this adapter serves

    def fetch(self, ctx: TaskContext, market: Market, product: str, zone: str) -> BronzeFetch: ...

    def parse(
        self, raw: bytes, market: Market, product: str, zone: str, local_date: date
    ) -> pl.DataFrame: ...

    def checks(
        self, df: pl.DataFrame, market: Market, product: str, zone: str, local_date: date
    ) -> list[CheckResult]: ...


def write_bronze(
    ctx: TaskContext,
    *,
    source: str,
    dataset: str,
    partition_field: str,
    filename: str,
    data: bytes,
    fetched_at: datetime,
    source_url: str,
    http_status: int | None,
    code_version: str,
) -> BronzeFetch:
    """Store one fetch exactly as received and return its materialization + fetch checks.

    Layer-1 fetch checks recorded here (blocking): HTTP 200 and non-empty body.
    Adapters add content checks (expected table present, requested date present).
    """
    directory = bronze_dir(ctx.lake_root, source, dataset, partition_field, ctx.partition_key, fetched_at)
    path = directory / check_segment(filename, "file name")
    write_bytes_atomic(path, data)  # refuses to overwrite: bronze is immutable
    ok_status = http_status == 200
    checks = [
        CheckResult("http_200", "blocking", ok_status, {"http_status": http_status}),
        CheckResult("non_empty_body", "blocking", len(data) > 0, {"bytes": len(data)}),
    ]
    mat = Materialization(
        asset_key=f"bronze.{source}_{dataset}",
        partition_key=ctx.partition_key,
        uri=relative_uri(ctx.lake_root, path),
        row_count=0,  # raw files have no row semantics; parsers report rows in silver
        content_hash=sha256_bytes(data),
        schema_version="raw",
        code_version=code_version,
    )
    return BronzeFetch(
        materialization=mat,
        checks=checks,
        outcome="succeeded" if ok_status and data else "failed_definite",
        source_url=source_url,
        http_status=http_status,
        fetched_at=iso_utc(fetched_at),
    )


def revision_check(new_hash: str, previous_hash: str | None) -> CheckResult:
    """Warn (never block) when a re-fetch differs: source revisions are data, not errors."""
    revised = previous_hash is not None and previous_hash != new_hash
    return CheckResult(
        "source_revision", "warn", not revised, {"previous_hash": previous_hash, "new_hash": new_hash}
    )
