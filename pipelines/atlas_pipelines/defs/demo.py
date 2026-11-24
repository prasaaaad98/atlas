"""Demo pipeline: writes each market's interval grid for one delivery day.

Why it exists: B0 acceptance needs a dummy task that exercises the whole local
path — registry → TaskContext → attempt-specific Parquet → checks → current
pointer → DuckDB view — before any real source exists.

It contains NO market values (no prices, no volumes). Every row is derived
from the registry: which intervals a delivery day has in that market's time
zone and resolution. So it never fabricates data, and it shows DST and
resolution changes directly (e.g. 92 or 100 quarter-hours on a DST day).

One pipeline per (market, product): `demo_grid_<market>_<product>`, producing
asset `demo.grid_<market>_<product>`. Per-market assets avoid two markets
sharing one asset partition key; how one silver table holds many markets is
decided in ADR-B02 (B1).

Who calls it: defs/__init__.py builds these from the loaded registry.
"""

from __future__ import annotations

import polars as pl

from atlas_pipelines.checks.intervals import interval_checks
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.lake import attempt_dir, relative_uri, write_parquet_atomic
from atlas_pipelines.core.markets import Market, parse_partition_date
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, Materialization, TaskContext

CODE_VERSION = "demo_grid-1"
SCHEMA_VERSION = "1"


def grid_frame(market: Market, product: str, partition_key: str) -> pl.DataFrame:
    d = parse_partition_date(partition_key)
    p = market.product(product)
    rows = market.intervals(product, d)
    return pl.DataFrame(
        {
            "market": [p.canonical_market] * len(rows),
            "local_date": [d] * len(rows),
            "local_block": [i.block for i in rows],
            "interval_start_utc": [i.start_utc for i in rows],
            "interval_end_utc": [i.end_utc for i in rows],
            "interval_minutes": [p.interval_minutes_on(d)] * len(rows),
        },
        schema={
            "market": pl.String,
            "local_date": pl.Date,
            "local_block": pl.Int16,
            "interval_start_utc": pl.Datetime("us", "UTC"),
            "interval_end_utc": pl.Datetime("us", "UTC"),
            "interval_minutes": pl.Int16,
        },
    )


def make_task(market: Market, product: str):
    dataset = f"grid_{market.id}_{product.lower()}"
    asset_key = f"demo.{dataset}"

    def build_grid(ctx: TaskContext) -> tuple[list[Materialization], list[CheckResult]]:
        df = grid_frame(market, product, ctx.partition_key)
        out = attempt_dir(ctx.lake_root, "demo", dataset, "date", ctx.partition_key, ctx.attempt_id)
        path = out / "part-0.parquet"
        write_parquet_atomic(df, path)
        # Check what was written, as it will be read back, not the in-memory frame.
        written = pl.read_parquet(path)
        checks = interval_checks(written, market, product, parse_partition_date(ctx.partition_key))
        mat = Materialization(
            asset_key=asset_key,
            partition_key=ctx.partition_key,
            uri=relative_uri(ctx.lake_root, path),
            row_count=written.height,
            content_hash=sha256_file(path),
            schema_version=SCHEMA_VERSION,
            code_version=CODE_VERSION,
        )
        return [mat], checks

    return asset_key, build_grid


def demo_pipelines(registry: dict[str, Market]) -> list[Pipeline]:
    out = []
    for market in registry.values():
        for product in market.products:
            asset_key, fn = make_task(market, product)
            out.append(
                Pipeline(
                    name=f"demo_grid_{market.id}_{product.lower()}",
                    partition_tz=market.timezone,
                    tasks=(TaskDef("build_grid", fn, asset_outputs=(asset_key,)),),
                    description=f"Interval grid for {market.name} {product} (registry-derived, no market values)",
                )
            )
    return out
