"""EIA generator inventory (EIA-860/860M via API v2) → silver.storage_capacity (slice E5).

Why it exists: storage buildout per BA drives long-term battery revenue views (E7 gold,
Phase D). One row per storage generator per report month, as EIA lists it; totals are
computed in gold, never stored here.
Who calls it: defs/eia.py (eia_storage_monthly); tests with the real saved response.

Partition = report month (YYYY-MM-01). The whole US inventory for that month in one fetch
(about 4,500 rows in 2026, under one 5,000-row page; paging handles more).
What this route can't give (recorded, not invented): planned projects (no planned status
in the route), energy capacity in MWh (no such column). energy_mwh and
expected_online_month stay null; rows are status operating/standby/out-of-service only.
Invariant: every row's status and prime mover maps explicitly (unknown blocks), and every
row belongs to the partition's month.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.schemas import STORAGE_CAPACITY, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources.eia import client

CODE_VERSION = "eia_storage-1"
KEY_COLUMNS = ("plantid", "generatorid")
MW_COLUMNS = {"nameplate-capacity-mw": "capacity_mw", "net-summer-capacity-mw": "net_summer_mw",
              "net-winter-capacity-mw": "net_winter_mw"}


def load_config(path: Path | None = None) -> dict:
    path = path or config.REPO_ROOT / "config" / "storage" / "eia.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise ValueError(f"{path}: version must be 1")
    for k in ("route", "energy_source_code", "statuses", "prime_movers", "data_columns"):
        if not raw.get(k):
            raise ValueError(f"{path}: {k} is required")
    if len(set(raw["statuses"].values())) != len(raw["statuses"]):
        raise ValueError(f"{path}: two status codes map to the same status")
    return raw


def month_of(partition: date) -> str:
    if partition.day != 1:
        raise ValueError(f"partition {partition} must be the first day of a month")
    return partition.strftime("%Y-%m")


def request_params(cfg: dict, partition: date) -> list[tuple[str, str]]:
    m = month_of(partition)
    return ([("frequency", "monthly"), ("start", m), ("end", m), ("facets[energy_source_code][]", cfg["energy_source_code"])]
            + [("data[]", c) for c in cfg["data_columns"]])


def _month(value) -> date | None:
    return date.fromisoformat(value + "-01") if value else None


def parse(rows: list[dict], cfg: dict, market: str, partition: date, fetched_at: datetime,
          source_uri: str) -> tuple[pl.DataFrame, dict]:
    """Rows → canonical columns. Returns (frame, problems) where problems lists what could
    not be mapped or read; those rows are left out and the checks block on them."""
    problems = {"unknown_status": set(), "unknown_prime_mover": set(), "non_numeric": []}
    out = []
    for r in rows:
        status = cfg["statuses"].get(r.get("status"))
        kind = cfg["prime_movers"].get(r.get("prime_mover_code"))
        if status is None:
            problems["unknown_status"].add(r.get("status"))
        if kind is None:
            problems["unknown_prime_mover"].add(r.get("prime_mover_code"))
        try:
            mw = {col: client.to_number(r.get(src)) for src, col in MW_COLUMNS.items()}
        except (TypeError, ValueError):
            problems["non_numeric"].append(f"{r.get('plantid')}/{r.get('generatorid')}")
            continue
        if status is None or kind is None:
            continue
        out.append({
            "market": market, "zone": r.get("balancing_authority_code"), "state": r.get("stateid"),
            "report_month": _month(r.get("period")), "plant_id": str(r.get("plantid")),
            "generator_id": str(r.get("generatorid")), "plant_name": r.get("plantName"),
            "entity_name": r.get("entityName"), "storage_type": kind, "status": status,
            "source_status": r.get("status"), "source_prime_mover": r.get("prime_mover_code"),
            **mw, "energy_mwh": None,
            "operating_month": _month(r.get("operating-year-month")),
            "planned_retirement_month": _month(r.get("planned-retirement-year-month")),
            "expected_online_month": None,
            "fetched_at_utc": fetched_at, "source_uri": source_uri,
        })
    schema = {c: t for c, (t, _) in STORAGE_CAPACITY.items()}
    df = pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)
    return df.sort(["plant_id", "generator_id"]), problems


def checks(df: pl.DataFrame, rows: list[dict], cfg: dict, problems: dict, partition: date) -> list[CheckResult]:
    month = month_of(partition)
    other_months = sorted({r.get("period") for r in rows} - {month})
    units = sorted({r.get(f"{c}-units") for r in rows for c in MW_COLUMNS})
    sources = sorted({r.get("energy_source_code") for r in rows})
    dupes = df.height - df.select(["plant_id", "generator_id"]).unique().height
    negative = df.filter((pl.col("capacity_mw") < 0) | (pl.col("net_summer_mw") < 0)).height
    no_ba = df.filter(pl.col("zone").is_null()).height
    late = df.filter((pl.col("status") == "operating") & (pl.col("operating_month") > pl.col("report_month"))).height
    return [
        schema_check(df, STORAGE_CAPACITY, "storage_capacity"),
        CheckResult("report_month_matches", "blocking", not other_months, {"month": month, "other_periods": other_months}),
        CheckResult("energy_source_is_storage", "blocking", sources == [cfg["energy_source_code"]] or not rows,
                    {"expected": cfg["energy_source_code"], "found": sources}),
        CheckResult("units_are_mw", "blocking", units == ["MW"] or not rows, {"found": units}),
        CheckResult("status_mapped", "blocking", not problems["unknown_status"],
                    {"unknown": sorted(map(str, problems["unknown_status"]))}),
        CheckResult("prime_mover_mapped", "blocking", not problems["unknown_prime_mover"],
                    {"unknown": sorted(map(str, problems["unknown_prime_mover"]))}),
        CheckResult("capacity_numeric", "blocking", not problems["non_numeric"],
                    {"generators_first10": problems["non_numeric"][:10]}),
        CheckResult("primary_key_unique", "blocking", dupes == 0, {"duplicates": dupes}),
        CheckResult("capacity_non_negative", "blocking", negative == 0, {"negative": negative}),
        CheckResult("capacity_present", "warn", df["capacity_mw"].null_count() == 0,
                    {"null_nameplate": df["capacity_mw"].null_count()}),
        # Kept as reported (zone null); E7 gold reports them as "no BA".
        CheckResult("balancing_authority_present", "warn", no_ba == 0, {"rows_without_ba": no_ba}),
        CheckResult("operating_month_not_after_report", "warn", late == 0, {"operating_rows_dated_later": late}),
    ]
