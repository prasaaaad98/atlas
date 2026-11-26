"""Layer 4 monthly reconciliation (B6): silver aggregates vs independent official figures.

Why it exists: checks inside one source can't catch a systematic error (wrong unit, wrong
series, a missing zone). Comparing a monthly aggregate with a figure an independent body
publishes can (PHASE_AB_PLAN.md §3.6 Layer 4). WARN, reported; a mismatch becomes an
investigation note in docs/data/reconciliation/.
References are entered BY THE OWNER in config/reconciliation/<market>.yaml, each with its
source (title, URL, retrieved_on, table): no figure is ever written by code or invented.
Who calls it: `python -m atlas_pipelines reconcile <market> --month YYYY-MM` (local pointers).
"""

from __future__ import annotations

import calendar
from datetime import date
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.task import CheckResult

METRICS = ("time_weighted_mean", "energy_sum_mwh")
REQUIRED = ("asset", "column", "metric", "month", "value", "unit", "tolerance_pct", "source")


def load_references(market_id: str, path: Path | None = None) -> list[dict]:
    path = path or config.REPO_ROOT / "config" / "reconciliation" / f"{market_id}.yaml"
    if not Path(path).exists():
        return []
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if raw.get("version") != 1:
        raise ValueError(f"{path}: version must be 1")
    refs = raw.get("references") or []
    for i, r in enumerate(refs):
        missing = [k for k in REQUIRED if r.get(k) in (None, "")]
        src = r.get("source") or {}
        if missing or r["metric"] not in METRICS or not all(src.get(k) for k in ("title", "url", "retrieved_on")):
            raise ValueError(f"{path}: reference {i} needs {REQUIRED}, metric in {METRICS}, "
                             f"source.title/url/retrieved_on; missing {missing}")
    return refs


def month_days(month: str) -> list[date]:
    y, m = (int(x) for x in month.split("-"))
    return [date(y, m, d) for d in range(1, calendar.monthrange(y, m)[1] + 1)]


def metric_value(frame: pl.DataFrame, column: str, metric: str) -> float:
    """time_weighted_mean: Σ v·minutes / Σ minutes (intervals of different lengths weigh correctly);
    energy_sum_mwh: Σ v·minutes/60 (average MW over an interval → MWh)."""
    df = frame.filter(pl.col(column).is_not_null())
    minutes = df["interval_minutes"].cast(pl.Float64)
    if metric == "time_weighted_mean":
        return float((df[column] * minutes).sum() / minutes.sum())
    return float((df[column] * minutes / 60).sum())


def reconcile(ref: dict, frames: dict[date, pl.DataFrame]) -> CheckResult:
    """One reference vs the month's current silver partitions. An incomplete month is reported, not compared."""
    days = month_days(ref["month"])
    missing = [str(d) for d in days if d not in frames]
    name = f"reconcile_{ref['metric']}_{ref['asset']}_{ref['month']}"
    base = {"reference": ref["value"], "unit": ref["unit"], "source": ref["source"], "tolerance_pct": ref["tolerance_pct"]}
    if missing:
        return CheckResult(name, "warn", False, {**base, "month_complete": False, "missing_days": missing[:31]})
    frame = pl.concat([frames[d] for d in days], how="vertical_relaxed")
    if ref.get("kind"):
        frame = frame.filter(pl.col("kind") == ref["kind"])
    ours = metric_value(frame, ref["column"], ref["metric"])
    diff_pct = (ours - ref["value"]) / ref["value"] * 100 if ref["value"] else None
    ok = diff_pct is not None and abs(diff_pct) <= float(ref["tolerance_pct"])
    return CheckResult(name, "warn", ok, {**base, "month_complete": True, "ours": ours,
                                          "diff_pct": None if diff_pct is None else round(diff_pct, 3)})
