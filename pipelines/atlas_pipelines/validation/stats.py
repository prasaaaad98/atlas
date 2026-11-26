"""Layer 3 statistical checks (B6): robust outliers and day structure against trailing partitions.

Why it exists: EIA-930 and day-ahead prices contain real spikes and real errors; neither may
be "fixed". These checks flag values far outside the recent distribution so a human looks
(PHASE_AB_PLAN.md §3.6 Layer 3). WARN only: they never block a partition.
Who calls it: ingest tasks (defs/prices.py, defs/eia.py) with the current silver of the
previous N partitions (via TaskContext.resolve_input: runner-agnostic).
Method: robust z = 0.6745 · (x − median) / MAD (median absolute deviation) over all values
of the trailing partitions. MAD = 0 (a flat history) or too little history → the check
passes and says why, instead of guessing.
"""

from __future__ import annotations

import statistics
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.lake import absolute_path
from atlas_pipelines.core.task import CheckResult, TaskContext


def load_config(path: Path | None = None) -> dict:
    path = path or config.REPO_ROOT / "config" / "validation.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    t = raw.get("trailing") or {}
    if raw.get("version") != 1 or not (1 <= int(t.get("min_history_days", 0)) <= int(t.get("window_days", 0))):
        raise ValueError(f"{path}: needs version 1 and 1 <= min_history_days <= window_days")
    if float(raw.get("robust_z_threshold", 0)) <= 0:
        raise ValueError(f"{path}: robust_z_threshold must be > 0")
    return raw


def trailing_frames(ctx: TaskContext, asset: str, day: date, window_days: int) -> list[pl.DataFrame]:
    """Current silver of the `window_days` partitions before `day` that exist (gaps skipped)."""
    out = []
    for k in range(1, window_days + 1):
        cur = ctx.resolve_input(asset, (day - timedelta(days=k)).isoformat())
        if cur:
            out.append(pl.read_parquet(absolute_path(ctx.lake_root, cur["uri"])))
    return out


def _robust(history: list[float]) -> tuple[float, float]:
    med = statistics.median(history)
    return med, statistics.median(abs(x - med) for x in history)


def outlier_check(name: str, today: pl.DataFrame, column: str, history: list[pl.DataFrame], cfg: dict,
                  time_column: str = "interval_start_utc") -> CheckResult:
    """Values of `column` today far outside the trailing distribution (warn)."""
    thr, need = float(cfg["robust_z_threshold"]), int(cfg["trailing"]["min_history_days"])
    values = [v for h in history for v in h[column].drop_nulls().to_list()]
    base = {"history_days": len(history), "threshold": thr}
    if len(history) < need or not values:
        return CheckResult(name, "warn", True, {**base, "skipped": f"insufficient history (< {need} days)"})
    med, mad = _robust(values)
    if mad == 0:
        return CheckResult(name, "warn", True, {**base, "skipped": "flat history (MAD = 0)", "median": med})
    flagged = []
    for t, x in zip(today[time_column].to_list(), today[column].to_list()):
        if x is None:
            continue
        z = 0.6745 * (x - med) / mad
        if abs(z) > thr:
            flagged.append({"at": str(t), "value": x, "z": round(z, 2)})
    return CheckResult(name, "warn", not flagged, {**base, "median": med, "mad": mad, "flagged": len(flagged),
                                                   "first10": flagged[:10]})


def day_structure_check(name: str, today: pl.DataFrame, history: list[pl.DataFrame], cfg: dict,
                        column: str = "price") -> CheckResult:
    """Today's max−min spread vs the trailing days' spreads; share of non-positive values reported (warn)."""
    thr, need = float(cfg["robust_z_threshold"]), int(cfg["trailing"]["min_history_days"])
    vals = today[column].drop_nulls()
    spread = (vals.max() - vals.min()) if vals.len() else None
    share = (vals <= 0).sum() / vals.len() if vals.len() else None
    base = {"spread": spread, "non_positive_share": share, "history_days": len(history)}
    spreads = [h[column].max() - h[column].min() for h in history if h[column].drop_nulls().len()]
    if len(spreads) < need or spread is None:
        return CheckResult(name, "warn", True, {**base, "skipped": f"insufficient history (< {need} days)"})
    med, mad = _robust(spreads)
    if mad == 0:
        return CheckResult(name, "warn", True, {**base, "skipped": "flat history (MAD = 0)"})
    z = 0.6745 * (spread - med) / mad
    return CheckResult(name, "warn", abs(z) <= thr, {**base, "trailing_median_spread": med, "spread_z": round(z, 2)})
