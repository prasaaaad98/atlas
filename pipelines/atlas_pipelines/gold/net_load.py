"""Gold net load and generation mix from ENTSO-E silver (ADR-B10, ADR-B09).

Why it exists: Phase D needs net load (demand − wind − solar), actual and day-ahead forecast, per
zone and interval; cross-market views need generation by gold fuel_group. Gold is computed ONLY
from silver, so it can always be rebuilt (same silver bytes → byte-identical Parquet, tested).
Who calls it: defs/entsoe_gold.py (net_load_<zone> tasks); tests.

Definitions (config/gold/net_load.yaml; status proposed):
  inputs (declared)  demand + every generation-direction solar/wind series present that day +
                     the zone's required wind/solar types (registry); latest vintage of each
  net_load_mw        demand − Σ solar − Σ wind, per interval; NULL where any input lacks a value,
                     with missing_inputs naming it ("B18", "demand", "wind_offshore (absent)")
  generation_mix     Σ generation per (kind, fuel_group, direction); NULL where any series of the
                     group lacks a value
Invariants:
  - Blocking is per interval and per declared input: a gap in a series nothing here uses blocks
    nothing; a gap in an input blocks exactly its intervals. Nothing is filled or resampled.
  - Inputs at different resolutions are not combined (blocking check).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core import config
from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.schemas import GENERATION_MIX, NET_LOAD, schema_check
from atlas_pipelines.core.task import CheckResult
from atlas_pipelines.sources import fuels

DEFINITION_VERSION = "net_load-1 (proposed 2026-11-26)"
KINDS = ("actual", "forecast")


def load_definitions(path: Path | None = None) -> dict:
    path = path or config.REPO_ROOT / "config" / "gold" / "net_load.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    canonical = fuels.load_canonical()
    for key in ("solar_fuel_types", "wind_fuel_types"):
        if not raw.get(key) or set(raw[key]) - set(canonical):
            raise ValueError(f"{path}: {key} must list canonical fuel types")
    return raw


def latest(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """Rows of the most recent vintage (fetched_at_utc) of each series."""
    if df.is_empty():
        return df
    last = df.group_by(by).agg(pl.col("fetched_at_utc").max().alias("_last"))
    return df.join(last, on=by).filter(pl.col("fetched_at_utc") == pl.col("_last")).drop("_last")


def _grid(market: Market, day: date, minutes: int) -> list:
    start, end = market.day_bounds_utc(day)
    return [start + timedelta(minutes=minutes * i) for i in range(int((end - start).total_seconds() // 60 // minutes))]


def _series(df: pl.DataFrame) -> dict[str, dict]:
    """{series name: {interval_start: value or None}} for generation rows of one kind."""
    out: dict[str, dict] = {}
    for r in df.iter_rows(named=True):
        name = r["source_type"] + ("/consumption" if r["direction"] == "consumption" else "")
        out.setdefault(name, {})[r["interval_start_utc"]] = r["generation_mw"]
    return out


def build(demand: pl.DataFrame, generation: pl.DataFrame, market: Market, zone: str, day: date,
          required: list[str], defs: dict, hashes: dict[str, str]) -> tuple[pl.DataFrame, pl.DataFrame, list[CheckResult]]:
    """One zone's local day of silver demand + generation → (net load rows, generation mix rows, checks)."""
    solar_t, wind_t = set(defs["solar_fuel_types"]), set(defs["wind_fuel_types"])
    groups = fuels.load_canonical()
    nl_rows, mix_rows, checks, blocked = [], [], [], {}
    for kind in KINDS:
        d = latest(demand.filter(pl.col("kind") == kind), ["kind"])
        g = latest(generation.filter(pl.col("kind") == kind), ["kind", "source_type", "direction"])
        if d.is_empty() and g.is_empty():
            continue
        res = set(d["interval_minutes"].to_list()) | set(g["interval_minutes"].to_list())
        checks.append(CheckResult(f"resolution_matches_{kind}", "blocking", len(res) == 1, {"minutes": sorted(res)}))
        if len(res) != 1:
            continue
        minutes = res.pop()
        grid = _grid(market, day, minutes)
        dem = dict(zip(d["interval_start_utc"].to_list(), d["demand_mw"].to_list()))
        gen = g.filter(pl.col("direction") == "generation")
        solar = _series(gen.filter(pl.col("fuel_type").is_in(list(solar_t))))
        wind = _series(gen.filter(pl.col("fuel_type").is_in(list(wind_t))))
        absent = sorted(set(required) - set(gen["fuel_type"].to_list()))
        d_at = d["fetched_at_utc"].max() if d.height else None
        g_at = g["fetched_at_utc"].max() if g.height else None
        n_blocked = 0
        for t in grid:
            missing = (["demand"] if dem.get(t) is None else []) + \
                      sorted(n for n, s in {**solar, **wind}.items() if s.get(t) is None) + [f"{a} (absent)" for a in absent]
            sv = None if any(s.get(t) is None for s in solar.values()) or not solar else sum(s[t] for s in solar.values())
            wv = None if any(s.get(t) is None for s in wind.values()) or not wind else sum(s[t] for s in wind.values())
            net = None if missing else dem[t] - (sv or 0.0) - (wv or 0.0)
            n_blocked += net is None
            loc = t.astimezone(market.tz)
            nl_rows.append({"market": market.id, "zone": zone, "kind": kind, "interval_start_utc": t,
                            "interval_end_utc": t + timedelta(minutes=minutes), "interval_minutes": minutes,
                            "local_date": loc.date(), "local_hour": loc.hour, "demand_mw": dem.get(t),
                            "solar_mw": sv, "wind_mw": wv, "net_load_mw": net,
                            "missing_inputs": ",".join(missing) or None, "demand_fetched_at_utc": d_at,
                            "generation_fetched_at_utc": g_at, "demand_source_hash": hashes["demand"],
                            "generation_source_hash": hashes["generation"]})
        blocked[kind] = n_blocked
        # Generation mix by gold group and direction (every series, required or not).
        by_group: dict[tuple[str, str], dict] = {}
        for (code, direction), s in g.group_by(["source_type", "direction"]):
            key = (groups[s["fuel_type"][0]], direction)
            by_group.setdefault(key, {})[code] = dict(zip(s["interval_start_utc"].to_list(), s["generation_mw"].to_list()))
        for (group, direction), series in sorted(by_group.items()):
            for t in grid:
                gone = sorted(c for c, s in series.items() if s.get(t) is None)
                mix_rows.append({"market": market.id, "zone": zone, "kind": kind, "interval_start_utc": t,
                                 "interval_end_utc": t + timedelta(minutes=minutes), "interval_minutes": minutes,
                                 "local_date": t.astimezone(market.tz).date(), "fuel_group": group, "direction": direction,
                                 "generation_mw": None if gone else sum(s[t] for s in series.values()),
                                 "series": len(series), "missing_series": ",".join(gone) or None,
                                 "generation_source_hash": hashes["generation"]})
    nl = pl.DataFrame(nl_rows, schema={c: t for c, (t, _) in NET_LOAD.items()}).sort("kind", "interval_start_utc")
    mix = pl.DataFrame(mix_rows, schema={c: t for c, (t, _) in GENERATION_MIX.items()}).sort(
        "kind", "fuel_group", "direction", "interval_start_utc")
    checks += [
        schema_check(nl, NET_LOAD, "net_load"),
        schema_check(mix, GENERATION_MIX, "generation_mix"),
        # Recorded, never fixed: intervals where a declared input is missing (ADR-B10).
        CheckResult("net_load_intervals_complete", "warn", not any(blocked.values()),
                    {"blocked_intervals": blocked, "definition": DEFINITION_VERSION}),
    ]
    return nl, mix, checks
