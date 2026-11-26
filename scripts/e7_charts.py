"""Net-load duck curve per BA from gold.fundamentals_hourly (E7 walkthrough charts).

Why: the E7 acceptance asks for one chart per BA: average net load by local hour,
weekday vs weekend, computed only from current gold partitions in the lake. Plain SVG
(standard library only: no plotting dependency). Prints the data behind each chart
(days used, per-hour means) so the figure can be checked against gold.
Usage: python scripts/e7_charts.py --lake-root lake --out docs/walkthroughs   (make e7-charts)
Days with any null net load are left out and counted (never filled).
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

import polars as pl

from atlas_pipelines.core.manifest import list_pointers

W, H, PAD = 640, 320, 48


def svg(ba: str, series: dict[str, list[float | None]], days: dict[str, int]) -> str:
    vals = [v for s in series.values() for v in s if v is not None]
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    x = lambda h: PAD + h * (W - 2 * PAD) / 23  # noqa: E731
    y = lambda v: H - PAD - (v - lo) * (H - 2 * PAD) / span  # noqa: E731
    colors = {"weekday": "#2a6fdb", "weekend": "#d9822b"}
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="sans-serif" font-size="11">',
             f'<rect width="{W}" height="{H}" fill="white"/>',
             f'<text x="{PAD}" y="20" font-size="13">{ba}: average net load (demand - solar - wind), MW, by local hour</text>']
    for v in (lo, (lo + hi) / 2, hi):
        parts.append(f'<line x1="{PAD}" x2="{W - PAD}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="#ddd"/>'
                     f'<text x="4" y="{y(v) + 4:.1f}">{v:,.0f}</text>')
    for h in range(0, 24, 3):
        parts.append(f'<text x="{x(h) - 4:.1f}" y="{H - PAD + 16}">{h:02d}</text>')
    for i, (name, s) in enumerate(series.items()):
        pts = " ".join(f"{x(h):.1f},{y(v):.1f}" for h, v in enumerate(s) if v is not None)
        parts.append(f'<polyline fill="none" stroke="{colors[name]}" stroke-width="2" points="{pts}"/>')
        parts.append(f'<text x="{W - PAD - 150}" y="{40 + 14 * i}" fill="{colors[name]}">{name} ({days[name]} days)</text>')
    parts.append(f'<text x="{PAD}" y="{H - 8}" fill="#666">Source: U.S. Energy Information Administration (EIA-930), via Atlas gold</text>')
    return "\n".join(parts + ["</svg>"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default="lake")
    ap.add_argument("--out", default="docs/walkthroughs")
    args = ap.parse_args()
    lake = Path(args.lake_root)
    by_ba = defaultdict(list)
    for p in list_pointers(str(lake)):
        if p["asset_key"].startswith("gold.fundamentals_hourly__us_ba_"):
            by_ba[p["asset_key"].rsplit("_", 1)[-1].upper()].append(lake / p["uri"])
    if not by_ba:
        print("no gold.fundamentals_hourly partitions in the lake; build them first (make gold-eia)")
        return 1
    Path(args.out).mkdir(parents=True, exist_ok=True)
    for ba, files in sorted(by_ba.items()):
        df = pl.concat([pl.read_parquet(f) for f in files])
        bad_days = df.filter(pl.col("net_load_mw").is_null())["local_date"].unique()
        df = df.filter(~pl.col("local_date").is_in(bad_days.implode()))
        df = df.with_columns(pl.when(pl.col("local_date").dt.weekday() >= 6).then(pl.lit("weekend"))
                             .otherwise(pl.lit("weekday")).alias("day_type"))
        means = df.group_by("day_type", "local_hour").agg(pl.col("net_load_mw").mean().alias("mean"))
        series, days = {}, {}
        for kind in ("weekday", "weekend"):
            m = {r["local_hour"]: r["mean"] for r in means.filter(pl.col("day_type") == kind).iter_rows(named=True)}
            series[kind] = [m.get(h) for h in range(24)]
            days[kind] = df.filter(pl.col("day_type") == kind)["local_date"].n_unique()
        if not any(v is not None for s in series.values() for v in s):
            print(f"{ba}: no complete days")
            continue
        out = Path(args.out) / f"e7-net-load-{ba.lower()}.svg"
        out.write_text(svg(ba, series, days), encoding="utf-8")
        print(f"{ba}: {out}  days weekday={days['weekday']} weekend={days['weekend']} "
              f"left out (null net load)={bad_days.len()}  local dates {df['local_date'].min()}..{df['local_date'].max()}")
        for kind, s in series.items():
            print(f"   {kind:<8}" + " ".join("    -" if v is None else f"{v:7.0f}" for v in s))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
