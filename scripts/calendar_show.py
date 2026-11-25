"""Show one year of silver.calendar per market and zone (make demo-b4e).

Reads the lake's DuckDB catalog (built by `make calendar`): days, public
holidays, weekend days and the DST days for each zone.
Usage: python scripts/calendar_show.py [year] [lake_root]
"""

import sys
from pathlib import Path

import duckdb

year = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
lake = Path(sys.argv[2] if len(sys.argv) > 2 else "lake")
con = duckdb.connect(str(lake / "catalog.duckdb"), read_only=True)
rows = con.execute("""
    SELECT market, zone, count(*) AS days, sum(is_public_holiday::int) AS holidays,
           sum(is_weekend::int) AS weekend_days,
           coalesce(string_agg(CASE WHEN dst_transition <> 'none' THEN dst_transition || ' ' || local_date END, ', '), '-')
    FROM silver.calendar WHERE year(local_date) = ? GROUP BY 1, 2 ORDER BY 1, 2""", [year]).fetchall()
print(f"silver.calendar {year}")
print(f"{'market':<7} {'zone':<7} {'days':>4} {'holidays':>8} {'weekend':>7}  dst days")
for m, z, d, h, w, dst in rows:
    print(f"{m:<7} {z:<7} {d:>4} {h:>8} {w:>7}  {dst}")
