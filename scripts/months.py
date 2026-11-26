"""Print the first day of each month from FROM to TO (inclusive), one per line.

Why: monthly-partition pipelines (eia_henry_hub_monthly, partition = YYYY-MM-01) need
month starts; `atlas run --to` steps by day. Called by `make ingest-eia-gas`.
"""
import sys
from datetime import date

first, last = (date.fromisoformat(a).replace(day=1) for a in sys.argv[1:3])
while first <= last:
    print(first.isoformat())
    first = date(first.year + first.month // 12, first.month % 12 + 1, 1)
