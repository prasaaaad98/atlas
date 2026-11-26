"""E6: EIA schedules config and request sizing. No network, no control plane.

Proves: config/schedules/eia.yaml is valid, names only real pipelines, and maps to the
engine's schedule body (grain, month offset, re-fetch days); one partition = one request
under EIA's 5,000-row page, measured on every saved real response (paging handles more).
"""

import json

import pytest

from atlas_pipelines.cli import load_schedules, schedule_body
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.defs import build_pipelines
from atlas_pipelines.sources.eia import client
from conftest import FIXTURES, REPO

CONFIG = REPO / "config" / "schedules" / "eia.yaml"


def test_every_scheduled_pipeline_exists_and_maps_to_the_engine_body():
    schedules = load_schedules(CONFIG)
    names = set(build_pipelines(load_registry(REPO / "config" / "markets")))
    assert set(schedules) == {"eia930_region_daily", "eia930_fuel_daily", "eia_henry_hub_monthly", "eia_storage_monthly"}
    assert set(schedules) <= names
    region = schedule_body(schedules["eia930_region_daily"])
    assert region["refetch_after_days"] == [3, 30] and region["partition_offset_days"] == -1
    assert region["partition_grain"] == "day" and region["catchup"] and region["start_partition"] == "2026-11-01"
    storage = schedule_body(schedules["eia_storage_monthly"])
    assert (storage["partition_grain"], storage["partition_offset_months"], storage["partition_offset_days"]) == ("month", -2, 0)


def test_bad_schedule_files_are_refused_before_any_call(tmp_path):
    bad = tmp_path / "s.yaml"
    bad.write_text("version: 1\nschedules:\n  x: {cron: '0 6 * * *', tz: UTC, refetch_days: [3]}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="refetch_days"):
        load_schedules(bad)
    bad.write_text("version: 1\nschedules:\n  x: {tz: UTC}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="cron"):
        load_schedules(bad)


def saved_totals(pattern):
    out = {}
    for f in sorted((FIXTURES / "eia").glob(pattern)):
        if f.name.endswith(".meta.json"):
            continue
        try:
            out[f.name] = int(json.loads(f.read_text(encoding="utf-8"))["response"]["total"])
        except (ValueError, KeyError):
            continue  # saved error pages (e.g. a 504) have no total
    return out


@pytest.mark.parametrize("pattern,partition", [
    ("data__region-data__hourly__*.json", "one BA, one UTC day"),
    ("data__fuel-type-data__hourly__*.json", "one BA, one UTC day"),
    ("data__natural-gas_pri_fut__RNGWHHD__daily.json", "~2 months (a month partition is smaller)"),
    ("data__electricity_operating-generator-capacity__energy_source_code_MWH__monthly.json", "the whole US, one month"),
])
def test_one_partition_fits_one_page(pattern, partition):
    totals = saved_totals(pattern)
    if not totals:
        pytest.skip(f"no saved responses for {pattern}")
    assert max(totals.values()) <= client.MAX_PAGE_ROWS, (partition, totals)
