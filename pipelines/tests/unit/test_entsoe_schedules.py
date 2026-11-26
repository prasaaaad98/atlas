"""config/schedules/entsoe.yaml is valid and names real pipelines; re-fetches cover late series (ADR-B10)."""

from atlas_pipelines.cli import load_schedules, schedule_body
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.defs import build_pipelines
from conftest import REPO


def test_entsoe_schedules_name_real_pipelines_and_refetch():
    specs = load_schedules(REPO / "config" / "schedules" / "entsoe.yaml")
    pipelines = build_pipelines(load_registry(REPO / "config" / "markets"))
    assert set(specs) == {"load_entsoe_daily", "generation_entsoe_daily", "entsoe_gold_daily"} <= set(pipelines)
    for name, spec in specs.items():
        assert spec["tz"] == "Europe/Brussels", name
        schedule_body(spec)  # the body the control plane receives builds without error
    # Point in time: silver fetches TOMORROW (forecast before the day); actuals via re-fetches.
    for name in ("load_entsoe_daily", "generation_entsoe_daily"):
        assert specs[name]["offset_days"] == 1 and specs[name]["refetch_after_days"] == [1, 2, 7], name
    # Gold: the day before yesterday (after the +1 re-fetch), again after +2/+7.
    assert specs["entsoe_gold_daily"]["offset_days"] == -2 and specs["entsoe_gold_daily"]["refetch_after_days"] == [2, 7]
    # gold runs after both ingests on the same day
    def hm(name):
        minute, hour = specs[name]["cron"].split()[:2]
        return int(hour), int(minute)
    assert hm("entsoe_gold_daily") > hm("generation_entsoe_daily") > hm("load_entsoe_daily")
