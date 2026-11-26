"""Pipeline definitions, built from the market registry.

Who calls it: cli.py (local runner) and, from B7, the Atlas registration step.
"""

from __future__ import annotations

from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.defs.calendar import calendar_pipelines
from atlas_pipelines.defs.demo import demo_pipelines
from atlas_pipelines.defs.eia import eia_pipelines
from atlas_pipelines.defs.engine_demo import engine_demo_pipelines
from atlas_pipelines.defs.entsoe_generation import generation_pipelines
from atlas_pipelines.defs.entsoe_load import load_pipelines
from atlas_pipelines.defs.prices import price_pipelines
from atlas_pipelines.defs.weather import weather_pipelines


def build_pipelines(registry: dict[str, Market]) -> dict[str, Pipeline]:
    pipelines = demo_pipelines(registry) + calendar_pipelines(registry) + engine_demo_pipelines() + eia_pipelines(registry) + price_pipelines(registry) + load_pipelines(registry) + generation_pipelines(registry) + weather_pipelines(registry)
    by_name = {p.name: p for p in pipelines}
    if len(by_name) != len(pipelines):
        raise ValueError("duplicate pipeline names")
    return by_name
