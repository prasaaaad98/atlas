"""Pipeline definitions, built from the market registry.

Who calls it: cli.py (local runner) and, from B7, the Atlas registration step.
"""

from __future__ import annotations

from atlas_pipelines.core.markets import Market
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.defs.demo import demo_pipelines


def build_pipelines(registry: dict[str, Market]) -> dict[str, Pipeline]:
    pipelines = demo_pipelines(registry)
    by_name = {p.name: p for p in pipelines}
    if len(by_name) != len(pipelines):
        raise ValueError("duplicate pipeline names")
    return by_name
