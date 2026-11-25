"""Pipeline definitions: a named static DAG of tasks over daily partitions.

Why it exists: both runners need the same description of a pipeline — task
functions, dependencies, declared asset outputs and the time zone that turns a
partition key into a UTC data interval. In B7 this is serialised into the
`pipelines.definition` jsonb that Atlas stores.

Who calls it: defs/ builds Pipeline objects; core/runner.py executes them.

Invariants protected (checked at construction, so a bad DAG never runs):
  - task names are unique; every dependency names an existing task; no cycles.
  - a task may only produce asset keys it declared (enforced by the runner).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from atlas_pipelines.core.lake import check_asset_key, check_segment
from atlas_pipelines.core.markets import iso_utc, parse_partition_date
from atlas_pipelines.core.task import TaskFn


@dataclass(frozen=True)
class TaskDef:
    name: str
    fn: TaskFn
    deps: tuple[str, ...] = ()
    asset_outputs: tuple[str, ...] = ()
    # Used by Atlas only (the local runner runs each task once): how many
    # attempts in total, including re-runs after an expired lease.
    max_attempts: int = 1
    # Retry policy for Atlas (A4, ADR-005): initial_interval_seconds,
    # backoff_coefficient, max_interval_seconds, non_retryable_error_classes.
    # None = Atlas's default policy.
    retry: dict | None = None
    # Source politeness budget (Atlas limit key, A5): config/limits/<key>.yaml,
    # registered with `python -m atlas_pipelines limits sync`. The local runner
    # ignores it (its fetchers use an in-process RateLimiter on the same key).
    limit_key: str | None = None


@dataclass(frozen=True)
class Pipeline:
    name: str
    partition_tz: str  # IANA zone the partition date is defined in (from the market registry)
    tasks: tuple[TaskDef, ...]
    description: str = ""
    order: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        check_segment(self.name, "pipeline name")
        ZoneInfo(self.partition_tz)
        names = [t.name for t in self.tasks]
        if not names or len(set(names)) != len(names):
            raise ValueError(f"pipeline {self.name}: task names must be non-empty and unique")
        for t in self.tasks:
            check_segment(t.name, "task name")
            for a in t.asset_outputs:
                check_asset_key(a)
            for d in t.deps:
                if d not in names:
                    raise ValueError(f"pipeline {self.name}: task {t.name} depends on unknown {d!r}")
        object.__setattr__(self, "order", _topological_order(self))

    def task(self, name: str) -> TaskDef:
        return next(t for t in self.tasks if t.name == name)

    def data_interval(self, partition_key: str) -> tuple[str, str]:
        """UTC bounds of a daily partition: local midnight to next local midnight.

        For an IST partition 2026-11-24 this is 2026-11-23T18:30Z -> 2026-11-24T18:30Z.
        """
        d = parse_partition_date(partition_key)
        tz = ZoneInfo(self.partition_tz)
        start = datetime.combine(d, time(0), tzinfo=tz)
        end = datetime.combine(d + timedelta(days=1), time(0), tzinfo=tz)
        return iso_utc(start), iso_utc(end)


def _topological_order(p: Pipeline) -> tuple[str, ...]:
    """Kahn's algorithm; ties keep declaration order so runs are reproducible."""
    remaining = {t.name: set(t.deps) for t in p.tasks}
    order: list[str] = []
    while remaining:
        ready = [t.name for t in p.tasks if t.name in remaining and not remaining[t.name]]
        if not ready:
            raise ValueError(f"pipeline {p.name}: dependency cycle among {sorted(remaining)}")
        for name in ready:
            order.append(name)
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return tuple(order)
