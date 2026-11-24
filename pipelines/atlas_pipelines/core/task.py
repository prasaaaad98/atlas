"""The runner-agnostic task contract (PHASE_AB_PLAN.md §2.8).

Why it exists: every pipeline task is a plain function
`run(ctx: TaskContext) -> tuple[list[Materialization], list[CheckResult]]`.
The local runner and the Atlas worker both build a TaskContext and call the
same function, so task code never changes between runners.

Who uses it: every task in defs/ and sources/; core/runner.py; later the
Atlas worker (B7).

Invariant: these types carry only plain data (str/int/bool/dict), so they can be
serialised to JSON for the Atlas completion request without conversion.

One deliberate difference from the plan's sketch: `attempt_id` is always set.
The local runner generates a `local-...` id, because the materialization
protocol needs an attempt-specific output directory in both runners and a task
should not have to branch on which runner it is in.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

SEVERITIES = ("blocking", "warn")


@dataclass(frozen=True)
class TaskContext:
    pipeline: str
    task: str
    partition_key: str  # business date, e.g. "2026-11-24" (delivery date for day-ahead)
    data_interval_start: str  # ISO 8601 UTC, e.g. "2026-11-23T18:30:00Z"
    data_interval_end: str  # ISO 8601 UTC
    attempt_id: str  # Atlas attempt id, or "local-..." from the local runner
    checkpoint: dict | None  # details from the last heartbeat of a previous attempt
    lake_root: str  # path or file:// URI; see core/lake.py
    heartbeat: Callable[[dict | None], bool]  # returns False when cancel is requested


@dataclass(frozen=True)
class Materialization:
    asset_key: str  # "<layer>.<name>", e.g. "silver.power_prices"
    partition_key: str
    uri: str  # relative to the lake root, forward slashes
    row_count: int
    content_hash: str  # "sha256:<hex>" of the file bytes
    schema_version: str
    code_version: str


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    severity: str  # "blocking" | "warn"
    passed: bool
    metrics: dict

    def __post_init__(self) -> None:
        # A typo like "block" must not silently become a non-blocking check.
        if self.severity not in SEVERITIES:
            raise ValueError(f"check {self.check_name!r}: severity must be one of {SEVERITIES}")


TaskFn = Callable[[TaskContext], tuple[list[Materialization], list[CheckResult]]]


def blocking_failures(checks: list[CheckResult]) -> list[CheckResult]:
    """Checks that must stop the current pointer from moving (ADR-003 decision 5)."""
    return [c for c in checks if c.severity == "blocking" and not c.passed]
