"""Local runner: executes a pipeline partition in-process, without Atlas.

Why it exists: data work must never wait for engine work (PROJECT_PLAN.md §4).
This runner calls the same task functions an Atlas worker will, and applies the
same publish rule, so B1–B6 can ingest real data before Atlas is ready.

Who calls it: cli.py (`python -m atlas_pipelines run ...`), tests.

What it does per task, in topological order:
  1. If any dependency did not succeed → CANCELED (error_class dep_failed), not run.
  2. Build a TaskContext with a fresh attempt id and call the task.
  3. Verify what the task reported: declared asset keys only, the right
     partition, the file exists inside the lake and its hash and row count
     match. A mismatch fails the task (error_class materialization_invalid).
  4. Any failed blocking check → FAILED_TERMINAL (error_class data_check_failed),
     materializations recorded but pointers NOT moved.
  4a. RetryableTaskError (e.g. source not published yet) → FAILED_TERMINAL
     locally with retryable=True and the task's error class; what it recorded
     is kept, pointers NOT moved. Atlas would re-queue it (A4).
  5. Otherwise SUCCEEDED and every materialization becomes current.
Finally the run manifest is written. Status names match internal/domain.

Not provided locally (Atlas's job): retries, leases, fencing, deadlines. A
local task runs once; an exception fails it.
"""

from __future__ import annotations

import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

import polars as pl

from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.lake import absolute_path
from atlas_pipelines.core.manifest import read_pointer, set_pointer, write_run_manifest
from atlas_pipelines.core.markets import iso_utc
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.task import RetryableTaskError, TaskContext, blocking_failures


class MaterializationInvalid(Exception):
    pass


@dataclass
class TaskOutcome:
    task: str
    attempt_id: str | None
    status: str  # SUCCEEDED | FAILED_TERMINAL | CANCELED
    error_class: str | None = None
    error: str | None = None
    retryable: bool = False
    retry_after_s: float | None = None
    materializations: list[dict] = field(default_factory=list)
    checks: list[dict] = field(default_factory=list)


@dataclass
class RunResult:
    run_id: str
    pipeline: str
    partition_key: str
    status: str  # SUCCEEDED | FAILED | CANCELED (same rules as domain.DeriveRunStatus)
    tasks: list[TaskOutcome]
    manifest_path: str


def _now() -> str:
    return iso_utc(datetime.now(timezone.utc))


def _new_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def run_pipeline(pipeline: Pipeline, partition_key: str, lake_root: str) -> RunResult:
    start, end = pipeline.data_interval(partition_key)  # also validates the key
    run_id = _new_id("local")
    started_at = _now()
    outcomes: dict[str, TaskOutcome] = {}

    for name in pipeline.order:
        tdef = pipeline.task(name)
        if any(outcomes[d].status != "SUCCEEDED" for d in tdef.deps):
            outcomes[name] = TaskOutcome(name, None, "CANCELED", "dep_failed", "a dependency did not succeed")
            continue

        attempt_id = _new_id("attempt")
        ctx = TaskContext(
            pipeline=pipeline.name,
            task=name,
            partition_key=partition_key,
            data_interval_start=start,
            data_interval_end=end,
            attempt_id=attempt_id,
            checkpoint=None,
            lake_root=lake_root,
            heartbeat=lambda details: True,  # local runs are never canceled
            resolve_input=lambda asset, part: read_pointer(lake_root, asset, part),
        )
        try:
            mats, checks = tdef.fn(ctx)
            _verify(tdef.asset_outputs, partition_key, lake_root, mats)
        except RetryableTaskError as e:
            try:
                _verify(tdef.asset_outputs, partition_key, lake_root, e.materializations)
            except MaterializationInvalid as bad:
                outcomes[name] = TaskOutcome(name, attempt_id, "FAILED_TERMINAL", "materialization_invalid", str(bad))
                continue
            outcomes[name] = TaskOutcome(
                name, attempt_id, "FAILED_TERMINAL", e.error_class, str(e), True, e.retry_after_s,
                [{**asdict(m), "materialization_id": f"{attempt_id}:{m.asset_key}", "created_at": _now()}
                 for m in e.materializations],
                [asdict(c) for c in e.checks],
            )
            continue  # nothing published; Atlas would retry
        except MaterializationInvalid as e:
            outcomes[name] = TaskOutcome(name, attempt_id, "FAILED_TERMINAL", "materialization_invalid", str(e))
            continue
        except Exception:
            outcomes[name] = TaskOutcome(
                name, attempt_id, "FAILED_TERMINAL", "task_error", traceback.format_exc(limit=5)
            )
            continue

        mat_records = [
            {**asdict(m), "materialization_id": f"{attempt_id}:{m.asset_key}", "created_at": _now()}
            for m in mats
        ]
        check_records = [asdict(c) for c in checks]
        failed = blocking_failures(checks)
        if failed:
            outcomes[name] = TaskOutcome(
                name,
                attempt_id,
                "FAILED_TERMINAL",
                "data_check_failed",
                "blocking checks failed: " + ", ".join(c.check_name for c in failed),
                False,
                None,
                mat_records,
                check_records,
            )
            continue  # pointer stays where it was

        for rec in mat_records:
            set_pointer(
                lake_root,
                {**rec, "run_id": run_id, "task": name, "attempt_id": attempt_id, "checks": check_records},
            )
        outcomes[name] = TaskOutcome(name, attempt_id, "SUCCEEDED", None, None, False, None, mat_records, check_records)

    statuses = [o.status for o in outcomes.values()]
    if "FAILED_TERMINAL" in statuses:
        run_status = "FAILED"
    elif "CANCELED" in statuses:
        run_status = "CANCELED"
    else:
        run_status = "SUCCEEDED"

    manifest = {
        "format_version": 1,
        "runner": "local",
        "run_id": run_id,
        "pipeline": pipeline.name,
        "partition_key": partition_key,
        "data_interval_start": start,
        "data_interval_end": end,
        "started_at": started_at,
        "finished_at": _now(),
        "status": run_status,
        "tasks": [asdict(outcomes[n]) for n in pipeline.order],
    }
    path = write_run_manifest(lake_root, manifest)
    return RunResult(run_id, pipeline.name, partition_key, run_status, list(outcomes.values()), str(path))


def _verify(declared: tuple[str, ...], partition_key: str, lake_root: str, mats) -> None:
    """Don't trust what a task reports about its own outputs; check the files."""
    for m in mats:
        if m.asset_key not in declared:
            raise MaterializationInvalid(f"undeclared asset {m.asset_key!r} (declared: {list(declared)})")
        if m.partition_key != partition_key:
            raise MaterializationInvalid(f"{m.asset_key}: partition {m.partition_key!r} != run partition")
        path = absolute_path(lake_root, m.uri)
        if not path.is_file():
            raise MaterializationInvalid(f"{m.asset_key}: file {m.uri} does not exist")
        actual = sha256_file(path)
        if actual != m.content_hash:
            raise MaterializationInvalid(f"{m.asset_key}: reported hash {m.content_hash} != file {actual}")
        if path.suffix == ".parquet":
            rows = pl.scan_parquet(path).select(pl.len()).collect().item()
            if rows != m.row_count:
                raise MaterializationInvalid(f"{m.asset_key}: reported {m.row_count} rows, file has {rows}")
