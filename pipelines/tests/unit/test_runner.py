"""Local runner: publish rule, idempotency, catalog, and failure handling.

B0 acceptance: a dummy task writes a partition through the local runner,
re-running produces the same content hash, and the DuckDB catalog exposes it.
Failure tests: failing blocking check, crashing task, task lying about its
output, undeclared output, bad DAG.
"""

import json

import duckdb
import pytest

from atlas_pipelines.core.catalog import build_catalog
from atlas_pipelines.core.hashing import sha256_file
from atlas_pipelines.core.lake import attempt_dir, relative_uri, write_bytes_atomic
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.core.task import CheckResult, Materialization
from atlas_pipelines.defs import build_pipelines

DAY = "2026-11-24"


def test_demo_run_publishes_and_rerun_is_idempotent(registry, lake):
    p = build_pipelines(registry)["demo_grid_dummy_ist_dam"]
    first = run_pipeline(p, DAY, lake)
    assert first.status == "SUCCEEDED"
    ptr1 = read_pointer(lake, "demo.grid_dummy_ist_dam", DAY)
    assert ptr1["row_count"] == 96 and ptr1["run_id"] == first.run_id

    second = run_pipeline(p, DAY, lake)
    ptr2 = read_pointer(lake, "demo.grid_dummy_ist_dam", DAY)
    assert second.status == "SUCCEEDED"
    assert ptr2["content_hash"] == ptr1["content_hash"]  # same inputs, same bytes
    assert ptr2["uri"] != ptr1["uri"]  # but a new attempt directory
    assert ptr2["run_id"] == second.run_id  # pointer moved to the newest good attempt

    manifest = json.loads(open(second.manifest_path, encoding="utf-8").read())
    assert manifest["status"] == "SUCCEEDED" and manifest["data_interval_start"] == "2026-11-23T18:30:00Z"


def test_catalog_exposes_current_partitions_of_two_markets(registry, lake):
    pipes = build_pipelines(registry)
    assert run_pipeline(pipes["demo_grid_dummy_ist_dam"], DAY, lake).status == "SUCCEEDED"
    assert run_pipeline(pipes["demo_grid_dummy_ist_dam"], DAY, lake).status == "SUCCEEDED"  # 2 attempts on disk
    assert run_pipeline(pipes["demo_grid_dummy_cet_dam"], "2026-10-25", lake).status == "SUCCEEDED"

    path, counts = build_catalog(lake)
    assert counts == {"demo.grid_dummy_ist_dam": 1, "demo.grid_dummy_cet_dam": 1}
    con = duckdb.connect(str(path), read_only=True)
    try:
        # Only the current attempt is visible: 96 rows, not 192.
        assert con.execute("SELECT count(*) FROM demo.grid_dummy_ist_dam").fetchone()[0] == 96
        # DST fall-back day in the 15-min era: 100 intervals.
        assert con.execute("SELECT count(*) FROM demo.grid_dummy_cet_dam").fetchone()[0] == 100
        cols = [r[0] for r in con.execute("DESCRIBE demo.grid_dummy_ist_dam").fetchall()]
        assert "attempt" not in cols  # hive directory names don't leak into data
    finally:
        con.close()


def test_catalog_rebuild_is_safe_when_empty(lake):
    _, counts = build_catalog(lake)
    assert counts == {}


# --- failure tests with small purpose-built pipelines ------------------------


def _writer(asset="demo.t", passed=True, rows=b"x", lie_about_hash=False):
    """A task that writes one file and reports one blocking check."""

    def fn(ctx):
        d = attempt_dir(ctx.lake_root, "demo", "t", "date", ctx.partition_key, ctx.attempt_id)
        path = d / "out.bin"
        write_bytes_atomic(path, rows)
        h = "sha256:" + "0" * 64 if lie_about_hash else sha256_file(path)
        mat = Materialization(asset, ctx.partition_key, relative_uri(ctx.lake_root, path), 0, h, "1", "t")
        return [mat], [CheckResult("must_pass", "blocking", passed, {})]

    return fn


def _pipe(*tasks):
    return Pipeline("t", "Asia/Kolkata", tuple(tasks))


def test_failed_blocking_check_leaves_pointer_unchanged(lake):
    good = run_pipeline(_pipe(TaskDef("w", _writer(rows=b"good"), asset_outputs=("demo.t",))), DAY, lake)
    before = read_pointer(lake, "demo.t", DAY)
    assert good.status == "SUCCEEDED"

    bad = run_pipeline(
        _pipe(
            TaskDef("w", _writer(rows=b"bad", passed=False), asset_outputs=("demo.t",)),
            TaskDef("downstream", _writer(), deps=("w",), asset_outputs=("demo.t",)),
        ),
        DAY,
        lake,
    )
    assert bad.status == "FAILED"
    w, down = bad.tasks
    assert (w.status, w.error_class) == ("FAILED_TERMINAL", "data_check_failed")
    assert w.materializations, "the failed materialization is still recorded"
    assert (down.status, down.error_class) == ("CANCELED", "dep_failed")  # downstream not run
    assert read_pointer(lake, "demo.t", DAY) == before


def test_warn_check_failure_still_publishes(lake):
    def fn(ctx):
        mats, _ = _writer()(ctx)
        return mats, [CheckResult("looks_odd", "warn", False, {"note": "recorded only"})]

    r = run_pipeline(_pipe(TaskDef("w", fn, asset_outputs=("demo.t",))), DAY, lake)
    assert r.status == "SUCCEEDED"
    assert read_pointer(lake, "demo.t", DAY)["checks"][0]["passed"] is False


def test_crashing_task_fails_run_and_cancels_dependents(lake):
    def crash(ctx):
        raise RuntimeError("source returned garbage")

    r = run_pipeline(_pipe(TaskDef("a", crash), TaskDef("b", _writer(), deps=("a",))), DAY, lake)
    assert r.status == "FAILED"
    assert r.tasks[0].error_class == "task_error" and "source returned garbage" in r.tasks[0].error
    assert r.tasks[1].status == "CANCELED"


def test_task_lying_about_hash_is_rejected(lake):
    r = run_pipeline(_pipe(TaskDef("w", _writer(lie_about_hash=True), asset_outputs=("demo.t",))), DAY, lake)
    assert r.tasks[0].error_class == "materialization_invalid"
    assert read_pointer(lake, "demo.t", DAY) is None


def test_undeclared_asset_is_rejected(lake):
    r = run_pipeline(_pipe(TaskDef("w", _writer(asset="demo.other"), asset_outputs=("demo.t",))), DAY, lake)
    assert r.tasks[0].error_class == "materialization_invalid"


def test_wrong_day_partition_rejected(registry, lake):
    p = build_pipelines(registry)["demo_grid_dummy_ist_dam"]
    with pytest.raises(ValueError):
        run_pipeline(p, "2026/11/24", lake)


def test_bad_dags_rejected():
    noop = lambda ctx: ([], [])  # noqa: E731
    with pytest.raises(ValueError, match="cycle"):
        _pipe(TaskDef("a", noop, deps=("b",)), TaskDef("b", noop, deps=("a",)))
    with pytest.raises(ValueError, match="unknown"):
        _pipe(TaskDef("a", noop, deps=("zzz",)))
    with pytest.raises(ValueError, match="unique"):
        _pipe(TaskDef("a", noop), TaskDef("a", noop))


def test_topological_order_respects_deps():
    noop = lambda ctx: ([], [])  # noqa: E731
    p = _pipe(TaskDef("c", noop, deps=("a", "b")), TaskDef("b", noop, deps=("a",)), TaskDef("a", noop))
    assert p.order == ("a", "b", "c")
