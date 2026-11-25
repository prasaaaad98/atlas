"""Atlas worker loop (A2) against a fake Atlas: protocol, fencing, outcomes.

No network: `send` is a fake that plays the control plane's documented
responses. The Go side of the same contract is tested in internal/api.
"""

import urllib.error

import pytest

from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, RetryableTaskError
from atlas_pipelines.defs.demo import demo_pipelines
from atlas_pipelines.worker import AtlasClient, AtlasError, pipeline_definition, run_one, work

DAY = "2026-11-24"


class FakeAtlas:
    """Hands out queued claims; records every report."""

    def __init__(self, claims, start_status=200, report_status=200, flaky=0):
        self.claims, self.calls = list(claims), []
        self.start_status, self.report_status, self.flaky = start_status, report_status, flaky

    def send(self, method, path, body):
        self.calls.append((method, path, body))
        if path == "/claim":
            return (200, self.claims.pop(0)) if self.claims else (204, None)
        if self.flaky:
            self.flaky -= 1
            raise urllib.error.URLError("connection reset")
        kind = path.rsplit("/", 1)[1]
        status = self.start_status if kind == "start" else self.report_status
        if status != 200:
            return status, {"error": "stale_attempt"}
        return 200, {"task_status": "RUNNING" if kind == "start" else "SUCCEEDED", "run_status": "SUCCEEDED"}

    def reports(self, kind):
        return [b for _, p, b in self.calls if p.endswith("/" + kind)]


def claim(pipeline, task, interval=("2026-11-23T18:30:00Z", "2026-11-24T18:30:00Z"), token=1):
    return {"attempt_id": "a-1", "fencing_token": token, "pipeline": pipeline, "task": task, "partition_key": DAY,
            "data_interval_start": interval[0], "data_interval_end": interval[1], "checkpoint": None}


def one_task_pipeline(fn, name="p"):
    return {name: Pipeline(name, "Asia/Kolkata", (TaskDef("t", fn),))}


def test_runs_the_registered_task_and_completes_with_checks(registry, lake):
    pipes = {p.name: p for p in demo_pipelines(registry)}
    name = "demo_grid_dummy_ist_dam"
    task = pipes[name].tasks[0].name
    fake = FakeAtlas([claim(name, task)])
    out = run_one(AtlasClient(fake.send), pipes, lake, "w1")
    assert (out.reported, out.task_status) == ("complete", "SUCCEEDED")
    [start], [done] = fake.reports("start"), fake.reports("complete")
    assert start == {"fencing_token": 1} and done["fencing_token"] == 1
    assert done["materializations"] and all(m["partition_key"] == DAY for m in done["materializations"])
    assert {c["severity"] for c in done["checks"]} <= {"blocking", "warn"}


def test_failed_blocking_check_is_still_a_completion(lake):
    # Atlas, not the worker, turns a failed blocking check into data_check_failed.
    fake = FakeAtlas([claim("p", "t")])
    pipes = one_task_pipeline(lambda ctx: ([], [CheckResult("c", "blocking", False, {})]))
    assert run_one(AtlasClient(fake.send), pipes, lake, "w").reported == "complete"
    assert fake.reports("complete")[0]["checks"][0]["passed"] is False


def test_retryable_error_is_reported_retryable_with_outcome(lake):
    def fn(ctx):
        raise RetryableTaskError("network", "timeout", retry_after_s=30)
    fake = FakeAtlas([claim("p", "t")])
    run_one(AtlasClient(fake.send), one_task_pipeline(fn), lake, "w")
    [f] = fake.reports("fail")
    assert (f["error_class"], f["outcome"], f["retryable"], f["retry_after_seconds"]) == ("network", "unknown", True, 30)


def test_unexpected_exception_is_not_retryable_and_outcome_unknown(lake):
    def fn(ctx):
        raise ValueError("boom")
    fake = FakeAtlas([claim("p", "t")])
    run_one(AtlasClient(fake.send), one_task_pipeline(fn), lake, "w")
    [f] = fake.reports("fail")
    assert (f["error_class"], f["outcome"], f["retryable"]) == ("task_error", "unknown", False)
    assert "boom" in f["detail"]


def test_unknown_task_and_interval_mismatch_fail_without_running(lake):
    ran = []
    pipes = one_task_pipeline(lambda ctx: ran.append(1) or ([], []))
    fake = FakeAtlas([claim("other", "t"), claim("p", "t", interval=("2026-11-24T00:00:00Z", "2026-11-25T00:00:00Z"))])
    client = AtlasClient(fake.send)
    assert run_one(client, pipes, lake, "w").detail.startswith("this worker has no")
    assert "data interval" in run_one(client, pipes, lake, "w").detail
    assert [f["error_class"] for f in fake.reports("fail")] == ["unknown_task", "definition_mismatch"]
    assert not ran and not fake.reports("start")


def test_stale_attempt_stops_without_running_or_retrying(lake):
    ran = []
    fake = FakeAtlas([claim("p", "t")], start_status=409)
    out = run_one(AtlasClient(fake.send), one_task_pipeline(lambda ctx: ran.append(1) or ([], [])), lake, "w")
    assert out.reported == "stale" and not ran and not fake.reports("complete")


def test_stale_completion_is_discarded(lake):
    fake = FakeAtlas([claim("p", "t")], report_status=409)
    fake.start_status = 200
    out = run_one(AtlasClient(fake.send), one_task_pipeline(lambda ctx: ([], [])), lake, "w")
    assert out.reported == "stale"


def test_lost_response_is_retried_with_the_same_report(lake):
    fake = FakeAtlas([claim("p", "t")], flaky=1)  # the start report's first try fails in transit
    out = run_one(AtlasClient(fake.send, sleep=lambda s: None), one_task_pipeline(lambda ctx: ([], [])), lake, "w")
    assert out.reported == "complete"
    assert fake.reports("start") == [{"fencing_token": 1}, {"fencing_token": 1}]


def test_unreachable_atlas_raises_outcome_unknown(lake):
    fake = FakeAtlas([claim("p", "t")], flaky=10)
    with pytest.raises(AtlasError, match="outcome unknown"):
        run_one(AtlasClient(fake.send, sleep=lambda s: None), one_task_pipeline(lambda ctx: ([], [])), lake, "w")


def test_work_drains_the_queue_then_exits(lake):
    fake = FakeAtlas([claim("p", "t"), claim("p", "t")])
    seen = []
    n = work(AtlasClient(fake.send), one_task_pipeline(lambda ctx: ([], [])), lake, "w",
             exit_when_idle=True, on_outcome=seen.append)
    assert n == 2 and [o.reported for o in seen] == ["complete", "complete"]


def test_pipeline_definition_matches_the_go_contract(registry):
    p = demo_pipelines(registry)[0]
    d = pipeline_definition(p)
    assert d["partition_kind"] == "daily" and d["partition_tz"] == p.partition_tz
    assert [t["name"] for t in d["tasks"]] == [t.name for t in p.tasks]
    assert set(d["tasks"][0]) == {"name", "deps", "asset_outputs"}
