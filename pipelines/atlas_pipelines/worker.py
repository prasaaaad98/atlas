"""Minimal Atlas worker: claim a task over HTTP, run the same task function the
local runner runs, report the result (Slice A2, ADR-001).

Why it exists: the runner-agnostic task contract (core/task.py) is only
proven once a second runner executes the same functions. This worker is that
runner. It holds no orchestration state: Atlas decides what to run, and the
fencing token it returns at claim is sent back with every report.

Who calls it: cli.py (`python -m atlas_pipelines worker`, `register`, `submit`).

One loop iteration (run_one):
  POST /claim → 204: nothing ready.
  → find the pipeline + task in this process's definitions; check the run's
    data interval equals the local definition's (guards against a worker
    running code that differs from what was registered).
  POST /attempts/{id}/start
  heartbeat thread: POST /attempts/{id}/heartbeat every lease/3 (ADR-004);
    ctx.heartbeat(details) sends a checkpoint and returns False once Atlas
    says cancel_requested (replaced, reaped or canceled)
  call task(ctx) → verify reported files (same rules as the local runner)
  POST /attempts/{id}/complete with materializations + checks
     (Atlas decides SUCCEEDED vs data_check_failed from the blocking checks)
  or POST /attempts/{id}/fail with error class, outcome and retryable.

Invariants:
  - A 409 stale_attempt means this attempt no longer holds the task: the
    worker stops and discards its result (fencing). It never retries with a
    different token.
  - A transport error on a report has outcome `unknown`: the worker retries
    the same report with the same token; Atlas answers a repeat as a replay.
  - Pointer files are NOT moved here: with Atlas, the current pointer lives in
    PostgreSQL (A8). Only the local runner writes local pointers.
  - A lost heartbeat is not fatal: the lease (30 s) survives two missed beats.
"""

from __future__ import annotations

import json
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from atlas_pipelines.core.markets import iso_utc
from atlas_pipelines.core.pipeline import Pipeline
from atlas_pipelines.core.runner import MaterializationInvalid, _verify
from atlas_pipelines.core.task import RetryableTaskError, TaskContext

USER_AGENT = "atlas-grid-worker/0.1"


class AtlasError(Exception):
    def __init__(self, status: int | None, body: dict | None, message: str):
        super().__init__(message)
        self.status, self.body = status, body or {}


# --- HTTP client ---------------------------------------------------------------------

Send = Callable[[str, str, dict | None], tuple[int, dict | None]]


def urllib_send(base_url: str, timeout: float = 30.0) -> Send:
    def send(method: str, path: str, body: dict | None) -> tuple[int, dict | None]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base_url.rstrip("/") + path, data=data, method=method,
                                     headers={"Content-Type": "application/json", "User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read() if e.fp else b""
            try:
                return e.code, json.loads(raw) if raw else None
            except ValueError:
                return e.code, {"error": "non_json", "detail": raw[:200].decode(errors="replace")}
    return send


class AtlasClient:
    """Thin JSON client. Reports are retried on transport errors (outcome
    unknown) with the same body, which Atlas treats as a replay."""

    def __init__(self, send: Send, report_retries: int = 3, sleep=time.sleep):
        self.send, self.report_retries, self.sleep = send, report_retries, sleep

    def call(self, method: str, path: str, body: dict | None = None, *, retry: bool = False) -> tuple[int, dict | None]:
        tries = self.report_retries if retry else 1
        for i in range(tries):
            try:
                return self.send(method, path, body)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if i == tries - 1:
                    raise AtlasError(None, None, f"{method} {path}: outcome unknown: {e!r}") from e
                self.sleep(min(2 ** i, 10))
        raise AssertionError("unreachable")

    def report(self, attempt_id: str, kind: str, body: dict) -> dict:
        status, out = self.call("POST", f"/attempts/{attempt_id}/{kind}", body, retry=True)
        if status != 200:
            raise AtlasError(status, out, f"{kind} {attempt_id}: HTTP {status} {out}")
        return out


# --- definitions -----------------------------------------------------------------------


class Heartbeater:
    """Keeps one attempt's lease alive and carries its checkpoint.

    The background thread beats every `interval` seconds with no checkpoint
    (Atlas keeps the stored one). The task's ctx.heartbeat(details) beats at
    once with a checkpoint. Both return/record cancel_requested.
    """

    def __init__(self, client: "AtlasClient", attempt_id: str, token: int, lease_seconds: int):
        self.client, self.attempt_id, self.token, self.lease = client, attempt_id, token, lease_seconds
        self.cancel_requested = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name=f"heartbeat-{attempt_id[:8]}")

    def beat(self, checkpoint: dict | None = None) -> bool:
        body = {"fencing_token": self.token, "lease_seconds": self.lease}
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        try:
            status, out = self.client.call("POST", f"/attempts/{self.attempt_id}/heartbeat", body)
        except AtlasError:
            return not self.cancel_requested.is_set()  # transport hiccup: the lease covers it
        if status == 200 and (out or {}).get("cancel_requested"):
            self.cancel_requested.set()
        return not self.cancel_requested.is_set()

    def _loop(self) -> None:
        while not self._stop.wait(self.lease / 3):
            self.beat()

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)
        return False


def pipeline_definition(p: Pipeline) -> dict:
    """The JSON Atlas stores for a pipeline (POST /pipelines)."""
    return {
        "name": p.name,
        "partition_kind": "daily",
        "partition_tz": p.partition_tz,
        **({"deadline": p.deadline} if p.deadline else {}),
        "tasks": [
            {"name": t.name, "deps": list(t.deps), "asset_outputs": list(t.asset_outputs),
             "max_attempts": t.max_attempts, **({"retry": t.retry} if t.retry else {}),
             **({"limit_key": t.limit_key} if t.limit_key else {}),
             **({"estimate_seconds": t.estimate_seconds} if t.estimate_seconds else {})}
            for t in p.tasks
        ],
    }


# --- the loop --------------------------------------------------------------------------


@dataclass
class Outcome:
    attempt_id: str
    pipeline: str
    task: str
    partition_key: str
    reported: str  # complete | fail | stale
    task_status: str | None
    run_status: str | None
    detail: str = ""


def _iso(value: str) -> str:
    # Go writes RFC 3339 ("2026-11-23T23:00:00Z"); normalise to our form.
    return iso_utc(datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc))


def run_one(client: AtlasClient, pipelines: dict[str, Pipeline], lake_root: str, worker_id: str,
            lease_seconds: int = 30) -> Outcome | None:
    status, claim = client.call("POST", "/claim", {"worker_id": worker_id, "lease_seconds": lease_seconds})
    if status == 204:
        return None
    if status != 200:
        raise AtlasError(status, claim, f"claim: HTTP {status} {claim}")
    aid, token = claim["attempt_id"], claim["fencing_token"]
    base = dict(attempt_id=aid, pipeline=claim["pipeline"], task=claim["task"], partition_key=claim["partition_key"])

    def fail(error_class: str, detail: str, outcome: str, retryable: bool, retry_after: float | None = None) -> Outcome:
        body = {"fencing_token": token, "error_class": error_class, "detail": detail[:8000],
                "outcome": outcome, "retryable": retryable}
        if retry_after:
            body["retry_after_seconds"] = retry_after
        return _report(client, aid, "fail", body, base, detail)

    pipeline = pipelines.get(claim["pipeline"])
    tdef = next((t for t in pipeline.tasks if t.name == claim["task"]), None) if pipeline else None
    if tdef is None:
        # Nothing ran, so nothing external happened: failed_definite, not retryable here.
        return fail("unknown_task", f"this worker has no {claim['pipeline']}.{claim['task']}", "failed_definite", False)
    local = pipeline.data_interval(claim["partition_key"])
    remote = (_iso(claim["data_interval_start"]), _iso(claim["data_interval_end"]))
    if local != remote:
        return fail("definition_mismatch", f"data interval {remote} from Atlas != local {local}", "failed_definite", False)

    try:
        client.report(aid, "start", {"fencing_token": token})
    except AtlasError as e:
        if e.status == 409:
            return Outcome(**base, reported="stale", task_status=None, run_status=None, detail=str(e))
        raise

    hb = Heartbeater(client, aid, token, lease_seconds)
    ctx = TaskContext(
        pipeline=claim["pipeline"], task=claim["task"], partition_key=claim["partition_key"],
        data_interval_start=local[0], data_interval_end=local[1], attempt_id=aid,
        checkpoint=claim.get("checkpoint"), lake_root=lake_root,
        heartbeat=hb.beat,
    )
    try:
        with hb:
            mats, checks = tdef.fn(ctx)
        _verify(tdef.asset_outputs, claim["partition_key"], lake_root, mats)
    except RetryableTaskError as e:
        outcome = "unknown" if e.error_class == "network" else "failed_definite"
        return fail(e.error_class, str(e), outcome, True, e.retry_after_s)
    except MaterializationInvalid as e:
        return fail("materialization_invalid", str(e), "failed_definite", False)
    except Exception:
        # Unknown failure: we can't tell whether an external call took effect.
        return fail("task_error", traceback.format_exc(limit=5), "unknown", False)

    body = {"fencing_token": token, "materializations": [asdict(m) for m in mats],
            "checks": [asdict(c) for c in checks]}
    return _report(client, aid, "complete", body, base, "")


def _report(client: AtlasClient, aid: str, kind: str, body: dict, base: dict, detail: str) -> Outcome:
    try:
        out = client.report(aid, kind, body)
    except AtlasError as e:
        if e.status == 409:  # fenced out: our result is discarded by design
            return Outcome(**base, reported="stale", task_status=None, run_status=None, detail=str(e))
        raise
    return Outcome(**base, reported=kind, task_status=out.get("task_status"), run_status=out.get("run_status"),
                   detail=detail)


def work(client: AtlasClient, pipelines: dict[str, Pipeline], lake_root: str, worker_id: str, *,
         max_tasks: int | None = None, exit_when_idle: bool = False, poll_seconds: float = 2.0,
         lease_seconds: int = 30,
         on_outcome: Callable[[Outcome], None] = lambda o: None, sleep=time.sleep) -> int:
    """Claim and run tasks until max_tasks is reached, or the queue is empty and exit_when_idle."""
    done = 0
    while max_tasks is None or done < max_tasks:
        out = run_one(client, pipelines, lake_root, worker_id, lease_seconds)
        if out is None:
            if exit_when_idle:
                break
            sleep(poll_seconds)  # polling; LISTEN/NOTIFY is measured against it in A9
            continue
        done += 1
        on_outcome(out)
    return done
