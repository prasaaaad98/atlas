"""Engine demo pipelines: fake tasks that exercise Atlas Core, with no market data.

Why it exists: the A-track demos (make demo-a3/a4/a5) need tasks whose
behaviour is controlled (slow, chunked, flaky, rate-limited) so a kill, a
retry or a pause can be shown on a real control plane and real workers. They
write no lake data and touch no network, so they're safe to run anywhere.
Who calls it: defs/__init__.py; scripts/demo-a*.sh run them through workers.

  engine_chunked   one task, 6 chunks; heartbeats {"next_chunk": i} after each
                   chunk and resumes from the checkpoint after a kill (A3).
  engine_flaky     fails twice (server_error, retryable), then succeeds; the
                   failure count survives in the checkpoint (A4).
  engine_fatal     fails with `auth`: the worker says retryable, Atlas's
                   classifier says terminal, so there is exactly one attempt (A4).
Timing knob: ATLAS_DEMO_CHUNK_SECONDS (default 1.0) per chunk.
"""

from __future__ import annotations

import os
import time

from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, RetryableTaskError, TaskContext

CHUNKS = 6


def chunked(ctx: TaskContext):
    """Resumable work: start from the checkpoint a previous attempt left."""
    start = int((ctx.checkpoint or {}).get("next_chunk", 0))
    seconds = float(os.environ.get("ATLAS_DEMO_CHUNK_SECONDS", "1.0"))
    done = []
    for i in range(start, CHUNKS):
        time.sleep(seconds)  # the "work" for chunk i
        done.append(i)
        if not ctx.heartbeat({"next_chunk": i + 1}):
            # Atlas says this attempt no longer holds the task: stop now.
            raise RuntimeError(f"cancel requested after chunk {i}")
    return [], [CheckResult("chunks_processed", "warn", True,
                            {"resumed_from": start, "chunks_this_attempt": done, "attempt_id": ctx.attempt_id})]


def flaky(ctx: TaskContext):
    failures = int((ctx.checkpoint or {}).get("failures", 0))
    if failures < 2:
        ctx.heartbeat({"failures": failures + 1})  # remembered by Atlas for the next attempt
        raise RetryableTaskError("server_error", f"simulated HTTP 503 (failure {failures + 1} of 2)")
    return [], [CheckResult("succeeded_after_failures", "warn", True, {"failures_before": failures})]


def fatal(ctx: TaskContext):
    raise RetryableTaskError("auth", "simulated HTTP 401: the worker calls it retryable, Atlas must not")


DEMO_RETRY = {"initial_interval_seconds": 2, "backoff_coefficient": 2, "max_interval_seconds": 10}


def engine_demo_pipelines() -> list[Pipeline]:
    return [
        Pipeline("engine_chunked", "UTC", (TaskDef("process", chunked, max_attempts=3),),
                 description="Engine demo (A3): chunked task that resumes from its heartbeat checkpoint"),
        Pipeline("engine_flaky", "UTC", (TaskDef("fetch", flaky, max_attempts=4, retry=DEMO_RETRY),),
                 description="Engine demo (A4): fails twice with server_error, then succeeds"),
        Pipeline("engine_fatal", "UTC", (TaskDef("fetch", fatal, max_attempts=4, retry=DEMO_RETRY),),
                 description="Engine demo (A4): auth failure is never retried"),
    ]
