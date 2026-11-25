"""Engine demo pipelines: fake tasks that exercise Atlas Core, with no market data.

Why it exists: the A-track demos (make demo-a3/a4/a5) need tasks whose
behaviour is controlled (slow, chunked, flaky, rate-limited) so a kill, a
retry or a pause can be shown on a real control plane and real workers. They
write no lake data and touch no network, so they're safe to run anywhere.
Who calls it: defs/__init__.py; scripts/demo-a*.sh run them through workers.

  engine_chunked   one task, 6 chunks; heartbeats {"next_chunk": i} after each
                   chunk and resumes from the checkpoint after a kill (A3).
Timing knob: ATLAS_DEMO_CHUNK_SECONDS (default 1.0) per chunk.
"""

from __future__ import annotations

import os
import time

from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import CheckResult, TaskContext

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


def engine_demo_pipelines() -> list[Pipeline]:
    return [
        Pipeline("engine_chunked", "UTC", (TaskDef("process", chunked, max_attempts=3),),
                 description="Engine demo (A3): chunked task that resumes from its heartbeat checkpoint"),
    ]
