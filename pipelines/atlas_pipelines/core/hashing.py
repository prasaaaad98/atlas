"""Content hashing for materializations and bronze fetches.

Why it exists: a content hash is how we detect source revisions (bronze), prove
re-runs are idempotent (same inputs → same hash), and check that the file a
pointer names is the file the task reported.
Who calls it: tasks, core/runner.py, sources/adapter.py.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

PREFIX = "sha256:"


def sha256_bytes(data: bytes) -> str:
    return PREFIX + hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        # Stream in 1 MiB chunks so large raw files don't load into memory.
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return PREFIX + h.hexdigest()
