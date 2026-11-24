"""Local materialization manifest and current pointers (the local runner's catalog).

Why it exists: the local runner needs the same publish rule Atlas will enforce
in Postgres (§2.7): a materialization is recorded first, and the asset
partition's "current" pointer moves to it only if its blocking checks passed.
Until B7 these JSON files stand in for the `materializations` and
`asset_partitions` tables, with the same fields.

Layout under the lake root:
  _meta/runs/<run_id>.json                         one manifest per local run (append-only)
  _meta/current/<asset_key>/<partition_key>.json   current pointer for one asset partition

Pointer format (format_version 1):
  {"format_version": 1, "asset_key", "partition_key", "materialization_id",
   "run_id", "task", "attempt_id", "uri", "row_count", "content_hash",
   "schema_version", "code_version", "created_at",
   "checks": [{"check_name", "severity", "passed", "metrics"}]}

Who calls it: core/runner.py (writes), core/catalog.py and cli.py (read).

Invariant: a pointer file is only ever replaced whole (temp file + os.replace),
so a reader sees the old pointer or the new one, never a mix. Run manifests
are never rewritten.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from atlas_pipelines.core.lake import check_asset_key, check_segment, resolve_lake_root, write_bytes_atomic

FORMAT_VERSION = 1


def _meta(lake_root: str) -> Path:
    return resolve_lake_root(lake_root) / "_meta"


def _dump(obj: dict) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")


def write_run_manifest(lake_root: str, manifest: dict) -> Path:
    path = _meta(lake_root) / "runs" / f"{check_segment(manifest['run_id'], 'run id')}.json"
    write_bytes_atomic(path, _dump(manifest))  # refuses to overwrite
    return path


def pointer_path(lake_root: str, asset_key: str, partition_key: str) -> Path:
    return (
        _meta(lake_root)
        / "current"
        / check_asset_key(asset_key)
        / f"{check_segment(partition_key, 'partition key')}.json"
    )


def set_pointer(lake_root: str, record: dict) -> None:
    """Swap the current pointer. This is the one file in the lake that is replaced."""
    target = pointer_path(lake_root, record["asset_key"], record["partition_key"])
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    tmp.write_bytes(_dump({"format_version": FORMAT_VERSION, **record}))
    with open(tmp, "rb+") as f:
        os.fsync(f.fileno())
    os.replace(tmp, target)


def read_pointer(lake_root: str, asset_key: str, partition_key: str) -> dict | None:
    path = pointer_path(lake_root, asset_key, partition_key)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def list_pointers(lake_root: str) -> list[dict]:
    base = _meta(lake_root) / "current"
    if not base.exists():
        return []
    out = []
    for f in sorted(base.glob("*/*.json")):
        rec = json.loads(f.read_text(encoding="utf-8"))
        if rec.get("format_version") != FORMAT_VERSION:
            raise ValueError(f"{f}: unsupported pointer format {rec.get('format_version')!r}")
        out.append(rec)
    return out
