"""Lake layout: where bronze, silver and gold files live, and how they are written.

Why it exists: the materialization protocol (PHASE_AB_PLAN.md §2.7) depends on
every writer following the same path rules:
  bronze: <layer>/<source>/<dataset>/<pfield>=<pvalue>/fetched_at=<UTC>/<name>
  silver/gold/demo: <layer>/<dataset>/<pfield>=<pvalue>/attempt=<attempt_id>/<name>
Readers find the "current" file only through a pointer (core/manifest.py),
never by listing directories, so a half-written or stale-attempt file is
never read.

Who calls it: tasks (to get output paths), core/runner.py, core/catalog.py.

Invariants protected:
  - Nothing is ever overwritten. Bronze is immutable (CLAUDE.md) and attempt
    directories are unique, so `write_*_atomic` refuses an existing target.
  - A file at its final name is complete: writers write a temp file, flush and
    fsync it, then rename it into place.
  - Path segments are validated, so a partition value like "../x" can't escape the lake.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

LAYERS = ("bronze", "silver", "gold", "demo")
_SEGMENT = re.compile(r"^[A-Za-z0-9_.\-]+$")
_ASSET_KEY = re.compile(r"^(bronze|silver|gold|demo)\.[a-z0-9_]+$")


def resolve_lake_root(lake_root: str) -> Path:
    """Turn the configured lake root (a path or URI) into a local directory.

    The lake root is a URI so it can move to S3-compatible storage later
    without changing task code; only local paths and file:// URIs work today.
    """
    parsed = urlparse(lake_root)
    if parsed.scheme == "file":
        path = unquote(parsed.path)
        # file:///D:/lake parses to "/D:/lake" on Windows.
        if re.match(r"^/[A-Za-z]:", path):
            path = path[1:]
        return Path(path)
    # A single letter "scheme" is a Windows drive (D:\lake), not a URI.
    if parsed.scheme and len(parsed.scheme) > 1:
        raise NotImplementedError(f"lake root scheme {parsed.scheme!r} is not supported yet")
    return Path(lake_root)


def check_segment(value: str, what: str) -> str:
    if not _SEGMENT.match(value) or value in (".", ".."):
        raise ValueError(f"invalid {what} {value!r}: use letters, digits, '_', '-', '.'")
    return value


def check_asset_key(key: str) -> str:
    if not _ASSET_KEY.match(key):
        raise ValueError(f"invalid asset key {key!r}: expected '<layer>.<name>' in lower case")
    return key


def fetched_at_segment(fetched_at: datetime) -> str:
    """UTC fetch time as a sortable path segment, e.g. 20261123T113005Z."""
    if fetched_at.tzinfo is None or fetched_at.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("fetched_at must be a timezone-aware UTC datetime")
    return fetched_at.strftime("%Y%m%dT%H%M%SZ")


def bronze_dir(
    lake_root: str,
    source: str,
    dataset: str,
    partition_field: str,
    partition_value: str,
    fetched_at: datetime,
) -> Path:
    return (
        resolve_lake_root(lake_root)
        / "bronze"
        / check_segment(source, "source")
        / check_segment(dataset, "dataset")
        / f"{check_segment(partition_field, 'partition field')}={check_segment(partition_value, 'partition value')}"
        / f"fetched_at={fetched_at_segment(fetched_at)}"
    )


def attempt_dir(
    lake_root: str,
    layer: str,
    dataset: str,
    partition_field: str,
    partition_value: str,
    attempt_id: str,
) -> Path:
    if layer not in LAYERS or layer == "bronze":
        raise ValueError(f"attempt directories are for silver/gold/demo, not {layer!r}")
    return (
        resolve_lake_root(lake_root)
        / layer
        / check_segment(dataset, "dataset")
        / f"{check_segment(partition_field, 'partition field')}={check_segment(partition_value, 'partition value')}"
        / f"attempt={check_segment(attempt_id, 'attempt id')}"
    )


def relative_uri(lake_root: str, path: Path) -> str:
    """The uri stored in a Materialization: relative to the lake root, forward slashes."""
    rel = Path(path).resolve().relative_to(resolve_lake_root(lake_root).resolve())
    return rel.as_posix()


def absolute_path(lake_root: str, uri: str) -> Path:
    root = resolve_lake_root(lake_root).resolve()
    path = (root / uri).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"uri {uri!r} escapes the lake root")
    return path


def _tmp_name(target: Path) -> Path:
    return target.with_name(f".{target.name}.tmp-{os.getpid()}")


def write_bytes_atomic(target: Path, data: bytes) -> None:
    """Write bytes to a new file; the final name appears only when complete."""
    _publish(target, lambda tmp: tmp.write_bytes(data))


def write_parquet_atomic(df, target: Path) -> None:
    """Write a Polars DataFrame as Parquet with fixed settings.

    Fixed compression and statistics settings keep the bytes identical for
    identical data, which the idempotency test relies on.
    """
    _publish(target, lambda tmp: df.write_parquet(tmp, compression="zstd", statistics=True))


def _publish(target: Path, write) -> None:
    target = Path(target)
    if target.exists():
        raise FileExistsError(f"refusing to overwrite {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_name(target)
    try:
        write(tmp)
        with open(tmp, "rb+") as f:
            os.fsync(f.fileno())
        # Not atomic against another writer of the same target, but targets are
        # unique per attempt (or per fetch timestamp), so there is no such writer.
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()
