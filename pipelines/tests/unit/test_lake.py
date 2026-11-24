"""Lake path conventions and write rules (immutability, atomic publish)."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from atlas_pipelines.core.hashing import sha256_bytes, sha256_file
from atlas_pipelines.core.lake import (
    absolute_path,
    attempt_dir,
    bronze_dir,
    relative_uri,
    resolve_lake_root,
    write_bytes_atomic,
)

T = datetime(2026, 11, 23, 11, 30, 5, tzinfo=timezone.utc)


def test_bronze_layout(lake):
    d = bronze_dir(lake, "iex", "dam_market_snapshot", "delivery_date", "2026-11-24", T)
    assert relative_uri(lake, d / "raw.html") == (
        "bronze/iex/dam_market_snapshot/delivery_date=2026-11-24/fetched_at=20261123T113005Z/raw.html"
    )


def test_attempt_layout(lake):
    d = attempt_dir(lake, "silver", "power_prices", "delivery_date", "2026-11-24", "attempt-42")
    assert relative_uri(lake, d / "part-0.parquet") == (
        "silver/power_prices/delivery_date=2026-11-24/attempt=attempt-42/part-0.parquet"
    )


def test_bronze_has_no_attempt_dirs(lake):
    with pytest.raises(ValueError):
        attempt_dir(lake, "bronze", "x", "date", "2026-11-24", "a1")


@pytest.mark.parametrize("bad", ["..", "../etc", "a/b", "a b", ""])
def test_path_segments_cannot_escape(lake, bad):
    with pytest.raises(ValueError):
        attempt_dir(lake, "silver", "power_prices", "date", bad, "a1")


def test_uri_cannot_escape_lake(lake):
    with pytest.raises(ValueError):
        absolute_path(lake, "../outside.parquet")


def test_fetched_at_must_be_utc(lake):
    with pytest.raises(ValueError):
        bronze_dir(lake, "iex", "x", "date", "2026-11-24", datetime(2026, 11, 23, 11, 30))
    ist = timezone(timedelta(hours=5, minutes=30))
    with pytest.raises(ValueError):
        bronze_dir(lake, "iex", "x", "date", "2026-11-24", T.astimezone(ist))


def test_write_is_atomic_and_never_overwrites(tmp_path):
    target = tmp_path / "a" / "raw.bin"
    write_bytes_atomic(target, b"first")
    assert target.read_bytes() == b"first"
    with pytest.raises(FileExistsError):
        write_bytes_atomic(target, b"second")
    assert target.read_bytes() == b"first"  # immutable
    assert [p.name for p in target.parent.iterdir()] == ["raw.bin"]  # no temp files left


def test_failed_write_leaves_nothing(tmp_path):
    from atlas_pipelines.core.lake import _publish

    target = tmp_path / "x.bin"

    def boom(tmp):
        Path(tmp).write_bytes(b"partial")
        raise RuntimeError("crash mid-write")

    with pytest.raises(RuntimeError):
        _publish(target, boom)
    assert list(tmp_path.iterdir()) == []  # no final file, no temp file


def test_lake_root_uri(tmp_path):
    assert resolve_lake_root(str(tmp_path)) == tmp_path
    assert resolve_lake_root(tmp_path.as_uri()).resolve() == tmp_path.resolve()
    with pytest.raises(NotImplementedError):
        resolve_lake_root("s3://bucket/lake")


def test_hash_of_file_matches_bytes(tmp_path):
    p = tmp_path / "f"
    p.write_bytes(b"abc")
    assert sha256_file(p) == sha256_bytes(b"abc")
    assert sha256_bytes(b"abc").startswith("sha256:ba7816bf")  # known SHA-256 of "abc"
