"""Shared interval checks and the bronze-write helper used by every adapter.

Data failure tests: missing block, duplicate key, wrong day, DST day, HTTP
error, re-fetch revision.
"""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from atlas_pipelines.checks.intervals import interval_checks
from atlas_pipelines.core.task import CheckResult, TaskContext
from atlas_pipelines.defs.demo import grid_frame
from atlas_pipelines.sources.adapter import revision_check, write_bronze

DAY = "2026-11-24"


def _by_name(checks):
    return {c.check_name: c for c in checks}


def test_complete_day_passes(registry):
    df = grid_frame(registry["dummy_ist"], "DAM", DAY)
    c = _by_name(interval_checks(df, registry["dummy_ist"], "DAM", date(2026, 11, 24)))
    assert c["interval_completeness"].passed and c["primary_key_unique"].passed


def test_missing_block_fails(registry):
    df = grid_frame(registry["dummy_ist"], "DAM", DAY).filter(pl.col("local_block") != 37)
    c = _by_name(interval_checks(df, registry["dummy_ist"], "DAM", date(2026, 11, 24)))
    assert not c["interval_completeness"].passed
    assert c["interval_completeness"].metrics["found"] == 95
    assert c["interval_completeness"].metrics["missing_first10"] == ["2026-11-24T03:30:00Z"]


def test_duplicate_key_fails(registry):
    df = grid_frame(registry["dummy_ist"], "DAM", DAY)
    df = pl.concat([df, df.head(1)])
    c = _by_name(interval_checks(df, registry["dummy_ist"], "DAM", date(2026, 11, 24)))
    assert not c["primary_key_unique"].passed and c["primary_key_unique"].metrics["duplicate_rows"] == 1


def test_wrong_day_fails(registry):
    df = grid_frame(registry["dummy_ist"], "DAM", "2026-11-23")
    c = _by_name(interval_checks(df, registry["dummy_ist"], "DAM", date(2026, 11, 24)))
    assert not c["interval_completeness"].passed


def test_normal_day_grid_fails_on_dst_day(registry):
    # A 24-hour grid is wrong on the 25-hour fall-back day.
    m = registry["dummy_cet"]
    df = grid_frame(m, "DAM", "2025-10-25")  # 24 hourly intervals
    c = _by_name(interval_checks(df, m, "DAM", date(2025, 10, 26)))
    assert not c["interval_completeness"].passed and c["interval_completeness"].metrics["expected"] == 25


def test_invalid_severity_rejected():
    with pytest.raises(ValueError):
        CheckResult("x", "block", True, {})


def _ctx(lake):
    return TaskContext("p", "fetch", DAY, "", "", "attempt-1", None, lake, lambda d: True)


T = datetime(2026, 11, 23, 11, 30, 5, tzinfo=timezone.utc)
PAYLOAD = b"test payload, not market data"


def _bronze(lake, status=200, data=PAYLOAD, when=T):
    return write_bronze(
        _ctx(lake),
        source="testsrc",
        dataset="snap",
        partition_field="delivery_date",
        filename="raw.html",
        data=data,
        fetched_at=when,
        source_url="https://example.invalid/x",
        http_status=status,
        code_version="t",
    )


def test_bronze_stores_bytes_unchanged(lake):
    from atlas_pipelines.core.lake import absolute_path

    f = _bronze(lake)
    assert f.outcome == "succeeded"
    assert absolute_path(lake, f.materialization.uri).read_bytes() == PAYLOAD
    assert f.materialization.asset_key == "bronze.testsrc_snap"
    assert all(c.passed for c in f.checks)


def test_bronze_same_fetch_time_cannot_overwrite(lake):
    _bronze(lake)
    with pytest.raises(FileExistsError):
        _bronze(lake, data=b"different")


def test_http_error_is_blocking_and_definite(lake):
    f = _bronze(lake, status=503)
    assert f.outcome == "failed_definite"
    assert not _by_name(f.checks)["http_200"].passed


def test_revision_is_a_warning_not_an_error():
    assert revision_check("sha256:a", None).passed
    assert revision_check("sha256:a", "sha256:a").passed
    r = revision_check("sha256:b", "sha256:a")
    assert (r.severity, r.passed) == ("warn", False)
