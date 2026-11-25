"""ENTSO-E adapter and prices_entsoe_daily: failure cases and the task's retry rules.

No network: a fake opener serves real saved fixtures (tests/fixtures/entsoe/).
Failure cases are built by changing ONE thing in a real document inside the
test (a duplicated Point, a shortened Period, an extra series); the saved
files themselves are never edited.
"""

import io
import json
import re
import urllib.error
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.manifest import read_pointer
from atlas_pipelines.core.markets import RegistryError, load_market, load_registry
from atlas_pipelines.core.runner import run_pipeline
from atlas_pipelines.core.task import TaskContext
from atlas_pipelines.defs.prices import price_pipelines
from atlas_pipelines.sources.entsoe import day_ahead as da
from conftest import FIXTURES, REPO

REGISTRY = load_registry(REPO / "config" / "markets")
M = REGISTRY["entsoe"]
FX = FIXTURES / "entsoe"
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
TOKEN = "test-token-not-real-0123456789"
DAY = "2026-11-18"
# Fixtures are local-only (exchange-owned prices); tests that read them skip in CI.
needs_fixtures = pytest.mark.skipif(not FX.exists(), reason="ENTSO-E fixtures are local-only and not present")
BEFORE = lambda: datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)  # D-1 noon UTC: before the deadline
AFTER = lambda: datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)


def raw(name: str) -> bytes:
    return (FX / name).read_bytes()


def rows(name: str, zone: str, day: str, data: bytes | None = None):
    doc = da.parse_document(data if data is not None else raw(name))
    s, sel = da.select_series(doc, da.series_rule(M, zone))
    df = da.build_silver(s, M, zone, date.fromisoformat(day), "fixture") if s else None
    return doc, s, sel, df


def failed(results):
    return sorted(c.check_name for c in results if not c.passed and c.severity == "blocking")


# --- requests ---------------------------------------------------------------------


@pytest.mark.parametrize("day,start,end", [
    ("2026-11-18", "202611172300", "202611182300"),
    ("2025-10-26", "202510252200", "202510262300"),  # fall back: 25 h
    ("2026-03-29", "202603282300", "202603292200"),  # spring forward: 23 h
])
def test_request_covers_the_local_day_with_daily_contract_type(day, start, end):
    p = da.request_params(M, "DE_LU", date.fromisoformat(day))
    assert p == {"documentType": "A44", "contract_MarketAgreement.type": "A01",
                 "in_Domain": "10Y1001A1001A82H", "out_Domain": "10Y1001A1001A82H",
                 "periodStart": start, "periodEnd": end}
    assert "securityToken" not in p  # added only by the fetcher


# --- data failures (each must block) -------------------------------------------


@needs_fixtures
def test_wrong_day_blocks():
    # Real 2026-11-18 document checked as if it were 2026-11-17.
    doc, s, sel, _ = rows("SE3_2026-11-18_normal.xml", "SE3", DAY)
    df = da.build_silver(s, M, "SE3", date(2026, 11, 17), "fixture")
    got = failed(da.checks(df, s, doc, M, date(2026, 11, 17)))
    assert {"document_covers_requested_day", "interval_completeness", "rows_in_delivery_day"} <= set(got)


@needs_fixtures
def test_missing_interval_blocks():
    # Shorten the real Period by its last quarter-hour (A03 can't express a gap
    # inside a Period, so a missing interval is a Period that ends early).
    data = raw("SE3_2026-11-18_normal.xml").replace(
        b"<end>2026-11-18T23:00Z</end>", b"<end>2026-11-18T22:45Z</end>")
    last = re.findall(rb"<Point>\s*<position>96</position>.*?</Point>", data, re.S)
    assert len(last) == 1  # the real file lists position 96; drop it with the interval
    data = data.replace(last[0], b"")
    doc, s, sel, df = rows("", "SE3", DAY, data)
    results = sel + da.checks(df, s, doc, M, date(2026, 11, 18))
    assert df.height == 95
    assert "interval_completeness" in failed(results)
    assert da.publication_pending(results)  # short series: a retry may fix it


@needs_fixtures
def test_duplicate_position_is_rejected():
    data = raw("SE3_2026-11-18_normal.xml")
    first_point = re.search(rb"<Point>.*?</Point>", data, re.S).group(0)
    data = data.replace(first_point, first_point + first_point, 1)
    doc = da.parse_document(data)
    s, _ = da.select_series(doc, da.series_rule(M, "SE3"))
    with pytest.raises(da.ParseError, match="duplicate positions"):
        da.build_silver(s, M, "SE3", date(2026, 11, 18), "fixture")


@needs_fixtures
def test_unknown_series_blocks_and_is_not_retried():
    # A new auction appears: copy the real SE3 series with a sequence number.
    data = raw("SE3_2026-11-18_normal.xml")
    ts = re.search(rb"<TimeSeries>.*?</TimeSeries>", data, re.S).group(0)
    extra = ts.replace(b"<curveType>", b"<classificationSequence_AttributeInstanceComponent.position>7"
                       b"</classificationSequence_AttributeInstanceComponent.position><curveType>")
    doc, s, sel, df = rows("", "SE3", DAY, data.replace(ts, ts + extra))
    results = sel + da.checks(df, s, doc, M, date(2026, 11, 18))
    assert failed(results) == ["known_series_only"]
    assert not da.publication_pending(results)


@needs_fixtures
def test_missing_configured_series_waits_for_publication():
    # DE_LU without its SDAC series (sequence 1): EXAA alone must never be used.
    data = raw("DE_LU_2026-11-18_normal.xml")
    for ts in re.findall(rb"<TimeSeries>.*?</TimeSeries>", data, re.S):
        if b"position>1</classificationSequence" in ts:
            data = data.replace(ts, b"")
    doc, s, sel, _ = rows("", "DE_LU", DAY, data)
    assert s is None and failed(sel) == ["day_ahead_series_present"]
    assert da.publication_pending(sel)


@pytest.mark.parametrize("mutate,match", [
    (lambda d: d.replace(b"<curveType>A03", b"<curveType>A04"), "curveType"),
    (lambda d: d.replace(b"<resolution>PT15M", b"<resolution>P1D"), "resolution"),
    (lambda d: b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]>' + d.split(b"?>", 1)[1], "DTD"),
    (lambda d: d.replace(b"<position>1</position>", b"<position>2</position>", 1), "duplicate|first position"),
])
@needs_fixtures
def test_documents_we_dont_understand_are_rejected(mutate, match):
    data = mutate(raw("SE3_2026-11-18_normal.xml"))
    with pytest.raises(da.ParseError, match=match):
        doc = da.parse_document(data)
        s, _ = da.select_series(doc, da.series_rule(M, "SE3"))
        da.build_silver(s, M, "SE3", date(2026, 11, 18), "fixture")


def test_a01_must_list_every_position():
    p = da.Period(datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
                  15, ((1, 1.0), (3, 2.0)))
    assert [x[3] for x in da.expand(p, "A03")] == [1.0, 1.0, 2.0, 2.0]
    with pytest.raises(da.ParseError, match="A01"):
        da.expand(p, "A01")


# --- registry ---------------------------------------------------------------------


def test_selection_must_be_known_and_have_provenance(tmp_path):
    text = (REPO / "config" / "markets" / "entsoe.yaml").read_text(encoding="utf-8")
    bad = tmp_path / "entsoe.yaml"
    bad.write_text(text.replace("select: {contract: A01, sequence: 1}", "select: {contract: A01, sequence: 9}"),
                   encoding="utf-8")
    with pytest.raises(RegistryError, match="not in `known`"):
        da.series_rule(load_market(bad), "DE_LU")
    bad.write_text(text.replace("      status: sourced\n", "      status: probably\n", 1), encoding="utf-8")
    with pytest.raises(RegistryError, match="status"):
        da.series_rule(load_market(bad), "DE_LU")


def test_revision_refetch_date_from_registry():
    assert da.revision_refetch_date(M, date(2026, 11, 18)) == date(2026, 11, 21)


# --- the pipeline task --------------------------------------------------------------


class Resp:
    def __init__(self, body):
        self.status, self._b, self.headers = 200, body, {}

    def read(self, n=-1):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


EIC_ZONE = {"10Y1001A1001A82H": "DE_LU", "10YES-REE------0": "ES", "10Y1001A1001A46L": "SE3"}


def serving(by_zone: dict, seen: list | None = None):
    def opener(req, timeout):
        q = parse_qs(urlsplit(req.full_url).query)
        if seen is not None:
            seen.append(q)
        body = by_zone[EIC_ZONE[q["in_Domain"][0]]]
        if isinstance(body, Exception):
            raise body
        return Resp(body)
    return opener


def normal_day():
    return {z: raw(f"{z}_2026-11-18_normal.xml") for z in ("DE_LU", "ES", "SE3")}


def pipeline(opener, now):
    [p] = price_pipelines(REGISTRY, opener=opener, now=now, limiter=NOLIMIT)
    return p


@needs_fixtures
def test_pipeline_ingests_all_zones_and_never_stores_the_token(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    seen = []
    r = run_pipeline(pipeline(serving(normal_day(), seen), AFTER), DAY, lake)
    assert r.status == "SUCCEEDED", [(t.task, t.error) for t in r.tasks]
    assert all(q["securityToken"] == [TOKEN] for q in seen) and len(seen) == 3
    for z in ("de_lu", "es", "se3"):
        ptr = read_pointer(lake, f"silver.power_prices__entsoe_{z}", DAY)
        assert ptr["row_count"] == 96
    for f in Path(lake).rglob("*"):
        if f.is_file():
            assert TOKEN.encode() not in f.read_bytes(), f  # bronze, silver, manifests, pointers


@needs_fixtures
def test_refetch_with_changed_document_records_a_revision(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    assert run_pipeline(pipeline(serving(normal_day()), AFTER), DAY, lake).status == "SUCCEEDED"
    changed = normal_day()
    changed["SE3"] = changed["SE3"].replace(b"<revisionNumber>1<", b"<revisionNumber>2<")
    later = lambda: datetime(2026, 11, 26, 12, 0, tzinfo=timezone.utc)  # a re-fetch happens later
    r = run_pipeline(pipeline(serving(changed), later), DAY, lake)
    se3 = next(t for t in r.tasks if t.task == "ingest_se3")
    rev = next(c for c in se3.checks if c["check_name"] == "source_revision")
    assert se3.status == "SUCCEEDED" and rev["passed"] is False and rev["severity"] == "warn"
    de = next(t for t in r.tasks if t.task == "ingest_de_lu")
    assert next(c for c in de.checks if c["check_name"] == "source_revision")["passed"] is True


@needs_fixtures
def test_not_published_is_retryable_before_deadline_and_blocks_after(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    day = {**normal_day(), "DE_LU": raw("DE_LU_future_no_data.xml")}
    r = run_pipeline(pipeline(serving(day), BEFORE), DAY, lake)
    de = next(t for t in r.tasks if t.task == "ingest_de_lu")
    assert (de.status, de.error_class, de.retryable, de.retry_after_s) == ("FAILED_TERMINAL", "source_not_ready", True, 900)
    assert de.materializations and read_pointer(lake, "bronze.entsoe_day_ahead__de_lu", DAY) is None
    assert read_pointer(lake, "silver.power_prices__entsoe_es", DAY) is not None  # other zones unaffected

    r = run_pipeline(pipeline(serving(day), AFTER), DAY, lake)
    de = next(t for t in r.tasks if t.task == "ingest_de_lu")
    assert (de.error_class, de.retryable) == ("data_check_failed", False)
    assert "day_ahead_published" in de.error


def test_http_failures_are_classified(lake, monkeypatch):
    monkeypatch.setenv("ENTSOE_TOKEN", TOKEN)
    err = lambda code: urllib.error.HTTPError("https://x.invalid", code, "x", {}, io.BytesIO(b"<html/>"))
    day = {"DE_LU": err(401), "ES": err(429), "SE3": urllib.error.URLError("timeout")}
    r = {t.task: t for t in run_pipeline(pipeline(serving(day), AFTER), DAY, lake).tasks}
    assert (r["ingest_de_lu"].error_class, r["ingest_de_lu"].retryable) == ("data_check_failed", False)
    assert (r["ingest_es"].error_class, r["ingest_es"].retryable) == ("rate_limited", True)
    assert (r["ingest_se3"].error_class, r["ingest_se3"].retryable) == ("network", True)
    assert TOKEN not in json.dumps([t.error for t in r.values()])


@needs_fixtures
def test_missing_token_fails_without_fetching(lake, monkeypatch):
    monkeypatch.delenv("ENTSOE_TOKEN", raising=False)
    seen = []
    r = run_pipeline(pipeline(serving(normal_day(), seen), AFTER), DAY, lake)
    assert r.status == "FAILED" and not seen
