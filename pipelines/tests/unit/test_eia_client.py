"""EIA client core (E1) against REAL saved EIA responses (pipelines/tests/fixtures/eia/,
public domain, saved by the owner's `make fixture-eia` run). No network: a fake opener
serves the saved bodies. Failure cases change one thing in a real response, or play
back the failure the owner's run actually saw (a read timeout on page 3)."""

import io
import json
import socket
import urllib.error
from urllib.parse import parse_qs, urlsplit

import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.sources.eia import client
from conftest import FIXTURES

FX = FIXTURES / "eia"
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)
KEY = "test-eia-key-not-real-0123456789"
REGION = "data__region-data__hourly__CISO__2026-11-18.json"
PAGES = {o: f"page__fuel-type-data__CISO__2026-11-18__offset{o:05d}.json" for o in (0, 100, 200, 300)}


def ok(name):
    p = FX / name
    if not p.exists():
        return False
    return json.loads(p.with_name(p.name.replace(".json", ".meta.json")).read_text())["http_status"] == 200


def body(name):
    return (FX / name).read_bytes()


class Resp:
    def __init__(self, data, status=200):
        self.status, self._d, self.headers = status, data, {}

    def read(self, n=-1):
        return self._d

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def by_offset(table, seen=None):
    """Serve table[offset] (bytes or an exception) for each request."""
    def opener(req, timeout):
        if seen is not None:
            seen.append(req)
        off = int(parse_qs(urlsplit(req.full_url).query)["offset"][0])
        v = table[off]
        if isinstance(v, Exception):
            raise v
        return Resp(v)
    return opener


needs = lambda *names: pytest.mark.skipif(not all(ok(n) for n in names), reason=f"real EIA fixture missing or not 200: {names}")


def failed(checks):
    return sorted(c.check_name for c in checks if c.severity == "blocking" and not c.passed)


@needs(REGION)
def test_single_page_region_data_is_complete_and_numeric():
    seen = []
    f = client.fetch("electricity/rto/region-data", [("frequency", "hourly")], key=KEY, limiter=NOLIMIT,
                     sort=("respondent", "type"), opener=by_offset({0: body(REGION)}, seen))
    assert f.outcome == "succeeded" and f.total == len(f.rows) == 148 and len(f.pages) == 1
    assert failed(client.checks(f, ("respondent", "type"))) == []
    assert {r["type"] for r in f.rows} == {"D", "DF", "NG", "TI"} and {r["value-units"] for r in f.rows} == {"megawatthours"}
    assert all(len(r["period"]) == 13 and r["period"][10] == "T" for r in f.rows)  # hourly UTC "YYYY-MM-DDTHH"
    # The key travels in a header only: never in the URL, the recorded URL or the bronze file.
    req = seen[0]
    assert KEY not in req.full_url and req.get_header(client.KEY_HEADER.capitalize()) == KEY
    assert KEY not in f.pages[0].url and KEY.encode() not in client.bronze_bytes(f)
    q = parse_qs(urlsplit(req.full_url).query)
    assert q["sort[0][column]"] == ["period"] and q["sort[1][column]"] == ["respondent"] and q["length"] == ["5000"]


@needs(PAGES[0], PAGES[100])
def test_page_failure_mid_way_is_never_a_complete_fetch():
    # What the owner's run actually saw: pages 1-2 fine, page 3 timed out.
    timeout = urllib.error.URLError(socket.timeout("The read operation timed out"))
    f = client.fetch("electricity/rto/fuel-type-data", [], key=KEY, limiter=NOLIMIT, page_length=100,
                     opener=by_offset({0: body(PAGES[0]), 100: body(PAGES[100]), 200: timeout}))
    assert f.total == 333 and len(f.rows) == 200 and f.outcome == "unknown" and f.error_class == "network"
    assert failed(client.checks(f, ("respondent", "fueltype"))) == ["all_pages_http_200", "rows_match_total"]
    assert "offset 200" in f.stopped


@needs(PAGES[0])
def test_capped_download_fails_rows_match_total():
    f = client.fetch("electricity/rto/fuel-type-data", [], key=KEY, limiter=NOLIMIT, page_length=100, max_pages=1,
                     opener=by_offset({0: body(PAGES[0])}))
    assert (len(f.rows), f.total) == (100, 333)
    assert failed(client.checks(f, ("respondent", "fueltype"))) == ["rows_match_total"]


@needs(PAGES[0])
def test_repeated_rows_across_pages_are_caught():
    # An unstable order would hand the same rows out twice: serve page 1 for every offset.
    f = client.fetch("electricity/rto/fuel-type-data", [], key=KEY, limiter=NOLIMIT, page_length=100,
                     opener=by_offset({o: body(PAGES[0]) for o in (0, 100, 200, 300)}))
    assert "no_duplicate_rows" in failed(client.checks(f, ("respondent", "fueltype")))
    assert "rows_match_total" in failed(client.checks(f, ("respondent", "fueltype")))  # 400 rows != 333


@needs(*PAGES.values(), "data__fuel-type-data__hourly__CISO__2026-11-18.json")
def test_four_pages_reassemble_exactly_the_single_request():
    f = client.fetch("electricity/rto/fuel-type-data", [], key=KEY, limiter=NOLIMIT, page_length=100,
                     opener=by_offset({o: body(n) for o, n in PAGES.items()}))
    single = json.loads(body("data__fuel-type-data__hourly__CISO__2026-11-18.json"))["response"]["data"]
    key = lambda r: (r["period"], r["fueltype"], r["value"])
    assert sorted(map(key, f.rows)) == sorted(map(key, single)) and failed(client.checks(f, ("respondent", "fueltype"))) == []


@needs(REGION)
def test_non_numeric_value_blocks_and_null_only_warns():
    raw = body(REGION)
    first_value = json.loads(raw)["response"]["data"][0]["value"]
    bad = raw.replace(f'"value":"{first_value}"'.encode(), b'"value":"n/a"', 1)
    f = client.fetch("x", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: bad}))
    assert failed(client.checks(f, ("respondent", "type"))) == ["values_numeric"]
    null = raw.replace(f'"value":"{first_value}"'.encode(), b'"value":null', 1)
    f = client.fetch("x", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: null}))
    cs = client.checks(f, ("respondent", "type"))
    assert failed(cs) == [] and [c.passed for c in cs if c.check_name == "values_present"] == [False]


def http_error(code, data=b""):
    return urllib.error.HTTPError("https://api.eia.gov/v2/x", code, "x", {}, io.BytesIO(data))


@pytest.mark.parametrize("code,cls", [(401, "auth"), (403, "auth"), (429, "rate_limited"), (500, "server_error"), (504, "server_error")])
def test_http_failures_are_classified(code, cls):
    data = (FX / "errors" / "gateway_504.html").read_bytes() if code == 504 and (FX / "errors" / "gateway_504.html").exists() else b""
    f = client.fetch("x", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: http_error(code, data)}))
    assert (f.outcome, f.error_class) == ("failed_definite", cls) and f.rows == []
    assert failed(client.checks(f, ())) == ["all_pages_http_200", "rows_match_total"]


def test_bronze_round_trip_and_key_scrubbing():
    echoed = b'{"response":{"total":"0","data":[]},"request":{"params":{"api_key":"' + KEY.encode() + b'"}}}'
    f = client.fetch("x", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: echoed}))
    raw = client.bronze_bytes(f)
    assert KEY.encode() not in raw and b"REDACTED" in raw
    [page] = client.read_bronze(raw)
    assert page["offset"] == 0 and page["http_status"] == 200 and json.loads(page["body"])["response"]["total"] == "0"


def test_to_number():
    assert client.to_number("21770") == 21770 and client.to_number("-3.5") == -3.5 and client.to_number(None) is None
    for bad in ("n/a", "", "nan", "inf"):
        with pytest.raises(ValueError):
            client.to_number(bad)
    with pytest.raises(ValueError):
        client.fetch("x", [], key="", limiter=NOLIMIT)


SAMPLES = sorted(p.name for p in FX.glob("data__*-data__hourly__*.json")
                 if not p.name.endswith(".meta.json") and ok(p.name)) if FX.exists() else []


@pytest.mark.skipif(not SAMPLES, reason="no real EIA hourly samples saved")
@pytest.mark.parametrize("name", SAMPLES)
def test_every_saved_hourly_sample(name):
    _, route, _, ba, day = name[:-5].split("__")
    f = client.fetch(f"electricity/rto/{route}", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: body(name)}))
    keys = {"region-data": ("respondent", "type"), "fuel-type-data": ("respondent", "fueltype"),
            "interchange-data": ("fromba", "toba")}[route]
    assert failed(client.checks(f, keys)) == [] and f.rows, name
    assert {r[keys[0]] for r in f.rows} == {ba}                    # only the BA we asked for
    periods = sorted(r["period"] for r in f.rows)
    assert periods[0].startswith(day) and all(len(p) == 13 for p in periods)   # UTC hours from that day on


def test_bronze_file_holds_every_page_and_no_key(lake):
    from datetime import datetime, timezone
    from atlas_pipelines.core.task import TaskContext
    echoed = b'{"response":{"total":"0","data":[]}}'
    f = client.fetch("x", [], key=KEY, limiter=NOLIMIT, opener=by_offset({0: echoed}))
    ctx = TaskContext("p", "t", "2026-11-18", "a", "b", "att-1", None, lake, lambda d: True)
    b = client.write_fetch(ctx, f, "region_data__ciso", datetime(2026, 11, 26, tzinfo=timezone.utc), "eia-1")
    assert all(c.passed for c in b.checks) and b.materialization.uri.endswith("pages.jsonl")
    assert KEY not in b.materialization.uri
    import pathlib
    raw = (pathlib.Path(lake) / b.materialization.uri).read_bytes()
    assert len(client.read_bronze(raw)) == 1 and KEY.encode() not in raw
