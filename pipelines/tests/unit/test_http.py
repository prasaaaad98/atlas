"""Shared fetch layer: outcome classes, secret redaction, rate limiting.

No network: every test injects a fake opener. Response bodies here are test
strings, not market data.
"""

import io
import socket
import urllib.error

import pytest

from atlas_pipelines.core.http import RateLimiter, build_url, http_get, redact
from atlas_pipelines.core.limits import load_limit
from conftest import REPO


class FakeResponse:
    def __init__(self, status=200, body=b"ok", headers=None):
        self.status, self._body, self.headers = status, body, headers or {}

    def read(self, n=-1):
        return self._body if n < 0 else self._body[:n]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def opener_returning(resp=None, exc=None, seen=None):
    def opener(req, timeout):
        if seen is not None:
            seen.append(req)
        if exc:
            raise exc
        return resp

    return opener


def http_error(code, body=b"", headers=None):
    return urllib.error.HTTPError("https://example.invalid", code, "x", headers or {}, io.BytesIO(body))


NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)


def test_success_is_succeeded():
    r = http_get("https://example.invalid/a", {"x": "1"}, limiter=NOLIMIT, opener=opener_returning(FakeResponse()))
    assert (r.status, r.outcome, r.error_class, r.body) == (200, "succeeded", None, b"ok")


@pytest.mark.parametrize(
    "code,error_class",
    [(400, "bad_request"), (401, "auth"), (403, "auth"), (404, "bad_request"), (429, "rate_limited"),
     (500, "server_error"), (503, "server_error")],
)
def test_http_errors_are_definite_with_class(code, error_class):
    r = http_get("https://example.invalid/a", {}, limiter=NOLIMIT, opener=opener_returning(exc=http_error(code, b"why")))
    assert (r.status, r.outcome, r.error_class, r.body) == (code, "failed_definite", error_class, b"why")


def test_429_reports_retry_after():
    exc = http_error(429, headers={"Retry-After": "120"})
    r = http_get("https://example.invalid/a", {}, limiter=NOLIMIT, opener=opener_returning(exc=exc))
    assert r.retry_after_s == 120.0


@pytest.mark.parametrize("exc", [socket.timeout("timed out"), urllib.error.URLError("connection reset"), ConnectionResetError()])
def test_network_failures_are_unknown(exc):
    # We can't tell whether the source served the request.
    r = http_get("https://example.invalid/a", {}, limiter=NOLIMIT, opener=opener_returning(exc=exc))
    assert (r.status, r.outcome, r.error_class) == (None, "unknown", "network")


def test_secret_params_are_sent_but_never_reported():
    seen = []
    r = http_get(
        "https://example.invalid/api",
        {"documentType": "A44", "securityToken": "s3cr3t-value"},
        secret_params=("securityToken",),
        limiter=NOLIMIT,
        opener=opener_returning(FakeResponse(), seen=seen),
    )
    assert "s3cr3t-value" in seen[0].full_url  # sent to the source
    assert "s3cr3t-value" not in r.url_redacted and "securityToken=REDACTED" in r.url_redacted
    assert "s3cr3t-value" not in repr(r)


def test_secret_never_leaks_into_error_detail():
    exc = urllib.error.URLError("failed for https://example.invalid/api?securityToken=s3cr3t-value")
    r = http_get("https://example.invalid/api", {"securityToken": "s3cr3t-value"}, secret_params=("securityToken",),
                 limiter=NOLIMIT, opener=opener_returning(exc=exc))
    assert "s3cr3t-value" not in r.detail


def test_oversized_body_is_rejected():
    r = http_get("https://example.invalid/a", {}, limiter=NOLIMIT, max_bytes=10,
                 opener=opener_returning(FakeResponse(body=b"x" * 11)))
    assert (r.outcome, r.error_class) == ("failed_definite", "response_too_large")


def test_build_url_and_redact():
    url = build_url("https://example.invalid/v1", {"b": "2", "a": "x y"})
    assert url == "https://example.invalid/v1?b=2&a=x+y"
    assert redact(url + "&token=abc", ("token",)).endswith("token=REDACTED")


def test_rate_limiter_spaces_requests():
    now = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        now[0] += s

    lim = RateLimiter(rpm=60, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        lim.acquire()
    # 60 rpm = one request per second: first immediate, then 1 s waits.
    assert slept == [1.0, 1.0]


def test_rate_limiter_rejects_nonsense():
    with pytest.raises(ValueError):
        RateLimiter(rpm=0)


def test_entsoe_limit_config():
    lim = load_limit("entsoe", REPO / "config" / "limits")
    assert (lim.key, lim.rpm, lim.max_concurrency) == ("entsoe", 60, 2)


def test_unknown_limit_key_is_an_error():
    with pytest.raises(FileNotFoundError):
        load_limit("nope", REPO / "config" / "limits")
