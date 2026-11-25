"""Shared HTTP GET for source adapters: outcome classes, redaction, politeness.

Why it exists: every external call attempt must get an outcome class
(`succeeded`, `failed_definite`, `unknown`; CLAUDE.md). Secrets such as API
tokens must never reach logs, bronze manifests or error text. And every request
must go through the source's limit key. Doing this once here keeps adapters
from each getting it slightly wrong.
Who calls it: sources/<source>/ fetch functions (never tests with real network).

Uses only the standard library (urllib): one GET with a timeout needs nothing more.

Classification:
  2xx                      -> succeeded
  400, 404, other 4xx      -> failed_definite, bad_request
  401, 403                 -> failed_definite, auth (don't retry)
  429                      -> failed_definite, rate_limited (retry after Retry-After)
  5xx                      -> failed_definite, server_error (source said it failed)
  timeout / connection     -> unknown, network (we can't tell if it was served)
Adapters may refine a 2xx further (e.g. ENTSO-E "no data" acknowledgements).
"""

from __future__ import annotations

import socket
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

USER_AGENT = "atlas-grid-pipelines/0.1"  # generic: no personal or project-owner details
DEFAULT_MAX_BYTES = 50 * 1024 * 1024


@dataclass(frozen=True)
class FetchResult:
    status: int | None
    body: bytes = field(repr=False)
    headers: dict
    outcome: str  # succeeded | failed_definite | unknown
    error_class: str | None
    url_redacted: str
    retry_after_s: float | None = None
    detail: str = ""


def build_url(base: str, params: dict) -> str:
    return f"{base}?{urlencode(params)}" if params else base


def redact(url: str, secret_names) -> str:
    parts = urlsplit(url)
    q = [(k, "REDACTED" if k in secret_names else v) for k, v in parse_qsl(parts.query, keep_blank_values=True)]
    return urlunsplit(parts._replace(query=urlencode(q)))


class RateLimiter:
    """Spaces requests at most `rpm` per minute within this process.

    Across processes the limit is enforced by Atlas at claim (A5); the local
    runner runs one process, so this is enough there.
    """

    def __init__(self, rpm: int, clock=time.monotonic, sleep=time.sleep):
        if rpm <= 0:
            raise ValueError("rpm must be positive")
        self.interval = 60.0 / rpm
        self._clock, self._sleep = clock, sleep
        self._next = None
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            if self._next is not None and now < self._next:
                self._sleep(self._next - now)
                now = self._next
            self._next = now + self.interval


def _classify(status: int) -> tuple[str, str | None]:
    if 200 <= status < 300:
        return "succeeded", None
    if status in (401, 403):
        return "failed_definite", "auth"
    if status == 429:
        return "failed_definite", "rate_limited"
    if status >= 500:
        return "failed_definite", "server_error"
    return "failed_definite", "bad_request"


def _retry_after(headers) -> float | None:
    value = (headers or {}).get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None  # HTTP-date form: caller falls back to its own backoff


def http_get(
    base_url: str,
    params: dict | list[tuple[str, str]],
    *,
    limiter: RateLimiter,
    secret_params: tuple[str, ...] = (),
    headers: dict[str, str] | None = None,
    secret_headers: tuple[str, ...] = (),
    timeout: float = 60.0,
    max_bytes: int = DEFAULT_MAX_BYTES,
    opener=urllib.request.urlopen,
) -> FetchResult:
    """params: a dict, or a list of pairs when a key repeats (EIA: facets[x][]=a&facets[x][]=b).
    secret_params are sent in the URL but redacted from what is reported; secret_headers
    (e.g. EIA's X-Api-Key) never appear in the URL, and their values are scrubbed from errors."""
    pairs = list(params.items()) if isinstance(params, dict) else list(params)
    url = build_url(base_url, pairs)
    shown = redact(url, secret_params)
    secrets = [str(v) for k, v in pairs if k in secret_params and v]
    secrets += [str((headers or {})[h]) for h in secret_headers if (headers or {}).get(h)]

    def scrub(text: str) -> str:
        for s in secrets:
            text = text.replace(s, "REDACTED")
        return text

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    limiter.acquire()
    try:
        with opener(req, timeout=timeout) as resp:
            body = resp.read(max_bytes + 1)
            status, headers = resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read(max_bytes + 1) if e.fp else b""
        status, headers = e.code, dict(e.headers or {})
    except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as e:
        return FetchResult(None, b"", {}, "unknown", "network", shown, detail=scrub(repr(e)))

    if len(body) > max_bytes:
        return FetchResult(status, b"", headers, "failed_definite", "response_too_large", shown,
                           detail=f"body larger than {max_bytes} bytes")
    outcome, error_class = _classify(status)
    return FetchResult(status, body, headers, outcome, error_class, shown,
                       retry_after_s=_retry_after(headers) if status == 429 else None)
