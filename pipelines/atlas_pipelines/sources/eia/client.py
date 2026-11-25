"""EIA API v2 client core: header auth, pagination, truncation check, value parsing (slice E1).

Why it exists: every EIA dataset (EIA-930 demand, generation, interchange; Henry Hub;
storage capacity) is fetched the same way, and the ways it can go wrong are the
same: a response silently capped at 5,000 rows, values sent as strings, an
unstable order across pages, a slow or failing gateway. This module handles them
once; E2–E5 only choose routes, facets and how to map rows to silver.
Who calls it: E2+ fetch tasks (defs/), tests (with a fake opener and real saved responses).

Facts used (docs/data/sources/eia.md, confirmed by the owner's first run 2026-11-25):
  - base https://api.eia.gov/v2/<route>/data; key accepted in the X-Api-Key header
    (we never put it in the URL);
  - max 5,000 rows per JSON response; `offset`/`length` paginate; `response.total` is
    the number of rows responsive to the request (a string); overflow adds warning
    "parameter out of range";
  - values are strings (or null).

Invariants:
  - Rows fetched = `total`, with no duplicate rows across pages, or a BLOCKING check fails.
    A capped or partial download is never mistaken for a complete one.
  - Pages are requested in a stable order (sorted by period, then the caller's key
    columns), so offsets don't skip or repeat rows.
  - Every page body is kept unchanged in one bronze file per fetch (JSON lines:
    offset, status, redacted URL, body), with the key scrubbed if EIA ever echoes it.
  - A value that isn't a number fails a check; it never becomes 0. Null stays null.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field

from atlas_pipelines.core.http import FetchResult, RateLimiter, http_get
from atlas_pipelines.core.task import CheckResult

BASE_URL = "https://api.eia.gov/v2/"
KEY_HEADER = "X-Api-Key"
MAX_PAGE_ROWS = 5000
MAX_PAGES = 200  # 1,000,000 rows: far above any one partition; stops a runaway loop
TRUNCATION_WARNING = "parameter out of range"


@dataclass
class Page:
    offset: int
    status: int | None
    outcome: str
    error_class: str | None
    url: str  # redacted (the key is in a header anyway)
    body: bytes
    detail: str = ""
    retry_after_s: float | None = None


@dataclass
class Fetch:
    """All pages of one request. complete=False if a page failed or the loop stopped early."""
    route: str
    pages: list[Page] = field(default_factory=list)
    total: int | None = None
    rows: list[dict] = field(default_factory=list)
    warnings: list = field(default_factory=list)
    stopped: str | None = None  # why paging stopped before reaching total (None = finished)

    @property
    def outcome(self) -> str:
        """Outcome class of the whole fetch: the worst page wins."""
        classes = [p.outcome for p in self.pages]
        if "unknown" in classes:
            return "unknown"
        if "failed_definite" in classes:
            return "failed_definite"
        return "succeeded"

    @property
    def error_class(self) -> str | None:
        return next((p.error_class for p in self.pages if p.error_class), None)


def fetch(route: str, params: list[tuple[str, str]], *, key: str, limiter: RateLimiter,
          sort: tuple[str, ...] = (), page_length: int = MAX_PAGE_ROWS, max_pages: int = MAX_PAGES,
          opener=None) -> Fetch:
    """GET <route>/data page by page until `total` rows are in hand or a page fails.

    params: route-specific filters (frequency, start, end, facets[...][], data[]).
    sort: key columns after `period` that make the order stable across pages.
    """
    if not key:
        raise ValueError("EIA key is empty (EIA_API_KEY)")
    if not 1 <= page_length <= MAX_PAGE_ROWS:
        raise ValueError(f"page_length must be 1..{MAX_PAGE_ROWS}")
    order = [("sort[0][column]", "period"), ("sort[0][direction]", "asc")]
    for i, col in enumerate(sort, start=1):
        order += [(f"sort[{i}][column]", col), (f"sort[{i}][direction]", "asc")]
    f = Fetch(route=route)
    offset = 0
    kwargs = {"opener": opener} if opener else {}
    while True:
        if len(f.pages) >= max_pages:
            f.stopped = f"max_pages={max_pages} reached"
            break
        res: FetchResult = http_get(
            BASE_URL + route.strip("/") + "/data",
            list(params) + order + [("offset", str(offset)), ("length", str(page_length))],
            limiter=limiter, headers={KEY_HEADER: key}, secret_headers=(KEY_HEADER,), **kwargs)
        page = Page(offset, res.status, res.outcome, res.error_class, res.url_redacted, scrub(res.body, key),
                    res.detail, res.retry_after_s)
        f.pages.append(page)
        if res.outcome != "succeeded":
            f.stopped = f"page at offset {offset}: {res.error_class or res.outcome}"
            break
        try:
            resp = json.loads(page.body)["response"]
            total = int(resp["total"])
            data = resp["data"]
        except (ValueError, KeyError, TypeError) as e:
            page.outcome, page.error_class = "failed_definite", "bad_response"
            f.stopped = f"page at offset {offset}: not an EIA data response ({e!r})"
            break
        f.total = total
        f.warnings += resp.get("warnings") or []
        f.rows += data
        offset += len(data)
        if offset >= total or not data:
            if not data and offset < total:
                f.stopped = f"empty page at offset {offset} before total {total}"
            break
    return f


def scrub(body: bytes, key: str) -> bytes:
    return body.replace(key.encode(), b"REDACTED") if key else body


def to_number(value) -> float | None:
    """EIA sends numbers as strings. Null stays None; anything non-numeric raises."""
    if value is None:
        return None
    x = float(value)  # ValueError for non-numeric text: the caller turns it into a check failure
    if math.isnan(x) or math.isinf(x):
        raise ValueError(f"non-finite value {value!r}")
    return x


def bronze_bytes(f: Fetch) -> bytes:
    """One bronze file per fetch: JSON lines, one per page, bodies kept verbatim (UTF-8)."""
    lines = [json.dumps({"offset": p.offset, "http_status": p.status, "url": p.url,
                         "body": p.body.decode("utf-8", errors="replace")}, ensure_ascii=False)
             for p in f.pages]
    return ("\n".join(lines) + "\n").encode("utf-8")


def read_bronze(data: bytes) -> list[dict]:
    """Pages back from a bronze file (for re-parsing without re-fetching)."""
    return [json.loads(line) for line in data.decode("utf-8").splitlines() if line.strip()]


def write_fetch(ctx, f: Fetch, dataset: str, fetched_at, code_version: str):
    """Store one fetch (all pages) as one immutable bronze file; returns write_bronze's BronzeFetch.

    The recorded HTTP status is 200 only if every page was 200; otherwise the first other one.
    """
    from atlas_pipelines.sources.adapter import write_bronze  # sources-level helper, no runner import

    status = next((p.status for p in f.pages if p.status != 200), 200) if f.pages else None
    return write_bronze(ctx, source="eia", dataset=dataset, partition_field="date", filename="pages.jsonl",
                        data=bronze_bytes(f), fetched_at=fetched_at, source_url=f.pages[0].url if f.pages else "",
                        http_status=status, code_version=code_version)


def checks(f: Fetch, key_columns: tuple[str, ...]) -> list[CheckResult]:
    """Blocking fetch-level checks; route-specific checks come on top in E2+."""
    statuses = [p.status for p in f.pages]
    keys = [tuple(r.get(c) for c in ("period",) + key_columns) for r in f.rows]
    dupes = len(keys) - len(set(keys))
    bad_values, nulls = [], 0
    for r in f.rows:
        try:
            nulls += to_number(r.get("value")) is None
        except (TypeError, ValueError):
            bad_values.append(r.get("value"))
    truncated = any(TRUNCATION_WARNING in json.dumps(w).lower() for w in f.warnings)
    return [
        CheckResult("all_pages_http_200", "blocking", bool(statuses) and all(s == 200 for s in statuses),
                    {"statuses": statuses, "stopped": f.stopped}),
        CheckResult("rows_match_total", "blocking",
                    f.total is not None and len(f.rows) == f.total and f.stopped is None and not truncated,
                    {"rows": len(f.rows), "total": f.total, "pages": len(f.pages), "stopped": f.stopped,
                     "truncation_warning": truncated}),
        CheckResult("no_duplicate_rows", "blocking", dupes == 0, {"duplicates": dupes, "key": ["period", *key_columns]}),
        CheckResult("values_numeric", "blocking", not bad_values, {"non_numeric_first10": bad_values[:10]}),
        # Null values are allowed here (EIA-930 has gaps); completeness checks in E2+ decide.
        CheckResult("values_present", "warn", nulls == 0, {"null_values": nulls}),
    ]
