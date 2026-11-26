"""EIA reachability check and fixture capture (slice E0). OWNER-RUN: network + EIA_API_KEY.

Why it exists: before any EIA code is written (E1), we need to know (a) that the
EIA API answers from this machine, (b) how the key must be sent (the track plan
says an X-Api-Key header; EIA's documentation says the key "must always appear
in the URL"), (c) the exact routes, frequencies and facets, and (d) real raw
responses to build golden tests from. Nothing here guesses a route or a series:
child routes come from EIA's own route metadata, the Henry Hub series is found
by name in EIA's facet list, and the battery-storage facet value is found by name.

What it saves (pipelines/tests/fixtures/eia/, skipped if a file exists):
  key_placement_*.json       the two key-placement tests
  meta__<route>.json         route metadata (child routes, frequencies, facets, data columns)
  facet__<route>__<f>.json   facet value lists used to find Henry Hub / storage
  data__<route>__...json     small real data samples: a normal day and both 2025/26 DST
                             days for CISO (Pacific), ERCO (Central), PJM (Eastern), per frequency
  page__...json              one day fetched in pages of 100 rows (E1 pagination test)
  summary.json               what worked, what was found, every file and HTTP status
Each response file has a .meta.json: redacted URL, HTTP status, fetch time (UTC),
SHA-256 of the saved bytes, and how many key occurrences were redacted.

Secrets: the key is read from the environment or the repo's .env (EIA_API_KEY only),
never printed, and replaced by REDACTED in every saved URL and body (EIA echoes
request parameters in its responses). That redaction is the only change made to
the raw bytes; the .meta.json records it.

Usage: python scripts/eia_reachability.py
About 110 requests, at most ~1 per second. EIA's data queries can be slow
(median ~34 s, many 502/504 and timeouts observed on 2026-11-26), so a run can
take 30-60 minutes. Each request is retried up to 3 times on 5xx or timeout.
Re-running keeps files that succeeded (HTTP 200) and redoes only the others.
EIA data is public domain; attribution: "Source: U.S. Energy Information Administration".
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "pipelines" / "tests" / "fixtures" / "eia"
BASE = "https://api.eia.gov/v2/"
USER_AGENT = "atlas-grid-pipelines/0.1"  # generic, no personal details
MIN_GAP_S = 1.1  # stays under the `eia` limit key (60/min, one at a time)

# Sample BAs, one per time zone in config/markets/us_ba.yaml, and the US DST days.
SAMPLE_BAS = ["CISO", "ERCO", "PJM"]
# Local zones for the local-hourly frequency's offset (config/markets/us_ba.yaml, tz_status assumed).
BA_TZ = {"CISO": "America/Los_Angeles", "ERCO": "America/Chicago", "PJM": "America/New_York"}
RETRY_WAITS_S = [15, 45, 90]  # after a 5xx or timeout; EIA's gateway is often slow
NORMAL_DAY = "2026-11-18"
DST_DAYS = ["2025-03-09", "2025-11-02", "2026-03-08"]  # spring forward, fall back, spring forward

KEY = ""
MODE = "url"  # key placement for data requests, set by the placement test
_last = 0.0
summary: dict = {"started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "files": [], "notes": []}


def load_key() -> str:
    key = os.environ.get("EIA_API_KEY", "")
    env = REPO / ".env"
    if not key and env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            k, sep, v = line.strip().partition("=")
            if sep and k.removeprefix("export ").strip() == "EIA_API_KEY":
                key = v.strip().strip('"').strip("'")
    if not key:
        sys.exit("EIA_API_KEY is not set (environment or .env)")
    return key


def scrub(data: bytes | str) -> tuple[bytes, int]:
    b = data.encode() if isinstance(data, str) else data
    n = b.count(KEY.encode())
    return b.replace(KEY.encode(), b"REDACTED"), n


def get(route: str, params: list[tuple[str, str]] | None = None, *, key_in: str | None = None) -> tuple[int | None, bytes, str]:
    """GET with retries on 5xx / timeout (EIA's gateway errors), never on 4xx or 429."""
    for attempt in range(len(RETRY_WAITS_S) + 1):
        status, body, shown = _get_once(route, params, key_in=key_in)
        if status is not None and status < 500:
            return status, body, shown
        if attempt < len(RETRY_WAITS_S):
            print(f"        {status or 'timeout'}; retry in {RETRY_WAITS_S[attempt]} s")
            time.sleep(RETRY_WAITS_S[attempt])
    return status, body, shown


def _get_once(route: str, params: list[tuple[str, str]] | None = None, *, key_in: str | None = None) -> tuple[int | None, bytes, str]:
    """One GET, spaced by MIN_GAP_S. Returns (status or None, body, redacted URL)."""
    global _last
    params = list(params or [])
    key_in = key_in or MODE
    headers = {"User-Agent": USER_AGENT}
    if key_in == "url":
        params = [("api_key", KEY)] + params
    elif key_in == "header":
        headers["X-Api-Key"] = KEY
    url = BASE + route + ("?" + urlencode(params) if params else "")
    shown = scrub(url)[0].decode()
    wait = _last + MIN_GAP_S - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=120) as r:
            return r.status, r.read(), shown
    except urllib.error.HTTPError as e:
        return e.code, (e.read() if e.fp else b""), shown
    except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as e:
        return None, scrub(repr(e))[0], shown  # outcome unknown: nothing reliable came back


def save(name: str, status, body: bytes, url: str) -> dict | None:
    """Save a response unchanged except key redaction; returns parsed JSON (or None)."""
    OUT.mkdir(parents=True, exist_ok=True)
    path, meta_path = OUT / f"{name}.json", OUT / f"{name}.meta.json"
    clean, n = scrub(body)
    entry = {"file": path.name, "http_status": status, "url": url, "bytes": len(clean)}
    if path.exists() and meta_path.exists() and json.loads(meta_path.read_text(encoding="utf-8")).get("http_status") == 200:
        entry["skipped"] = "exists (200)"
    else:
        path.write_bytes(clean)
        meta_path.write_text(json.dumps({
            "source": "eia", "url": url, "http_status": status,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "sha256": hashlib.sha256(clean).hexdigest(), "bytes": len(clean),
            "key_occurrences_redacted": n,
            "attribution": "Source: U.S. Energy Information Administration",
        }, indent=2) + "\n", encoding="utf-8")
    summary["files"].append(entry)
    print(f"  {status if status is not None else 'ERR':>4}  {path.name}")
    try:
        return json.loads(clean)
    except ValueError:
        return None


def fetch(name: str, route: str, params=None) -> dict | None:
    status, body, url = get(route, params)
    if status == 429:
        save(name, status, body, url)
        summary["notes"].append("HTTP 429: stopped to respect EIA's throttle")
        finish()
        sys.exit("EIA answered 429 (throttled); stopped. Wait a few minutes and run again (saved files are kept).")
    return save(name, status, body, url)


def children(meta: dict | None) -> list[str]:
    return [r["id"] for r in ((meta or {}).get("response") or {}).get("routes", []) if "id" in r]


def frequencies(meta: dict | None) -> list[str]:
    return [f["id"] for f in ((meta or {}).get("response") or {}).get("frequency", []) if "id" in f]


def facets(meta: dict | None) -> list[str]:
    return [f["id"] for f in ((meta or {}).get("response") or {}).get("facets", []) if "id" in f]


def data_columns(meta: dict | None) -> list[str]:
    return list((((meta or {}).get("response") or {}).get("data") or {}).keys())


def local_window(day: str, tz: str) -> tuple[str, str]:
    # local-hourly needs the offset in start/end (EIA metadata format YYYY-MM-DD"T"HH24TZH).
    from zoneinfo import ZoneInfo
    d = datetime.fromisoformat(day)
    z = ZoneInfo(tz)

    def fmt(t):
        off = int(t.replace(tzinfo=z).utcoffset().total_seconds() // 3600)
        return t.strftime("%Y-%m-%dT%H") + f"{off:+03d}"
    return fmt(d), fmt(d.replace(hour=23))


def window(day: str) -> tuple[str, str]:
    # UTC hours from day 00:00 to day+1 12:00: covers that LOCAL day in every US
    # time zone (a US local day runs from ~04-08 UTC to ~04-08 UTC the next day).
    d = datetime.fromisoformat(day)
    return d.strftime("%Y-%m-%d") + "T00", (d + timedelta(days=1)).strftime("%Y-%m-%d") + "T12"


def finish() -> None:
    summary["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    global KEY, MODE
    KEY = load_key()
    print(f"EIA reachability; saving to {OUT}")

    # 1. Key placement: header (the plan) vs URL parameter (EIA's documentation).
    print("1. key placement")
    results = {}
    for mode in ("header", "url"):
        status, body, url = get("electricity/rto", key_in=mode)
        save(f"key_placement_{mode}", status, body, url)
        results[mode] = status
    summary["key_placement"] = {"header_X-Api-Key": results["header"], "url_api_key": results["url"],
                                "use": "header" if results["header"] == 200 else ("url" if results["url"] == 200 else None)}
    MODE = summary["key_placement"]["use"] or "url"  # header preferred: keeps the key out of URLs
    if results["url"] != 200 and results["header"] != 200:
        summary["notes"].append("EIA not reachable with this key in either placement")
        finish()
        sys.exit(f"EIA not reachable (header: {results['header']}, url: {results['url']}); see summary.json")

    # 2. EIA-930 hourly grid routes, from EIA's own metadata.
    print("2. electricity/rto routes and samples")
    rto = fetch("meta__electricity_rto", "electricity/rto")
    found = {}
    for child in children(rto):
        route = f"electricity/rto/{child}"
        meta = fetch(f"meta__{route.replace('/', '_')}", route)
        found[child] = {"frequencies": frequencies(meta), "facets": facets(meta), "data": data_columns(meta)}
        if "sub" in child:  # sub-BA regions: not used in E1–E4
            continue
        ba_facet = next((f for f in ("respondent", "fromba", "balancing_authority") if f in found[child]["facets"]), None)
        if not ba_facet:
            summary["notes"].append(f"{route}: no BA facet among {found[child]['facets']}; no data sample taken")
            continue
        for freq in [f for f in found[child]["frequencies"] if "hourly" in f]:
            cases = [("CISO", NORMAL_DAY)] + [(ba, d) for ba in SAMPLE_BAS for d in DST_DAYS]
            for ba, day in cases:
                start, end = local_window(day, BA_TZ[ba]) if freq == "local-hourly" else window(day)
                params = [("frequency", freq), ("start", start), ("end", end), ("length", "5000"),
                          (f"facets[{ba_facet}][]", ba)] + [("data[]", c) for c in found[child]["data"]]
                fetch(f"data__{child}__{freq}__{ba}__{day}", f"{route}/data", params)
        found[child]["ba_facet"] = ba_facet
    summary["rto_routes"] = found
    # The complete list of EIA-930 fuel codes, for E3's mapping (config/fuel_types.yaml).
    if "fuel-type-data" in found:
        fl = fetch("facet__electricity_rto_fuel-type-data__fueltype", "electricity/rto/fuel-type-data/facet/fueltype")
        summary["eia930_fuel_codes"] = [(v.get("id"), v.get("name"))
                                        for v in ((fl or {}).get("response") or {}).get("facets", [])]

    # A real overflow response (length above EIA's 5,000-row maximum), for E1's truncation check.
    region = next((c for c in found if c == "region-data"), None)
    if region and found[region].get("ba_facet"):
        print("   overflow sample (length=6000)")
        start, end = window(NORMAL_DAY)
        fetch(f"overflow__region-data__hourly__CISO__{NORMAL_DAY}", "electricity/rto/region-data/data",
              [("frequency", "hourly"), ("start", start), ("end", end), ("length", "6000"),
               ("facets[respondent][]", "CISO"), ("data[]", "value")])

    # Pagination sample (E1): one day of fuel-type data in pages of 100 rows.
    fuel = next((c for c in found if "fuel" in c), None)
    if fuel and found[fuel].get("ba_facet"):
        print("   pagination sample")
        freq = next((f for f in found[fuel]["frequencies"] if f == "hourly"), found[fuel]["frequencies"][0])
        start, end = window(NORMAL_DAY)
        offset, total = 0, None
        while total is None or offset < total:
            params = [("frequency", freq), ("start", start), ("end", end), ("length", "100"), ("offset", str(offset)),
                      (f"facets[{found[fuel]['ba_facet']}][]", "CISO"), ("sort[0][column]", "period"),
                      ("sort[0][direction]", "asc")] + [("data[]", c) for c in found[fuel]["data"]]
            page = fetch(f"page__{fuel}__CISO__{NORMAL_DAY}__offset{offset:05d}", f"electricity/rto/{fuel}/data", params)
            total = int(((page or {}).get("response") or {}).get("total") or 0)
            offset += 100
            if offset > 2000:
                break

    # 3. Natural gas prices: find the Henry Hub series by name in EIA's facet lists.
    print("3. natural-gas/pri routes, Henry Hub series")
    pri = fetch("meta__natural-gas_pri", "natural-gas/pri")
    henry = []
    for child in children(pri):
        route = f"natural-gas/pri/{child}"
        meta = fetch(f"meta__{route.replace('/', '_')}", route)
        if "series" not in facets(meta):
            continue
        fl = fetch(f"facet__{route.replace('/', '_')}__series", f"{route}/facet/series")
        for f in ((fl or {}).get("response") or {}).get("facets", []):
            name = f.get("name") or ""
            if "henry hub" in name.lower():
                henry.append({"route": route, "series": f.get("id"), "name": name, "frequencies": frequencies(meta)})
    summary["henry_hub_series"] = henry
    for h in henry:
        if "daily" in h["frequencies"] and h["series"]:
            fetch(f"data__{h['route'].replace('/', '_')}__{h['series']}__daily", f"{h['route']}/data",
                  [("frequency", "daily"), ("start", "2026-10-01"), ("end", "2026-11-25"), ("length", "5000"),
                   ("facets[series][]", h["series"]), ("data[]", "value")])

    # 4. Generator capacity (storage buildout): find battery facet values by name.
    print("4. electricity/operating-generator-capacity, storage facets")
    route = "electricity/operating-generator-capacity"
    meta = fetch(f"meta__{route.replace('/', '_')}", route)
    storage = {"facets": facets(meta), "frequencies": frequencies(meta), "data": data_columns(meta), "matches": {}}
    for fct in facets(meta):
        if not any(w in fct.lower() for w in ("tech", "energy_source", "status", "prime")):
            continue
        fl = fetch(f"facet__{route.replace('/', '_')}__{fct}", f"{route}/facet/{fct}")
        vals = ((fl or {}).get("response") or {}).get("facets", [])
        storage["matches"][fct] = [v for v in vals if any(w in (v.get("name") or "").lower() for w in ("batter", "storage"))]
        if "status" in fct:
            storage["matches"][fct + "_all"] = vals
    summary["storage"] = storage
    tech = next(((f, v[0]["id"]) for f, v in storage["matches"].items() if v and not f.endswith("_all") and "status" not in f), None)
    if tech and storage["frequencies"]:
        freq = "monthly" if "monthly" in storage["frequencies"] else storage["frequencies"][0]
        fetch(f"data__{route.replace('/', '_')}__{tech[0]}_{tech[1]}__{freq}", f"{route}/data",
              [("frequency", freq), ("start", "2026-06"), ("length", "500"), (f"facets[{tech[0]}][]", tech[1]),
               ("sort[0][column]", "period"), ("sort[0][direction]", "desc")] + [("data[]", c) for c in storage["data"]])
    else:
        summary["notes"].append("no battery/storage facet value found; see facet__ files")

    finish()
    print(f"done. key placement: {summary['key_placement']}; summary: {OUT / 'summary.json'}")


if __name__ == "__main__":
    main()
