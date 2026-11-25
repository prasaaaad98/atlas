"""A local stand-in for the ENTSO-E API that serves SAVED REAL responses unchanged.

Why it exists: B7's end-to-end demo (Atlas + workers + kills + gold) must run
offline and without the owner's token. This server answers A44 requests with
the owner's saved fixture files (pipelines/tests/fixtures/entsoe/, local-only),
chosen by zone (in_Domain EIC) and delivery day (periodStart). For any day
without a saved file it returns the real saved "No matching data" answer
(DE_LU_future_no_data.xml), so missing days stay missing. Nothing is generated
or edited: every byte comes from a real ENTSO-E response.

Usage: python scripts/fake_entsoe.py [port]   (binds 127.0.0.1 only)
The pipeline uses it when ATLAS_ENTSOE_BASE_URL=http://127.0.0.1:<port>/api.
The security token in requests is never logged (log lines are redacted).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import yaml

REPO = Path(__file__).resolve().parents[1]
FIXTURES = REPO / "pipelines" / "tests" / "fixtures" / "entsoe"
REGISTRY = yaml.safe_load((REPO / "config" / "markets" / "entsoe.yaml").read_text(encoding="utf-8"))
ZONE_BY_EIC = {eic: zone for zone, eic in REGISTRY["entsoe"]["zone_eic"].items()}
TZ = ZoneInfo(REGISTRY["timezone"])
NO_DATA = FIXTURES / "DE_LU_future_no_data.xml"


def pick(query: dict) -> Path:
    zone = ZONE_BY_EIC.get((query.get("in_Domain") or [""])[0])
    start = datetime.strptime((query.get("periodStart") or ["000000000000"])[0], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    day = (start + timedelta(hours=1)).astimezone(TZ).date().isoformat()  # periodStart is local midnight
    matches = sorted(p for p in FIXTURES.glob(f"{zone}_{day}_*.xml") if not p.stem.endswith("_A01"))
    return matches[0] if zone and matches else NO_DATA


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (http.server API)
        q = parse_qs(urlsplit(self.path).query)
        if not q.get("securityToken"):
            self.send_response(401)
            self.end_headers()
            return
        body = pick(q).read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # never print the token
        q = parse_qs(urlsplit(self.path).query)
        served = pick(q).name if q.get("securityToken") else "401"
        sys.stderr.write(f"fake-entsoe {(q.get('in_Domain') or ['?'])[0]} {(q.get('periodStart') or ['?'])[0]} -> {served}\n")


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18099
    if not FIXTURES.exists():
        sys.exit("ENTSO-E fixtures are local-only and not present; nothing to serve")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
