"""Open-Meteo Historical Weather (archive) adapter → silver.weather_hourly.

Why it exists: weather drives demand and renewable output; Phase C/D need it
per covered zone. Terms: docs/data/sources/openmeteo.md. Use is limited to
non-commercial engine testing (owner decision, 2026-11-25); nothing derived
from it may be published.

API facts used here, all from https://open-meteo.com/en/docs/historical-weather-api
(read 2026-11-25): endpoint /v1/archive on archive-api.open-meteo.com;
parameters latitude, longitude, start_date, end_date (yyyy-mm-dd), hourly,
timezone (default "GMT"), timeformat (iso8601), wind_speed_unit ("ms");
variables temperature_2m (instant), shortwave_radiation ("preceding hour
mean"), cloud_cover (instant), wind_speed_100m (instant); errors are HTTP 400
with {"error": true, "reason": ...}.

Time handling: we always request timezone=GMT, so every timestamp is UTC and a
fall-back day never produces two identical local hour labels. A partition is a
local delivery day in the market's time zone (registry); we request the UTC
dates that cover it and keep only hours in [local midnight, next local midnight).
DST days therefore have 23 or 25 rows.

Never fills: a null value from the source stays null and fails completeness.
Who calls it: defs/weather.py tasks; tests call parse() on real saved responses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import yaml

from atlas_pipelines.core.http import FetchResult, RateLimiter, http_get
from atlas_pipelines.core.markets import Market, iso_utc
from atlas_pipelines.core.schemas import WEATHER_HOURLY, schema_check
from atlas_pipelines.core.task import CheckResult, TaskContext
from atlas_pipelines.sources.adapter import BronzeFetch, write_bronze

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
CODE_VERSION = "openmeteo_archive-1"
SCHEMA_VERSION = "weather_hourly-1"

# source variable -> (silver column, expected unit string in hourly_units)
VARIABLES = {
    "temperature_2m": ("temperature_c", "°C"),
    "shortwave_radiation": ("ghi_w_m2", "W/m²"),
    "cloud_cover": ("cloud_cover_pct", "%"),
    "wind_speed_100m": ("wind_speed_100m_m_s", "m/s"),
}


@dataclass(frozen=True)
class Location:
    id: str
    market: str
    zone: str
    label: str
    latitude: float
    longitude: float
    status: str

    @property
    def slug(self) -> str:
        return f"{self.market}_{self.zone.lower()}"


def load_locations(path: Path, registry: dict[str, Market], only_loaded_markets: bool = False) -> list[Location]:
    """Load and validate weather points. With only_loaded_markets, points for
    markets absent from `registry` are skipped (tests load a dummy registry)."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise ValueError(f"{path}: version must be 1")
    out, seen = [], set()
    for r in raw.get("locations") or []:
        if only_loaded_markets and r.get("market") not in registry:
            continue
        loc = Location(
            str(r["id"]), str(r["market"]), str(r["zone"]), str(r["label"]),
            float(r["latitude"]), float(r["longitude"]), str(r["status"]),
        )
        if loc.market not in registry or loc.zone not in registry[loc.market].zones:
            raise ValueError(f"{path}: {loc.id}: market/zone not in the registry")
        if loc.id != f"{loc.market}:{loc.zone}" or loc.id in seen:
            raise ValueError(f"{path}: {loc.id}: id must be '<market>:<zone>' and unique")
        if not (-90 <= loc.latitude <= 90 and -180 <= loc.longitude <= 180):
            raise ValueError(f"{path}: {loc.id}: coordinates out of range")
        if loc.status not in ("proposed", "confirmed"):
            raise ValueError(f"{path}: {loc.id}: status must be proposed or confirmed")
        seen.add(loc.id)
        out.append(loc)
    return out


def request_params(loc: Location, market: Market, local_date: date) -> dict:
    start, end = market.day_bounds_utc(local_date)
    last_hour = end - timedelta(hours=1)
    return {
        "latitude": f"{loc.latitude:.4f}",
        "longitude": f"{loc.longitude:.4f}",
        "start_date": start.date().isoformat(),
        "end_date": last_hour.date().isoformat(),
        "hourly": ",".join(VARIABLES),
        "timezone": "GMT",
        "timeformat": "iso8601",
        "wind_speed_unit": "ms",
    }


def fetch(ctx: TaskContext, loc: Location, market: Market, *, limiter: RateLimiter, opener=None):
    """One request; stores the response body unchanged in bronze (even an error body)."""
    local_date = date.fromisoformat(ctx.partition_key)
    kwargs = {"opener": opener} if opener else {}
    res: FetchResult = http_get(BASE_URL, request_params(loc, market, local_date), limiter=limiter, **kwargs)
    if res.outcome == "unknown":
        raise ConnectionError(f"open-meteo {loc.id}: outcome unknown ({res.error_class}): {res.detail}")
    bronze = write_bronze(
        ctx,
        source="openmeteo",
        dataset=f"archive_hourly__{loc.slug}",
        partition_field="date",
        filename="raw.json",
        data=res.body,
        fetched_at=datetime.now(timezone.utc),
        source_url=res.url_redacted,
        http_status=res.status,
        code_version=CODE_VERSION,
    )
    return bronze, res


def parse(raw: bytes, loc: Location, market: Market, local_date: date, source_uri: str) -> pl.DataFrame:
    doc = json.loads(raw)
    if doc.get("error"):
        raise ValueError(f"open-meteo error response: {doc.get('reason')!r}")
    if doc.get("utc_offset_seconds") != 0:
        raise ValueError(f"expected GMT timestamps, got utc_offset_seconds={doc.get('utc_offset_seconds')}")
    hourly = doc["hourly"]
    times = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc) for t in hourly["time"]]
    for var in VARIABLES:
        if len(hourly.get(var, [])) != len(times):
            raise ValueError(f"variable {var}: {len(hourly.get(var, []))} values for {len(times)} timestamps")
    start, end = market.day_bounds_utc(local_date)
    keep = [i for i, t in enumerate(times) if start <= t < end]
    data = {
        "location_id": [loc.id] * len(keep),
        "ts_utc": [times[i] for i in keep],
        **{col: [_float(hourly[var][i]) for i in keep] for var, (col, _) in VARIABLES.items()},
        "is_forecast": [False] * len(keep),
        "issued_at_utc": [None] * len(keep),
        "source_uri": [source_uri] * len(keep),
    }
    return pl.DataFrame(data, schema={c: t for c, (t, _) in WEATHER_HOURLY.items()})


def _float(v):
    return None if v is None else float(v)


def checks(df: pl.DataFrame, raw: bytes, market: Market, local_date: date) -> list[CheckResult]:
    doc = json.loads(raw)
    units = doc.get("hourly_units", {})
    wrong_units = {v: units.get(v) for v, (_, u) in VARIABLES.items() if units.get(v) != u}

    start, end = market.day_bounds_utc(local_date)
    expected = []
    t = start
    while t < end:
        expected.append(t)
        t += timedelta(hours=1)
    got = df["ts_utc"].to_list()
    missing = sorted(set(expected) - set(got))
    nulls = {col: df[col].null_count() for _, (col, _) in VARIABLES.items() if df[col].null_count()}

    def count(expr) -> int:
        return int(df.select(expr.sum()).item() or 0)

    return [
        schema_check(df, WEATHER_HOURLY, "weather_hourly"),
        CheckResult("units_match", "blocking", not wrong_units, {"mismatched": wrong_units}),
        CheckResult(
            "hourly_completeness",
            "blocking",
            not missing and len(got) == len(expected) and len(set(got)) == len(got),
            {"expected": len(expected), "found": len(got), "missing_first10": [iso_utc(m) for m in missing[:10]]},
        ),
        CheckResult("no_missing_values", "blocking", not nulls, {"nulls": nulls}),
        CheckResult(
            "physical_ranges",
            "blocking",
            count(pl.col("ghi_w_m2") < 0) == 0
            and count((pl.col("cloud_cover_pct") < 0) | (pl.col("cloud_cover_pct") > 100)) == 0
            and count(pl.col("wind_speed_100m_m_s") < 0) == 0,
            {
                "ghi_negative": count(pl.col("ghi_w_m2") < 0),
                "cloud_out_of_0_100": count((pl.col("cloud_cover_pct") < 0) | (pl.col("cloud_cover_pct") > 100)),
                "wind_negative": count(pl.col("wind_speed_100m_m_s") < 0),
            },
        ),
    ]


def bronze_checks(bronze: BronzeFetch, raw: bytes) -> list[CheckResult]:
    """Layer-1 content check on top of HTTP 200 / non-empty: a JSON document without an error flag."""
    try:
        doc = json.loads(raw)
        ok, reason = not doc.get("error"), doc.get("reason")
    except ValueError as e:
        ok, reason = False, f"not JSON: {e}"
    return bronze.checks + [CheckResult("valid_weather_document", "blocking", ok, {"reason": reason})]
