"""Open-Meteo adapter pieces that need no saved response: config, request, outcomes.

Parsing is golden-tested on real saved responses (tests/golden/test_openmeteo_golden.py).
"""

import io
import urllib.error
from datetime import date

import pytest

from atlas_pipelines.core.http import RateLimiter
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.core.task import TaskContext
from atlas_pipelines.defs.weather import weather_pipelines
from atlas_pipelines.sources.openmeteo import archive
from conftest import REPO

REGISTRY = load_registry(REPO / "config" / "markets")
LOCATIONS = archive.load_locations(REPO / "config" / "locations" / "openmeteo.yaml", REGISTRY)
NOLIMIT = RateLimiter(rpm=60_000, sleep=lambda s: None)


def test_real_location_config_loads():
    assert [l.id for l in LOCATIONS] == ["entsoe:DE_LU", "entsoe:ES", "entsoe:SE3"]
    assert all(l.status == "proposed" for l in LOCATIONS)  # until the owner confirms


def test_location_for_unknown_zone_rejected(tmp_path):
    p = tmp_path / "l.yaml"
    p.write_text(
        "version: 1\nlocations:\n  - {id: 'entsoe:XX', market: entsoe, zone: XX, label: x,"
        " latitude: 1, longitude: 1, status: proposed}\n"
    )
    with pytest.raises(ValueError, match="not in the registry"):
        archive.load_locations(p, REGISTRY)


@pytest.mark.parametrize(
    "day,start,end",
    [
        ("2026-11-18", "2026-11-17", "2026-11-18"),  # CET day = 23:00Z prev day .. 23:00Z
        ("2026-03-29", "2026-03-28", "2026-03-29"),  # spring DST
        ("2025-10-26", "2025-10-25", "2025-10-26"),  # fall DST
    ],
)
def test_request_covers_the_local_day_in_utc(day, start, end):
    params = archive.request_params(LOCATIONS[0], REGISTRY["entsoe"], date.fromisoformat(day))
    assert (params["start_date"], params["end_date"]) == (start, end)
    assert params["timezone"] == "GMT" and params["wind_speed_unit"] == "ms"
    assert params["hourly"] == "temperature_2m,shortwave_radiation,cloud_cover,wind_speed_100m"


def ctx(lake):
    return TaskContext("weather_entsoe_daily", "ingest_de_lu", "2026-11-18", "", "", "attempt-1", None, lake,
                       lambda d: True)


def test_error_response_is_stored_raw_and_blocks(lake):
    # Documented error format (docs: HTTP 400 + {"error": true, "reason": ...}).
    body = b'{"error": true, "reason": "test reason"}'

    def opener(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 400, "Bad Request", {}, io.BytesIO(body))

    bronze, res = archive.fetch(ctx(lake), LOCATIONS[0], REGISTRY["entsoe"], limiter=NOLIMIT, opener=opener)
    assert (res.outcome, res.error_class) == ("failed_definite", "bad_request")
    checks = {c.check_name: c for c in archive.bronze_checks(bronze, res.body)}
    assert not checks["http_200"].passed and not checks["valid_weather_document"].passed
    assert bronze.materialization.uri.startswith(
        "bronze/openmeteo/archive_hourly__entsoe_de_lu/date=2026-11-18/fetched_at="
    )


def test_network_failure_raises_unknown(lake):
    def opener(req, timeout):
        raise urllib.error.URLError("connection reset")

    with pytest.raises(ConnectionError, match="unknown"):
        archive.fetch(ctx(lake), LOCATIONS[0], REGISTRY["entsoe"], limiter=NOLIMIT, opener=opener)


def test_weather_pipeline_fans_out_per_location():
    (p,) = weather_pipelines(REGISTRY)
    assert p.name == "weather_entsoe_daily" and p.partition_tz == "Europe/Brussels"
    assert p.order == ("ingest_de_lu", "ingest_es", "ingest_se3")
    assert p.task("ingest_es").asset_outputs == (
        "bronze.openmeteo_archive_hourly__entsoe_es",
        "silver.weather_hourly__entsoe_es",
    )
