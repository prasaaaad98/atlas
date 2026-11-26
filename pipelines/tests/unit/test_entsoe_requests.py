"""ENTSO-E load/generation request definitions (B4a/B4b), as the official guide states them. No network."""

from datetime import date

import pytest

from atlas_pipelines.core import config
from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources.entsoe import day_ahead as da
from atlas_pipelines.sources.entsoe import requests as er

M = load_registry(config.markets_dir())["entsoe"]
EIC = da.series_rule(M, "DE_LU").eic


@pytest.mark.parametrize("item,doc,proc,domain", [
    ("load_actual", "A65", "A16", "outBiddingZone_Domain"),
    ("load_forecast_da", "A65", "A01", "outBiddingZone_Domain"),
    ("generation_actual", "A75", "A16", "in_Domain"),
    ("wind_solar_forecast_da", "A69", "A01", "in_Domain"),
])
def test_request_matches_the_guide(item, doc, proc, domain):
    q = er.request_params(item, M, EIC, date(2026, 11, 18))
    assert (q["documentType"], q["processType"], q[domain]) == (doc, proc, EIC)
    assert "securityToken" not in q


@pytest.mark.parametrize("day,hours", [(date(2026, 11, 18), 24), (date(2026, 3, 29), 23), (date(2025, 10, 26), 25)])
def test_request_covers_the_local_delivery_day(day, hours):
    q = er.request_params("load_actual", M, EIC, day)
    start, end = M.day_bounds_utc(day)
    assert (q["periodStart"], q["periodEnd"]) == (start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"))
    assert (end - start).total_seconds() == hours * 3600


@pytest.mark.parametrize("zone", ["DE_LU", "ES", "SE3"])
def test_balancing_requests_match_the_guide(zone):
    eic = da.series_rule(M, zone).eic
    r = er.request_params("reserves_daily", M, eic, date(2026, 11, 18))
    assert (r["documentType"], r["businessType"], r["Type_MarketAgreement.Type"], r["controlArea_Domain"]) == \
        ("A81", "B95", "A01", eic) and "processType" not in r  # every reserve product
    i = er.request_params("imbalance_prices", M, eic, date(2026, 11, 18))
    assert (i["documentType"], i["controlArea_Domain"]) == ("A85", eic) and set(i) == {
        "documentType", "controlArea_Domain", "periodStart", "periodEnd"}


def test_unknown_item_is_refused():
    with pytest.raises(ValueError):
        er.request_params("prices_intraday", M, EIC, date(2026, 11, 18))
