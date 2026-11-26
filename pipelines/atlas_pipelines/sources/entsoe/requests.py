"""ENTSO-E Transparency Platform request definitions: load, generation (B4a/B4b), balancing (B4c).

Why it exists: one place for the query parameters of each data item, taken from the
official RESTful API guide (Postman collection 7009892/2s93JtP3F6, JSON export read
2026-11-26; recorded in docs/data/sources/entsoe.md). Used by `fetch-fixture entsoe
--data <item>` now and by the B4a/B4b adapters later. The token is added by the fetcher.
Guide, for every item below: "Each request may cover a period of up to 1 year."
"""

from __future__ import annotations

from datetime import date

from atlas_pipelines.core.markets import Market

# item -> (guide section, documentType, processType or None (optional, omitted), domain parameter name)
ITEMS: dict[str, tuple[str, str, str | None, str]] = {
    "load_actual": ("6.1.A Actual Total Load", "A65", "A16", "outBiddingZone_Domain"),
    "load_forecast_da": ("6.1.B Day-ahead Total Load Forecast", "A65", "A01", "outBiddingZone_Domain"),
    # Guide: "Time series with inBiddingZone_Domain attribute reflects Generation values while
    # outBiddingZone_Domain reflects Consumption values." (storage charging, pumping)
    "generation_actual": ("16.1.B&C Actual Generation per Production Type", "A75", "A16", "in_Domain"),
    "wind_solar_forecast_da": ("14.1.D Generation Forecasts for Wind and Solar", "A69", "A01", "in_Domain"),
    # B4c. Guide: controlArea_Domain = "[M] EIC code of a Scheduling Area" (17.1.B&C) and "a Scheduling
    # Area or Market Balancing Area" (17.1.G). ENTSO-E's EIC register (Y_eicCodes.csv, read 2026-11-27)
    # lists DE_LU 10Y1001A1001A82H, ES 10YES-REE------0 and SE3 10Y1001A1001A46L as Scheduling Areas,
    # so each zone's own EIC is used. processType omitted for A81 = every reserve product
    # (A51 aFRR, A52 FCR, A47 mFRR, A46 RR). 17.1.B&C answers in ZIP, max 100 TimeSeries (offset pages).
    "reserves_daily": ("17.1.B&C Volumes and Prices of Contracted Reserves", "A81", None, "controlArea_Domain"),
    "imbalance_prices": ("17.1.G Imbalance prices", "A85", None, "controlArea_Domain"),
}

# Fixed extra parameters per item, as the guide marks them mandatory.
EXTRA: dict[str, dict[str, str]] = {
    "reserves_daily": {"businessType": "B95",                 # [M] B95 = Procured capacity
                       "Type_MarketAgreement.Type": "A01"},   # [M] A01 = Daily (auctions per day)
}

# psrType, exactly as the guide lists it (optional request filter; also the code on each time series).
PSR_TYPES = {
    "B01": "Biomass", "B02": "Fossil Brown coal/Lignite", "B03": "Fossil Coal-derived gas", "B04": "Fossil Gas",
    "B05": "Fossil Hard coal", "B06": "Fossil Oil", "B07": "Fossil Oil shale", "B08": "Fossil Peat",
    "B09": "Geothermal", "B10": "Hydro Pumped Storage", "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir", "B13": "Marine", "B14": "Nuclear", "B15": "Other renewable", "B16": "Solar",
    "B17": "Waste", "B18": "Wind Offshore", "B19": "Wind Onshore", "B20": "Other", "B25": "Energy storage",
}


def request_params(item: str, market: Market, eic: str, local_date: date) -> dict:
    """Query for one local delivery day of `item` in the zone with EIC `eic`."""
    if item not in ITEMS:
        raise ValueError(f"unknown ENTSO-E item {item!r}; one of {sorted(ITEMS)}")
    _, doc_type, process_type, domain = ITEMS[item]
    start, end = market.day_bounds_utc(local_date)
    fmt = "%Y%m%d%H%M"
    q = {"documentType": doc_type, **({"processType": process_type} if process_type else {}), domain: eic,
         **EXTRA.get(item, {}), "periodStart": start.strftime(fmt), "periodEnd": end.strftime(fmt)}
    return q
