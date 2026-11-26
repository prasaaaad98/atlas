"""The shared generation-type mapping, version 2 (config/mappings/fuel_types.yaml, ADR-B09).

Pins the owner's ENTSO-E table (2026-11-26) and the EIA codes the owner named, so a config
edit that changes a mapping fails here first. Also: every type has one gold group, an
unknown group or type is rejected, nothing is mapped to `other` by default.
"""

import pytest

from atlas_pipelines.sources import fuels

OWNER_ENTSOE = {
    "B01": "biomass", "B02": "lignite", "B03": "coal_gas", "B04": "gas", "B05": "coal", "B06": "oil",
    "B07": "oil_shale", "B08": "peat", "B09": "geothermal", "B10": "hydro_pumped_storage",
    "B11": "hydro_run_of_river", "B12": "hydro_reservoir", "B13": "marine", "B14": "nuclear",
    "B15": "other_renewable", "B16": "solar", "B17": "waste", "B18": "wind_offshore", "B19": "wind_onshore",
    "B20": "other", "B25": "battery_storage",
}
OWNER_EIA = {"SUN": "solar", "WND": "wind_onshore", "PS": "hydro_pumped_storage", "BAT": "battery_storage"}


def test_entsoe_mapping_is_the_owners_table():
    mapping, unmapped = fuels.load_mapping("entsoe")
    assert {c: m.fuel_type for c, m in mapping.items()} == OWNER_ENTSOE and unmapped == {}


def test_eia_codes_named_by_the_owner():
    mapping, unmapped = fuels.load_mapping("eia930")
    assert {c: mapping[c].fuel_type for c in OWNER_EIA} == OWNER_EIA
    assert set(unmapped) == {"OES", "SNB", "UNK", "WNB"}  # known, still blocking if they appear


def test_every_type_has_one_gold_group_and_storage_is_its_own():
    canonical = fuels.load_canonical()
    assert set(canonical.values()) <= {"renewable", "fossil", "nuclear", "storage", "other"}
    assert {t for t, g in canonical.items() if g == "storage"} == {"hydro_pumped_storage", "battery_storage", "other_storage"}
    assert canonical["wind_onshore"] == canonical["wind_offshore"] == "renewable"  # split in silver, grouped in gold


@pytest.mark.parametrize("change", [
    ("  solar: {group: renewable}", "  solar: {group: sunny}"),                       # unknown group
    ("    B16: {fuel_type: solar,", "    B16: {fuel_type: sunshine,"),                 # unknown type
    ("version: 2", "version: 1"),                                                      # old layout
])
def test_bad_mapping_is_rejected(tmp_path, change):
    src = fuels._path(None).read_text(encoding="utf-8")
    assert change[0] in src
    bad = tmp_path / "fuel_types.yaml"
    bad.write_text(src.replace(change[0], change[1]), encoding="utf-8")
    with pytest.raises(fuels.FuelConfigError):
        fuels.load_mapping("entsoe", bad)
