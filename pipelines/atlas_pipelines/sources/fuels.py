"""Canonical generation-type mapping (config/mappings/fuel_types.yaml), shared by every market adapter.

Why it exists: generation from different sources must land in one vocabulary (solar,
wind_onshore, gas, …) so shared checks, gold and the simulator never special-case a source.
A source code maps to exactly one canonical type and every type belongs to one gold group
(renewable | fossil | nuclear | storage | other). An unknown code is a blocking failure,
never a default (ADR-B09). The source's own code is kept on every row (`source_type`).
Who calls it: sources/eia/fuel.py (E3), sources/entsoe/generation.py (B4b), gold/.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from atlas_pipelines.core import config

VERSION = 2
DIRECTIONS = ("generation", "consumption", "net")  # net = one signed series from the source (EIA-930)


class FuelConfigError(ValueError):
    pass


@dataclass(frozen=True)
class FuelCode:
    code: str
    fuel_type: str
    fuel_group: str
    negative: str  # allowed | warn
    name: str


def _path(path: Path | None) -> Path:
    return Path(path or config.REPO_ROOT / "config" / "mappings" / "fuel_types.yaml")


def _raw(path: Path | None) -> dict:
    p = _path(path)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if raw.get("version") != VERSION:
        raise FuelConfigError(f"{p}: version must be {VERSION}")
    return raw


def load_canonical(path: Path | None = None) -> dict[str, str]:
    """{canonical fuel_type: group}, validated (every type in exactly one known group)."""
    raw = _raw(path)
    groups = raw.get("groups") or []
    out = {}
    for t, m in (raw.get("canonical") or {}).items():
        if (m or {}).get("group") not in groups:
            raise FuelConfigError(f"{_path(path)}: {t} needs a group in {groups}")
        out[str(t)] = m["group"]
    return out


def load_mapping(source: str, path: Path | None = None) -> tuple[dict[str, FuelCode], dict[str, str]]:
    """Returns ({code: FuelCode}, {unmapped code: reason}) for a source, validated."""
    raw, canonical = _raw(path), load_canonical(path)
    codes = (raw.get("sources") or {}).get(source)
    if not codes:
        raise FuelConfigError(f"{_path(path)}: no mapping for source {source!r}")
    out = {}
    for code, m in codes.items():
        if m.get("fuel_type") not in canonical:
            raise FuelConfigError(f"{_path(path)}: {source}.{code} maps to unknown fuel type {m.get('fuel_type')!r}")
        if m.get("negative") not in ("allowed", "warn"):
            raise FuelConfigError(f"{_path(path)}: {source}.{code} negative must be allowed or warn")
        out[str(code)] = FuelCode(str(code), m["fuel_type"], canonical[m["fuel_type"]], m["negative"],
                                  str(m.get("name", "")))
    unmapped = {str(k): str(v) for k, v in ((raw.get("sources") or {}).get(f"{source}_unmapped") or {}).items()}
    if set(unmapped) & set(out):
        raise FuelConfigError(f"{_path(path)}: codes both mapped and unmapped: {sorted(set(unmapped) & set(out))}")
    return out, unmapped
