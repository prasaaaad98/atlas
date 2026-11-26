"""Canonical fuel-type mapping (config/fuel_types.yaml), shared by every market adapter.

Why it exists: generation from different sources must land in one vocabulary
(coal, gas, solar, …) so shared checks, gold and the simulator never special-case a
source. A source code maps to exactly one canonical type; an unknown code is a
blocking failure, never a default (PHASE_AB_PLAN.md B4b).
Who calls it: sources/eia/fuel.py (E3); later ENTSO-E and other generation adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from atlas_pipelines.core import config


class FuelConfigError(ValueError):
    pass


@dataclass(frozen=True)
class FuelCode:
    code: str
    fuel_type: str
    negative: str  # allowed | warn
    name: str


def load_mapping(source: str, path: Path | None = None) -> tuple[dict[str, FuelCode], dict[str, str]]:
    """Returns ({code: FuelCode}, {unmapped code: reason}) for a source, validated."""
    path = path or config.REPO_ROOT / "config" / "fuel_types.yaml"
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw.get("version") != 1:
        raise FuelConfigError(f"{path}: version must be 1")
    canonical = raw.get("canonical") or {}
    codes = (raw.get("sources") or {}).get(source)
    if not codes:
        raise FuelConfigError(f"{path}: no mapping for source {source!r}")
    out = {}
    for code, m in codes.items():
        if m.get("fuel_type") not in canonical:
            raise FuelConfigError(f"{path}: {source}.{code} maps to unknown fuel type {m.get('fuel_type')!r}")
        if m.get("negative") not in ("allowed", "warn"):
            raise FuelConfigError(f"{path}: {source}.{code} negative must be allowed or warn")
        out[str(code)] = FuelCode(str(code), m["fuel_type"], m["negative"], str(m.get("name", "")))
    unmapped = {str(k): str(v) for k, v in ((raw.get("sources") or {}).get(f"{source}_unmapped") or {}).items()}
    if set(unmapped) & set(out):
        raise FuelConfigError(f"{path}: codes both mapped and unmapped: {sorted(set(unmapped) & set(out))}")
    return out, unmapped
