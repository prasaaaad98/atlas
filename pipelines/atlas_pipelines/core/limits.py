"""Source politeness limits from config/limits/<key>.yaml.

Why it exists: every fetch goes through its source's limit key (CLAUDE.md);
the numbers are versioned config, not code. The same key names are used by
Atlas limit-key admission at claim (A5).
Who calls it: source adapters when building their RateLimiter.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Limit:
    key: str
    rpm: int
    max_concurrency: int


def load_limit(key: str, limits_dir: Path) -> Limit:
    path = Path(limits_dir) / f"{key}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"no limit config for key {key!r} at {path}: every source needs one")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw.get("key") != key:
        raise ValueError(f"{path}: key must be {key!r}")
    rpm, conc = raw.get("rpm"), raw.get("max_concurrency")
    if not (isinstance(rpm, int) and rpm > 0 and isinstance(conc, int) and conc > 0):
        raise ValueError(f"{path}: rpm and max_concurrency must be positive integers")
    return Limit(key, rpm, conc)
