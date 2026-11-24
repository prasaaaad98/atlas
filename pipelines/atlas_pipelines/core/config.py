"""Where the registry and the lake live.

Why it exists: one place resolves LAKE_ROOT and the market registry directory,
so the CLI, tests and (later) the Atlas worker agree on locations.
Who calls it: cli.py; tests pass explicit paths instead.

  LAKE_ROOT           lake root path or file:// URI. Default: <repo>/lake
                      (gitignored). On the owner's PC, point it at a D: folder.
  ATLAS_MARKETS_DIR   market registry directory. Default: <repo>/config/markets
"""

from __future__ import annotations

import os
from pathlib import Path

# pipelines/atlas_pipelines/core/config.py -> parents[3] is the repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]


def lake_root() -> str:
    return os.environ.get("LAKE_ROOT") or str(REPO_ROOT / "lake")


def markets_dir() -> Path:
    return Path(os.environ.get("ATLAS_MARKETS_DIR") or REPO_ROOT / "config" / "markets")
