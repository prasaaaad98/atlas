"""Shared fixtures. No test touches the network or the real lake."""

from pathlib import Path

import pytest

from atlas_pipelines.core.markets import load_registry

FIXTURES = Path(__file__).parent / "fixtures"
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def registry():
    """Two test-only dummy markets: IST/15 min and CET/60→15 min."""
    return load_registry(FIXTURES / "markets")


@pytest.fixture
def lake(tmp_path) -> str:
    return str(tmp_path / "lake")
