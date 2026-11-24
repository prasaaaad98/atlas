"""Market adapters, one sub-package per market or provider (iex/, ercot/, entsoe/, ...).

ADAPTERS maps a registry market id to its adapter. Empty until B1 adds IEX:
no source is fetched until its terms are recorded in docs/data/sources/<source>.md.
"""

from atlas_pipelines.sources.adapter import MarketAdapter

ADAPTERS: dict[str, MarketAdapter] = {}
