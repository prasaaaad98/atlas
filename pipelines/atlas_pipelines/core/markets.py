"""Market registry: loads config/markets/<market>.yaml and answers time questions.

Why it exists: shared code must never assume one country, currency, time zone
or interval length (CLAUDE.md, "Multi-market from day one"). Everything
market-specific that isn't parsing logic lives in versioned YAML, and this
module is the only reader of it. A new market = a registry entry + an adapter.

Who calls it: pipeline definitions (defs/), market adapters (sources/), checks.

Invariants protected:
  - Interval length is looked up by local delivery date through effective-dated
    entries (Europe moved day-ahead from 60 to 15 minutes); a date before the
    first entry is an error, never a default.
  - A day's intervals are generated in UTC between two local midnights, so DST
    days get 23 or 25 hours worth of intervals and IST days always the same count.
  - Price floor/cap are optional here; a check that needs one must fail loudly
    when it is unset rather than assume a number (values are owner-confirmed).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

REGISTRY_VERSION = 1
_ID = re.compile(r"^[a-z][a-z0-9_]*$")
_PRODUCT = re.compile(r"^[A-Z][A-Z0-9_]*$")
_CURRENCY = re.compile(r"^[A-Z]{3}$")
_PARTITION = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class RegistryError(ValueError):
    """A registry file is malformed, or a question can't be answered from it."""


@dataclass(frozen=True)
class IntervalRule:
    effective_from: date  # first local delivery date this length applies to
    minutes: int


@dataclass(frozen=True)
class Product:
    code: str  # e.g. "DAM"
    canonical_market: str  # value of silver `market` column, e.g. "IEX_DAM"
    intervals: tuple[IntervalRule, ...]  # sorted by effective_from
    price_floor: float | None
    price_cap: float | None
    gate_closure_local: str | None  # "HH:MM" local, set from source docs
    publication_local: str | None  # "HH:MM" local, set from measurements

    def interval_minutes_on(self, local_date: date) -> int:
        rule = None
        for r in self.intervals:
            if r.effective_from <= local_date:
                rule = r
        if rule is None:
            raise RegistryError(
                f"{self.canonical_market}: no interval rule covers {local_date} "
                f"(registry starts {self.intervals[0].effective_from})"
            )
        return rule.minutes


@dataclass(frozen=True)
class Interval:
    block: int  # 1-based position within the local day
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class Market:
    id: str  # registry id, e.g. "iex"
    name: str
    timezone: str  # IANA name
    currency: str  # ISO 4217
    zones: tuple[str, ...]
    products: dict[str, Product]
    source_file: str

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def product(self, code: str) -> Product:
        try:
            return self.products[code]
        except KeyError:
            raise RegistryError(f"market {self.id!r} has no product {code!r}") from None

    def day_bounds_utc(self, local_date: date) -> tuple[datetime, datetime]:
        """UTC start and end of one local day (local midnight to next local midnight)."""
        start = datetime.combine(local_date, time(0), tzinfo=self.tz).astimezone(timezone.utc)
        end = datetime.combine(local_date + timedelta(days=1), time(0), tzinfo=self.tz).astimezone(
            timezone.utc
        )
        return start, end

    def intervals(self, product_code: str, local_date: date) -> list[Interval]:
        """Every interval of a delivery day, in UTC. Completeness checks compare against this."""
        minutes = self.product(product_code).interval_minutes_on(local_date)
        start, end = self.day_bounds_utc(local_date)
        length = timedelta(minutes=minutes)
        if (end - start) % length:
            raise RegistryError(f"{self.id}: a {end - start} day is not a multiple of {minutes} min")
        out, t, block = [], start, 1
        while t < end:
            out.append(Interval(block, t, t + length))
            t, block = t + length, block + 1
        return out


def parse_partition_date(partition_key: str) -> date:
    """Strict YYYY-MM-DD; date.fromisoformat alone would also accept '20261124'."""
    if not _PARTITION.match(partition_key):
        raise ValueError(f"partition key {partition_key!r} is not YYYY-MM-DD")
    return date.fromisoformat(partition_key)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- loading ---------------------------------------------------------------


def load_market(path: Path) -> Market:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    where = str(path)
    if not isinstance(raw, dict):
        raise RegistryError(f"{where}: expected a mapping")
    if raw.get("registry_version") != REGISTRY_VERSION:
        raise RegistryError(f"{where}: registry_version must be {REGISTRY_VERSION}")

    mid = _require(raw, "id", where)
    if not _ID.match(mid) or Path(path).stem != mid:
        raise RegistryError(f"{where}: id {mid!r} must be lower_snake and match the file name")
    tz = _require(raw, "timezone", where)
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise RegistryError(f"{where}: unknown time zone {tz!r}") from None
    currency = _require(raw, "currency", where)
    if not _CURRENCY.match(str(currency)):
        raise RegistryError(f"{where}: currency must be an ISO 4217 code, got {currency!r}")

    zones = tuple(raw.get("zones") or ())
    if len(set(zones)) != len(zones):
        raise RegistryError(f"{where}: duplicate zone codes")

    products_raw = _require(raw, "products", where)
    if not isinstance(products_raw, dict) or not products_raw:
        raise RegistryError(f"{where}: products must be a non-empty mapping")
    products = {code: _load_product(code, p, f"{where}:{code}") for code, p in products_raw.items()}

    return Market(
        id=mid,
        name=str(_require(raw, "name", where)),
        timezone=tz,
        currency=currency,
        zones=zones,
        products=products,
        source_file=where,
    )


def _load_product(code: str, raw: dict, where: str) -> Product:
    if not _PRODUCT.match(code):
        raise RegistryError(f"{where}: product code must be UPPER_SNAKE")
    rules = []
    for r in _require(raw, "intervals", where):
        eff = r.get("effective_from")
        minutes = r.get("minutes")
        if not isinstance(eff, date):
            raise RegistryError(f"{where}: effective_from must be a YYYY-MM-DD date")
        if not isinstance(minutes, int) or minutes <= 0 or 1440 % minutes:
            raise RegistryError(f"{where}: minutes must be a positive divisor of 1440")
        rules.append(IntervalRule(eff, minutes))
    if not rules:
        raise RegistryError(f"{where}: at least one interval rule is required")
    if any(a.effective_from >= b.effective_from for a, b in zip(rules, rules[1:])):
        raise RegistryError(f"{where}: interval rules must have strictly increasing effective_from")

    floor, cap = raw.get("price_floor"), raw.get("price_cap")
    if floor is not None and cap is not None and floor > cap:
        raise RegistryError(f"{where}: price_floor > price_cap")
    return Product(
        code=code,
        canonical_market=str(_require(raw, "canonical_market", where)),
        intervals=tuple(rules),
        price_floor=floor,
        price_cap=cap,
        gate_closure_local=raw.get("gate_closure_local"),
        publication_local=raw.get("publication_local"),
    )


def _require(raw: dict, key: str, where: str):
    if raw.get(key) in (None, "", [], {}):
        raise RegistryError(f"{where}: missing required field {key!r}")
    return raw[key]


def load_registry(markets_dir: Path) -> dict[str, Market]:
    markets_dir = Path(markets_dir)
    files = sorted(markets_dir.glob("*.yaml"))
    if not files:
        raise RegistryError(f"no market files in {markets_dir}")
    registry: dict[str, Market] = {}
    seen: dict[str, str] = {}
    for f in files:
        m = load_market(f)
        registry[m.id] = m
        for p in m.products.values():
            # The silver `market` column must identify one registry product.
            if p.canonical_market in seen:
                raise RegistryError(f"{f}: canonical_market {p.canonical_market} also in {seen[p.canonical_market]}")
            seen[p.canonical_market] = str(f)
    return registry
