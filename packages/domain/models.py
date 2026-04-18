"""Minimal domain models for broker-neutral market data contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

from .enums import (
    AssetClass,
    InstrumentType,
    Provider,
    RuntimeState,
    StorageBindingScope,
    TradeSide,
    Venue,
    external_provider_value,
)


def build_canonical_symbol(
    *,
    provider: Provider | str | None,
    venue: Venue | str | None,
    instrument_type: InstrumentType | str | None,
    symbol: str | None = None,
    asset_class: AssetClass | str | None = None,
    display_symbol: str | None = None,
    settle_asset: str | None = None,
    expiry: str | None = None,
) -> str:
    """Build the canonical multiprovider identity string.

    Format: ``<provider>:<venue>:<asset_class>:<instrument_type>:<display_symbol>[:<settle_asset>][:<expiry>]``

    Examples::

        kxt:krx:equity:spot:005930
        ccxt:binance:crypto:spot:BTC/USDT
        ccxt:binance:crypto:perpetual:BTC/USDT:USDT

    The ``provider`` axis is always the externally exposed value
    (``kxt`` or ``ccxt``); ``ccxt_pro`` is collapsed to ``ccxt``.

    ``symbol`` is accepted as a backwards-compatible alias for
    ``display_symbol`` so step1/step2 callers keep working.
    """

    def _norm(value: object) -> str:
        if value is None:
            return "unknown"
        if hasattr(value, "value"):
            value = value.value
        text = str(value).strip().lower()
        return text or "unknown"

    provider_external = external_provider_value(provider) if provider is not None else "unknown"

    resolved_display = display_symbol or symbol
    if resolved_display is None:
        resolved_display = "unknown"
    display_text = str(resolved_display).strip() or "unknown"

    parts = [
        provider_external,
        _norm(venue),
        _norm(asset_class),
        _norm(instrument_type),
        display_text,
    ]
    if settle_asset:
        parts.append(str(settle_asset).strip().upper() or "unknown")
    if expiry:
        parts.append(str(expiry).strip() or "unknown")
    return ":".join(parts)


@dataclass(frozen=True, slots=True)
class InstrumentRef:
    """Stable instrument reference independent from adapter-specific symbols.

    ``provider`` captures the hub/provider axis (KXT vs CCXT/CCXT Pro).
    ``canonical_symbol`` is an optional pre-computed multiprovider identity;
    when omitted callers can derive one via :func:`build_canonical_symbol`.
    ``raw_symbol`` is the venue-native symbol (e.g. Binance ``BTCUSDT`` or
    KRX ``005930``) and is split from ``symbol`` (the unified/display
    symbol such as ``BTC/USDT``) so external surfaces never conflate the
    two identities.
    """

    symbol: str
    instrument_id: str | None = None
    venue: Venue | None = None
    asset_class: AssetClass | None = None
    instrument_type: InstrumentType | None = None
    provider: Provider | None = None
    canonical_symbol: str | None = None
    raw_symbol: str | None = None


@dataclass(frozen=True, slots=True)
class Trade:
    """Individual execution fact without cumulative, book, or transport metadata."""

    instrument: InstrumentRef
    occurred_at: datetime
    price: Decimal
    quantity: Decimal
    side: TradeSide | None = None
    trade_id: str | None = None
    sequence: int | str | None = None


@dataclass(frozen=True, slots=True)
class QuoteLevel:
    """Single price level with only the facts required for broker-neutral depth."""

    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """Snapshot-only order book state without source-specific totals or deltas."""

    instrument: InstrumentRef
    occurred_at: datetime
    asks: tuple[QuoteLevel, ...] = ()
    bids: tuple[QuoteLevel, ...] = ()


@dataclass(frozen=True, slots=True)
class ProgramTrade:
    """Program trading flow facts kept separate from ordinary trade executions."""

    instrument: InstrumentRef
    occurred_at: datetime
    sell_quantity: Decimal
    buy_quantity: Decimal
    net_buy_quantity: Decimal
    sell_notional: Decimal
    buy_notional: Decimal
    net_buy_notional: Decimal
    program_sell_depth: Decimal | None = None
    program_buy_depth: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Provenance:
    """Source metadata that keeps adapter details outside the core payload."""

    source_id: str
    adapter_id: str
    raw_event_id: str | None = None
    trace_id: str | None = None


@dataclass(frozen=True, slots=True)
class InstrumentSearchResult:
    """Stable search/registry surface for admin instrument discovery."""

    instrument: InstrumentRef
    display_name: str
    market_scope: str
    provider_instrument_id: str | None = None
    venue_code: str | None = None
    is_active: bool = True
    provider: Provider | None = None
    canonical_symbol: str | None = None


@dataclass(frozen=True, slots=True)
class CollectionTarget:
    """Collector-owned declaration of what should be streamed upstream.

    ``provider`` lets the hub route a target to the correct adapter
    (KXT for KRX equity today, CCXT/CCXT Pro for crypto in the next step).
    ``market_scope`` remains a KRX-only selector (``krx|nxt|total``) and is
    treated as not-applicable for non-KRX providers (empty string).
    """

    target_id: str
    instrument: InstrumentRef
    market_scope: str
    event_types: tuple[str, ...]
    owner_service: str = "collector"
    enabled: bool = True
    provider: Provider | None = None
    canonical_symbol: str | None = None


@dataclass(frozen=True, slots=True)
class StorageBinding:
    """Mapping rule from incoming event types into a storage destination."""

    binding_id: str
    storage_backend: str
    destination: str
    event_types: tuple[str, ...]
    scope: StorageBindingScope = StorageBindingScope.ALL_TARGETS
    collection_target_id: str | None = None
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class CollectionTargetStatus:
    """Runtime status for one collection target, suitable for reactive monitoring."""

    target_id: str
    state: RuntimeState
    observed_at: datetime
    owner_service: str = "collector"
    last_event_at: datetime | None = None
    last_error: str | None = None
    # Permanent failure metadata from KSXT KISSubscriptionError.  When
    # permanent_failure is True the collector will not retry this target;
    # admin UI renders an explicit "영구 실패" badge.
    permanent_failure: bool = False
    failure_reason: str | None = None
    failure_rt_cd: str | None = None
    failure_msg: str | None = None
    failure_attempts: int | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStatus:
    """Service-level runtime status for the admin/control plane."""

    component: str
    state: RuntimeState
    observed_at: datetime
    active_collection_target_ids: tuple[str, ...] = ()
    active_storage_binding_ids: tuple[str, ...] = ()
    last_error: str | None = None
