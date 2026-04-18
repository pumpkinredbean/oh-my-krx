"""Shared broker-agnostic enums for the core domain."""

from __future__ import annotations

from enum import StrEnum


class Venue(StrEnum):
    """Trading venue identifiers used by broker-neutral domain models."""

    KRX = "krx"
    NASDAQ = "nasdaq"
    NYSE = "nyse"
    CME = "cme"
    BINANCE = "binance"
    OTHER = "other"


class AssetClass(StrEnum):
    """Lean asset-class labels for instrument references."""

    EQUITY = "equity"
    ETF = "etf"
    FUTURE = "future"
    OPTION = "option"
    FX = "fx"
    CRYPTO = "crypto"
    INDEX = "index"
    OTHER = "other"


class TradeSide(StrEnum):
    """Optional aggressor or reported trade side when reliably available."""

    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class InstrumentType(StrEnum):
    """Instrument type classification supporting multiprovider semantics.

    Includes both legacy labels (equity/etf/fx_pair/crypto_pair) and
    multiprovider-ready semantics (spot/future/perpetual/option) so crypto
    venues can distinguish spot vs USDT-perpetual from day 1 while KRX
    equity defaults keep their existing ``equity`` label.
    """

    EQUITY = "equity"
    ETF = "etf"
    SPOT = "spot"
    FUTURE = "future"
    PERPETUAL = "perpetual"
    OPTION = "option"
    FX_PAIR = "fx_pair"
    CRYPTO_PAIR = "crypto_pair"
    INDEX = "index"
    OTHER = "other"


class Provider(StrEnum):
    """First-class provider/hub axis for multiprovider targets.

    Externally exposed provider values are restricted to ``kxt`` and
    ``ccxt``.  ``CCXT_PRO`` is retained as an internal transport detail
    (the adapter id used by :class:`BinanceLiveAdapter`) and MUST NOT
    appear on outward-facing contracts (target/snapshot/event/canonical
    symbol/UI).  Inputs of ``"ccxt_pro"`` are accepted as a deprecated
    alias and remapped to ``Provider.CCXT`` at the boundary.
    """

    KXT = "kxt"
    CCXT = "ccxt"
    CCXT_PRO = "ccxt_pro"  # internal transport detail only — see external_provider_value()
    OTHER = "other"


# ---------------------------------------------------------------------------
# External-facing normalisation
# ---------------------------------------------------------------------------

# Inputs accepted from UI/API for the provider axis.  ``ccxt_pro`` is kept
# as a deprecated alias and silently remapped to ``ccxt``.
EXTERNAL_PROVIDER_VALUES: frozenset[str] = frozenset({"kxt", "ccxt"})


def external_provider_value(provider: "Provider | str | None") -> str:
    """Return the externally exposed provider string for ``provider``.

    ``Provider.CCXT_PRO`` collapses to ``"ccxt"`` because the CCXT Pro
    transport is an internal implementation detail.  Empty/unknown input
    defaults to ``"kxt"`` to preserve backwards-compatible KXT envelopes.
    """

    if provider is None:
        return Provider.KXT.value
    value = provider.value if isinstance(provider, Provider) else str(provider).strip().lower()
    if not value:
        return Provider.KXT.value
    if value in {Provider.CCXT.value, Provider.CCXT_PRO.value}:
        return Provider.CCXT.value
    return value


class MarketSide(StrEnum):
    """Side semantics used by trades and book updates."""

    BID = "bid"
    ASK = "ask"
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class BarInterval(StrEnum):
    """Common bar interval labels for canonical bar events."""

    TICK = "tick"
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    DAY_1 = "1d"


class SessionState(StrEnum):
    """Trading session states when a source can report them."""

    PREOPEN = "preopen"
    OPEN = "open"
    HALTED = "halted"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class RuntimeState(StrEnum):
    """Lifecycle states for collector/admin control-plane runtime reporting."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    ERROR = "error"


class StorageBindingScope(StrEnum):
    """Scope labels for mapping incoming events into storage destinations."""

    ALL_TARGETS = "all_targets"
    COLLECTION_TARGET = "collection_target"
