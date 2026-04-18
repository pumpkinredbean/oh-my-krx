"""CCXT / CCXT Pro live adapter for Binance spot + USDT perpetual.

Step 2 of the multiprovider migration replaces the previous step-1
descriptor stubs with a real (minimal) live adapter built on top of the
``ccxt.pro`` async streaming surface.

Scope:

* Binance spot via the ``binance`` exchange id.
* Binance USDT perpetual via the ``binanceusdm`` exchange id.
* Public ``watchTrades`` and ``watchOrderBook`` watchers — no auth, no
  user-data, no order placement.

The adapter intentionally exposes thin async generator helpers
(``stream_trades`` / ``stream_order_book_snapshots``) so the collector
runtime can subscribe per ``(instrument_type, symbol, event_name)``
channel without leaking ccxt types into the rest of the hub.

``ccxt.pro`` is imported lazily so the package keeps importing cleanly
on machines without ccxt installed (e.g. KXT-only deployments).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator

from packages.domain.enums import InstrumentType, Provider, Venue


logger = logging.getLogger(__name__)


_SUPPORTED_INSTRUMENT_TYPES: tuple[InstrumentType, ...] = (
    InstrumentType.SPOT,
    InstrumentType.PERPETUAL,
)


# ---------------------------------------------------------------------------
# Adapter descriptors (kept for registry compatibility)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CCXTAdapterStub:
    """Descriptor for the externally exposed CCXT provider boundary.

    Step 3 collapses the CCXT and CCXT Pro descriptors into a single
    ``CCXT`` provider.  ``ccxt.pro`` is kept as the internal transport
    detail used by :class:`BinanceLiveAdapter`; it must not appear on
    outward-facing contracts.
    """

    adapter_id: str = "ccxt"
    provider: Provider = Provider.CCXT
    default_venue: Venue = Venue.BINANCE
    implemented: bool = True
    supported_instrument_types: tuple[InstrumentType, ...] = _SUPPORTED_INSTRUMENT_TYPES
    notes: str = "Streaming served by BinanceLiveAdapter; ccxt.pro is internal transport detail."

    def healthcheck(self) -> bool:
        return True

    def describe(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "provider": self.provider.value,
            "implemented": self.implemented,
            "supported_instrument_types": tuple(t.value for t in self.supported_instrument_types),
            "notes": self.notes,
        }


@dataclass(frozen=True, slots=True)
class CCXTProAdapterStub:
    """Internal-only descriptor for the ccxt.pro transport.

    Retained for backwards compatibility with step1/step2 imports.
    Not registered in the public provider registry — the externally
    exposed surface uses ``CCXTAdapterStub`` (provider=ccxt) only.
    """

    adapter_id: str = "ccxt_pro"
    provider: Provider = Provider.CCXT_PRO
    default_venue: Venue = Venue.BINANCE
    implemented: bool = True
    supported_instrument_types: tuple[InstrumentType, ...] = _SUPPORTED_INSTRUMENT_TYPES
    notes: str = (
        "Internal transport detail behind Provider.CCXT — do not expose externally."
    )

    def healthcheck(self) -> bool:
        return True

    def describe(self) -> dict[str, object]:
        return {
            "adapter_id": self.adapter_id,
            "provider": self.provider.value,
            "implemented": self.implemented,
            "supported_instrument_types": tuple(t.value for t in self.supported_instrument_types),
            "notes": self.notes,
        }


def build_ccxt_adapter_stub() -> CCXTAdapterStub:
    return CCXTAdapterStub()


def build_ccxt_pro_adapter_stub() -> CCXTProAdapterStub:
    return CCXTProAdapterStub()


# ---------------------------------------------------------------------------
# Symbol normalisation (Binance spot vs USDT perpetual)
# ---------------------------------------------------------------------------


_KNOWN_QUOTES: tuple[str, ...] = ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB")


def to_unified_symbol(symbol: str, instrument_type: InstrumentType | str | None) -> str:
    """Normalise a hub symbol into a CCXT unified symbol.

    * Spot ``"BTCUSDT"`` → ``"BTC/USDT"`` (or pass-through if already unified).
    * USDT perpetual ``"BTCUSDT"`` → ``"BTC/USDT:USDT"``.

    The function is intentionally lenient: if the input already looks
    unified it is returned unchanged after only tightening the perpetual
    suffix when relevant.
    """

    raw = (symbol or "").strip()
    if not raw:
        raise ValueError("symbol is required")

    instrument_type_value = (
        instrument_type.value if isinstance(instrument_type, InstrumentType) else (instrument_type or "spot")
    )
    instrument_type_value = str(instrument_type_value).strip().lower() or "spot"

    base_quote = raw
    settle: str | None = None
    if ":" in raw:
        base_quote, settle = raw.split(":", 1)

    if "/" not in base_quote:
        upper = base_quote.upper()
        for quote in _KNOWN_QUOTES:
            if upper.endswith(quote) and len(upper) > len(quote):
                base_quote = f"{upper[: -len(quote)]}/{quote}"
                break
        else:
            # Default: assume USDT quote when nothing else matched.
            base_quote = f"{upper}/USDT"

    if instrument_type_value == "perpetual":
        if settle is None:
            quote = base_quote.split("/", 1)[1]
            settle = quote
        return f"{base_quote}:{settle}"
    return base_quote


def exchange_id_for(instrument_type: InstrumentType | str | None) -> str:
    """Return the CCXT exchange id Binance uses for this instrument type."""

    instrument_type_value = (
        instrument_type.value if isinstance(instrument_type, InstrumentType) else (instrument_type or "spot")
    )
    return "binanceusdm" if str(instrument_type_value).lower() == "perpetual" else "binance"


# ---------------------------------------------------------------------------
# BinanceLiveAdapter — real CCXT Pro streaming adapter
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BinanceTrade:
    """Lean trade payload returned by :meth:`BinanceLiveAdapter.stream_trades`."""

    symbol: str
    price: Decimal
    quantity: Decimal
    side: str | None
    occurred_at: datetime
    trade_id: str | None = None


@dataclass(frozen=True, slots=True)
class BinanceOrderBookSnapshot:
    """Lean order-book snapshot for :meth:`BinanceLiveAdapter.stream_order_book_snapshots`."""

    symbol: str
    occurred_at: datetime
    asks: tuple[tuple[Decimal, Decimal], ...]
    bids: tuple[tuple[Decimal, Decimal], ...]


class BinanceLiveAdapter:
    """Minimal real CCXT Pro adapter for Binance spot + USDT perpetual.

    Holds at most two exchange instances (``binance`` + ``binanceusdm``)
    that are constructed lazily on first use and torn down via
    :meth:`aclose`.  All streaming methods are public-data only.
    """

    adapter_id: str = "binance_live_ccxt_pro"

    def __init__(self) -> None:
        self._exchanges: dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def _exchange(self, instrument_type: InstrumentType | str | None) -> Any:
        ex_id = exchange_id_for(instrument_type)
        async with self._lock:
            if self._closed:
                raise RuntimeError("BinanceLiveAdapter is closed")
            existing = self._exchanges.get(ex_id)
            if existing is not None:
                return existing
            try:
                import ccxt.pro as ccxtpro  # type: ignore[import-not-found]
            except ImportError as exc:  # pragma: no cover - defensive
                raise RuntimeError(
                    "ccxt.pro is required for BinanceLiveAdapter; install ccxt>=4"
                ) from exc
            factory = getattr(ccxtpro, ex_id, None)
            if factory is None:
                raise RuntimeError(f"ccxt.pro has no exchange named {ex_id!r}")
            exchange = factory({"enableRateLimit": True})
            self._exchanges[ex_id] = exchange
            return exchange

    async def aclose(self) -> None:
        async with self._lock:
            self._closed = True
            exchanges = list(self._exchanges.values())
            self._exchanges.clear()
        for exchange in exchanges:
            try:
                await exchange.close()
            except Exception:  # pragma: no cover - defensive
                logger.exception("BinanceLiveAdapter exchange close failed")

    async def stream_trades(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.SPOT,
    ) -> AsyncIterator[BinanceTrade]:
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        while not self._closed:
            try:
                trades = await exchange.watch_trades(unified)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_trades failed for %s", unified)
                await asyncio.sleep(1.0)
                continue
            for raw in trades or ():
                yield _parse_ccxt_trade(unified, raw)

    async def stream_order_book_snapshots(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.SPOT,
        limit: int = 10,
    ) -> AsyncIterator[BinanceOrderBookSnapshot]:
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        while not self._closed:
            try:
                book = await exchange.watch_order_book(unified, limit)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_order_book failed for %s", unified)
                await asyncio.sleep(1.0)
                continue
            yield _parse_ccxt_order_book(unified, book, limit)


def _parse_ccxt_trade(symbol: str, raw: dict[str, Any]) -> BinanceTrade:
    timestamp_ms = raw.get("timestamp")
    occurred_at = (
        datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        if isinstance(timestamp_ms, (int, float))
        else datetime.now(tz=timezone.utc)
    )
    return BinanceTrade(
        symbol=symbol,
        price=Decimal(str(raw.get("price") or 0)),
        quantity=Decimal(str(raw.get("amount") or raw.get("quantity") or 0)),
        side=raw.get("side"),
        occurred_at=occurred_at,
        trade_id=str(raw.get("id")) if raw.get("id") is not None else None,
    )


def _parse_ccxt_order_book(symbol: str, raw: dict[str, Any], limit: int) -> BinanceOrderBookSnapshot:
    timestamp_ms = raw.get("timestamp")
    occurred_at = (
        datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        if isinstance(timestamp_ms, (int, float))
        else datetime.now(tz=timezone.utc)
    )

    def _levels(side: list[list[Any]] | None) -> tuple[tuple[Decimal, Decimal], ...]:
        if not side:
            return ()
        levels: list[tuple[Decimal, Decimal]] = []
        for entry in side[:limit]:
            if not entry:
                continue
            price = Decimal(str(entry[0]))
            amount = Decimal(str(entry[1])) if len(entry) > 1 else Decimal(0)
            levels.append((price, amount))
        return tuple(levels)

    return BinanceOrderBookSnapshot(
        symbol=symbol,
        occurred_at=occurred_at,
        asks=_levels(raw.get("asks")),
        bids=_levels(raw.get("bids")),
    )


def build_binance_live_adapter() -> BinanceLiveAdapter:
    return BinanceLiveAdapter()
