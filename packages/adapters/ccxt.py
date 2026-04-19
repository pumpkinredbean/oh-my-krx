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


@dataclass(frozen=True, slots=True)
class BinanceTicker:
    """Lean ticker payload returned by :meth:`BinanceLiveAdapter.stream_tickers`."""

    symbol: str
    instrument_type: str
    occurred_at: datetime
    last: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    bid_size: Decimal | None
    ask_size: Decimal | None
    high: Decimal | None
    low: Decimal | None
    base_volume: Decimal | None
    quote_volume: Decimal | None


@dataclass(frozen=True, slots=True)
class BinanceBar:
    """Lean OHLCV bar payload returned by :meth:`BinanceLiveAdapter.stream_ohlcv`."""

    symbol: str
    instrument_type: str
    timeframe: str
    open_time_ms: int
    occurred_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True, slots=True)
class BinanceMarkPrice:
    """Perpetual mark-price payload returned by :meth:`BinanceLiveAdapter.stream_mark_price`."""

    symbol: str
    instrument_type: str
    occurred_at: datetime
    mark_price: Decimal | None
    index_price: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_timestamp_ms: int | None = None
    next_funding_timestamp_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceFundingRate:
    """Perpetual funding-rate payload from :meth:`BinanceLiveAdapter.poll_funding_rate`."""

    symbol: str
    instrument_type: str
    occurred_at: datetime
    funding_rate: Decimal | None
    funding_timestamp_ms: int | None = None
    next_funding_timestamp_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BinanceOpenInterest:
    """Perpetual open-interest payload from :meth:`BinanceLiveAdapter.poll_open_interest`."""

    symbol: str
    instrument_type: str
    occurred_at: datetime
    open_interest_amount: Decimal | None
    open_interest_value: Decimal | None = None


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

    async def stream_tickers(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.SPOT,
    ) -> AsyncIterator[BinanceTicker]:
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        it_value = _instrument_type_value(instrument_type)
        while not self._closed:
            try:
                raw = await exchange.watch_ticker(unified)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_ticker failed for %s", unified)
                await asyncio.sleep(1.0)
                continue
            if raw is None:
                continue
            yield _parse_ccxt_ticker(unified, it_value, raw)

    async def stream_ohlcv(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.SPOT,
        timeframe: str = "1m",
    ) -> AsyncIterator[BinanceBar]:
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        it_value = _instrument_type_value(instrument_type)
        while not self._closed:
            try:
                bars = await exchange.watchOHLCV(unified, timeframe)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watchOHLCV failed for %s (%s)", unified, timeframe)
                await asyncio.sleep(1.0)
                continue
            if not bars:
                continue
            latest = bars[-1]
            yield _parse_ccxt_ohlcv_bar(unified, it_value, timeframe, latest)

    async def stream_mark_price(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.PERPETUAL,
    ) -> AsyncIterator[BinanceMarkPrice]:
        it_value = _instrument_type_value(instrument_type)
        if it_value != "perpetual":
            raise ValueError("stream_mark_price requires instrument_type='perpetual'")
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        while not self._closed:
            try:
                raw = await exchange.watch_mark_price(unified)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_mark_price failed for %s", unified)
                await asyncio.sleep(1.0)
                continue
            if raw is None:
                continue
            yield _parse_ccxt_mark_price(unified, it_value, raw)

    async def poll_funding_rate(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.PERPETUAL,
        interval: float = 30.0,
    ) -> AsyncIterator[BinanceFundingRate]:
        it_value = _instrument_type_value(instrument_type)
        if it_value != "perpetual":
            raise ValueError("poll_funding_rate requires instrument_type='perpetual'")
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        while not self._closed:
            try:
                raw = await exchange.fetch_funding_rate(unified)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("fetch_funding_rate failed for %s", unified)
                await asyncio.sleep(interval)
                continue
            if raw is not None:
                yield _parse_ccxt_funding_rate(unified, it_value, raw)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise

    async def poll_open_interest(
        self,
        *,
        symbol: str,
        instrument_type: InstrumentType | str | None = InstrumentType.PERPETUAL,
        interval: float = 30.0,
    ) -> AsyncIterator[BinanceOpenInterest]:
        it_value = _instrument_type_value(instrument_type)
        if it_value != "perpetual":
            raise ValueError("poll_open_interest requires instrument_type='perpetual'")
        unified = to_unified_symbol(symbol, instrument_type)
        exchange = await self._exchange(instrument_type)
        while not self._closed:
            try:
                raw = await exchange.fetch_open_interest(unified)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("fetch_open_interest failed for %s", unified)
                await asyncio.sleep(interval)
                continue
            if raw is not None:
                yield _parse_ccxt_open_interest(unified, it_value, raw)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise


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


def _instrument_type_value(instrument_type: InstrumentType | str | None) -> str:
    if isinstance(instrument_type, InstrumentType):
        return instrument_type.value
    return str(instrument_type or "spot").strip().lower() or "spot"


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _timestamp_to_datetime(timestamp_ms: Any) -> datetime:
    if isinstance(timestamp_ms, (int, float)) and timestamp_ms > 0:
        return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _parse_ccxt_ticker(symbol: str, instrument_type: str, raw: dict[str, Any]) -> BinanceTicker:
    occurred_at = _timestamp_to_datetime(raw.get("timestamp"))
    return BinanceTicker(
        symbol=symbol,
        instrument_type=instrument_type,
        occurred_at=occurred_at,
        last=_to_decimal(raw.get("last") if raw.get("last") is not None else raw.get("close")),
        bid=_to_decimal(raw.get("bid")),
        ask=_to_decimal(raw.get("ask")),
        bid_size=_to_decimal(raw.get("bidVolume")),
        ask_size=_to_decimal(raw.get("askVolume")),
        high=_to_decimal(raw.get("high")),
        low=_to_decimal(raw.get("low")),
        base_volume=_to_decimal(raw.get("baseVolume")),
        quote_volume=_to_decimal(raw.get("quoteVolume")),
    )


def _parse_ccxt_ohlcv_bar(
    symbol: str,
    instrument_type: str,
    timeframe: str,
    raw: list[Any] | tuple[Any, ...],
) -> BinanceBar:
    # ccxt OHLCV format: [timestamp_ms, open, high, low, close, volume]
    open_time_ms = int(raw[0]) if raw and raw[0] is not None else 0
    occurred_at = _timestamp_to_datetime(open_time_ms)
    return BinanceBar(
        symbol=symbol,
        instrument_type=instrument_type,
        timeframe=timeframe,
        open_time_ms=open_time_ms,
        occurred_at=occurred_at,
        open=_to_decimal(raw[1]) or Decimal(0),
        high=_to_decimal(raw[2]) or Decimal(0),
        low=_to_decimal(raw[3]) or Decimal(0),
        close=_to_decimal(raw[4]) or Decimal(0),
        volume=_to_decimal(raw[5]) if len(raw) > 5 else Decimal(0),
    )


def _parse_ccxt_mark_price(symbol: str, instrument_type: str, raw: dict[str, Any]) -> BinanceMarkPrice:
    # ccxt may expose the mark-price either at the dict root or under .info
    info = raw.get("info") if isinstance(raw, dict) else None
    mark_raw: Any = None
    for key in ("markPrice", "mark_price", "mark"):
        if isinstance(raw, dict) and raw.get(key) is not None:
            mark_raw = raw.get(key)
            break
    if mark_raw is None and isinstance(info, dict):
        for key in ("markPrice", "mark_price", "p"):
            if info.get(key) is not None:
                mark_raw = info.get(key)
                break
    if mark_raw is None and isinstance(raw, dict):
        # Some builds return a ticker shape with ``last`` as the mark.
        mark_raw = raw.get("last") if raw.get("last") is not None else raw.get("close")
    index_raw: Any = None
    if isinstance(raw, dict):
        index_raw = raw.get("indexPrice")
    if index_raw is None and isinstance(info, dict):
        index_raw = info.get("indexPrice") or info.get("i")

    # Funding rate extraction: ccxt root then WS info (Binance USD-M fields).
    funding_rate_raw: Any = None
    if isinstance(raw, dict):
        funding_rate_raw = raw.get("fundingRate")
    if funding_rate_raw is None and isinstance(info, dict):
        for key in ("fundingRate", "lastFundingRate", "r"):
            if info.get(key) is not None:
                funding_rate_raw = info.get(key)
                break

    def _numeric_ms(value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                try:
                    return int(float(value))
                except ValueError:
                    return None
        return None

    next_funding_ms: int | None = None
    if isinstance(raw, dict):
        candidate = raw.get("nextFundingTimestamp")
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            next_funding_ms = int(candidate)
    if next_funding_ms is None and isinstance(info, dict):
        for key in ("T", "nextFundingTime"):
            if key in info and info.get(key) is not None:
                next_funding_ms = _numeric_ms(info.get(key))
                if next_funding_ms is not None:
                    break

    funding_ts_ms: int | None = None
    if isinstance(raw, dict):
        candidate = raw.get("fundingTimestamp")
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            funding_ts_ms = int(candidate)
        if funding_ts_ms is None:
            candidate = raw.get("timestamp")
            if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
                funding_ts_ms = int(candidate)
    if funding_ts_ms is None and isinstance(info, dict):
        candidate = info.get("E")
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            funding_ts_ms = int(candidate)

    occurred_at = _timestamp_to_datetime(raw.get("timestamp") if isinstance(raw, dict) else None)
    return BinanceMarkPrice(
        symbol=symbol,
        instrument_type=instrument_type,
        occurred_at=occurred_at,
        mark_price=_to_decimal(mark_raw),
        index_price=_to_decimal(index_raw),
        funding_rate=_to_decimal(funding_rate_raw),
        funding_timestamp_ms=funding_ts_ms,
        next_funding_timestamp_ms=next_funding_ms,
    )


def _parse_ccxt_funding_rate(symbol: str, instrument_type: str, raw: dict[str, Any]) -> BinanceFundingRate:
    rate = raw.get("fundingRate")
    if rate is None:
        info = raw.get("info") if isinstance(raw, dict) else None
        if isinstance(info, dict):
            rate = info.get("lastFundingRate") or info.get("fundingRate")
    funding_ts = raw.get("fundingTimestamp") or raw.get("timestamp")
    next_funding_ts = raw.get("nextFundingTimestamp") or raw.get("fundingDatetime")
    if isinstance(next_funding_ts, str):
        next_funding_ts = None
    occurred_at = _timestamp_to_datetime(raw.get("timestamp"))
    return BinanceFundingRate(
        symbol=symbol,
        instrument_type=instrument_type,
        occurred_at=occurred_at,
        funding_rate=_to_decimal(rate),
        funding_timestamp_ms=int(funding_ts) if isinstance(funding_ts, (int, float)) else None,
        next_funding_timestamp_ms=int(next_funding_ts) if isinstance(next_funding_ts, (int, float)) else None,
    )


def _parse_ccxt_open_interest(symbol: str, instrument_type: str, raw: dict[str, Any]) -> BinanceOpenInterest:
    amount = raw.get("openInterestAmount")
    if amount is None:
        amount = raw.get("openInterest")
    value = raw.get("openInterestValue")
    if value is None:
        info = raw.get("info") if isinstance(raw, dict) else None
        if isinstance(info, dict):
            value = info.get("sumOpenInterestValue") or info.get("notional")
    occurred_at = _timestamp_to_datetime(raw.get("timestamp"))
    return BinanceOpenInterest(
        symbol=symbol,
        instrument_type=instrument_type,
        occurred_at=occurred_at,
        open_interest_amount=_to_decimal(amount),
        open_interest_value=_to_decimal(value),
    )
