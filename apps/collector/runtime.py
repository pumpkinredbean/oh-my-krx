from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

from kxt import (
    BarTimeframe as KSXTBarTimeframe,
    InstrumentRef as KSXTInstrumentRef,
    KISClient,
    KISRealtimeSession,
    KISSubscriptionError,
    MarketBar as KSXTMarketBar,
    OrderBookEvent as KSXTOrderBookEvent,
    OrderBookSnapshot as KSXTOrderBookSnapshot,
    RealtimeState,
    RealtimeSessionConfig,
    StreamKind,
    Trade as KSXTTrade,
    TradeEvent as KSXTTradeEvent,
    Venue as KSXTVenue,
)
from kxt.requests import Subscription as KSXTSubscription

from packages.contracts import EventType
from packages.domain.enums import AssetClass, InstrumentType, Provider, Venue, external_provider_value
from packages.adapters.ccxt import (
    BinanceBar,
    BinanceFundingRate,
    BinanceLiveAdapter,
    BinanceMarkPrice,
    BinanceOpenInterest,
    BinanceOrderBookSnapshot,
    BinanceTicker,
    BinanceTrade,
    to_unified_symbol,
)
from packages.domain.models import build_canonical_symbol

logger = logging.getLogger(__name__)


SUPPORTED_MARKET_SCOPES = {"krx", "nxt", "total"}

# Event type names as used by the admin UI / control plane.
_EVENT_TRADE = EventType.TRADE.value
_EVENT_ORDER_BOOK = EventType.ORDER_BOOK_SNAPSHOT.value
_EVENT_PROGRAM_TRADE = EventType.PROGRAM_TRADE.value
_EVENT_TICKER = EventType.TICKER.value
_EVENT_OHLCV = EventType.OHLCV.value
_EVENT_MARK_PRICE = EventType.MARK_PRICE.value
_EVENT_FUNDING_RATE = EventType.FUNDING_RATE.value
_EVENT_OPEN_INTEREST = EventType.OPEN_INTEREST.value

# Map admin event names to KSXT StreamKind.  ``program_trade`` is not exposed by
# the KSXT realtime session (only trades + order_book).  Targets requesting
# program_trade will be flagged as permanent failures for that channel — this
# is a scoped limitation of KSXT v0.1.0 noted in hub-B report §8.
_STREAM_KIND_BY_EVENT_NAME: dict[str, StreamKind] = {
    _EVENT_TRADE: StreamKind.trades,
    _EVENT_ORDER_BOOK: StreamKind.order_book,
}

ALL_EVENT_NAMES: tuple[str, ...] = tuple(event_type.value for event_type in EventType)

_KST = timezone(timedelta(hours=9))


@dataclass(frozen=True, slots=True)
class DashboardStreamKey:
    symbol: str
    market_scope: str


@dataclass(frozen=True, slots=True)
class RuntimeTargetRegistration:
    owner_id: str
    stream_key: DashboardStreamKey
    event_types: tuple[str, ...]
    provider: Provider = Provider.KXT
    canonical_symbol: str | None = None
    instrument_type: InstrumentType | None = None
    raw_symbol: str | None = None


@dataclass(frozen=True, slots=True)
class _ChannelKey:
    symbol: str
    market_scope: str
    event_name: str  # trade | order_book_snapshot | program_trade


@dataclass(frozen=True, slots=True)
class _CryptoChannelKey:
    """Channel key for crypto live channels.

    ``canonical_symbol`` (provider:venue:instrument_type:symbol) keeps
    spot vs USDT-perpetual BTCUSDT distinct so they cannot collide on
    dedupe / matching.
    """

    canonical_symbol: str
    event_name: str


@dataclass(slots=True)
class _ChannelEntry:
    subscription: KSXTSubscription | None
    task: asyncio.Task[None] | None
    owners: set[str]
    permanent_failure: bool = False
    events_seen: bool = False
    ack_watchdog: asyncio.Task[None] | None = None


@dataclass(slots=True)
class _CryptoChannelEntry:
    task: asyncio.Task[None] | None
    owners: set[str]
    instrument_type: InstrumentType
    symbol: str
    canonical_symbol: str
    provider: Provider


@dataclass(slots=True)
class _CryptoMarkPriceSourceEntry:
    canonical_symbol: str
    symbol: str
    instrument_type: InstrumentType
    provider: Provider
    task: asyncio.Task[None] | None
    active_event_names: set[str]


def _decimal_to_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _decimal_to_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _number(value: Any) -> int | float | str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _format_trade_event(event: KSXTTradeEvent | KSXTTrade) -> tuple[str, dict[str, Any]]:
    occurred_at = event.occurred_at
    if occurred_at.tzinfo is None:
        occurred_at_kst = occurred_at.replace(tzinfo=timezone.utc).astimezone(_KST)
    else:
        occurred_at_kst = occurred_at.astimezone(_KST)
    payload = {
        "체결시각": occurred_at_kst.strftime("%H:%M:%S"),
        "현재가": _number(event.price),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    return "trade_price", payload


def _format_order_book_event(event: KSXTOrderBookEvent | KSXTOrderBookSnapshot) -> tuple[str, dict[str, Any]]:
    occurred_at = event.occurred_at
    if occurred_at.tzinfo is None:
        occurred_at_kst = occurred_at.replace(tzinfo=timezone.utc).astimezone(_KST)
    else:
        occurred_at_kst = occurred_at.astimezone(_KST)
    payload: dict[str, Any] = {
        "호가시각": occurred_at_kst.strftime("%H%M%S"),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    for index, level in enumerate(event.asks[:10], start=1):
        payload[f"매도호가{index}"] = _number(level.price)
        payload[f"매도잔량{index}"] = _number(level.quantity)
    for index, level in enumerate(event.bids[:10], start=1):
        payload[f"매수호가{index}"] = _number(level.price)
        payload[f"매수잔량{index}"] = _number(level.quantity)
    payload["총매도잔량"] = _number(event.total_ask_quantity)
    payload["총매수잔량"] = _number(event.total_bid_quantity)
    return "order_book", payload


def _enum_value(value: Any) -> Any:
    """Return the ``.value`` of an enum-like object; pass strings through unchanged."""

    if value is None:
        return None
    inner = getattr(value, "value", None)
    return inner if inner is not None else value


def _instrument_ref_to_dict(instrument: Any) -> dict[str, Any] | None:
    """Serialize a KXT InstrumentRef into a plain dict (DTO field names preserved)."""

    if instrument is None:
        return None
    return {
        "symbol": getattr(instrument, "symbol", None),
        "venue": _enum_value(getattr(instrument, "venue", None)),
        "market_segment": _enum_value(getattr(instrument, "market_segment", None)),
        "instrument_id": getattr(instrument, "instrument_id", None),
        "name": getattr(instrument, "name", None),
        "isin": getattr(instrument, "isin", None),
        "asset_class": _enum_value(getattr(instrument, "asset_class", None)),
        "instrument_type": _enum_value(getattr(instrument, "instrument_type", None)),
    }


def _isoformat_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _kxt_trade_event_dto_payload(event: KSXTTradeEvent | KSXTTrade) -> dict[str, Any]:
    """KXT TradeEvent/Trade serialized using KXT DTO field names (no Korean keys)."""

    payload: dict[str, Any] = {
        "instrument": _instrument_ref_to_dict(getattr(event, "instrument", None)),
        "occurred_at": _isoformat_utc(event.occurred_at),
        "price": _number(getattr(event, "price", None)),
        "quantity": _number(getattr(event, "quantity", None)),
        "side": _enum_value(getattr(event, "side", None)),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    # Trade-specific optional fields (when serialising KSXTTrade rather than TradeEvent)
    for optional_field in ("trade_id", "sequence"):
        if hasattr(event, optional_field):
            value = getattr(event, optional_field)
            if value is not None:
                payload[optional_field] = value
    for optional_decimal in ("ask_price", "bid_price"):
        if hasattr(event, optional_decimal):
            value = getattr(event, optional_decimal)
            if value is not None:
                payload[optional_decimal] = _number(value)
    return payload


def _kxt_order_book_event_dto_payload(
    event: KSXTOrderBookEvent | KSXTOrderBookSnapshot,
) -> dict[str, Any]:
    """KXT OrderBookEvent/Snapshot serialized using KXT DTO field names."""

    asks = [
        {"price": _number(level.price), "quantity": _number(level.quantity)}
        for level in getattr(event, "asks", ()) or ()
    ]
    bids = [
        {"price": _number(level.price), "quantity": _number(level.quantity)}
        for level in getattr(event, "bids", ()) or ()
    ]
    return {
        "instrument": _instrument_ref_to_dict(getattr(event, "instrument", None)),
        "occurred_at": _isoformat_utc(event.occurred_at),
        "asks": asks,
        "bids": bids,
        "total_ask_quantity": _number(getattr(event, "total_ask_quantity", None)),
        "total_bid_quantity": _number(getattr(event, "total_bid_quantity", None)),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def _format_market_bars(bars: tuple[KSXTMarketBar, ...]) -> list[dict[str, Any]]:
    """Format KSXT MarketBar tuple into dashboard candle dicts (shape-only)."""

    formatted: list[dict[str, Any]] = []
    for bar in bars:
        opened_at = bar.opened_at
        if opened_at.tzinfo is None:
            opened_at_kst = opened_at.replace(tzinfo=_KST)
        else:
            opened_at_kst = opened_at.astimezone(_KST)

        formatted.append(
            {
                "time": int(opened_at_kst.timestamp()),
                "label": opened_at_kst.strftime("%H:%M"),
                "session_date": opened_at_kst.date().isoformat(),
                "source_time": opened_at_kst.strftime("%H%M%S"),
                "open": _decimal_to_float(bar.open),
                "high": _decimal_to_float(bar.high),
                "low": _decimal_to_float(bar.low),
                "close": _decimal_to_float(bar.close),
                "volume": _decimal_to_int(bar.volume),
            }
        )
    return formatted[-120:]


def _format_crypto_trade(event: BinanceTrade) -> tuple[str, dict[str, Any]]:
    occurred_at = event.occurred_at
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    payload = {
        "체결시각": occurred_at.astimezone(timezone.utc).strftime("%H:%M:%S"),
        "현재가": _number(event.price),
        "거래량": _number(event.quantity),
        "side": event.side,
        "trade_id": event.trade_id,
        "symbol": event.symbol,
        "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    return "trade_price", payload


def _format_crypto_order_book(event: BinanceOrderBookSnapshot) -> tuple[str, dict[str, Any]]:
    occurred_at = event.occurred_at
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    payload: dict[str, Any] = {
        "호가시각": occurred_at.astimezone(timezone.utc).strftime("%H%M%S"),
        "symbol": event.symbol,
        "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    for index, (price, quantity) in enumerate(event.asks[:10], start=1):
        payload[f"매도호가{index}"] = _number(price)
        payload[f"매도잔량{index}"] = _number(quantity)
    for index, (price, quantity) in enumerate(event.bids[:10], start=1):
        payload[f"매수호가{index}"] = _number(price)
        payload[f"매수잔량{index}"] = _number(quantity)
    return "order_book", payload


def _utc_iso(event_dt: datetime) -> str:
    if event_dt.tzinfo is None:
        event_dt = event_dt.replace(tzinfo=timezone.utc)
    return event_dt.astimezone(timezone.utc).isoformat()


def _format_crypto_ticker(event: BinanceTicker) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "last": _number(event.last),
        "bid": _number(event.bid),
        "ask": _number(event.ask),
        "bid_size": _number(event.bid_size),
        "ask_size": _number(event.ask_size),
        "high": _number(event.high),
        "low": _number(event.low),
        "base_volume": _number(event.base_volume),
        "quote_volume": _number(event.quote_volume),
    }
    return _EVENT_TICKER, payload


def _format_crypto_bar(event: BinanceBar) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "timeframe": event.timeframe,
        "open_time_ms": event.open_time_ms,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "open": _number(event.open),
        "high": _number(event.high),
        "low": _number(event.low),
        "close": _number(event.close),
        "volume": _number(event.volume),
    }
    return _EVENT_OHLCV, payload


def _format_crypto_mark_price(event: BinanceMarkPrice) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "mark_price": _number(event.mark_price),
        "index_price": _number(event.index_price),
    }
    return _EVENT_MARK_PRICE, payload


def _format_crypto_funding_rate_from_mark(event: BinanceMarkPrice) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "funding_rate": _number(event.funding_rate),
        "funding_timestamp_ms": event.funding_timestamp_ms,
        "next_funding_timestamp_ms": event.next_funding_timestamp_ms,
    }
    return _EVENT_FUNDING_RATE, payload


def _format_crypto_funding_rate(event: BinanceFundingRate) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "funding_rate": _number(event.funding_rate),
        "funding_timestamp_ms": event.funding_timestamp_ms,
        "next_funding_timestamp_ms": event.next_funding_timestamp_ms,
    }
    return _EVENT_FUNDING_RATE, payload


def _format_crypto_open_interest(event: BinanceOpenInterest) -> tuple[str, dict[str, Any]]:
    payload = {
        "symbol": event.symbol,
        "instrument_type": event.instrument_type,
        "occurred_at": _utc_iso(event.occurred_at),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "open_interest_amount": _number(event.open_interest_amount),
        "open_interest_value": _number(event.open_interest_value),
    }
    return _EVENT_OPEN_INTEREST, payload


def _crypto_raw_control_plane_payload(
    *,
    event_name: str,
    event: Any,
    normalized: dict[str, Any],
) -> dict[str, Any]:
    """Build the Binance/CCXT admin payload from raw provider data only.

    The dashboard payload may retain legacy display keys for compatibility,
    but admin Events/control-plane/charts must not receive those display
    fields.  This payload preserves the CCXT object under ``raw`` and keeps a
    small English-key normalized summary for matching/debugging.
    """

    return {
        "event_name": event_name,
        "provider": "ccxt",
        "venue": "binance",
        "symbol": getattr(event, "symbol", None),
        "instrument_type": getattr(event, "instrument_type", None),
        "occurred_at": _utc_iso(getattr(event, "occurred_at")),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "normalized": normalized,
        "raw": getattr(event, "raw", None),
    }


def _crypto_trade_control_plane_payload(event: BinanceTrade) -> dict[str, Any]:
    return _crypto_raw_control_plane_payload(
        event_name=_EVENT_TRADE,
        event=event,
        normalized={
            "symbol": event.symbol,
            "price": _number(event.price),
            "quantity": _number(event.quantity),
            "side": event.side,
            "trade_id": event.trade_id,
            "occurred_at": _utc_iso(event.occurred_at),
        },
    )


def _crypto_order_book_control_plane_payload(event: BinanceOrderBookSnapshot) -> dict[str, Any]:
    return _crypto_raw_control_plane_payload(
        event_name=_EVENT_ORDER_BOOK,
        event=event,
        normalized={
            "symbol": event.symbol,
            "occurred_at": _utc_iso(event.occurred_at),
            "asks": [[_number(price), _number(quantity)] for price, quantity in event.asks[:10]],
            "bids": [[_number(price), _number(quantity)] for price, quantity in event.bids[:10]],
        },
    )


def _crypto_ticker_control_plane_payload(event: BinanceTicker) -> dict[str, Any]:
    _, normalized = _format_crypto_ticker(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_TICKER, event=event, normalized=normalized)


def _crypto_bar_control_plane_payload(event: BinanceBar) -> dict[str, Any]:
    _, normalized = _format_crypto_bar(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_OHLCV, event=event, normalized=normalized)


def _crypto_mark_price_control_plane_payload(event: BinanceMarkPrice) -> dict[str, Any]:
    _, normalized = _format_crypto_mark_price(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_MARK_PRICE, event=event, normalized=normalized)


def _crypto_funding_rate_from_mark_control_plane_payload(event: BinanceMarkPrice) -> dict[str, Any]:
    _, normalized = _format_crypto_funding_rate_from_mark(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_FUNDING_RATE, event=event, normalized=normalized)


def _crypto_funding_rate_control_plane_payload(event: BinanceFundingRate) -> dict[str, Any]:
    _, normalized = _format_crypto_funding_rate(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_FUNDING_RATE, event=event, normalized=normalized)


def _crypto_open_interest_control_plane_payload(event: BinanceOpenInterest) -> dict[str, Any]:
    _, normalized = _format_crypto_open_interest(event)
    return _crypto_raw_control_plane_payload(event_name=_EVENT_OPEN_INTEREST, event=event, normalized=normalized)


class CollectorRuntime:
    """Collector-owned live runtime driven by the KSXT ``KISRealtimeSession``.

    This runtime owns a single :class:`KISRealtimeSession` and maps admin
    collection targets onto per-``(symbol, event_type)`` KSXT subscriptions.
    Subscription retries, reconnects, and permanent-failure classification
    are delegated to KSXT itself; the collector only consumes events and
    surfaces session/subscription signals to the admin control plane.
    """

    _SESSION_READY_TIMEOUT: float = 10.0
    _SUBSCRIBE_ACK_TIMEOUT: float = 10.0

    def __init__(
        self,
        settings: Any,
        *,
        on_event: Callable[..., Awaitable[None]] | None = None,
        on_failure: Callable[..., Awaitable[None]] | None = None,
        on_recovery: Callable[..., Awaitable[None]] | None = None,
        on_session_state_change: Callable[..., Awaitable[None]] | None = None,
        on_permanent_failure: Callable[..., Awaitable[None]] | None = None,
    ):
        self._client = KISClient(
            app_key=settings.app_key,
            app_secret=settings.app_secret,
            sandbox=False,
        )
        # Access the underlying transport so we can build a session with
        # our own state/recovery callbacks.  ``client.realtime`` constructs
        # a session without callbacks; for the hub we need the ctor path.
        # Private access is documented in the hub-B migration report.
        self._session = KISRealtimeSession(
            self._client._transport,  # noqa: SLF001 — documented private access
            config=RealtimeSessionConfig(),
            on_state_change=self._handle_state_change,
            on_recovery=self._handle_recovery,
        )
        self._on_event = on_event
        self._on_failure = on_failure
        self._on_recovery = on_recovery
        self._on_session_state_change = on_session_state_change
        self._on_permanent_failure = on_permanent_failure
        self._lock = asyncio.Lock()
        self._registrations_by_owner: dict[str, RuntimeTargetRegistration] = {}
        self._channels: dict[_ChannelKey, _ChannelEntry] = {}
        self._crypto_channels: dict[_CryptoChannelKey, _CryptoChannelEntry] = {}
        self._crypto_mark_sources: dict[str, _CryptoMarkPriceSourceEntry] = {}
        self._crypto_adapter: BinanceLiveAdapter | None = None
        self._closed = False

    # ---- lifecycle ------------------------------------------------------

    async def aclose(self) -> None:
        self._closed = True
        async with self._lock:
            channels = tuple(self._channels.values())
            crypto_channels = tuple(self._crypto_channels.values())
            crypto_mark_sources = tuple(self._crypto_mark_sources.values())
            self._channels.clear()
            self._crypto_channels.clear()
            self._crypto_mark_sources.clear()
            self._registrations_by_owner.clear()
            crypto_adapter = self._crypto_adapter
            self._crypto_adapter = None

        for entry in channels:
            task = entry.task
            subscription = entry.subscription
            watchdog = entry.ack_watchdog
            if watchdog is not None:
                watchdog.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await watchdog
            if subscription is not None:
                with contextlib.suppress(Exception):
                    await subscription.aclose()
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

        for crypto_entry in crypto_channels:
            task = crypto_entry.task
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

        for source_entry in crypto_mark_sources:
            source_task = source_entry.task
            if source_task is not None:
                source_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await source_task

        if crypto_adapter is not None:
            with contextlib.suppress(Exception):
                await crypto_adapter.aclose()

        with contextlib.suppress(Exception):
            await self._session.aclose()
        with contextlib.suppress(Exception):
            await self._client.aclose()

    # ---- registrations --------------------------------------------------

    async def register_target(
        self,
        *,
        owner_id: str,
        symbol: str,
        market_scope: str,
        event_types: tuple[str, ...] | list[str] | None = None,
        provider: str | Provider | None = None,
        canonical_symbol: str | None = None,
        instrument_type: str | InstrumentType | None = None,
        raw_symbol: str | None = None,
    ) -> RuntimeTargetRegistration:
        normalized_owner_id = owner_id.strip()
        if not normalized_owner_id:
            raise ValueError("owner_id is required")

        resolved_provider = self._resolve_provider(provider)
        resolved_instrument_type = self._resolve_instrument_type(instrument_type, provider=resolved_provider)
        normalized_event_types = self._normalize_event_types(event_types)

        if resolved_provider == Provider.KXT:
            return await self._register_kxt_target(
                owner_id=normalized_owner_id,
                symbol=symbol,
                market_scope=market_scope,
                event_types=normalized_event_types,
                provider=resolved_provider,
                canonical_symbol=canonical_symbol,
                instrument_type=resolved_instrument_type,
                raw_symbol=raw_symbol or symbol,
            )

        if resolved_provider == Provider.CCXT:
            return await self._register_crypto_target(
                owner_id=normalized_owner_id,
                symbol=symbol,
                event_types=normalized_event_types,
                provider=resolved_provider,
                canonical_symbol=canonical_symbol,
                instrument_type=resolved_instrument_type,
                raw_symbol=raw_symbol,
            )

        raise NotImplementedError(
            f"runtime adapter for provider={resolved_provider.value} is not wired yet"
        )

    async def _register_kxt_target(
        self,
        *,
        owner_id: str,
        symbol: str,
        market_scope: str,
        event_types: tuple[str, ...],
        provider: Provider,
        canonical_symbol: str | None,
        instrument_type: InstrumentType | None,
        raw_symbol: str | None,
    ) -> RuntimeTargetRegistration:
        stream_key = self._build_stream_key(symbol=symbol, market_scope=market_scope)
        registration = RuntimeTargetRegistration(
            owner_id=owner_id,
            stream_key=stream_key,
            event_types=event_types,
            provider=provider,
            canonical_symbol=canonical_symbol,
            instrument_type=instrument_type,
            raw_symbol=raw_symbol,
        )

        await self._wait_session_ready(timeout=self._SESSION_READY_TIMEOUT)

        async with self._lock:
            previous = self._registrations_by_owner.get(owner_id)
            self._registrations_by_owner[owner_id] = registration

            previous_channels = (
                {_ChannelKey(stream_key.symbol, stream_key.market_scope, ev) for ev in previous.event_types}
                if previous is not None and previous.provider == Provider.KXT
                else set()
            )
            new_channels = {_ChannelKey(stream_key.symbol, stream_key.market_scope, ev) for ev in event_types}
            to_add = new_channels - previous_channels
            to_remove = previous_channels - new_channels

        for channel_key in to_add:
            await self._acquire_channel(channel_key, owner_id=owner_id)
        for channel_key in to_remove:
            await self._release_channel(channel_key, owner_id=owner_id)

        return registration

    async def _register_crypto_target(
        self,
        *,
        owner_id: str,
        symbol: str,
        event_types: tuple[str, ...],
        provider: Provider,
        canonical_symbol: str | None,
        instrument_type: InstrumentType | None,
        raw_symbol: str | None,
    ) -> RuntimeTargetRegistration:
        normalized_symbol = symbol.strip()
        if not normalized_symbol:
            raise ValueError("symbol is required")

        resolved_instrument_type = instrument_type or InstrumentType.SPOT
        if resolved_instrument_type not in (InstrumentType.SPOT, InstrumentType.PERPETUAL):
            raise ValueError(
                f"unsupported crypto instrument_type: {resolved_instrument_type.value}"
            )

        # Display symbol (unified ccxt form, e.g. BTC/USDT) and raw symbol
        # (venue-native, e.g. BTCUSDT) are split on the external surface.
        display_symbol = to_unified_symbol(normalized_symbol, resolved_instrument_type)
        if resolved_instrument_type == InstrumentType.PERPETUAL:
            base_quote, _, settle = display_symbol.partition(":")
            settle_asset = (settle or base_quote.split("/", 1)[1]).upper()
            base_quote_for_raw = base_quote
        else:
            settle_asset = None
            base_quote_for_raw = display_symbol
        resolved_raw = raw_symbol or normalized_symbol or base_quote_for_raw.replace("/", "")

        resolved_canonical = canonical_symbol or build_canonical_symbol(
            provider=provider,
            venue=Venue.BINANCE,
            asset_class=AssetClass.CRYPTO,
            instrument_type=resolved_instrument_type,
            display_symbol=base_quote_for_raw if resolved_instrument_type == InstrumentType.PERPETUAL else display_symbol,
            settle_asset=settle_asset,
        )

        # Crypto targets do not carry a KRX market_scope; use the empty scope
        # so dashboard plumbing receives a not-applicable signal.
        stream_key = DashboardStreamKey(symbol=resolved_raw, market_scope="")
        registration = RuntimeTargetRegistration(
            owner_id=owner_id,
            stream_key=stream_key,
            event_types=event_types,
            provider=provider,
            canonical_symbol=resolved_canonical,
            instrument_type=resolved_instrument_type,
            raw_symbol=resolved_raw,
        )

        async with self._lock:
            previous = self._registrations_by_owner.get(owner_id)
            self._registrations_by_owner[owner_id] = registration

            previous_channels: set[_CryptoChannelKey] = set()
            if previous is not None and previous.provider in (Provider.CCXT, Provider.CCXT_PRO):
                previous_canonical = previous.canonical_symbol or build_canonical_symbol(
                    provider=previous.provider,
                    venue=Venue.BINANCE,
                    asset_class=AssetClass.CRYPTO,
                    instrument_type=previous.instrument_type or InstrumentType.SPOT,
                    display_symbol=to_unified_symbol(
                        previous.raw_symbol or previous.stream_key.symbol,
                        previous.instrument_type or InstrumentType.SPOT,
                    ).split(":", 1)[0],
                )
                previous_channels = {
                    _CryptoChannelKey(canonical_symbol=previous_canonical, event_name=ev)
                    for ev in previous.event_types
                }
            new_channels = {
                _CryptoChannelKey(canonical_symbol=resolved_canonical, event_name=ev)
                for ev in event_types
            }
            to_add = new_channels - previous_channels
            to_remove = previous_channels - new_channels

        for channel_key in to_add:
            await self._acquire_crypto_channel(
                channel_key,
                owner_id=owner_id,
                provider=provider,
                instrument_type=resolved_instrument_type,
                symbol=normalized_symbol,
            )
        for channel_key in to_remove:
            await self._release_crypto_channel(channel_key, owner_id=owner_id)

        return registration

    async def unregister_target(self, *, owner_id: str) -> RuntimeTargetRegistration | None:
        normalized_owner_id = owner_id.strip()
        async with self._lock:
            registration = self._registrations_by_owner.pop(normalized_owner_id, None)
            kxt_to_release: set[_ChannelKey] = set()
            crypto_to_release: set[_CryptoChannelKey] = set()
            if registration is not None:
                if registration.provider == Provider.KXT:
                    kxt_to_release = {
                        _ChannelKey(registration.stream_key.symbol, registration.stream_key.market_scope, ev)
                        for ev in registration.event_types
                    }
                elif registration.provider in (Provider.CCXT, Provider.CCXT_PRO):
                    canonical = registration.canonical_symbol or build_canonical_symbol(
                        provider=registration.provider,
                        venue=Venue.BINANCE,
                        asset_class=AssetClass.CRYPTO,
                        instrument_type=registration.instrument_type or InstrumentType.SPOT,
                        display_symbol=to_unified_symbol(
                            registration.raw_symbol or registration.stream_key.symbol,
                            registration.instrument_type or InstrumentType.SPOT,
                        ).split(":", 1)[0],
                    )
                    crypto_to_release = {
                        _CryptoChannelKey(canonical_symbol=canonical, event_name=ev)
                        for ev in registration.event_types
                    }
        for channel_key in kxt_to_release:
            await self._release_channel(channel_key, owner_id=normalized_owner_id)
        for channel_key in crypto_to_release:
            await self._release_crypto_channel(channel_key, owner_id=normalized_owner_id)
        return registration

    def is_target_active(self, owner_id: str) -> bool:
        return owner_id in self._registrations_by_owner and not self._closed

    # ---- channel management --------------------------------------------

    async def _acquire_channel(self, channel_key: _ChannelKey, *, owner_id: str) -> None:
        async with self._lock:
            entry = self._channels.get(channel_key)
            if entry is not None:
                entry.owners.add(owner_id)
                already_permanent = entry.permanent_failure
            else:
                entry = _ChannelEntry(subscription=None, task=None, owners={owner_id})
                self._channels[channel_key] = entry
                already_permanent = False

        if already_permanent:
            # Surface the existing permanent failure so the new owner also
            # shows up as permanently failed in the admin UI.
            await self._dispatch_permanent_failure(
                channel_key,
                reason="unsupported_by_ksxt" if _STREAM_KIND_BY_EVENT_NAME.get(channel_key.event_name) is None else "previously_failed",
                rt_cd=None,
                msg=None,
                attempts=None,
            )
            return

        if entry.subscription is not None:
            # Channel already streaming — nothing else to do.
            return

        stream_kind = _STREAM_KIND_BY_EVENT_NAME.get(channel_key.event_name)
        if stream_kind is None:
            # Event type not supported by KSXT realtime (e.g., program_trade).
            # Mark a synthetic permanent failure so admin UI reflects reality
            # rather than leaving the target in a pending state forever.
            logger.warning(
                "event_type=%s is not supported by KSXT realtime; marking target as permanent failure",
                channel_key.event_name,
            )
            entry.permanent_failure = True
            await self._dispatch_permanent_failure(
                channel_key,
                reason="unsupported_by_ksxt",
                rt_cd=None,
                msg=f"KSXT realtime does not expose {channel_key.event_name} stream",
                attempts=0,
            )
            return

        instrument = KSXTInstrumentRef(symbol=channel_key.symbol, venue=KSXTVenue.KRX)
        try:
            sub = await self._session.subscribe(stream_kind, instrument)
        except KISSubscriptionError as exc:
            entry.permanent_failure = True
            await self._dispatch_permanent_failure(
                channel_key,
                reason=exc.reason,
                rt_cd=exc.rt_cd,
                msg=exc.msg,
                attempts=exc.attempts,
            )
            return
        except Exception as exc:
            logger.exception("subscribe failed for %s", channel_key)
            await self._dispatch_failure(channel_key, error=str(exc))
            return

        async with self._lock:
            entry.subscription = sub
            entry.task = asyncio.create_task(
                self._consume_subscription(channel_key, sub),
                name=f"ksxt-sub-{channel_key.symbol}-{channel_key.event_name}",
            )
            entry.ack_watchdog = asyncio.create_task(
                self._watch_subscription_ack(channel_key),
                name=f"ksxt-ack-watchdog-{channel_key.symbol}-{channel_key.event_name}",
            )
        logger.info(
            "KSXT subscribe sent tr_type=1 symbol=%s event=%s tr_id=%s",
            channel_key.symbol,
            channel_key.event_name,
            stream_kind.value if hasattr(stream_kind, "value") else stream_kind,
        )

    async def _release_channel(self, channel_key: _ChannelKey, *, owner_id: str) -> None:
        async with self._lock:
            entry = self._channels.get(channel_key)
            if entry is None:
                return
            entry.owners.discard(owner_id)
            if entry.owners:
                return
            # No owners remain — tear down.
            self._channels.pop(channel_key, None)
            task = entry.task
            subscription = entry.subscription
            watchdog = entry.ack_watchdog

        if watchdog is not None:
            watchdog.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await watchdog
        if subscription is not None:
            with contextlib.suppress(Exception):
                await subscription.aclose()
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _consume_subscription(self, channel_key: _ChannelKey, subscription: KSXTSubscription) -> None:
        try:
            async for event in subscription.events():
                # First event arrival implies the KIS server accepted the
                # subscription; cancel any outstanding ack watchdog.
                async with self._lock:
                    entry = self._channels.get(channel_key)
                    if entry is not None and not entry.events_seen:
                        entry.events_seen = True
                        logger.info(
                            "KSXT subscribe acked via event symbol=%s event=%s",
                            channel_key.symbol,
                            channel_key.event_name,
                        )
                try:
                    if isinstance(event, (KSXTTradeEvent, KSXTTrade)):
                        published_event_name, payload = _format_trade_event(event)
                        dto_payload = _kxt_trade_event_dto_payload(event)
                        event_name_canonical = _EVENT_TRADE
                    elif isinstance(event, (KSXTOrderBookEvent, KSXTOrderBookSnapshot)):
                        published_event_name, payload = _format_order_book_event(event)
                        dto_payload = _kxt_order_book_event_dto_payload(event)
                        event_name_canonical = _EVENT_ORDER_BOOK
                    else:
                        logger.debug("dropping unknown KSXT event type: %r", type(event))
                        continue
                    # Filter to only publish the canonical event_name a target asked for.
                    if event_name_canonical != channel_key.event_name:
                        continue
                    await self._publish_event(
                        symbol=channel_key.symbol,
                        market_scope=channel_key.market_scope,
                        event_name=published_event_name,
                        payload=payload,
                        control_plane_payload=dto_payload,
                    )
                except Exception:
                    logger.exception("failed to publish event for %s", channel_key)
        except asyncio.CancelledError:
            raise
        except KISSubscriptionError as exc:
            logger.warning(
                "KSXT subscription permanently failed: %s rt_cd=%s msg=%s attempts=%s",
                channel_key,
                exc.rt_cd,
                exc.msg,
                exc.attempts,
            )
            async with self._lock:
                entry = self._channels.get(channel_key)
                if entry is not None:
                    entry.permanent_failure = True
                    entry.subscription = None
                    watchdog = entry.ack_watchdog
                    entry.ack_watchdog = None
                else:
                    watchdog = None
            if watchdog is not None:
                watchdog.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await watchdog
            await self._dispatch_permanent_failure(
                channel_key,
                reason=exc.reason,
                rt_cd=exc.rt_cd,
                msg=exc.msg,
                attempts=exc.attempts,
            )
        except Exception as exc:
            logger.exception("subscription consumer crashed: %s", channel_key)
            await self._dispatch_failure(channel_key, error=str(exc))

    # ---- crypto channel management -------------------------------------

    def _ensure_crypto_adapter(self) -> BinanceLiveAdapter:
        if self._crypto_adapter is None:
            self._crypto_adapter = BinanceLiveAdapter()
        return self._crypto_adapter

    async def _acquire_crypto_channel(
        self,
        channel_key: _CryptoChannelKey,
        *,
        owner_id: str,
        provider: Provider,
        instrument_type: InstrumentType,
        symbol: str,
    ) -> None:
        async with self._lock:
            entry = self._crypto_channels.get(channel_key)
            if entry is not None:
                entry.owners.add(owner_id)
                already_running = entry.task is not None
            else:
                entry = _CryptoChannelEntry(
                    task=None,
                    owners={owner_id},
                    instrument_type=instrument_type,
                    symbol=symbol,
                    canonical_symbol=channel_key.canonical_symbol,
                    provider=provider,
                )
                self._crypto_channels[channel_key] = entry
                already_running = False

        if already_running:
            return

        if channel_key.event_name in (_EVENT_MARK_PRICE, _EVENT_FUNDING_RATE):
            if instrument_type != InstrumentType.PERPETUAL:
                logger.warning(
                    "crypto event_type=%s requires perpetual; skipping (instrument_type=%s)",
                    channel_key.event_name,
                    instrument_type.value,
                )
                async with self._lock:
                    self._crypto_channels.pop(channel_key, None)
                return
            await self._attach_crypto_mark_price_source(
                canonical_symbol=channel_key.canonical_symbol,
                event_name=channel_key.event_name,
                symbol=symbol,
                instrument_type=instrument_type,
                provider=provider,
            )
            return

        if channel_key.event_name == _EVENT_TRADE:
            stream_factory = self._crypto_trade_stream
        elif channel_key.event_name == _EVENT_ORDER_BOOK:
            stream_factory = self._crypto_order_book_stream
        elif channel_key.event_name == _EVENT_TICKER:
            stream_factory = self._crypto_ticker_stream
        elif channel_key.event_name == _EVENT_OHLCV:
            stream_factory = self._crypto_ohlcv_stream
        elif channel_key.event_name == _EVENT_OPEN_INTEREST:
            if instrument_type != InstrumentType.PERPETUAL:
                logger.warning(
                    "crypto event_type=%s requires perpetual; skipping (instrument_type=%s)",
                    channel_key.event_name,
                    instrument_type.value,
                )
                async with self._lock:
                    self._crypto_channels.pop(channel_key, None)
                return
            stream_factory = self._crypto_open_interest_stream
        else:
            logger.warning(
                "crypto provider does not support event_type=%s; skipping",
                channel_key.event_name,
            )
            async with self._lock:
                self._crypto_channels.pop(channel_key, None)
            return

        adapter = self._ensure_crypto_adapter()
        task = asyncio.create_task(
            self._consume_crypto_channel(
                channel_key,
                adapter=adapter,
                stream_factory=stream_factory,
                provider=provider,
                instrument_type=instrument_type,
                symbol=symbol,
            ),
            name=f"ccxt-pro-{channel_key.canonical_symbol}-{channel_key.event_name}",
        )
        async with self._lock:
            entry = self._crypto_channels.get(channel_key)
            if entry is not None:
                entry.task = task
        logger.info(
            "ccxt.pro subscribe symbol=%s instrument_type=%s event=%s canonical=%s",
            symbol,
            instrument_type.value,
            channel_key.event_name,
            channel_key.canonical_symbol,
        )

    async def _release_crypto_channel(
        self,
        channel_key: _CryptoChannelKey,
        *,
        owner_id: str,
    ) -> None:
        entry_instrument_type: InstrumentType | None = None
        async with self._lock:
            entry = self._crypto_channels.get(channel_key)
            if entry is None:
                # Might be an MP/FR channel served by the shared source
                # whose channel record was discarded because no per-channel
                # task was ever started. In that case just proceed to detach.
                entry_instrument_type = None
                task = None
            else:
                entry.owners.discard(owner_id)
                if entry.owners:
                    return
                self._crypto_channels.pop(channel_key, None)
                task = entry.task
                entry_instrument_type = entry.instrument_type
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if channel_key.event_name in (_EVENT_MARK_PRICE, _EVENT_FUNDING_RATE) and (
            entry_instrument_type is None or entry_instrument_type == InstrumentType.PERPETUAL
        ):
            await self._detach_crypto_mark_price_source(
                channel_key.canonical_symbol, channel_key.event_name
            )

    def _crypto_trade_stream(self, adapter: BinanceLiveAdapter, *, symbol: str, instrument_type: InstrumentType):
        return adapter.stream_trades(symbol=symbol, instrument_type=instrument_type)

    def _crypto_order_book_stream(self, adapter: BinanceLiveAdapter, *, symbol: str, instrument_type: InstrumentType):
        return adapter.stream_order_book_snapshots(symbol=symbol, instrument_type=instrument_type)

    def _crypto_ticker_stream(self, adapter: BinanceLiveAdapter, *, symbol: str, instrument_type: InstrumentType):
        return adapter.stream_tickers(symbol=symbol, instrument_type=instrument_type)

    def _crypto_ohlcv_stream(self, adapter: BinanceLiveAdapter, *, symbol: str, instrument_type: InstrumentType):
        return adapter.stream_ohlcv(symbol=symbol, instrument_type=instrument_type)

    def _crypto_open_interest_stream(self, adapter: BinanceLiveAdapter, *, symbol: str, instrument_type: InstrumentType):
        return adapter.poll_open_interest(symbol=symbol, instrument_type=instrument_type)

    async def _consume_crypto_channel(
        self,
        channel_key: _CryptoChannelKey,
        *,
        adapter: BinanceLiveAdapter,
        stream_factory,
        provider: Provider,
        instrument_type: InstrumentType,
        symbol: str,
    ) -> None:
        try:
            async for event in stream_factory(adapter, symbol=symbol, instrument_type=instrument_type):
                if isinstance(event, BinanceTrade):
                    published_event_name, payload = _format_crypto_trade(event)
                    control_plane_payload = _crypto_trade_control_plane_payload(event)
                    canonical_event_name = _EVENT_TRADE
                elif isinstance(event, BinanceOrderBookSnapshot):
                    published_event_name, payload = _format_crypto_order_book(event)
                    control_plane_payload = _crypto_order_book_control_plane_payload(event)
                    canonical_event_name = _EVENT_ORDER_BOOK
                elif isinstance(event, BinanceTicker):
                    published_event_name, payload = _format_crypto_ticker(event)
                    control_plane_payload = _crypto_ticker_control_plane_payload(event)
                    canonical_event_name = _EVENT_TICKER
                elif isinstance(event, BinanceBar):
                    published_event_name, payload = _format_crypto_bar(event)
                    control_plane_payload = _crypto_bar_control_plane_payload(event)
                    canonical_event_name = _EVENT_OHLCV
                elif isinstance(event, BinanceOpenInterest):
                    published_event_name, payload = _format_crypto_open_interest(event)
                    control_plane_payload = _crypto_open_interest_control_plane_payload(event)
                    canonical_event_name = _EVENT_OPEN_INTEREST
                else:
                    logger.debug("dropping unknown crypto event type: %r", type(event))
                    continue
                if canonical_event_name != channel_key.event_name:
                    continue
                if control_plane_payload.get("instrument_type") is None:
                    control_plane_payload = {**control_plane_payload, "instrument_type": instrument_type.value}
                await self._publish_event(
                    symbol=symbol,
                    market_scope="",
                    event_name=published_event_name,
                    payload=payload,
                    provider=external_provider_value(provider),
                    canonical_symbol=channel_key.canonical_symbol,
                    instrument_type=instrument_type.value,
                    raw_symbol=symbol,
                    control_plane_payload=control_plane_payload,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("crypto subscription consumer crashed: %s", channel_key)
            await self._dispatch_failure(
                _ChannelKey(symbol=symbol, market_scope="", event_name=channel_key.event_name),
                error=str(exc),
            )

    # ---- crypto shared mark-price source -------------------------------

    async def _attach_crypto_mark_price_source(
        self,
        *,
        canonical_symbol: str,
        event_name: str,
        symbol: str,
        instrument_type: InstrumentType,
        provider: Provider,
    ) -> None:
        async with self._lock:
            source = self._crypto_mark_sources.get(canonical_symbol)
            if source is None:
                source = _CryptoMarkPriceSourceEntry(
                    canonical_symbol=canonical_symbol,
                    symbol=symbol,
                    instrument_type=instrument_type,
                    provider=provider,
                    task=None,
                    active_event_names=set(),
                )
                self._crypto_mark_sources[canonical_symbol] = source
            source.active_event_names.add(event_name)
            needs_start = source.task is None
        if needs_start:
            adapter = self._ensure_crypto_adapter()
            task = asyncio.create_task(
                self._run_crypto_mark_price_source(canonical_symbol, adapter),
                name=f"ccxt-pro-mark-source-{canonical_symbol}",
            )
            async with self._lock:
                current = self._crypto_mark_sources.get(canonical_symbol)
                if current is not None and current.task is None:
                    current.task = task
                else:
                    # Source was torn down between checks; stop the fresh task.
                    task.cancel()
            logger.info(
                "ccxt.pro shared mark-price source started canonical=%s symbol=%s instrument_type=%s event=%s",
                canonical_symbol, symbol, instrument_type.value, event_name,
            )

    async def _detach_crypto_mark_price_source(self, canonical_symbol: str, event_name: str) -> None:
        task_to_cancel: asyncio.Task[None] | None = None
        async with self._lock:
            source = self._crypto_mark_sources.get(canonical_symbol)
            if source is None:
                return
            source.active_event_names.discard(event_name)
            if not source.active_event_names:
                task_to_cancel = source.task
                self._crypto_mark_sources.pop(canonical_symbol, None)
        if task_to_cancel is not None:
            task_to_cancel.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task_to_cancel

    async def _run_crypto_mark_price_source(self, canonical_symbol: str, adapter: BinanceLiveAdapter) -> None:
        async with self._lock:
            source = self._crypto_mark_sources.get(canonical_symbol)
            if source is None:
                return
            symbol = source.symbol
            instrument_type = source.instrument_type
            provider = source.provider
        try:
            async for event in adapter.stream_mark_price(symbol=symbol, instrument_type=instrument_type):
                async with self._lock:
                    source = self._crypto_mark_sources.get(canonical_symbol)
                    active = set(source.active_event_names) if source is not None else set()
                if not active:
                    continue
                if _EVENT_MARK_PRICE in active:
                    published_event_name, payload = _format_crypto_mark_price(event)
                    control_plane_payload = _crypto_mark_price_control_plane_payload(event)
                    await self._publish_event(
                        symbol=symbol,
                        market_scope="",
                        event_name=published_event_name,
                        payload=payload,
                        provider=external_provider_value(provider),
                        canonical_symbol=canonical_symbol,
                        instrument_type=instrument_type.value,
                        raw_symbol=symbol,
                        control_plane_payload=control_plane_payload,
                    )
                if _EVENT_FUNDING_RATE in active:
                    published_event_name, payload = _format_crypto_funding_rate_from_mark(event)
                    control_plane_payload = _crypto_funding_rate_from_mark_control_plane_payload(event)
                    await self._publish_event(
                        symbol=symbol,
                        market_scope="",
                        event_name=published_event_name,
                        payload=payload,
                        provider=external_provider_value(provider),
                        canonical_symbol=canonical_symbol,
                        instrument_type=instrument_type.value,
                        raw_symbol=symbol,
                        control_plane_payload=control_plane_payload,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("crypto mark-price source crashed: %s", canonical_symbol)
            await self._dispatch_failure(
                _ChannelKey(symbol=symbol, market_scope="", event_name=_EVENT_MARK_PRICE),
                error=str(exc),
            )

    # ---- session-level callbacks ---------------------------------------

    async def _watch_subscription_ack(self, channel_key: _ChannelKey) -> None:
        """Ack watchdog: defense-in-depth vs KSXT silently-swallowed ack timeout.

        KSXT ``session.subscribe`` (see ksxt/clients/kis/realtime/session.py:130-141)
        silently swallows the internal ack timeout, returning a pending
        Subscription without surfacing the failure. This watchdog gives the
        subscription ``_SUBSCRIBE_ACK_TIMEOUT`` seconds to either receive its
        first event or raise ``KISSubscriptionError``. If neither happens, we
        mark a hub-level permanent failure with ``reason='subscribe_ack_timeout'``
        so the admin UI surfaces the stuck target instead of leaving it
        silently pending forever. Tracked as KSXT-FOLLOWUP-1 / -2.
        """
        try:
            await asyncio.sleep(self._SUBSCRIBE_ACK_TIMEOUT)
        except asyncio.CancelledError:
            return

        async with self._lock:
            entry = self._channels.get(channel_key)
            if entry is None:
                return
            if entry.events_seen or entry.permanent_failure:
                return
            entry.permanent_failure = True
            subscription = entry.subscription
            entry.subscription = None

        logger.warning(
            "KSXT subscribe ack timeout — marking permanent failure symbol=%s event=%s timeout=%.1fs",
            channel_key.symbol,
            channel_key.event_name,
            self._SUBSCRIBE_ACK_TIMEOUT,
        )
        await self._dispatch_permanent_failure(
            channel_key,
            reason="subscribe_ack_timeout",
            rt_cd=None,
            msg=(
                f"no KSXT subscribe ack within {self._SUBSCRIBE_ACK_TIMEOUT:.0f}s "
                "(no event received, no KISSubscriptionError raised)"
            ),
            attempts=1,
        )
        if subscription is not None:
            with contextlib.suppress(Exception):
                await subscription.aclose()

    async def _handle_state_change(self, old: RealtimeState, new: RealtimeState) -> None:
        logger.info("KSXT session state: %s -> %s", old.value, new.value)
        if self._on_session_state_change is None:
            return
        try:
            await self._on_session_state_change(state=new.value, previous=old.value)
        except Exception:
            logger.exception("on_session_state_change handler failed")

    async def _handle_recovery(self) -> None:
        logger.info("KSXT session recovery complete")
        if self._on_recovery is None:
            return
        try:
            await self._on_recovery()
        except Exception:
            logger.exception("on_recovery handler failed")

    # ---- session readiness ---------------------------------------------

    async def _wait_session_ready(self, *, timeout: float) -> None:
        """Poll ``session.state`` until HEALTHY or timeout.

        Triggers a session start on first call by invoking ``start()`` directly
        so ``subscribe(...)`` has a connection in flight rather than racing
        against a cold-start subscribe.
        """
        with contextlib.suppress(Exception):
            await self._session.start()
        deadline = asyncio.get_running_loop().time() + timeout
        while not self._closed:
            state = self._session.state
            if state == RealtimeState.HEALTHY:
                return
            if state == RealtimeState.CLOSED:
                raise RuntimeError("KSXT session is closed")
            if asyncio.get_running_loop().time() >= deadline:
                logger.warning("KSXT session did not reach HEALTHY within %.1fs (state=%s)", timeout, state.value)
                return
            await asyncio.sleep(0.1)

    # ---- dispatch helpers ----------------------------------------------

    async def _publish_event(
        self,
        *,
        symbol: str,
        market_scope: str,
        event_name: str,
        payload: dict[str, Any],
        provider: str | None = None,
        canonical_symbol: str | None = None,
        instrument_type: str | None = None,
        raw_symbol: str | None = None,
        control_plane_payload: dict[str, Any] | None = None,
    ) -> None:
        if self._on_event is None:
            return
        try:
            await self._on_event(
                symbol=symbol,
                market_scope=market_scope,
                event_name=event_name,
                payload=payload,
                provider=provider,
                canonical_symbol=canonical_symbol,
                instrument_type=instrument_type,
                raw_symbol=raw_symbol,
                control_plane_payload=control_plane_payload,
            )
        except TypeError:
            # Handler does not know about ``control_plane_payload`` yet — retry
            # with the pre-DTO kwarg signature.
            try:
                await self._on_event(
                    symbol=symbol,
                    market_scope=market_scope,
                    event_name=event_name,
                    payload=payload,
                    provider=provider,
                    canonical_symbol=canonical_symbol,
                    instrument_type=instrument_type,
                    raw_symbol=raw_symbol,
                )
            except TypeError:
                # Legacy KXT-only handler: only (symbol, market_scope, event_name, payload).
                try:
                    await self._on_event(
                        symbol=symbol,
                        market_scope=market_scope,
                        event_name=event_name,
                        payload=payload,
                    )
                except Exception:
                    logger.exception("on_event handler failed for %s/%s", symbol, event_name)
            except Exception:
                logger.exception("on_event handler failed for %s/%s", symbol, event_name)
        except Exception:
            logger.exception("on_event handler failed for %s/%s", symbol, event_name)

    async def _dispatch_failure(self, channel_key: _ChannelKey, *, error: str) -> None:
        if self._on_failure is None:
            return
        try:
            await self._on_failure(
                symbol=channel_key.symbol,
                market_scope=channel_key.market_scope,
                error=error,
            )
        except Exception:
            logger.exception("on_failure handler failed for %s", channel_key)

    async def _dispatch_permanent_failure(
        self,
        channel_key: _ChannelKey,
        *,
        reason: str,
        rt_cd: str | None,
        msg: str | None,
        attempts: int | None,
    ) -> None:
        if self._on_permanent_failure is None:
            return
        async with self._lock:
            entry = self._channels.get(channel_key)
            owners = tuple(entry.owners) if entry is not None else ()
        try:
            await self._on_permanent_failure(
                symbol=channel_key.symbol,
                market_scope=channel_key.market_scope,
                event_name=channel_key.event_name,
                owner_ids=owners,
                reason=reason,
                rt_cd=rt_cd,
                msg=msg,
                attempts=attempts,
            )
        except Exception:
            logger.exception("on_permanent_failure handler failed for %s", channel_key)

    # ---- helpers --------------------------------------------------------

    def _build_stream_key(self, *, symbol: str, market_scope: str) -> DashboardStreamKey:
        normalized_market_scope = market_scope.strip().lower()
        if normalized_market_scope not in SUPPORTED_MARKET_SCOPES:
            raise ValueError(f"unsupported market scope: {market_scope}")
        normalized_symbol = symbol.strip()
        if not normalized_symbol:
            raise ValueError("symbol is required")
        return DashboardStreamKey(symbol=normalized_symbol, market_scope=normalized_market_scope)

    @staticmethod
    def _resolve_provider(provider: str | Provider | None) -> Provider:
        """Resolve the externally exposed provider value (kxt or ccxt).

        ``ccxt_pro`` is accepted as a deprecated alias and remapped to
        :attr:`Provider.CCXT` so the runtime never carries the
        ccxt.pro transport detail past the boundary.
        """

        if provider is None or provider == "":
            return Provider.KXT
        if isinstance(provider, Provider):
            return Provider.CCXT if provider == Provider.CCXT_PRO else provider
        text = str(provider).strip().lower()
        if text == Provider.CCXT_PRO.value:
            return Provider.CCXT
        try:
            return Provider(text)
        except ValueError as exc:
            raise ValueError(f"unsupported provider: {provider}") from exc

    @staticmethod
    def _resolve_instrument_type(
        instrument_type: str | InstrumentType | None,
        *,
        provider: Provider,
    ) -> InstrumentType | None:
        if instrument_type is None or instrument_type == "":
            if provider == Provider.KXT:
                return InstrumentType.SPOT
            if provider in (Provider.CCXT, Provider.CCXT_PRO):
                return InstrumentType.SPOT
            return None
        if isinstance(instrument_type, InstrumentType):
            resolved = instrument_type
        else:
            try:
                resolved = InstrumentType(str(instrument_type).strip().lower())
            except ValueError as exc:
                raise ValueError(f"unsupported instrument_type: {instrument_type}") from exc
        if provider == Provider.KXT and resolved == InstrumentType.EQUITY:
            return InstrumentType.SPOT
        return resolved

    def _normalize_event_types(self, event_types: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        if event_types is None:
            return ALL_EVENT_NAMES

        normalized: list[str] = []
        seen: set[str] = set()
        allowed = set(ALL_EVENT_NAMES)
        for event_type in event_types:
            candidate = str(event_type or "").strip().lower()
            if candidate not in allowed:
                raise ValueError(f"unsupported event type: {event_type}")
            if candidate in seen:
                continue
            seen.add(candidate)
            normalized.append(candidate)
        if not normalized:
            raise ValueError("at least one event type is required")
        return tuple(normalized)

    async def fetch_price_chart(self, *, symbol: str, market_scope: str, interval: int) -> dict[str, Any]:
        normalized_scope = (market_scope or "").strip().lower() or "krx"
        scope_fallback = normalized_scope != "krx"
        if scope_fallback:
            logger.warning(
                "KSXT does not differentiate market_scope=%s; requesting KRX bars",
                normalized_scope,
            )

        instrument = KSXTInstrumentRef(symbol=symbol, venue=KSXTVenue.KRX)
        end = datetime.now(_KST).replace(hour=15, minute=30, second=0, microsecond=0)
        bars = await self._client.fetch_bars(
            instrument,
            timeframe=KSXTBarTimeframe.MINUTE,
            end=end,
            interval_minutes=interval,
        )
        candles = _format_market_bars(bars)
        session_date = (
            candles[-1]["session_date"]
            if candles
            else datetime.now(_KST).date().isoformat()
        )
        return {
            "symbol": symbol,
            "market_scope": market_scope,
            "market": market_scope,
            "interval": interval,
            "candles": candles,
            "session_date": session_date,
            "source": "ksxt:KISClient.fetch_bars",
            "tr_id": "FHKST03010200",
            "scope_fallback": scope_fallback,
        }
