"""Minimal broker-agnostic ingress event contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Generic, TypeVar


TPayload = TypeVar("TPayload")


class EventType(StrEnum):
    """Canonical broker-neutral ingress event types.

    The list is intentionally provider-neutral; whether a given
    (provider, venue, instrument_type) actually exposes a particular
    event is decided by the capability matrix in the control plane.
    """

    TRADE = "trade"
    ORDER_BOOK_SNAPSHOT = "order_book_snapshot"
    PROGRAM_TRADE = "program_trade"
    TICKER = "ticker"
    OHLCV = "ohlcv"
    MARK_PRICE = "mark_price"
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"


@dataclass(frozen=True, slots=True)
class CanonicalEvent(Generic[TPayload]):
    """Transport wrapper for canonical ingress payloads."""

    event_type: EventType
    provider: str
    occurred_at: datetime
    received_at: datetime
    payload: TPayload
    schema_version: str = "v1"
    raw_payload: Any | None = None


CanonicalEventEnvelope = CanonicalEvent


@dataclass(frozen=True, slots=True)
class DashboardEventEnvelope:
    """Broker-neutral envelope for live dashboard fan-out events.

    ``market_scope`` represents the KRX request selection scope
    (``krx|nxt|total``); it is empty / ignored for non-KRX providers.
    ``provider`` is the externally exposed identifier (``kxt`` or
    ``ccxt`` only — ``ccxt_pro`` is collapsed at the boundary).
    ``raw_symbol`` is the venue-native symbol kept separate from
    ``symbol`` (which carries the unified/display symbol such as
    ``BTC/USDT``).
    """

    symbol: str
    market_scope: str
    event_name: str
    payload: dict[str, Any]
    published_at: datetime
    schema_version: str = "v1"
    provider: str | None = None
    canonical_symbol: str | None = None
    instrument_type: str | None = None
    raw_symbol: str | None = None


@dataclass(frozen=True, slots=True)
class DashboardControlEnvelope:
    """Broker-neutral envelope for dashboard publication control.

    ``market_scope`` represents the KRX request selection scope
    (``krx|nxt|total``); it is empty / ignored for non-KRX providers.
    """

    action: str
    owner_id: str
    symbol: str
    market_scope: str
    requested_at: datetime
    schema_version: str = "v1"
    provider: str | None = None
    canonical_symbol: str | None = None
    instrument_type: str | None = None
    raw_symbol: str | None = None
