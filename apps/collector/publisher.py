"""Collector-side dashboard publisher helpers."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from packages.contracts.events import DashboardControlEnvelope, DashboardEventEnvelope
from packages.contracts.topics import DASHBOARD_CONTROL_TOPIC, DASHBOARD_EVENTS_TOPIC

def _to_transport_value(value: Any) -> Any:
    if is_dataclass(value):
        return {key: _to_transport_value(item) for key, item in asdict(value).items()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_to_transport_value(item) for item in value]
    if isinstance(value, list):
        return [_to_transport_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_transport_value(item) for key, item in value.items()}
    return value


class CollectorPublisher:
    """Publish collector dashboard events to the broker."""

    def __init__(self, broker: Any):
        self._broker = broker

    async def publish_dashboard_control(
        self,
        *,
        action: str,
        owner_id: str,
        symbol: str,
        market_scope: str,
    ) -> dict[str, Any]:
        message = _to_transport_value(
            DashboardControlEnvelope(
                action=action,
                owner_id=owner_id,
                symbol=symbol,
                market_scope=market_scope.lower(),
                requested_at=datetime.utcnow(),
            )
        )
        message["market"] = message["market_scope"]
        await self._broker.publish(topic=DASHBOARD_CONTROL_TOPIC, value=message, key=owner_id)
        return message

    async def publish_dashboard_event(
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
    ) -> dict[str, Any]:
        message = _to_transport_value(
            DashboardEventEnvelope(
                symbol=symbol,
                market_scope=market_scope.lower(),
                event_name=event_name,
                payload=payload,
                published_at=datetime.utcnow(),
                provider=provider,
                canonical_symbol=canonical_symbol,
                instrument_type=instrument_type,
                raw_symbol=raw_symbol,
            )
        )
        message["market"] = message["market_scope"]
        await self._broker.publish(topic=DASHBOARD_EVENTS_TOPIC, value=message, key=f"{market_scope.lower()}:{symbol}")
        return message
