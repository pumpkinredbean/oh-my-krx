"""Transport-safe shared contracts for broker-agnostic events."""

from .admin import ControlPlaneSnapshot, EventTypeCatalogEntry, SourceCapability
from .events import CanonicalEvent, CanonicalEventEnvelope, DashboardControlEnvelope, DashboardEventEnvelope, EventType
from .subscriptions import ChannelType, SubscriptionSpec
from .topics import (
    CANONICAL_EVENTS_TOPIC,
    CONTROL_PLANE_EVENTS_TOPIC,
    DASHBOARD_CONTROL_TOPIC,
    DASHBOARD_EVENTS_TOPIC,
    ORDER_BOOK_SNAPSHOT_TOPIC,
    PROGRAM_TRADE_TOPIC,
    RAW_EVENTS_TOPIC,
    TRADE_TOPIC,
)

__all__ = [
    "CANONICAL_EVENTS_TOPIC",
    "ChannelType",
    "CanonicalEvent",
    "CanonicalEventEnvelope",
    "CONTROL_PLANE_EVENTS_TOPIC",
    "ControlPlaneSnapshot",
    "DASHBOARD_CONTROL_TOPIC",
    "DashboardControlEnvelope",
    "DASHBOARD_EVENTS_TOPIC",
    "DashboardEventEnvelope",
    "EventTypeCatalogEntry",
    "EventType",
    "ORDER_BOOK_SNAPSHOT_TOPIC",
    "PROGRAM_TRADE_TOPIC",
    "RAW_EVENTS_TOPIC",
    "SourceCapability",
    "SubscriptionSpec",
    "TRADE_TOPIC",
]
