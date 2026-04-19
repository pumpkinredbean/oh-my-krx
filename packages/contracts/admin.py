"""Shared admin/control-plane contracts for catalog, targeting, and runtime views."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from packages.domain.models import (
    CollectionTarget,
    CollectionTargetStatus,
    InstrumentSearchResult,
    RuntimeStatus,
    StorageBinding,
)

from .events import EventType


@dataclass(frozen=True, slots=True)
class EventTypeCatalogEntry:
    """Catalog entry describing one supported canonical event type."""

    event_type: EventType
    topic_name: str
    description: str
    owner_service: str = "collector"


@dataclass(frozen=True, slots=True)
class SourceCapability:
    """Capability descriptor for one (provider, venue, asset_class, instrument_type)
    source advertised to admin UI / control-plane consumers.

    ``label`` is a UI-friendly preset name (e.g. ``"KRX Equity (KXT)"``,
    ``"Binance Spot (CCXT)"``).  ``supported_event_types`` is the union
    of canonical event types this source can stream — admin UI uses this
    to filter the event-type checkboxes per selected source.
    """

    provider: str
    venue: str
    asset_class: str
    instrument_type: str
    label: str
    supported_event_types: tuple[str, ...]
    market_scope_required: bool = False


@dataclass(frozen=True, slots=True)
class SourceRuntimeStatus:
    """Provider-level logical runtime status row.

    Surfaces ``kxt``/``ccxt`` as first-class logical runtime units in the
    admin/control-plane snapshot.  The Docker container lifecycle is
    tracked separately — this row describes whether the provider's
    publication side is currently enabled and how many of its targets
    are actively streaming.
    """

    provider: str
    state: str
    enabled: bool
    active_target_count: int
    last_error: str | None = None
    observed_at: datetime | None = None
    schema_version: str = "v1"


@dataclass(frozen=True, slots=True)
class ControlPlaneSnapshot:
    """API- or Kafka-ready snapshot of the first admin/control-plane surface."""

    captured_at: datetime
    source_service: str
    event_type_catalog: tuple[EventTypeCatalogEntry, ...] = ()
    instrument_results: tuple[InstrumentSearchResult, ...] = ()
    collection_targets: tuple[CollectionTarget, ...] = ()
    storage_bindings: tuple[StorageBinding, ...] = ()
    runtime_status: tuple[RuntimeStatus, ...] = ()
    collection_target_status: tuple[CollectionTargetStatus, ...] = ()
    # Capability matrix per (provider, venue, asset_class, instrument_type).
    # Replaces the previous global-static event-type catalog as the source
    # of truth for "what events can I subscribe to for this source?".
    source_capabilities: tuple[SourceCapability, ...] = ()
    # KSXT realtime session-level state (IDLE/CONNECTING/HEALTHY/DEGRADED/CLOSED).
    # Surfaces a separate admin-UI banner from the collector_offline state.
    session_state: str | None = None
    # Provider-level logical runtime rows (kxt, ccxt).  Additive; does
    # not replace ``runtime_status``.
    source_runtime_status: tuple[SourceRuntimeStatus, ...] = ()
    schema_version: str = "v1"


@dataclass(frozen=True, slots=True)
class RecentRuntimeEvent:
    """Bounded operator-facing event sample from current runtime activity.

    ``provider``/``canonical_symbol``/``instrument_type``/``raw_symbol``
    are additive multiprovider fields.  Provider values are externally
    normalised (``kxt`` or ``ccxt``); ``ccxt_pro`` is collapsed at the
    boundary.  ``raw_symbol`` is the venue-native identifier kept
    separate from the unified/display ``symbol``.
    """

    event_id: str
    topic_name: str
    event_name: str
    symbol: str
    market_scope: str
    published_at: datetime
    matched_target_ids: tuple[str, ...] = ()
    payload: dict[str, Any] | None = None
    schema_version: str = "v1"
    provider: str | None = None
    canonical_symbol: str | None = None
    instrument_type: str | None = None
    raw_symbol: str | None = None
