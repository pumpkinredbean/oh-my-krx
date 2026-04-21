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


# ─── Admin Charts + Indicator Runtime (step 1) ─────────────────────────────


@dataclass(frozen=True, slots=True)
class SeriesPoint:
    """Single series datum emitted by an indicator.

    ``timestamp`` is an ISO-8601 UTC string (no timezone suffix required;
    the receiver interprets it as UTC).  ``value`` is a plain number.
    ``meta`` is an optional dict for per-point annotations (e.g. the
    top-N used for an OBI computation); it must be JSON-serialisable.
    """

    timestamp: str
    value: float
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ChartPanelSpec:
    """Admin-UI persisted panel descriptor.

    Layout fields (``x``, ``y``, ``w``, ``h``) are integers in a 12-column
    grid, matching ``react-grid-layout`` semantics.  ``chart_type`` is
    ``"line"`` or ``"candle"``.  ``source`` selects the input stream
    (``"raw_event"`` or ``"indicator_output"``).  ``series_ref`` is a
    lightweight opaque string used by the frontend to bind the panel to
    a specific series (event_name for raw, or indicator_instance_id for
    indicator output).
    """

    panel_id: str
    chart_type: str
    symbol: str
    source: str
    series_ref: str
    x: int = 0
    y: int = 0
    w: int = 6
    h: int = 6
    title: str | None = None
    notes: str | None = None


@dataclass(frozen=True, slots=True)
class IndicatorScriptSpec:
    """Persisted user-authored indicator script.

    ``source`` is raw Python text that defines exactly one subclass of
    ``HubIndicator``.  ``class_name`` records which top-level class the
    runtime should instantiate.  ``builtin`` is true for indicators
    shipped with the hub (e.g. ``obi``) whose source is pinned and
    whose validation step is skipped.
    """

    script_id: str
    name: str
    source: str
    class_name: str
    builtin: bool = False
    description: str | None = None


@dataclass(frozen=True, slots=True)
class IndicatorInstanceSpec:
    """Runtime activation record for one indicator on one symbol."""

    instance_id: str
    script_id: str
    symbol: str
    market_scope: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class IndicatorOutputEnvelope:
    """Fan-out envelope emitted by the indicator runtime."""

    instance_id: str
    script_id: str
    name: str
    symbol: str
    market_scope: str
    output_kind: str
    published_at: datetime
    point: SeriesPoint
    schema_version: str = "v1"
