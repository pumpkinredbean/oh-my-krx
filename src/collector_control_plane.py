from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

from packages.contracts.admin import (
    ControlPlaneSnapshot,
    EventTypeCatalogEntry,
    RecentRuntimeEvent,
    SourceCapability,
    SourceRuntimeStatus,
)
from packages.contracts.events import EventType
from packages.contracts.topics import DASHBOARD_EVENTS_TOPIC
from packages.domain.enums import AssetClass, InstrumentType, Provider, RuntimeState, Venue, external_provider_value
from packages.domain.models import (
    CollectionTarget,
    CollectionTargetStatus,
    InstrumentRef,
    InstrumentSearchResult,
    RuntimeStatus,
    StorageBinding,
    build_canonical_symbol,
)


# KRX-only market selection scope.  Non-KXT providers pass ``""`` (empty)
# which means "not applicable".
SUPPORTED_MARKET_SCOPES = {"krx", "nxt", "total"}


# ---------------------------------------------------------------------------
# Capability matrix — per (provider, venue, instrument_type)
# ---------------------------------------------------------------------------
#
# This replaces the previous global static event-type catalog.  Each
# entry advertises which canonical events the source can stream and is
# the single source of truth for admin-UI event selection + control-plane
# validation.  Provider keys are the externally exposed values (kxt /
# ccxt) — ccxt_pro is collapsed at the boundary.

CapabilityKey = tuple[str, str, str]  # (provider_external, venue, instrument_type)

SOURCE_CAPABILITIES: tuple[SourceCapability, ...] = (
    SourceCapability(
        provider="kxt",
        venue="krx",
        asset_class="equity",
        instrument_type="spot",
        label="KRX Equity (KXT)",
        supported_event_types=(
            EventType.TRADE.value,
            EventType.ORDER_BOOK_SNAPSHOT.value,
            EventType.PROGRAM_TRADE.value,
        ),
        market_scope_required=True,
    ),
    SourceCapability(
        provider="ccxt",
        venue="binance",
        asset_class="crypto",
        instrument_type="spot",
        label="Binance Spot (CCXT)",
        supported_event_types=(
            EventType.TRADE.value,
            EventType.ORDER_BOOK_SNAPSHOT.value,
            EventType.TICKER.value,
            EventType.OHLCV.value,
        ),
        market_scope_required=False,
    ),
    SourceCapability(
        provider="ccxt",
        venue="binance",
        asset_class="crypto",
        instrument_type="perpetual",
        label="Binance USDT Perpetual (CCXT)",
        supported_event_types=(
            EventType.TRADE.value,
            EventType.ORDER_BOOK_SNAPSHOT.value,
            EventType.TICKER.value,
            EventType.OHLCV.value,
            EventType.MARK_PRICE.value,
            EventType.FUNDING_RATE.value,
            EventType.OPEN_INTEREST.value,
        ),
        market_scope_required=False,
    ),
)


def capability_for(
    *,
    provider: Provider | str,
    venue: Venue | str,
    instrument_type: InstrumentType | str,
) -> SourceCapability | None:
    provider_value = external_provider_value(provider)
    venue_value = venue.value if isinstance(venue, Venue) else str(venue or "").strip().lower()
    instrument_value = (
        instrument_type.value if isinstance(instrument_type, InstrumentType)
        else str(instrument_type or "").strip().lower()
    )
    for entry in SOURCE_CAPABILITIES:
        if (
            entry.provider == provider_value
            and entry.venue == venue_value
            and entry.instrument_type == instrument_value
        ):
            return entry
    return None


@dataclass(frozen=True, slots=True)
class PermanentFailureMeta:
    """Captures KSXT KISSubscriptionError metadata for admin UI display."""

    reason: str
    rt_cd: str | None = None
    msg: str | None = None
    attempts: int | None = None

EVENT_TYPE_ALIASES: dict[str, str] = {
    "trade": "trade",
    "trade_price": "trade",
    "order_book_snapshot": "order_book_snapshot",
    "order_book": "order_book_snapshot",
    "orderbook": "order_book_snapshot",
    "program_trade": "program_trade",
    "ticker": "ticker",
    "ohlcv": "ohlcv",
    "candle": "ohlcv",
    "mark_price": "mark_price",
    "funding_rate": "funding_rate",
    "open_interest": "open_interest",
}

EVENT_TYPE_DESCRIPTIONS: dict[EventType, str] = {
    EventType.TRADE: "실시간 체결 이벤트",
    EventType.ORDER_BOOK_SNAPSHOT: "실시간 호가 스냅샷 이벤트",
    EventType.PROGRAM_TRADE: "프로그램 매매 집계 이벤트 (KRX 전용)",
    EventType.TICKER: "심볼 단위 시세 요약 (best bid/ask + last)",
    EventType.OHLCV: "OHLCV 캔들/바 이벤트",
    EventType.MARK_PRICE: "선물/무기한 마크 프라이스",
    EventType.FUNDING_RATE: "무기한 펀딩비",
    EventType.OPEN_INTEREST: "선물/무기한 미결제약정",
}

BOOTSTRAP_INSTRUMENTS: tuple[tuple[str, str], ...] = (
    ("005930", "삼성전자"),
    ("000660", "SK하이닉스"),
    ("035420", "NAVER"),
    ("005380", "현대차"),
    ("035720", "카카오"),
)


class CollectorControlPlaneService:
    """In-memory collector-owned admin/control-plane state for the first slice."""

    def __init__(
        self,
        *,
        service_name: str,
        default_symbol: str,
        default_market_scope: str,
        start_publication: Callable[..., Awaitable[dict[str, object]]],
        stop_publication: Callable[..., Awaitable[dict[str, object]]],
        is_publication_active: Callable[[str], bool],
    ):
        self._service_name = service_name
        self._default_symbol = default_symbol.strip() or "005930"
        self._default_market_scope = self._normalize_market_scope(default_market_scope or "krx")
        self._start_publication = start_publication
        self._stop_publication = stop_publication
        self._is_publication_active = is_publication_active
        self._lock = asyncio.Lock()
        self._targets: dict[str, CollectionTarget] = {}
        self._target_errors: dict[str, str | None] = {}
        self._target_permanent_failures: dict[str, PermanentFailureMeta] = {}
        self._last_search_results: tuple[InstrumentSearchResult, ...] = ()
        self._service_state = RuntimeState.STARTING
        self._last_service_error: str | None = None
        self._last_event_at_by_target: dict[str, datetime] = {}
        self._recent_events: deque[RecentRuntimeEvent] = deque(maxlen=250)
        self._event_subscribers: list[asyncio.Queue[RecentRuntimeEvent]] = []
        # Meta subscribers receive SSE-ready tuples (event_type, payload) for
        # session-level signals (session_recovered, session_state_changed).
        self._meta_subscribers: list[asyncio.Queue[tuple[str, dict[str, Any]]]] = []
        self._session_state: str | None = None
        # Provider-level logical runtime control.  Default both first-class
        # external providers to enabled so current KXT + CCXT behavior is
        # preserved.  Disabling a provider releases its current
        # subscriptions via ``_stop_publication`` while keeping the target
        # rows (and their ``enabled`` attribute) intact so re-enabling can
        # restart them.
        self._provider_enabled: dict[str, bool] = {"kxt": True, "ccxt": True}
        self._provider_errors: dict[str, str | None] = {"kxt": None, "ccxt": None}

    async def mark_running(self) -> None:
        async with self._lock:
            self._service_state = RuntimeState.RUNNING
            self._last_service_error = None

    async def mark_stopping(self) -> None:
        async with self._lock:
            self._service_state = RuntimeState.STOPPING

    async def mark_stopped(self) -> None:
        async with self._lock:
            self._service_state = RuntimeState.STOPPED

    async def set_provider_enabled(self, provider: str, enabled: bool) -> None:
        """Enable or disable a provider at the logical runtime layer.

        Disabling releases the provider's current publications via
        ``_stop_publication`` but keeps the target rows (and their
        ``enabled`` attribute) intact.  Enabling iterates the stored
        targets and re-invokes ``_start_publication`` for each target
        whose ``target.enabled`` is True and whose provider matches.

        Idempotent: calling with the same state is a no-op (aside from
        clearing any prior recorded provider-level error).  Per-target
        failures are swallowed and recorded into ``_provider_errors``.
        """

        provider_key = external_provider_value(provider)
        if provider_key not in self._provider_enabled:
            raise ValueError(f"unsupported provider: {provider}")

        async with self._lock:
            previous = self._provider_enabled.get(provider_key, True)
            self._provider_enabled[provider_key] = enabled
            if previous == enabled:
                # Idempotent fast path — still clear any stale error.
                self._provider_errors[provider_key] = None
                return
            # Snapshot the matching targets under the lock.
            matching_targets = [
                target
                for target in self._targets.values()
                if external_provider_value(target.provider) == provider_key
            ]
            self._provider_errors[provider_key] = None

        last_error: str | None = None
        if not enabled:
            for target in matching_targets:
                try:
                    await self._stop_publication(subscription_id=target.target_id)
                except Exception as exc:  # noqa: BLE001 — record & continue
                    last_error = str(exc)
        else:
            for target in matching_targets:
                if not target.enabled:
                    continue
                try:
                    try:
                        await self._start_publication(
                            symbol=target.instrument.symbol,
                            market_scope=target.market_scope,
                            owner_id=target.target_id,
                            event_types=target.event_types,
                            provider=external_provider_value(target.provider),
                            instrument_type=(
                                target.instrument.instrument_type.value
                                if target.instrument.instrument_type is not None
                                else None
                            ),
                            canonical_symbol=target.canonical_symbol,
                            raw_symbol=target.instrument.raw_symbol,
                        )
                    except TypeError:
                        await self._start_publication(
                            symbol=target.instrument.symbol,
                            market_scope=target.market_scope,
                            owner_id=target.target_id,
                            event_types=target.event_types,
                        )
                except Exception as exc:  # noqa: BLE001 — record & continue
                    last_error = str(exc)

        if last_error is not None:
            async with self._lock:
                self._provider_errors[provider_key] = last_error

    async def snapshot(self) -> ControlPlaneSnapshot:
        async with self._lock:
            targets = tuple(self._targets.values())
            search_results = self._last_search_results
            target_errors = dict(self._target_errors)
            permanent_failures = dict(self._target_permanent_failures)
            service_state = self._service_state
            service_error = self._last_service_error
            last_event_at_by_target = dict(self._last_event_at_by_target)
            session_state = self._session_state
            provider_enabled = dict(self._provider_enabled)
            provider_errors = dict(self._provider_errors)

        statuses = tuple(
            self._build_target_status(
                target,
                target_errors.get(target.target_id),
                last_event_at=last_event_at_by_target.get(target.target_id),
                permanent=permanent_failures.get(target.target_id),
            )
            for target in targets
        )
        active_target_ids = tuple(target.target_id for target in targets if self._is_publication_active(target.target_id))
        degraded_error = next((status.last_error for status in statuses if status.last_error), None)
        runtime_state = RuntimeState.DEGRADED if degraded_error else service_state
        runtime_error = degraded_error or service_error

        observed_at = datetime.utcnow()
        source_runtime_rows: list[SourceRuntimeStatus] = []
        for provider_name in ("kxt", "ccxt"):
            provider_targets = [
                target
                for target in targets
                if external_provider_value(target.provider) == provider_name
            ]
            active_count = sum(
                1 for target in provider_targets if self._is_publication_active(target.target_id)
            )
            enabled = provider_enabled.get(provider_name, True)
            if active_count > 0:
                state = "running"
            elif not enabled:
                state = "disabled"
            else:
                state = "stopped"
            source_runtime_rows.append(
                SourceRuntimeStatus(
                    provider=provider_name,
                    state=state,
                    enabled=enabled,
                    active_target_count=active_count,
                    last_error=provider_errors.get(provider_name),
                    observed_at=observed_at,
                )
            )

        return ControlPlaneSnapshot(
            captured_at=datetime.utcnow(),
            source_service=self._service_name,
            event_type_catalog=self._event_type_catalog(),
            instrument_results=search_results,
            collection_targets=targets,
            storage_bindings=self._storage_bindings(),
            runtime_status=(
                RuntimeStatus(
                    component="collector-control-plane",
                    state=runtime_state,
                    observed_at=datetime.utcnow(),
                    active_collection_target_ids=active_target_ids,
                    active_storage_binding_ids=(),
                    last_error=runtime_error,
                ),
            ),
            collection_target_status=statuses,
            source_capabilities=SOURCE_CAPABILITIES,
            session_state=session_state,
            source_runtime_status=tuple(source_runtime_rows),
        )

    async def search_instruments(
        self,
        *,
        query: str,
        market_scope: str | None = None,
        limit: int = 10,
        provider: str | Provider | None = None,
        instrument_type: str | InstrumentType | None = None,
    ) -> tuple[InstrumentSearchResult, ...]:
        normalized_query = query.strip()
        resolved_provider = self._normalize_provider(provider)
        resolved_instrument_type = self._normalize_instrument_type(
            instrument_type,
            provider=resolved_provider,
        )
        resolved_market_scope = self._normalize_market_scope(
            market_scope if market_scope is not None else self._default_market_scope,
            provider=resolved_provider,
        )
        async with self._lock:
            target_symbols = tuple(target.instrument.symbol for target in self._targets.values())
        results = self._search_catalog(
            normalized_query,
            resolved_market_scope,
            target_symbols=target_symbols,
            limit=max(1, min(limit, 50)),
            provider=resolved_provider,
            instrument_type=resolved_instrument_type,
        )
        async with self._lock:
            self._last_search_results = results
        return results

    async def upsert_target(
        self,
        *,
        target_id: str | None,
        symbol: str,
        market_scope: str,
        event_types: list[str] | tuple[str, ...],
        enabled: bool,
        provider: str | Provider | None = None,
        instrument_type: str | InstrumentType | None = None,
        raw_symbol: str | None = None,
    ) -> dict[str, object]:
        normalized_symbol = symbol.strip()
        if not normalized_symbol:
            raise ValueError("symbol is required")

        resolved_provider = self._normalize_provider(provider)
        resolved_instrument_type = self._normalize_instrument_type(
            instrument_type,
            provider=resolved_provider,
        )
        resolved_market_scope = self._normalize_market_scope(market_scope, provider=resolved_provider)
        normalized_event_types = self._normalize_event_types(
            event_types,
            provider=resolved_provider,
            instrument_type=resolved_instrument_type,
        )
        requested_target_id = (target_id or "").strip()

        async with self._lock:
            existing_target_ids = [
                existing.target_id
                for existing in self._targets.values()
                if existing.instrument.symbol == normalized_symbol
                and existing.market_scope == resolved_market_scope
                and (existing.provider or Provider.KXT) == resolved_provider
                and (existing.instrument.instrument_type or InstrumentType.SPOT)
                == (resolved_instrument_type or InstrumentType.SPOT)
            ]

        resolved_target_id = requested_target_id or (existing_target_ids[0] if existing_target_ids else uuid.uuid4().hex)
        instrument_ref = self._build_instrument_ref(
            normalized_symbol,
            provider=resolved_provider,
            instrument_type=resolved_instrument_type,
            raw_symbol=raw_symbol,
        )
        target = CollectionTarget(
            target_id=resolved_target_id,
            instrument=instrument_ref,
            market_scope=resolved_market_scope,
            event_types=normalized_event_types,
            enabled=enabled,
            provider=resolved_provider,
            canonical_symbol=instrument_ref.canonical_symbol,
        )

        duplicate_target_ids = [existing_target_id for existing_target_id in existing_target_ids if existing_target_id != resolved_target_id]

        async with self._lock:
            self._targets[resolved_target_id] = target
            self._target_errors[resolved_target_id] = None
            self._target_permanent_failures.pop(resolved_target_id, None)
            for duplicate_target_id in duplicate_target_ids:
                self._targets.pop(duplicate_target_id, None)
                self._target_errors.pop(duplicate_target_id, None)
                self._target_permanent_failures.pop(duplicate_target_id, None)

        for duplicate_target_id in duplicate_target_ids:
            with contextlib.suppress(Exception):
                await self._stop_publication(subscription_id=duplicate_target_id)

        apply_error: str | None = None
        provider_disabled_skip = False
        if enabled:
            # Provider-level gating: when the provider is logically disabled,
            # persist the target with its enabled flag but do not start
            # publication.  Re-enabling the provider will restart it.
            if not self._provider_enabled.get(external_provider_value(resolved_provider), True):
                provider_disabled_skip = True
            else:
                try:
                    await self._start_publication(
                        symbol=normalized_symbol,
                        market_scope=resolved_market_scope,
                        owner_id=resolved_target_id,
                        event_types=normalized_event_types,
                        provider=external_provider_value(resolved_provider),
                        instrument_type=resolved_instrument_type.value if resolved_instrument_type else None,
                        canonical_symbol=instrument_ref.canonical_symbol,
                        raw_symbol=instrument_ref.raw_symbol,
                    )
                except TypeError:
                    # Backwards compatibility with start_publication callbacks that
                    # do not yet accept provider-aware kwargs (e.g. older tests).
                    await self._start_publication(
                        symbol=normalized_symbol,
                        market_scope=resolved_market_scope,
                        owner_id=resolved_target_id,
                        event_types=normalized_event_types,
                    )
                except Exception as exc:
                    apply_error = str(exc)
        else:
            try:
                await self._stop_publication(subscription_id=resolved_target_id)
            except Exception as exc:
                apply_error = str(exc)

        async with self._lock:
            self._target_errors[resolved_target_id] = apply_error
            self._last_service_error = apply_error

        return {
            "target": target,
            "status": self._build_target_status(target, apply_error),
            "applied": apply_error is None,
            "warning": apply_error,
            "deduplicated_target_ids": tuple(duplicate_target_ids),
            "provider_disabled": provider_disabled_skip,
        }

    async def delete_target(self, *, target_id: str) -> dict[str, object]:
        resolved_target_id = target_id.strip()
        async with self._lock:
            target = self._targets.pop(resolved_target_id, None)
            self._target_errors.pop(resolved_target_id, None)
            self._last_event_at_by_target.pop(resolved_target_id, None)
            self._target_permanent_failures.pop(resolved_target_id, None)

        if target is None:
            return {"target_id": resolved_target_id, "status": "not_found"}

        warning: str | None = None
        try:
            await self._stop_publication(subscription_id=resolved_target_id)
        except Exception as exc:
            warning = str(exc)

        return {
            "target_id": resolved_target_id,
            "status": "deleted",
            "warning": warning,
            "removed_target": target,
        }

    async def record_runtime_event(
        self,
        *,
        symbol: str,
        market_scope: str,
        event_name: str,
        payload: dict[str, Any],
        topic_name: str = DASHBOARD_EVENTS_TOPIC,
        provider: str | Provider | None = None,
        canonical_symbol: str | None = None,
        instrument_type: str | None = None,
        raw_symbol: str | None = None,
    ) -> None:
        published_at = datetime.utcnow()
        normalized_symbol = symbol.strip()
        resolved_provider = self._normalize_provider(provider)
        external_provider = external_provider_value(resolved_provider)
        normalized_market_scope = self._normalize_market_scope(market_scope, provider=resolved_provider)
        normalized_event_name = self._normalize_event_name(event_name)

        async with self._lock:
            matched_target_ids = tuple(
                target.target_id
                for target in self._targets.values()
                if target.enabled
                and self._target_matches_event(
                    target,
                    symbol=normalized_symbol,
                    market_scope=normalized_market_scope,
                    event_name=normalized_event_name,
                    provider=resolved_provider,
                    canonical_symbol=canonical_symbol,
                )
            )
            for target_id in matched_target_ids:
                self._last_event_at_by_target[target_id] = published_at
                self._target_errors[target_id] = None
            if matched_target_ids:
                self._last_service_error = None
            event = RecentRuntimeEvent(
                event_id=uuid.uuid4().hex,
                topic_name=topic_name,
                event_name=normalized_event_name,
                symbol=normalized_symbol,
                market_scope=normalized_market_scope,
                published_at=published_at,
                matched_target_ids=matched_target_ids,
                payload=payload,
                provider=external_provider,
                canonical_symbol=canonical_symbol,
                instrument_type=instrument_type,
                raw_symbol=raw_symbol,
            )
            self._recent_events.appendleft(event)
            subscribers = list(self._event_subscribers)

        # Notify subscribers outside the lock so no subscriber can block record_runtime_event.
        for queue in subscribers:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    @asynccontextmanager
    async def subscribe_events(self) -> AsyncIterator[asyncio.Queue[RecentRuntimeEvent]]:
        """Async context manager yielding a queue that receives every new RecentRuntimeEvent.

        Events are delivered immediately when record_runtime_event() is called.
        The queue is bounded (maxsize=500); events are silently dropped for slow consumers.
        """
        queue: asyncio.Queue[RecentRuntimeEvent] = asyncio.Queue(maxsize=500)
        async with self._lock:
            self._event_subscribers.append(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                with contextlib.suppress(ValueError):
                    self._event_subscribers.remove(queue)

    async def record_publication_failure(self, *, symbol: str, market_scope: str, error: str) -> None:
        normalized_symbol = symbol.strip()
        normalized_market_scope = self._normalize_market_scope(market_scope)

        async with self._lock:
            matched_target_ids = tuple(
                target.target_id
                for target in self._targets.values()
                if target.instrument.symbol == normalized_symbol and target.market_scope == normalized_market_scope
            )
            for target_id in matched_target_ids:
                self._target_errors[target_id] = error
            self._last_service_error = error

    async def clear_publication_errors(self, *, symbol: str, market_scope: str) -> None:
        """Clear per-target error state for a symbol/market_scope pair.

        Called when a new upstream session is established so stale errors set by a
        previous session failure are removed before fresh events start arriving.
        Only clears errors for targets that currently have one; targets without an
        error are left untouched.
        """
        normalized_symbol = symbol.strip()
        normalized_market_scope = self._normalize_market_scope(market_scope)

        async with self._lock:
            matched_target_ids = tuple(
                target.target_id
                for target in self._targets.values()
                if target.instrument.symbol == normalized_symbol and target.market_scope == normalized_market_scope
            )
            cleared_any = False
            for target_id in matched_target_ids:
                if self._target_errors.get(target_id):
                    self._target_errors[target_id] = None
                    cleared_any = True
            if cleared_any:
                self._last_service_error = None

    async def clear_all_publication_errors(self) -> int:
        """Clear transient per-target error state across the board.

        Called when the KSXT session reports ``on_recovery`` so stale errors
        set by a previous cycle failure are cleared before fresh events start
        arriving.  Permanent failures are intentionally left untouched.

        Returns the number of targets whose transient error was actually
        cleared so recovery paths can be observed in logs.
        """
        async with self._lock:
            cleared = 0
            for target_id in list(self._target_errors.keys()):
                if self._target_errors.get(target_id):
                    self._target_errors[target_id] = None
                    cleared += 1
            self._last_service_error = None
        logger.info(
            "control_plane.clear_all_publication_errors cleared=%d",
            cleared,
        )
        return cleared

    async def mark_target_permanent_failure(self, *, target_id: str, reason: str, rt_cd: str | None, msg: str | None, attempts: int | None) -> None:
        resolved = target_id.strip()
        async with self._lock:
            meta = PermanentFailureMeta(reason=reason, rt_cd=rt_cd, msg=msg, attempts=attempts)
            self._target_permanent_failures[resolved] = meta
            # Mirror into last_error so existing panels still show something.
            self._target_errors[resolved] = f"permanent_failure:{reason}"

    async def clear_target_permanent_failure(self, *, target_id: str) -> None:
        resolved = target_id.strip()
        async with self._lock:
            self._target_permanent_failures.pop(resolved, None)
            if self._target_errors.get(resolved, "").startswith("permanent_failure:"):
                self._target_errors[resolved] = None

    async def mark_session_state(self, *, state: str | None) -> None:
        async with self._lock:
            self._session_state = state
            subscribers = list(self._meta_subscribers)
        payload = {"state": state, "observed_at": datetime.utcnow().isoformat()}
        delivered = 0
        dropped = 0
        for queue in subscribers:
            try:
                queue.put_nowait(("session_state_changed", payload))
                delivered += 1
            except asyncio.QueueFull:
                dropped += 1
        logger.info(
            "control_plane.mark_session_state state=%s subscribers=%d delivered=%d dropped=%d",
            state,
            len(subscribers),
            delivered,
            dropped,
        )

    async def broadcast_session_recovered(self) -> None:
        async with self._lock:
            subscribers = list(self._meta_subscribers)
        payload = {"observed_at": datetime.utcnow().isoformat()}
        delivered = 0
        dropped = 0
        for queue in subscribers:
            try:
                queue.put_nowait(("session_recovered", payload))
                delivered += 1
            except asyncio.QueueFull:
                dropped += 1
        logger.info(
            "control_plane.broadcast_session_recovered subscribers=%d delivered=%d dropped=%d",
            len(subscribers),
            delivered,
            dropped,
        )

    @asynccontextmanager
    async def subscribe_meta_events(self) -> AsyncIterator[asyncio.Queue[tuple[str, dict[str, Any]]]]:
        """Subscribe to session-level meta events (session_recovered, session_state_changed)."""
        queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=100)
        async with self._lock:
            self._meta_subscribers.append(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                with contextlib.suppress(ValueError):
                    self._meta_subscribers.remove(queue)

    async def recent_events(
        self,
        *,
        target_id: str | None = None,
        symbol: str | None = None,
        market_scope: str | None = None,
        event_name: str | None = None,
        limit: int = 50,
    ) -> dict[str, object]:
        requested_target_id = (target_id or "").strip()
        requested_symbol = (symbol or "").strip()
        requested_market_scope = self._normalize_market_scope(market_scope) if market_scope else None
        requested_event_name = self._normalize_event_name(event_name) if event_name else ""
        resolved_limit = max(1, min(limit, 200))

        async with self._lock:
            events = tuple(self._recent_events)
            targets = tuple(self._targets.values())

        filtered_events: list[RecentRuntimeEvent] = []
        for event in events:
            if requested_target_id and requested_target_id not in event.matched_target_ids:
                continue
            if requested_symbol and event.symbol != requested_symbol:
                continue
            if requested_market_scope and event.market_scope != requested_market_scope:
                continue
            if requested_event_name and event.event_name != requested_event_name:
                continue
            filtered_events.append(event)
            if len(filtered_events) >= resolved_limit:
                break

        filtered_target_ids = {target_id for event in filtered_events for target_id in event.matched_target_ids}
        target_options = tuple(
            {
                "target_id": target.target_id,
                "symbol": target.instrument.symbol,
                "market_scope": target.market_scope,
                "enabled": target.enabled,
            }
            for target in targets
            if not filtered_target_ids or target.target_id in filtered_target_ids
        )

        return {
            "captured_at": datetime.utcnow(),
            "filters": {
                "target_id": requested_target_id or None,
                "symbol": requested_symbol or None,
                "market_scope": requested_market_scope,
                "event_name": requested_event_name or None,
                "limit": resolved_limit,
            },
            "available_event_names": tuple(sorted({event.event_name for event in events})),
            "recent_events": tuple(filtered_events),
            "target_options": target_options,
            "buffer_size": len(events),
            "schema_version": "v1",
        }

    def _event_type_catalog(self) -> tuple[EventTypeCatalogEntry, ...]:
        return tuple(
            EventTypeCatalogEntry(
                event_type=event_type,
                topic_name=DASHBOARD_EVENTS_TOPIC,
                description=EVENT_TYPE_DESCRIPTIONS[event_type],
            )
            for event_type in EventType
        )

    def _storage_bindings(self) -> tuple[StorageBinding, ...]:
        return ()

    def _build_target_status(self, target: CollectionTarget, last_error: str | None, *, last_event_at: datetime | None = None, permanent: PermanentFailureMeta | None = None) -> CollectionTargetStatus:
        is_active = self._is_publication_active(target.target_id)
        if permanent is not None:
            state = RuntimeState.ERROR
        elif last_error:
            state = RuntimeState.ERROR
        elif target.enabled and is_active:
            state = RuntimeState.RUNNING
        else:
            state = RuntimeState.STOPPED

        return CollectionTargetStatus(
            target_id=target.target_id,
            state=state,
            observed_at=datetime.utcnow(),
            last_event_at=last_event_at,
            last_error=last_error,
            permanent_failure=permanent is not None,
            failure_reason=permanent.reason if permanent is not None else None,
            failure_rt_cd=permanent.rt_cd if permanent is not None else None,
            failure_msg=permanent.msg if permanent is not None else None,
            failure_attempts=permanent.attempts if permanent is not None else None,
        )

    @staticmethod
    def _is_krx_symbol(query: str) -> bool:
        """Return True if the query looks like a bare 6-digit KRX stock code."""
        return len(query) == 6 and query.isdigit()

    def _search_catalog(
        self,
        query: str,
        market_scope: str,
        *,
        target_symbols: tuple[str, ...],
        limit: int,
        provider: Provider = Provider.KXT,
        instrument_type: InstrumentType | None = None,
    ) -> tuple[InstrumentSearchResult, ...]:
        if provider == Provider.KXT:
            return self._search_kxt_catalog(
                query,
                market_scope,
                target_symbols=target_symbols,
                limit=limit,
                provider=provider,
                instrument_type=instrument_type,
            )
        return self._search_crypto_catalog(
            query,
            limit=limit,
            provider=provider,
            instrument_type=instrument_type,
        )

    def _search_kxt_catalog(
        self,
        query: str,
        market_scope: str,
        *,
        target_symbols: tuple[str, ...],
        limit: int,
        provider: Provider,
        instrument_type: InstrumentType | None,
    ) -> tuple[InstrumentSearchResult, ...]:
        normalized_query = query.strip().lower()
        catalog_entries = list(BOOTSTRAP_INSTRUMENTS)
        if self._default_symbol not in {symbol for symbol, _name in catalog_entries}:
            catalog_entries.append((self._default_symbol, f"종목 {self._default_symbol}"))
        for target_symbol in target_symbols:
            if target_symbol not in {symbol for symbol, _name in catalog_entries}:
                catalog_entries.append((target_symbol, f"종목 {target_symbol}"))

        if self._is_krx_symbol(query) and query not in {s for s, _ in catalog_entries}:
            catalog_entries.insert(0, (query, f"종목 {query}"))

        results: list[InstrumentSearchResult] = []
        for symbol, display_name in catalog_entries:
            if normalized_query and normalized_query not in symbol.lower() and normalized_query not in display_name.lower():
                continue
            results.append(
                self._build_search_result(
                    symbol=symbol,
                    display_name=display_name,
                    market_scope=market_scope,
                    provider=provider,
                    instrument_type=instrument_type,
                )
            )
            if len(results) >= limit:
                return tuple(results)
        return tuple(results)

    def _search_crypto_catalog(
        self,
        query: str,
        *,
        limit: int,
        provider: Provider,
        instrument_type: InstrumentType | None,
    ) -> tuple[InstrumentSearchResult, ...]:
        """Minimal Binance crypto seed search.

        Step 3 keeps the seed list intentionally tiny — admins can still
        upsert any unified symbol manually via the form; the seeds are
        only here so the search affordance returns something usable for
        Binance spot/perpetual flows.
        """

        normalized_query = query.strip().upper()
        seeds: tuple[tuple[str, str, str], ...] = (
            ("BTC/USDT", "BTCUSDT", "Bitcoin / USDT"),
            ("ETH/USDT", "ETHUSDT", "Ethereum / USDT"),
            ("SOL/USDT", "SOLUSDT", "Solana / USDT"),
            ("XRP/USDT", "XRPUSDT", "XRP / USDT"),
        )
        if normalized_query and not any(normalized_query in s[0] or normalized_query in s[1] for s in seeds):
            # Admin asked for an explicit symbol — surface it as an
            # injected hit so they can register arbitrary Binance pairs.
            display = normalized_query if "/" in normalized_query else normalized_query
            seeds = ((display, normalized_query.replace("/", ""), display),) + seeds

        results: list[InstrumentSearchResult] = []
        for unified_symbol, raw_symbol, display_name in seeds:
            if normalized_query and normalized_query not in unified_symbol and normalized_query not in raw_symbol:
                continue
            results.append(
                self._build_search_result(
                    symbol=unified_symbol,
                    display_name=display_name,
                    market_scope="",
                    provider=provider,
                    instrument_type=instrument_type,
                    raw_symbol=raw_symbol,
                )
            )
            if len(results) >= limit:
                break
        return tuple(results)

    def _build_search_result(
        self,
        *,
        symbol: str,
        display_name: str,
        market_scope: str,
        provider: Provider = Provider.KXT,
        instrument_type: InstrumentType | None = None,
        raw_symbol: str | None = None,
    ) -> InstrumentSearchResult:
        instrument_ref = self._build_instrument_ref(
            symbol,
            provider=provider,
            instrument_type=instrument_type,
            raw_symbol=raw_symbol,
        )
        return InstrumentSearchResult(
            instrument=instrument_ref,
            display_name=display_name,
            market_scope=market_scope,
            provider_instrument_id=instrument_ref.raw_symbol or symbol,
            venue_code=(instrument_ref.venue.value.upper() if instrument_ref.venue else None),
            is_active=True,
            provider=provider,
            canonical_symbol=instrument_ref.canonical_symbol,
        )

    def _build_instrument_ref(
        self,
        symbol: str,
        *,
        provider: Provider = Provider.KXT,
        instrument_type: InstrumentType | None = None,
        raw_symbol: str | None = None,
    ) -> InstrumentRef:
        # KXT today means KRX equity (spot trading); other providers
        # default to crypto spot semantics unless the caller overrides.
        if provider == Provider.KXT:
            venue = Venue.KRX
            asset_class = AssetClass.EQUITY
            resolved_instrument_type = instrument_type or InstrumentType.SPOT
            if resolved_instrument_type == InstrumentType.EQUITY:
                # Backwards-compat: legacy callers using EQUITY as the
                # instrument-type axis are normalised to SPOT (KRX trades
                # equities on the spot market).  Asset class still EQUITY.
                resolved_instrument_type = InstrumentType.SPOT
            resolved_raw = raw_symbol or symbol
            display_symbol = symbol
            settle_asset: str | None = None
        else:
            venue = Venue.BINANCE
            asset_class = AssetClass.CRYPTO
            resolved_instrument_type = instrument_type or InstrumentType.SPOT
            display_symbol = symbol
            resolved_raw = raw_symbol or symbol.replace("/", "").split(":")[0]
            settle_asset = None
            if resolved_instrument_type == InstrumentType.PERPETUAL:
                # Settle asset = the quote currency (USDT for the
                # default Binance USDT-margined perpetuals).
                quote = display_symbol.split("/", 1)[1].split(":", 1)[0] if "/" in display_symbol else "USDT"
                settle_asset = quote.upper()

        canonical = build_canonical_symbol(
            provider=provider,
            venue=venue,
            asset_class=asset_class,
            instrument_type=resolved_instrument_type,
            display_symbol=display_symbol,
            settle_asset=settle_asset,
        )
        return InstrumentRef(
            symbol=display_symbol,
            instrument_id=resolved_raw,
            venue=venue,
            asset_class=asset_class,
            instrument_type=resolved_instrument_type,
            provider=provider,
            canonical_symbol=canonical,
            raw_symbol=resolved_raw,
        )

    def _normalize_event_types(
        self,
        event_types: list[str] | tuple[str, ...],
        *,
        provider: Provider | None = None,
        instrument_type: InstrumentType | None = None,
    ) -> tuple[str, ...]:
        available = {event_type.value for event_type in EventType}
        normalized = tuple(
            dict.fromkeys(
                self._normalize_event_name(event_type)
                for event_type in event_types
                if str(event_type).strip()
            )
        )
        if not normalized:
            raise ValueError("at least one event_type is required")
        invalid = [event_type for event_type in normalized if event_type not in available]
        if invalid:
            raise ValueError(f"unsupported event_types: {', '.join(invalid)}")

        # Capability gating: when provider + instrument_type are known,
        # reject events the source cannot expose (e.g. mark_price on
        # Binance spot, program_trade on a non-KRX source).
        if provider is not None and instrument_type is not None:
            venue = Venue.KRX if provider == Provider.KXT else Venue.BINANCE
            capability = capability_for(
                provider=provider,
                venue=venue,
                instrument_type=instrument_type,
            )
            if capability is not None:
                ungated = [
                    event_type
                    for event_type in normalized
                    if event_type not in capability.supported_event_types
                ]
                if ungated:
                    raise ValueError(
                        "event_types not supported by "
                        f"{capability.label}: {', '.join(ungated)}"
                    )
        return normalized

    def _normalize_event_name(self, event_name: str | None) -> str:
        normalized = str(event_name or "").strip().lower()
        return EVENT_TYPE_ALIASES.get(normalized, normalized)

    @staticmethod
    def _target_matches_event(
        target: CollectionTarget,
        *,
        symbol: str,
        market_scope: str,
        event_name: str,
        provider: Provider,
        canonical_symbol: str | None,
    ) -> bool:
        if event_name not in target.event_types:
            return False
        # Canonical-symbol comparison disambiguates spot vs perpetual on
        # the same base symbol (e.g. Binance BTCUSDT spot vs USDT-perp).
        if canonical_symbol and target.canonical_symbol:
            return canonical_symbol == target.canonical_symbol
        target_provider = target.provider or Provider.KXT
        if target_provider != provider:
            return False
        if target.instrument.symbol != symbol:
            return False
        if target_provider == Provider.KXT:
            return target.market_scope == market_scope
        # Non-KXT providers store empty market_scope; symbol+provider is enough.
        return True

    def _normalize_market_scope(self, market_scope: str | None, *, provider: Provider = Provider.KXT) -> str:
        """Normalise KRX selection scope.

        For non-KXT providers market_scope is a KRX-only concept; we store
        it as the empty string so the field never carries ``krx|nxt|total``
        semantics on crypto targets (see H23).
        """

        if provider != Provider.KXT:
            normalized = (market_scope or "").strip().lower()
            return normalized  # empty string == not applicable
        normalized = (market_scope or "").strip().lower()
        if normalized not in SUPPORTED_MARKET_SCOPES:
            raise ValueError(f"unsupported market scope: {market_scope}")
        return normalized

    def _normalize_provider(self, provider: str | Provider | None) -> Provider:
        """Map external provider input to the internal :class:`Provider`.

        Accepts only ``kxt`` and ``ccxt`` externally; ``ccxt_pro`` is
        kept as a deprecated input alias and remapped to
        :attr:`Provider.CCXT` so that downstream surfaces never expose
        the ccxt.pro transport detail.
        """

        if provider is None or provider == "":
            return Provider.KXT
        if isinstance(provider, Provider):
            if provider == Provider.CCXT_PRO:
                return Provider.CCXT
            return provider
        text = str(provider).strip().lower()
        if text == Provider.CCXT_PRO.value:
            return Provider.CCXT
        try:
            return Provider(text)
        except ValueError as exc:
            raise ValueError(f"unsupported provider: {provider}") from exc

    def _normalize_instrument_type(
        self,
        instrument_type: str | InstrumentType | None,
        *,
        provider: Provider,
    ) -> InstrumentType:
        if instrument_type is None or instrument_type == "":
            return InstrumentType.SPOT if provider == Provider.KXT else InstrumentType.SPOT
        if isinstance(instrument_type, InstrumentType):
            resolved = instrument_type
        else:
            try:
                resolved = InstrumentType(str(instrument_type).strip().lower())
            except ValueError as exc:
                raise ValueError(f"unsupported instrument_type: {instrument_type}") from exc
        # KXT-side EQUITY input is normalised to SPOT (KRX equities trade
        # on the spot market — see canonical_symbol H25 decision).
        if provider == Provider.KXT and resolved == InstrumentType.EQUITY:
            resolved = InstrumentType.SPOT
        return resolved
