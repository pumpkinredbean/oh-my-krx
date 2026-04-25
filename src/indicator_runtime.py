"""Admin-charts indicator runtime (step 1).

Hosts a small manager that subscribes to the collector control plane's
runtime event stream, fans events into per-instance indicator objects
(built-in OBI + user-authored Python scripts), and publishes
``IndicatorOutputEnvelope`` frames on a bounded fan-out channel.

Design constraints (see TASK_PACKET goal B):

* Never block the hot path.  Event fan-in uses the existing bounded
  queue from ``control_plane.subscribe_events()``; every ``on_event``
  invocation is wrapped in try/except so a single raising indicator can
  only mark its own instance state to ``error`` without killing the
  manager loop or other indicators.
* User-authored Python scripts are validated before activation: AST
  parse, forbidden-import gate, restricted exec namespace, and a
  synthetic dry-run for every declared input ``event_type``.
* Persistence is a single JSON snapshot file (``./state/admin_charts.json``)
  that is rewritten atomically under a per-manager ``asyncio.Lock``.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import logging
import os
import traceback
import uuid
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from packages.contracts.admin import (
    ChartInputSlot,
    ChartPanelBaseFeed,
    ChartPanelSpec,
    ChartSeriesBinding,
    IndicatorDeclaration,
    IndicatorInputDecl,
    IndicatorInstanceSpec,
    IndicatorOutputDecl,
    IndicatorOutputEnvelope,
    IndicatorParamDecl,
    IndicatorScriptSpec,
    SeriesPoint,
    param_values_as_dict,
    param_values_from_dict,
)

logger = logging.getLogger(__name__)


# Persistence path for admin-charts-only state.  Exposed as a module-level
# constant so tests can monkeypatch it onto a tmp path without touching
# the manager API.
ADMIN_CHARTS_STATE_PATH: Path = Path("./state/admin_charts.json")


# ─── HubIndicator base class (exposed to user scripts as ``hub_indicator``) ──


class HubIndicator:
    """Base class every user-authored indicator subclass must inherit from.

    Subclasses declare:

    * ``name`` (class attr): human-readable name of the indicator.
    * ``inputs`` (class attr): iterable of canonical ``event_type`` strings
      the runtime should forward into ``on_event``.
    * ``output_kind`` (class attr, optional): ``"line"`` by default.
    * ``on_event(event)``: called for every matching input event.  May
      return None, a single :class:`SeriesPoint`, or an iterable of
      :class:`SeriesPoint`.  Raising is caught by the runtime and marks
      the owning instance as ``error``.

    ``event`` is a small dict with canonical keys (``event_type``,
    ``symbol``, ``market_scope``, ``payload``, ``published_at``) mirroring
    the collector's ``DashboardEventEnvelope`` / ``RecentRuntimeEvent``.
    """

    name: str = "indicator"
    inputs: tuple[str, ...] = ()
    output_kind: str = "line"

    def __init__(self, **params: Any) -> None:
        self.params: dict[str, Any] = dict(params)

    def on_event(self, event: dict[str, Any]) -> Any:  # pragma: no cover - stub
        raise NotImplementedError


# ─── Built-in OBI indicator ──────────────────────────────────────────────────


class RawPassthroughIndicator(HubIndicator):
    """Pass-through indicator that re-emits one scalar field of a raw
    event as a :class:`SeriesPoint`.

    Declaration-driven: a single ``source`` input slot accepts ``ohlcv``,
    ``trade``, ``mark_price``, or ``funding_rate`` events.  The ``field``
    parameter (enum) selects which payload key to project.  A single
    ``value`` line output is declared.

    The runtime inspects ``inputs`` (here intentionally empty/wildcard)
    and routes the configured raw stream into ``on_event`` via the
    indicator-runtime's per-instance subscription path.  To keep the
    legacy dispatch loop honest we also accept any incoming event and
    extract ``params['field']`` (defaulting to ``close`` for ohlcv,
    ``price`` for trade, ``value``/``rate`` for mark/funding).
    """

    name = "Raw"
    inputs: tuple[str, ...] = ("ohlcv", "trade", "mark_price", "funding_rate")
    output_kind = "line"

    DEFAULT_FIELDS: dict[str, str] = {
        "ohlcv": "close",
        "trade": "price",
        "mark_price": "value",
        "funding_rate": "rate",
    }

    @staticmethod
    def _value_at_path(payload: dict[str, Any], field: str) -> Any:
        if not field:
            return None
        if field in payload:
            return payload[field]
        cur: Any = payload
        for part in field.split("."):
            if not part:
                return None
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def on_event(self, event: dict[str, Any]) -> SeriesPoint | None:
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            return None
        event_type = str(event.get("event_type") or "")
        field = str(self.params.get("field") or self.DEFAULT_FIELDS.get(event_type) or "")
        time_field = str(self.params.get("time_field") or "")
        raw = self._value_at_path(payload, field)
        if raw is None:
            for fallback in ("price", "close", "value", "rate", "mark_price"):
                if fallback in payload:
                    raw = payload[fallback]
                    field = fallback
                    break
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        time_raw = self._value_at_path(payload, time_field) if time_field else None
        if time_raw is None:
            time_field = ""
            for fallback in ("timestamp", "published_at", "occurred_at"):
                time_raw = payload.get(fallback)
                if time_raw is not None:
                    time_field = fallback
                    break
        if time_raw is None:
            time_raw = event.get("published_at") or datetime.now(timezone.utc).isoformat()
            time_field = "published_at"
        timestamp = str(time_raw)
        return SeriesPoint(
            timestamp=timestamp,
            value=value,
            meta={"field": field, "time_field": time_field, "event_type": event_type},
        )


_RAW_PASSTHROUGH_DECLARATION = IndicatorDeclaration(
    inputs=(
        IndicatorInputDecl(
            slot_name="source",
            event_names=("ohlcv", "trade", "mark_price", "funding_rate"),
            field_hints=(
                "close", "open", "high", "low", "volume",
                "price", "value", "rate", "mark_price",
            ),
            required=True,
        ),
    ),
    params=(
        IndicatorParamDecl(
            name="field",
            kind="enum",
            default="close",
            choices=("close", "open", "high", "low", "volume", "price", "value", "rate"),
            label="Field",
            help="Payload key to project as the line value.",
        ),
        IndicatorParamDecl(
            name="time_field",
            kind="str",
            default="",
            label="Time field",
            help="Payload key/path to use as the x-axis timestamp.",
        ),
    ),
    outputs=(
        IndicatorOutputDecl(name="value", kind="line", label="value", is_primary=True),
    ),
)


_OBI_DECLARATION = IndicatorDeclaration(
    inputs=(
        IndicatorInputDecl(
            slot_name="orderbook",
            event_names=("order_book_snapshot",),
            field_hints=(),
            required=True,
        ),
    ),
    params=(
        IndicatorParamDecl(
            name="top_n",
            kind="int",
            default=5,
            min=1,
            max=50,
            label="Top N",
            help="Number of levels to aggregate on each side.",
        ),
    ),
    outputs=(
        IndicatorOutputDecl(name="obi", kind="line", label="OBI", is_primary=True),
    ),
)


class OrderBookImbalanceIndicator(HubIndicator):
    """Top-N order book imbalance (OBI).

    ``OBI = (sum(bid_volume_top_N) - sum(ask_volume_top_N))
            / (sum(bid_volume_top_N) + sum(ask_volume_top_N))``

    Accepts flexible payload shapes to tolerate both KXT and CCXT
    canonical ``order_book_snapshot`` payloads.  Levels may live under
    ``bids``/``asks`` (preferred) or ``bid_levels``/``ask_levels``.
    Each level may be a ``[price, size]`` pair or a dict with
    ``price``/``size`` (or ``quantity``/``volume``) keys.
    """

    name = "OBI"
    inputs = ("order_book_snapshot",)
    output_kind = "line"

    def on_event(self, event: dict[str, Any]) -> SeriesPoint | None:
        if event.get("event_type") != "order_book_snapshot":
            return None
        payload = event.get("payload") or {}
        top_n = int(self.params.get("top_n") or self.params.get("N") or 5)
        top_n = max(1, top_n)

        def _extract(levels: Any) -> list[float]:
            out: list[float] = []
            if not isinstance(levels, (list, tuple)):
                return out
            for level in levels[:top_n]:
                if isinstance(level, (list, tuple)) and len(level) >= 2:
                    try:
                        out.append(float(level[1]))
                    except (TypeError, ValueError):
                        continue
                elif isinstance(level, dict):
                    for key in ("size", "quantity", "volume", "qty", "amount"):
                        if key in level:
                            try:
                                out.append(float(level[key]))
                            except (TypeError, ValueError):
                                pass
                            break
            return out

        bids = _extract(payload.get("bids") or payload.get("bid_levels"))
        asks = _extract(payload.get("asks") or payload.get("ask_levels"))
        if not bids and not asks:
            return None
        bid_sum = sum(bids)
        ask_sum = sum(asks)
        denom = bid_sum + ask_sum
        if denom <= 0:
            return None
        value = (bid_sum - ask_sum) / denom
        timestamp = str(event.get("published_at") or datetime.now(timezone.utc).isoformat())
        return SeriesPoint(
            timestamp=timestamp,
            value=float(value),
            meta={"top_n": top_n, "bid_sum": bid_sum, "ask_sum": ask_sum},
        )


BUILTIN_SCRIPTS: tuple[IndicatorScriptSpec, ...] = (
    IndicatorScriptSpec(
        script_id="builtin.raw",
        name="Raw passthrough",
        source="# builtin: see src.indicator_runtime.RawPassthroughIndicator\n",
        class_name="RawPassthroughIndicator",
        builtin=True,
        description="Re-emit one scalar field of a raw event as a line series.",
        declaration=_RAW_PASSTHROUGH_DECLARATION,
    ),
    IndicatorScriptSpec(
        script_id="builtin.obi",
        name="OBI (Order Book Imbalance)",
        source="# builtin: see src.indicator_runtime.OrderBookImbalanceIndicator\n",
        class_name="OrderBookImbalanceIndicator",
        builtin=True,
        description="Top-N order book imbalance; inputs=order_book_snapshot.",
        declaration=_OBI_DECLARATION,
    ),
)


# ─── Script validation ───────────────────────────────────────────────────────


FORBIDDEN_MODULES: frozenset[str] = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "socket",
        "threading",
        "multiprocessing",
        "ctypes",
        "builtins",
        "pathlib",
        "shutil",
        "requests",
        "urllib",
        "http",
        "asyncio",
        "importlib",
    }
)

ALLOWED_MODULES: frozenset[str] = frozenset(
    {
        "math",
        "statistics",
        "collections",
        "dataclasses",
        "typing",
        "itertools",
        "functools",
        "hub_indicator",
    }
)


class ScriptValidationError(Exception):
    """Raised with structured ``errors`` when a script fails validation."""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = list(errors)


_SYNTHETIC_PAYLOADS: dict[str, dict[str, Any]] = {
    "trade": {"price": 100.0, "size": 1.0, "side": "buy"},
    "order_book_snapshot": {
        "bids": [[100.0, 1.0], [99.5, 2.0], [99.0, 3.0]],
        "asks": [[100.5, 1.0], [101.0, 2.0], [101.5, 3.0]],
    },
    "program_trade": {"buy": 0.0, "sell": 0.0, "net": 0.0},
    "ticker": {"last": 100.0, "bid": 99.5, "ask": 100.5},
    "ohlcv": {"open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10, "timestamp": "2025-01-01T00:00:00Z"},
    "mark_price": {"mark_price": 100.0},
    "funding_rate": {"rate": 0.0001},
    "open_interest": {"open_interest": 1000.0},
}


def _synthetic_event(event_type: str) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "symbol": "TEST",
        "market_scope": "",
        "payload": dict(_SYNTHETIC_PAYLOADS.get(event_type, {})),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


def _hub_indicator_module() -> Any:
    """Build a minimal in-memory ``hub_indicator`` module exposed to scripts."""
    import types

    module = types.ModuleType("hub_indicator")
    module.HubIndicator = HubIndicator  # type: ignore[attr-defined]
    module.SeriesPoint = SeriesPoint  # type: ignore[attr-defined]
    return module


def _restricted_globals() -> dict[str, Any]:
    """Safe-ish global namespace for user script exec.

    We do NOT attempt to sandbox arbitrary Python — this is a guardrail,
    not a security boundary.  Forbidden imports are rejected statically;
    the exec namespace only pre-imports the allow-list.
    """
    safe_builtins = {
        name: getattr(__builtins__, name, None) if not isinstance(__builtins__, dict) else __builtins__.get(name)
        for name in (
            "abs", "all", "any", "bool", "dict", "divmod", "enumerate", "filter",
            "float", "frozenset", "hash", "int", "isinstance", "issubclass", "iter",
            "len", "list", "map", "max", "min", "next", "object", "ord", "pow",
            "range", "repr", "reversed", "round", "set", "slice", "sorted", "str",
            "sum", "tuple", "type", "zip", "print",
            # Exceptions — needed so scripts can raise/handle standard errors.
            "Exception", "ValueError", "TypeError", "KeyError", "IndexError",
            "StopIteration", "RuntimeError", "ZeroDivisionError",
        )
    }
    # Provide a restricted __import__ that only allows the allow-list.
    original_import = __import__

    def _safe_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # noqa: D401
        root = name.split(".")[0]
        if root in FORBIDDEN_MODULES:
            raise ImportError(f"import of '{name}' is not permitted in hub indicator scripts")
        if root not in ALLOWED_MODULES:
            raise ImportError(f"module '{name}' is not in the hub-indicator allow-list")
        if root == "hub_indicator":
            return _hub_indicator_module()
        return original_import(name, globals, locals, fromlist, level)

    safe_builtins["__import__"] = _safe_import
    # Class construction needs __build_class__; attribute access needs
    # __name__/__doc__; these are not security-sensitive.
    import builtins as _builtins_mod
    safe_builtins["__build_class__"] = _builtins_mod.__build_class__
    safe_builtins["__name__"] = "hub_indicator_script"
    safe_builtins["__doc__"] = None
    return {
        "__builtins__": safe_builtins,
        # Pre-inject the hub_indicator namespace for ergonomics so scripts
        # can do ``from hub_indicator import HubIndicator, SeriesPoint`` or
        # reference ``HubIndicator`` directly.
        "HubIndicator": HubIndicator,
        "SeriesPoint": SeriesPoint,
        "hub_indicator": _hub_indicator_module(),
    }


def _ast_reject_forbidden(tree: ast.AST) -> list[str]:
    errors: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in FORBIDDEN_MODULES:
                    errors.append(f"forbidden import: {alias.name}")
                elif root not in ALLOWED_MODULES:
                    errors.append(f"import not in allow-list: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            if root in FORBIDDEN_MODULES:
                errors.append(f"forbidden import-from: {node.module}")
            elif root and root not in ALLOWED_MODULES:
                errors.append(f"import-from not in allow-list: {node.module}")
        elif isinstance(node, ast.Attribute):
            if node.attr == "__import__":
                errors.append("attribute access to '__import__' is forbidden")
        elif isinstance(node, ast.Name):
            if node.id == "__import__":
                errors.append("use of '__import__' is forbidden")
    return errors


def validate_and_instantiate(
    source: str,
    *,
    class_name: str | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[HubIndicator, str]:
    """Validate a script source and return (instance, resolved_class_name).

    On failure raises :class:`ScriptValidationError` with a list of
    structured error strings.  The dry-run drives one synthetic event
    per declared input type; any raised exception is recorded as an
    error (and the instance is NOT returned).
    """
    errors: list[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise ScriptValidationError([f"syntax error: {exc.msg} (line {exc.lineno})"]) from exc

    errors.extend(_ast_reject_forbidden(tree))
    if errors:
        raise ScriptValidationError(errors)

    namespace = _restricted_globals()
    try:
        exec(compile(tree, "<hub_indicator>", "exec"), namespace, namespace)
    except Exception as exc:  # noqa: BLE001 — surface exec failure
        raise ScriptValidationError([f"script import failed: {exc}"]) from exc

    candidates: list[tuple[str, type]] = []
    for key, value in namespace.items():
        if key.startswith("__"):
            continue
        if isinstance(value, type) and issubclass(value, HubIndicator) and value is not HubIndicator:
            candidates.append((key, value))

    if not candidates:
        raise ScriptValidationError(
            ["no HubIndicator subclass found in script"]
        )
    if class_name:
        matched = [(k, v) for (k, v) in candidates if k == class_name]
        if not matched:
            raise ScriptValidationError([f"class '{class_name}' not found in script"])
        chosen_key, chosen_cls = matched[0]
    else:
        chosen_key, chosen_cls = candidates[0]

    # Required class attrs.
    for attr in ("name", "inputs"):
        if not hasattr(chosen_cls, attr):
            errors.append(f"class '{chosen_key}' missing required attr: {attr}")
    if errors:
        raise ScriptValidationError(errors)

    # Required method.
    if not callable(getattr(chosen_cls, "on_event", None)):
        raise ScriptValidationError([f"class '{chosen_key}' missing on_event method"])

    # Instantiate with params.
    try:
        instance = chosen_cls(**(params or {}))
    except Exception as exc:  # noqa: BLE001
        raise ScriptValidationError([f"instantiation failed: {exc}"]) from exc

    # Dry-run each declared input.
    inputs_iter: Iterable[str] = getattr(instance, "inputs", ()) or ()
    for event_type in inputs_iter:
        sample = _synthetic_event(str(event_type))
        try:
            instance.on_event(sample)
        except Exception as exc:  # noqa: BLE001
            raise ScriptValidationError(
                [f"on_event dry-run failed for '{event_type}': {exc}"]
            ) from exc

    return instance, chosen_key


# ─── State container (panels + scripts + instances) ──────────────────────────


class ChartsStateStore:
    """In-memory + JSON-snapshot state for admin charts.

    All mutators take an ``asyncio.Lock`` before reading/writing so
    concurrent API calls observe consistent state.  Persistence writes
    go through ``_persist_locked`` which serialises the whole snapshot
    via tmp-file + ``os.replace`` (atomic on POSIX).
    """

    def __init__(self, *, path: Path | None = None) -> None:
        self._path = Path(path) if path is not None else ADMIN_CHARTS_STATE_PATH
        self._lock = asyncio.Lock()
        self._panels: dict[str, ChartPanelSpec] = {}
        self._scripts: dict[str, IndicatorScriptSpec] = {}
        self._instances: dict[str, IndicatorInstanceSpec] = {}
        # Seed built-in scripts.
        for script in BUILTIN_SCRIPTS:
            self._scripts[script.script_id] = script

    # -- public read helpers ------------------------------------------------
    async def list_panels(self) -> list[ChartPanelSpec]:
        async with self._lock:
            return list(self._panels.values())

    async def list_scripts(self) -> list[IndicatorScriptSpec]:
        async with self._lock:
            return list(self._scripts.values())

    async def list_instances(self) -> list[IndicatorInstanceSpec]:
        async with self._lock:
            return list(self._instances.values())

    # -- mutators -----------------------------------------------------------
    async def upsert_panel(self, panel: ChartPanelSpec) -> ChartPanelSpec:
        panel = sanitize_chart_panel_spec(panel)
        async with self._lock:
            self._panels[panel.panel_id] = panel
            # Register panel-scoped scripts + instances into the global
            # registry so the runtime can build indicators referenced by
            # this panel's bindings without duplicating dispatch paths.
            for script in panel.scripts or ():
                if script.script_id and not script.script_id.startswith("builtin."):
                    self._scripts[script.script_id] = script
            for inst in panel.instances or ():
                if inst.instance_id:
                    self._instances[inst.instance_id] = inst
            self._persist_locked()
            return panel

    async def delete_panel(self, panel_id: str) -> bool:
        async with self._lock:
            removed = self._panels.pop(panel_id, None) is not None
            if removed:
                self._persist_locked()
            return removed

    async def upsert_script(self, script: IndicatorScriptSpec) -> IndicatorScriptSpec:
        async with self._lock:
            self._scripts[script.script_id] = script
            self._persist_locked()
            return script

    async def delete_script(self, script_id: str) -> bool:
        async with self._lock:
            if script_id in self._scripts and self._scripts[script_id].builtin:
                return False
            removed = self._scripts.pop(script_id, None) is not None
            # Cascade: drop instances referencing this script.
            if removed:
                drop = [iid for iid, inst in self._instances.items() if inst.script_id == script_id]
                for iid in drop:
                    self._instances.pop(iid, None)
                self._persist_locked()
            return removed

    async def upsert_instance(self, instance: IndicatorInstanceSpec) -> IndicatorInstanceSpec:
        async with self._lock:
            self._instances[instance.instance_id] = instance
            self._persist_locked()
            return instance

    async def delete_instance(self, instance_id: str) -> bool:
        async with self._lock:
            removed = self._instances.pop(instance_id, None) is not None
            if removed:
                self._persist_locked()
            return removed

    # -- persistence --------------------------------------------------------
    def _persist_locked(self) -> None:
        snapshot = {
            "panels": [asdict(p) for p in self._panels.values()],
            "scripts": [asdict(s) for s in self._scripts.values() if not s.builtin],
            "instances": [asdict(i) for i in self._instances.values()],
            "schema_version": "v1",
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp, self._path)
        except Exception:  # noqa: BLE001 — persistence never blocks hot path
            logger.exception("indicator_runtime.persist_failed path=%s", self._path)

    def load(self) -> None:
        """Load a previously persisted snapshot from disk (best-effort)."""
        try:
            if not self._path.exists():
                return
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            logger.exception("indicator_runtime.load_failed path=%s", self._path)
            return
        for entry in data.get("panels") or []:
            try:
                panel = _panel_spec_from_entry(entry)
                self._panels[panel.panel_id] = panel
            except Exception:  # noqa: BLE001
                continue
        for entry in data.get("scripts") or []:
            try:
                script = IndicatorScriptSpec(**entry)
                if not script.builtin:
                    self._scripts[script.script_id] = script
            except Exception:  # noqa: BLE001
                continue
        for entry in data.get("instances") or []:
            try:
                inst = IndicatorInstanceSpec(**entry)
                self._instances[inst.instance_id] = inst
            except Exception:  # noqa: BLE001
                continue


# ─── Manager ─────────────────────────────────────────────────────────────────


class _InstanceRuntime:
    __slots__ = ("spec", "indicator", "state", "last_error", "last_output_at", "output_count")

    def __init__(self, spec: IndicatorInstanceSpec, indicator: HubIndicator) -> None:
        self.spec = spec
        self.indicator = indicator
        self.state: str = "running"
        self.last_error: str | None = None
        self.last_output_at: datetime | None = None
        self.output_count: int = 0


class IndicatorRuntimeManager:
    """Background manager that drives indicator instances on runtime events.

    Attach to a FastAPI lifespan by awaiting :meth:`start` on startup and
    :meth:`aclose` on shutdown.  Call :meth:`set_state_store` before
    start so CRUD routes hit the same store that the manager refreshes
    from.
    """

    def __init__(
        self,
        *,
        control_plane: Any,
        state_store: ChartsStateStore,
    ) -> None:
        self._control_plane = control_plane
        self._store = state_store
        self._lock = asyncio.Lock()
        self._instances: dict[str, _InstanceRuntime] = {}
        self._output_subscribers: list[asyncio.Queue[IndicatorOutputEnvelope]] = []
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    # -- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        # Materialise persisted instances (built-in + user scripts).
        await self._reconcile_instances()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="indicator-runtime")

    async def aclose(self) -> None:
        self._stopping.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # -- subscriber channel -------------------------------------------------
    @asynccontextmanager
    async def subscribe_indicator_output(self) -> AsyncIterator[asyncio.Queue[IndicatorOutputEnvelope]]:
        """Bounded indicator-output fan-out channel.

        Mirrors ``CollectorControlPlaneService.subscribe_events`` — the
        queue is bounded (maxsize=500) and messages are dropped (never
        blocking) when the consumer falls behind.
        """
        queue: asyncio.Queue[IndicatorOutputEnvelope] = asyncio.Queue(maxsize=500)
        async with self._lock:
            self._output_subscribers.append(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                with contextlib.suppress(ValueError):
                    self._output_subscribers.remove(queue)

    # -- admin surface ------------------------------------------------------
    async def instance_states(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [
                {
                    "instance_id": runtime.spec.instance_id,
                    "script_id": runtime.spec.script_id,
                    "symbol": runtime.spec.symbol,
                    "state": runtime.state,
                    "last_error": runtime.last_error,
                    "last_output_at": runtime.last_output_at.isoformat() if runtime.last_output_at else None,
                    "output_count": runtime.output_count,
                }
                for runtime in self._instances.values()
            ]

    async def reconcile(self) -> None:
        """Public hook for API routes to trigger instance reconciliation."""
        await self._reconcile_instances()

    # -- internal loop ------------------------------------------------------
    async def _run(self) -> None:
        try:
            async with self._control_plane.subscribe_events() as queue:
                while not self._stopping.is_set():
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=0.5)
                    except asyncio.TimeoutError:
                        continue
                    await self._dispatch(event)
        except asyncio.CancelledError:  # pragma: no cover - cancellation path
            raise
        except Exception:  # noqa: BLE001
            logger.exception("indicator_runtime.manager_loop_crashed")

    async def _dispatch(self, event: Any) -> None:
        # ``event`` is a RecentRuntimeEvent (frozen dataclass).  Normalise
        # it into the dict shape the indicator API expects so user scripts
        # see a stable contract.
        normalized = _normalize_event(event)
        event_type = normalized["event_type"]
        symbol = normalized["symbol"]
        async with self._lock:
            runtimes = list(self._instances.values())
        for runtime in runtimes:
            if runtime.state == "error":
                # Keep error state sticky until the instance is reactivated.
                continue
            if not runtime.spec.enabled:
                continue
            if runtime.spec.symbol and runtime.spec.symbol != symbol:
                continue
            inputs = tuple(getattr(runtime.indicator, "inputs", ()) or ())
            if inputs and event_type not in inputs:
                continue
            try:
                result = runtime.indicator.on_event(normalized)
            except Exception as exc:  # noqa: BLE001 — isolate per-instance
                tb = traceback.format_exc(limit=6)
                runtime.state = "error"
                runtime.last_error = f"{exc!s}\n{tb}"[:2000]
                logger.warning(
                    "indicator_runtime.instance_error instance=%s: %s",
                    runtime.spec.instance_id,
                    exc,
                )
                continue
            for point in _coerce_points(result):
                envelope = IndicatorOutputEnvelope(
                    instance_id=runtime.spec.instance_id,
                    script_id=runtime.spec.script_id,
                    name=getattr(runtime.indicator, "name", runtime.spec.instance_id),
                    symbol=runtime.spec.symbol,
                    market_scope=runtime.spec.market_scope,
                    output_kind=getattr(runtime.indicator, "output_kind", "line"),
                    published_at=datetime.now(timezone.utc),
                    point=point,
                )
                runtime.last_output_at = envelope.published_at
                runtime.output_count += 1
                await self._fanout(envelope)

    async def _fanout(self, envelope: IndicatorOutputEnvelope) -> None:
        async with self._lock:
            subscribers = list(self._output_subscribers)
        for queue in subscribers:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(envelope)

    async def _reconcile_instances(self) -> None:
        scripts = {s.script_id: s for s in await self._store.list_scripts()}
        specs = await self._store.list_instances()
        new_runtimes: dict[str, _InstanceRuntime] = {}
        async with self._lock:
            existing = dict(self._instances)
        for spec in specs:
            if not spec.enabled:
                continue
            prev = existing.get(spec.instance_id)
            if prev is not None and prev.spec == spec:
                new_runtimes[spec.instance_id] = prev
                continue
            script = scripts.get(spec.script_id)
            if script is None:
                continue
            try:
                indicator = _build_indicator(script, spec.params)
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc(limit=6)
                runtime = _InstanceRuntime(
                    spec=spec,
                    indicator=HubIndicator(),
                )
                runtime.state = "error"
                runtime.last_error = f"activation failed: {exc}\n{tb}"[:2000]
                new_runtimes[spec.instance_id] = runtime
                continue
            new_runtimes[spec.instance_id] = _InstanceRuntime(spec=spec, indicator=indicator)
        async with self._lock:
            self._instances = new_runtimes


# ─── Helpers ────────────────────────────────────────────────────────────────


def _normalize_event(event: Any) -> dict[str, Any]:
    """Project a RecentRuntimeEvent / DashboardEventEnvelope to the script-facing dict."""
    if isinstance(event, dict):
        d = event
        event_type = d.get("event_type") or d.get("event_name") or ""
        payload = d.get("payload") or {}
        symbol = d.get("symbol") or ""
        market_scope = d.get("market_scope") or ""
        published_at = d.get("published_at") or datetime.now(timezone.utc).isoformat()
    else:
        event_type = getattr(event, "event_name", "") or getattr(event, "event_type", "")
        payload = getattr(event, "payload", None) or {}
        symbol = getattr(event, "symbol", "") or ""
        market_scope = getattr(event, "market_scope", "") or ""
        published_at = getattr(event, "published_at", None)
        if isinstance(published_at, datetime):
            published_at = published_at.isoformat()
        else:
            published_at = str(published_at or datetime.now(timezone.utc).isoformat())
    return {
        "event_type": event_type,
        "symbol": symbol,
        "market_scope": market_scope,
        "payload": payload if isinstance(payload, dict) else {},
        "published_at": published_at,
    }


def _coerce_points(result: Any) -> list[SeriesPoint]:
    if result is None:
        return []
    if isinstance(result, SeriesPoint):
        return [result]
    if isinstance(result, (list, tuple)):
        return [p for p in result if isinstance(p, SeriesPoint)]
    return []


def _build_indicator(script: IndicatorScriptSpec, params: dict[str, Any]) -> HubIndicator:
    if script.builtin:
        if script.class_name == "OrderBookImbalanceIndicator":
            return OrderBookImbalanceIndicator(**(params or {}))
        if script.class_name == "RawPassthroughIndicator":
            return RawPassthroughIndicator(**(params or {}))
        raise ValueError(f"unknown built-in indicator: {script.class_name}")
    instance, _ = validate_and_instantiate(
        script.source,
        class_name=script.class_name or None,
        params=params or {},
    )
    return instance


def new_panel_id() -> str:
    return f"panel-{uuid.uuid4().hex[:10]}"


def new_script_id() -> str:
    return f"script-{uuid.uuid4().hex[:10]}"


def new_instance_id() -> str:
    return f"inst-{uuid.uuid4().hex[:10]}"


# ─── Panel persistence helpers ──────────────────────────────────────────────


_CHART_INPUT_SLOT_FIELDS: frozenset[str] = frozenset(
    {"slot_name", "target_id", "event_name", "time_field_name", "field_name"}
)

_CHART_SERIES_BINDING_FIELDS: frozenset[str] = frozenset(
    {
        "binding_id",
        "indicator_ref",
        "instance_id",
        "input_bindings",
        "param_values",
        "output_name",
        "axis",
        "color",
        "label",
        "visible",
    }
)

_CHART_PANEL_FIELDS: frozenset[str] = frozenset(
    {
        "panel_id",
        "chart_type",
        "symbol",
        "x",
        "y",
        "w",
        "h",
        "title",
        "notes",
        "series_bindings",
        "base_feed",
        "scripts",
        "instances",
    }
)


def scrub_legacy_normalized_binding_value(value: Any) -> Any:
    """Reset stale persisted chart binding values that point at normalized.*."""
    if isinstance(value, str) and value.startswith("normalized."):
        return ""
    return value


def _scrub_chart_binding_text(value: Any) -> str:
    return str(scrub_legacy_normalized_binding_value(value) or "")


def _sanitize_chart_param_values(params: tuple[tuple[str, Any], ...]) -> tuple[tuple[str, Any], ...]:
    sanitized: list[tuple[str, Any]] = []
    for key, value in params:
        if key in {"field", "time_field"}:
            value = scrub_legacy_normalized_binding_value(value)
        sanitized.append((key, value))
    return tuple(sanitized)


def sanitize_chart_panel_spec(panel: ChartPanelSpec) -> ChartPanelSpec:
    """Return a panel with legacy normalized.* field/time bindings cleared."""
    bindings: list[ChartSeriesBinding] = []
    for binding in panel.series_bindings:
        inputs = tuple(
            replace(
                slot,
                time_field_name=_scrub_chart_binding_text(slot.time_field_name),
                field_name=_scrub_chart_binding_text(slot.field_name),
            )
            for slot in binding.input_bindings
        )
        bindings.append(
            replace(
                binding,
                input_bindings=inputs,
                param_values=_sanitize_chart_param_values(binding.param_values),
            )
        )
    base_feed = panel.base_feed
    if base_feed is not None:
        base_feed = replace(
            base_feed,
            time_field_name=_scrub_chart_binding_text(base_feed.time_field_name),
        )
    return replace(panel, series_bindings=tuple(bindings), base_feed=base_feed)


def _input_slot_from_entry(entry: Any) -> ChartInputSlot:
    if isinstance(entry, ChartInputSlot):
        return replace(
            entry,
            time_field_name=_scrub_chart_binding_text(entry.time_field_name),
            field_name=_scrub_chart_binding_text(entry.field_name),
        )
    if not isinstance(entry, dict):
        return ChartInputSlot(slot_name="")
    filtered = {k: str(v or "") for k, v in entry.items() if k in _CHART_INPUT_SLOT_FIELDS}
    if "time_field_name" in filtered:
        filtered["time_field_name"] = _scrub_chart_binding_text(filtered["time_field_name"])
    if "field_name" in filtered:
        filtered["field_name"] = _scrub_chart_binding_text(filtered["field_name"])
    return ChartInputSlot(**filtered)


def _migrate_legacy_binding(entry: dict[str, Any]) -> dict[str, Any]:
    """Convert a pre-step36 binding dict to the new indicator-first shape.

    Legacy bindings carried ``source_kind`` ∈ {raw, builtin, script}
    plus ``target_id``/``event_name``/``field_name``.  This function
    returns an entry that uses ``indicator_ref`` + ``input_bindings``
    + ``param_values`` instead.  Already-new entries pass through.
    """
    if "indicator_ref" in entry and entry.get("indicator_ref"):
        return entry
    source_kind = str(entry.get("source_kind") or "").strip().lower()
    if not source_kind:
        return entry  # nothing to migrate
    new_entry = {
        k: v for k, v in entry.items() if k in _CHART_SERIES_BINDING_FIELDS
    }
    new_entry["binding_id"] = entry.get("binding_id") or f"bind-{uuid.uuid4().hex[:10]}"
    new_entry.setdefault("axis", entry.get("axis") or "left")
    new_entry.setdefault("color", entry.get("color") or "")
    new_entry.setdefault("label", entry.get("label") or "")
    new_entry.setdefault("visible", bool(entry.get("visible", True)))
    if source_kind == "raw":
        target_id = str(entry.get("target_id") or entry.get("symbol") or "")
        event_name = str(entry.get("event_name") or "trade")
        field_name = _scrub_chart_binding_text(entry.get("field_name"))
        time_field_name = _scrub_chart_binding_text(entry.get("time_field_name"))
        new_entry["indicator_ref"] = "builtin.raw"
        new_entry["input_bindings"] = (
            {
                "slot_name": "source",
                "target_id": target_id,
                "event_name": event_name,
                "time_field_name": time_field_name,
                "field_name": field_name,
            },
        )
        params: list[tuple[str, Any]] = []
        if field_name:
            params.append(("field", field_name))
        if time_field_name:
            params.append(("time_field", time_field_name))
        if params:
            new_entry["param_values"] = tuple(params)
        new_entry["output_name"] = entry.get("output_name") or "value"
    else:  # builtin or script
        target_id = str(entry.get("target_id") or "")
        new_entry["indicator_ref"] = target_id or ("builtin.obi" if source_kind == "builtin" else "")
        new_entry["instance_id"] = target_id if source_kind == "script" else ""
        new_entry["output_name"] = entry.get("output_name") or "obi"
        new_entry["input_bindings"] = ()
        new_entry["param_values"] = ()
    return new_entry


def _series_binding_from_entry(entry: Any) -> ChartSeriesBinding:
    """Reconstruct a :class:`ChartSeriesBinding` from a persisted dict.

    Legacy snapshots are migrated in-place; unknown keys are ignored.
    """
    if isinstance(entry, ChartSeriesBinding):
        return entry
    if not isinstance(entry, dict):
        raise ValueError("series_binding entry must be a dict")
    migrated = _migrate_legacy_binding(entry)
    filtered: dict[str, Any] = {
        k: v for k, v in migrated.items() if k in _CHART_SERIES_BINDING_FIELDS
    }
    raw_inputs = filtered.get("input_bindings") or ()
    if isinstance(raw_inputs, (list, tuple)):
        filtered["input_bindings"] = tuple(_input_slot_from_entry(r) for r in raw_inputs)
    else:
        filtered["input_bindings"] = ()
    raw_params = filtered.get("param_values") or ()
    if isinstance(raw_params, dict):
        filtered["param_values"] = param_values_from_dict(raw_params)
    elif isinstance(raw_params, (list, tuple)):
        coerced: list[tuple[str, Any]] = []
        for pv in raw_params:
            if isinstance(pv, (list, tuple)) and len(pv) >= 2:
                coerced.append((str(pv[0]), pv[1]))
        filtered["param_values"] = tuple(coerced)
    else:
        filtered["param_values"] = ()
    filtered["param_values"] = _sanitize_chart_param_values(filtered["param_values"])
    filtered.setdefault("binding_id", f"bind-{uuid.uuid4().hex[:10]}")
    return ChartSeriesBinding(**filtered)


def _base_feed_from_entry(entry: Any) -> ChartPanelBaseFeed | None:
    if entry is None:
        return None
    if isinstance(entry, ChartPanelBaseFeed):
        return replace(entry, time_field_name=_scrub_chart_binding_text(entry.time_field_name))
    if not isinstance(entry, dict):
        return None
    return ChartPanelBaseFeed(
        target_id=str(entry.get("target_id") or ""),
        event_name=str(entry.get("event_name") or "ohlcv"),
        time_field_name=_scrub_chart_binding_text(entry.get("time_field_name")),
    )


def _script_spec_from_entry(entry: Any) -> IndicatorScriptSpec | None:
    if isinstance(entry, IndicatorScriptSpec):
        return entry
    if not isinstance(entry, dict):
        return None
    try:
        decl = entry.get("declaration")
        # Declaration round-trip is best-effort: drop on read errors so
        # custom panel scripts still load even if declaration shape drifts.
        return IndicatorScriptSpec(
            script_id=str(entry.get("script_id") or ""),
            name=str(entry.get("name") or ""),
            source=str(entry.get("source") or ""),
            class_name=str(entry.get("class_name") or ""),
            builtin=bool(entry.get("builtin", False)),
            description=entry.get("description") if isinstance(entry.get("description"), str) else None,
            declaration=decl if isinstance(decl, IndicatorDeclaration) else None,
        )
    except Exception:  # noqa: BLE001
        return None


def _instance_spec_from_entry(entry: Any) -> IndicatorInstanceSpec | None:
    if isinstance(entry, IndicatorInstanceSpec):
        return entry
    if not isinstance(entry, dict):
        return None
    try:
        return IndicatorInstanceSpec(
            instance_id=str(entry.get("instance_id") or ""),
            script_id=str(entry.get("script_id") or ""),
            symbol=str(entry.get("symbol") or ""),
            market_scope=str(entry.get("market_scope") or ""),
            params=entry.get("params") if isinstance(entry.get("params"), dict) else {},
            enabled=bool(entry.get("enabled", True)),
        )
    except Exception:  # noqa: BLE001
        return None


def _panel_spec_from_entry(entry: dict) -> ChartPanelSpec:
    """Reconstruct a :class:`ChartPanelSpec` tolerating legacy shapes."""
    filtered = {k: v for k, v in entry.items() if k in _CHART_PANEL_FIELDS}
    bindings = filtered.get("series_bindings")
    if bindings:
        filtered["series_bindings"] = tuple(
            _series_binding_from_entry(b) for b in bindings
        )
    else:
        filtered["series_bindings"] = ()
    filtered["base_feed"] = _base_feed_from_entry(filtered.get("base_feed"))
    raw_scripts = filtered.get("scripts") or ()
    if isinstance(raw_scripts, (list, tuple)):
        filtered["scripts"] = tuple(
            s for s in (_script_spec_from_entry(r) for r in raw_scripts) if s is not None
        )
    else:
        filtered["scripts"] = ()
    raw_instances = filtered.get("instances") or ()
    if isinstance(raw_instances, (list, tuple)):
        filtered["instances"] = tuple(
            i for i in (_instance_spec_from_entry(r) for r in raw_instances) if i is not None
        )
    else:
        filtered["instances"] = ()
    # Legacy panels: chart_type=='candle' with no base_feed but a first
    # raw binding can synthesise a base_feed for back-compat.
    if (
        filtered.get("chart_type") == "candle"
        and filtered.get("base_feed") is None
        and filtered["series_bindings"]
    ):
        first = filtered["series_bindings"][0]
        if first.indicator_ref == "builtin.raw" and first.input_bindings:
            slot = first.input_bindings[0]
            if (slot.event_name or "ohlcv") == "ohlcv":
                filtered["base_feed"] = ChartPanelBaseFeed(
                    target_id=slot.target_id, event_name="ohlcv", time_field_name=slot.time_field_name
                )
                filtered["series_bindings"] = filtered["series_bindings"][1:]
    return ChartPanelSpec(**filtered)
