from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import docker
import httpx
import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from packages.contracts.topics import DASHBOARD_CONTROL_TOPIC, DASHBOARD_EVENTS_TOPIC
from packages.infrastructure.kafka import AsyncKafkaJsonBroker
from packages.shared.config import load_service_settings

app = FastAPI(title="KIS Program Trade Realtime")
service_settings = load_service_settings("api-web")
dashboard_broker = AsyncKafkaJsonBroker(service_settings.bootstrap_servers)
collector_base_url = os.getenv("COLLECTOR_BASE_URL", "http://127.0.0.1:8001").rstrip("/")
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "korea-market-data-hub")
COLLECTOR_SERVICE_NAME = os.getenv("COLLECTOR_SERVICE_NAME", "collector")
WEB_DIR = Path(__file__).resolve().parent / "web"
ADMIN_DIST_DIR = WEB_DIR / "admin_dist"

app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
app.mount("/admin/assets", StaticFiles(directory=ADMIN_DIST_DIR / "assets", check_dir=False), name="admin-assets")


def _resolve_admin_index() -> Path:
    admin_spa_index = ADMIN_DIST_DIR / "index.html"
    if admin_spa_index.exists():
        return admin_spa_index
    return WEB_DIR / "admin.html"


def _serialize_sse_lines(lines: list[str]) -> str:
    return "".join(f"{line}\n" for line in lines)


def _collector_request(
    method: str,
    path: str,
    **kwargs: Any,
) -> requests.Response:
    with requests.Session() as session:
        session.trust_env = False
        response = session.request(method, f"{collector_base_url}{path}", **kwargs)
        return response


def _resolve_market_scope(*, scope: str | None = None, market: str | None = None) -> str:
    resolved = (scope or market or "total").strip().lower()
    if resolved not in {"krx", "nxt", "total"}:
        raise ValueError(f"unsupported market scope: {scope or market}")
    return resolved


def _build_dashboard_group_id(*, symbol: str, market_scope: str) -> str:
    subscription_key = f"{market_scope.lower()}-{symbol}"
    return f"api-web-dashboard-{subscription_key}-{uuid.uuid4().hex}"


async def _publish_dashboard_control(*, action: str, owner_id: str, symbol: str, market_scope: str) -> None:
    await dashboard_broker.publish(
        topic=DASHBOARD_CONTROL_TOPIC,
        key=owner_id,
        value={
            "action": action,
            "owner_id": owner_id,
            "symbol": symbol,
            "market_scope": market_scope.lower(),
            "market": market_scope.lower(),
            "requested_at": datetime.utcnow().isoformat(),
            "schema_version": "v1",
        },
    )


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/admin")
async def admin_index() -> FileResponse:
    return FileResponse(_resolve_admin_index())


@app.get("/admin/")
async def admin_index_slash() -> FileResponse:
    return FileResponse(_resolve_admin_index())


@app.get("/admin/{path:path}")
async def admin_spa_catchall(path: str) -> FileResponse:
    """Serve the admin SPA index for all client-side routes under /admin/."""
    # Only catch non-asset paths so the static file mount keeps working.
    return FileResponse(_resolve_admin_index())


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"ok": True})


@app.on_event("shutdown")
async def shutdown_runtime() -> None:
    await dashboard_broker.aclose()


@app.get("/api/price-chart")
async def price_chart(
    symbol: str = Query(..., min_length=1),
    scope: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    market: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    interval: int = Query(..., ge=1, le=60),
) -> JSONResponse:
    try:
        market_scope = _resolve_market_scope(scope=scope, market=market)
        response = await asyncio.to_thread(
            _collector_request,
            "GET",
            "/api/price-chart",
            params={"symbol": symbol, "scope": market_scope, "interval": interval},
            timeout=15,
        )
        response.raise_for_status()
        return JSONResponse(response.json())
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None:
            try:
                payload = response.json()
            except ValueError:
                payload = {"error": response.text or str(exc)}
            return JSONResponse(payload, status_code=response.status_code)
        return JSONResponse({"error": str(exc)}, status_code=502)
    except requests.RequestException as exc:
        return JSONResponse({"error": f"collector price-chart relay failed: {exc}"}, status_code=502)


async def _relay_collector_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_payload: dict[str, Any] | None = None,
    timeout: int = 15,
) -> JSONResponse:
    try:
        response = await asyncio.to_thread(
            _collector_request,
            method,
            path,
            params=params,
            json=json_payload,
            timeout=timeout,
        )
        response.raise_for_status()
        return JSONResponse(response.json())
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None:
            try:
                payload = response.json()
            except ValueError:
                payload = {"error": response.text or str(exc)}
            return JSONResponse(payload, status_code=response.status_code)
        return JSONResponse({"error": str(exc)}, status_code=502)
    except requests.RequestException as exc:
        return JSONResponse({"error": f"collector relay failed: {exc}"}, status_code=502)


@app.get("/api/admin/snapshot")
async def admin_snapshot() -> JSONResponse:
    """Relay snapshot from the collector.

    If the collector is unreachable *and* its container is intentionally
    stopped (exited / not_found), return HTTP 200 with ``collector_offline``
    so the UI can render a graceful degraded state instead of a top-banner
    error.  If the container appears to be running but the relay fails,
    fall through to a 502 so the real failure is still surfaced.
    """
    try:
        response = await asyncio.to_thread(
            _collector_request, "GET", "/admin/snapshot", timeout=15
        )
        response.raise_for_status()
        return JSONResponse(response.json())
    except requests.HTTPError as exc:
        resp = exc.response
        if resp is not None:
            try:
                payload = resp.json()
            except ValueError:
                payload = {"error": resp.text or str(exc)}
            return JSONResponse(payload, status_code=resp.status_code)
        return JSONResponse({"error": str(exc)}, status_code=502)
    except requests.RequestException:
        # Collector is unreachable.  Check whether the container is
        # intentionally offline before deciding to surface an error.
        container = await asyncio.to_thread(_collector_container_status)
        offline_statuses = {"exited", "not_found", "created", "dead"}
        if container.get("status") in offline_statuses:
            return JSONResponse(
                {
                    "collector_offline": True,
                    "collection_targets": [],
                    "runtime_status": [],
                    "collection_target_status": [],
                    "event_type_catalog": [],
                    "captured_at": None,
                    "source_service": "api-web",
                    "container_status": container.get("status"),
                }
            )
        # Container is supposedly running but relay failed — real unexpected error.
        return JSONResponse(
            {"error": f"collector relay failed (container: {container.get('status')})"},
            status_code=502,
        )


# ─── Collector container lifecycle (via Docker socket) ────────────────────────

def _get_collector_container() -> docker.models.containers.Container | None:  # type: ignore[name-defined]
    """Return the collector container object or None if not found."""
    try:
        client = docker.from_env()
        containers = client.containers.list(all=True, filters={
            "label": [
                f"com.docker.compose.project={COMPOSE_PROJECT}",
                f"com.docker.compose.service={COLLECTOR_SERVICE_NAME}",
            ]
        })
        if containers:
            return containers[0]
        return None
    except Exception:
        return None


def _collector_container_status() -> dict[str, Any]:
    """Return a status dict for the collector container; never raises."""
    try:
        client = docker.from_env()
        containers = client.containers.list(all=True, filters={
            "label": [
                f"com.docker.compose.project={COMPOSE_PROJECT}",
                f"com.docker.compose.service={COLLECTOR_SERVICE_NAME}",
            ]
        })
        if not containers:
            return {"status": "not_found", "container_id": None, "name": None}
        c = containers[0]
        c.reload()
        return {
            "status": c.status,  # running / exited / paused / etc.
            "container_id": c.short_id,
            "name": c.name,
        }
    except Exception as exc:
        return {"status": "error", "container_id": None, "name": None, "error": str(exc)}


@app.get("/api/admin/collector/status")
async def collector_container_status() -> JSONResponse:
    """Return the Docker container state of the collector service.

    This endpoint is implemented directly in api-web (not relayed to the
    collector) so it continues to work even when the collector is stopped.
    """
    result = await asyncio.to_thread(_collector_container_status)
    return JSONResponse(result)


@app.post("/api/admin/collector/start")
async def collector_container_start() -> JSONResponse:
    """Start the collector Docker container."""
    def _start() -> dict[str, Any]:
        try:
            client = docker.from_env()
            containers = client.containers.list(all=True, filters={
                "label": [
                    f"com.docker.compose.project={COMPOSE_PROJECT}",
                    f"com.docker.compose.service={COLLECTOR_SERVICE_NAME}",
                ]
            })
            if not containers:
                return {"ok": False, "error": "collector container not found"}
            c = containers[0]
            c.start()
            c.reload()
            return {"ok": True, "status": c.status, "container_id": c.short_id, "name": c.name}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    result = await asyncio.to_thread(_start)
    status_code = 200 if result.get("ok") else 502
    return JSONResponse(result, status_code=status_code)


@app.post("/api/admin/collector/stop")
async def collector_container_stop() -> JSONResponse:
    """Stop the collector Docker container."""
    def _stop() -> dict[str, Any]:
        try:
            client = docker.from_env()
            containers = client.containers.list(all=True, filters={
                "label": [
                    f"com.docker.compose.project={COMPOSE_PROJECT}",
                    f"com.docker.compose.service={COLLECTOR_SERVICE_NAME}",
                ]
            })
            if not containers:
                return {"ok": False, "error": "collector container not found"}
            c = containers[0]
            c.stop(timeout=10)
            c.reload()
            return {"ok": True, "status": c.status, "container_id": c.short_id, "name": c.name}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    result = await asyncio.to_thread(_stop)
    status_code = 200 if result.get("ok") else 502
    return JSONResponse(result, status_code=status_code)


@app.get("/api/admin/instruments")
async def admin_instrument_search(
    query: str = Query(..., min_length=1),
    scope: str | None = Query(None, pattern="^(krx|nxt|total)?$"),
    market: str | None = Query(None, pattern="^(krx|nxt|total)?$"),
    provider: str | None = Query(None, pattern="^(kxt|ccxt|ccxt_pro)$"),
    instrument_type: str | None = Query(None),
) -> JSONResponse:
    provider_text = (provider or "kxt").strip().lower()
    if provider_text == "ccxt_pro":
        # ccxt_pro is internal-only; collapse to ccxt at the boundary.
        provider_text = "ccxt"
    params: dict[str, Any] = {"query": query, "provider": provider_text}
    try:
        if provider_text == "kxt":
            params["scope"] = _resolve_market_scope(scope=scope, market=market)
        else:
            scoped = (scope or market or "").strip().lower()
            if scoped:
                params["scope"] = scoped
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    if instrument_type:
        params["instrument_type"] = instrument_type
    return await _relay_collector_json("GET", "/admin/instruments", params=params)


@app.put("/api/admin/targets")
async def admin_upsert_target(request: Request) -> JSONResponse:
    return await _relay_collector_json("PUT", "/admin/targets", json_payload=await request.json())


@app.delete("/api/admin/targets/{target_id}")
async def admin_delete_target(target_id: str) -> JSONResponse:
    return await _relay_collector_json("DELETE", f"/admin/targets/{target_id}")


@app.post("/api/admin/providers/{provider}/start")
async def admin_provider_start(provider: str) -> JSONResponse:
    if provider not in {"kxt", "ccxt"}:
        return JSONResponse({"error": f"unsupported provider: {provider}"}, status_code=400)
    return await _relay_collector_json("POST", f"/admin/providers/{provider}/start")


@app.post("/api/admin/providers/{provider}/stop")
async def admin_provider_stop(provider: str) -> JSONResponse:
    if provider not in {"kxt", "ccxt"}:
        return JSONResponse({"error": f"unsupported provider: {provider}"}, status_code=400)
    return await _relay_collector_json("POST", f"/admin/providers/{provider}/stop")


@app.get("/api/admin/events")
async def admin_recent_events(
    target_id: str | None = Query(None),
    symbol: str | None = Query(None),
    scope: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    market: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    event_name: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    params: dict[str, Any] = {"limit": limit}
    if target_id:
        params["target_id"] = target_id
    if symbol:
        params["symbol"] = symbol
    if event_name:
        params["event_name"] = event_name
    try:
        if scope or market:
            params["scope"] = _resolve_market_scope(scope=scope, market=market)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return await _relay_collector_json("GET", "/admin/events", params=params)


@app.get("/api/admin/events/stream")
async def admin_events_stream(
    request: Request,
    target_id: str | None = Query(None),
    symbol: str | None = Query(None),
    scope: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    market: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    event_name: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
) -> StreamingResponse:
    """SSE relay: proxies the collector push stream verbatim — no polling."""

    params: dict[str, Any] = {"limit": limit}
    if target_id:
        params["target_id"] = target_id
    if symbol:
        params["symbol"] = symbol
    if event_name:
        params["event_name"] = event_name
    try:
        if scope or market:
            params["scope"] = _resolve_market_scope(scope=scope, market=market)
    except ValueError as exc:
        async def _error_gen() -> Any:
            yield f"event: upstream_error\ndata: {json.dumps({'error': str(exc)}, ensure_ascii=False)}\n\n"
        return StreamingResponse(_error_gen(), media_type="text/event-stream")

    async def event_generator() -> Any:
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "GET",
                    f"{collector_base_url}/admin/events/stream",
                    params=params,
                    timeout=httpx.Timeout(connect=10.0, read=None, write=None, pool=None),
                ) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_text():
                        if await request.is_disconnected():
                            return
                        if chunk:
                            yield chunk
        except httpx.HTTPStatusError as exc:
            error_payload = {"error": f"collector stream error: {exc.response.status_code}"}
            yield f"event: upstream_error\ndata: {json.dumps(error_payload, ensure_ascii=False)}\n\n"
        except Exception as exc:
            error_payload = {"error": f"collector stream relay failed: {exc}"}
            yield f"event: upstream_error\ndata: {json.dumps(error_payload, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ─── Admin Charts relay routes (step 1) ──────────────────────────────────


@app.get("/api/admin/charts/panels")
async def admin_charts_list_panels_relay() -> JSONResponse:
    return await _relay_collector_json("GET", "/admin/charts/panels")


@app.put("/api/admin/charts/panels")
async def admin_charts_upsert_panel_relay(request: Request) -> JSONResponse:
    return await _relay_collector_json("PUT", "/admin/charts/panels", json_payload=await request.json())


@app.delete("/api/admin/charts/panels/{panel_id}")
async def admin_charts_delete_panel_relay(panel_id: str) -> JSONResponse:
    return await _relay_collector_json("DELETE", f"/admin/charts/panels/{panel_id}")


@app.get("/api/admin/charts/scripts")
async def admin_charts_list_scripts_relay() -> JSONResponse:
    return await _relay_collector_json("GET", "/admin/charts/scripts")


@app.put("/api/admin/charts/scripts")
async def admin_charts_upsert_script_relay(request: Request) -> JSONResponse:
    return await _relay_collector_json("PUT", "/admin/charts/scripts", json_payload=await request.json())


@app.delete("/api/admin/charts/scripts/{script_id}")
async def admin_charts_delete_script_relay(script_id: str) -> JSONResponse:
    return await _relay_collector_json("DELETE", f"/admin/charts/scripts/{script_id}")


@app.get("/api/admin/charts/instances")
async def admin_charts_list_instances_relay() -> JSONResponse:
    return await _relay_collector_json("GET", "/admin/charts/instances")


@app.put("/api/admin/charts/instances")
async def admin_charts_upsert_instance_relay(request: Request) -> JSONResponse:
    return await _relay_collector_json("PUT", "/admin/charts/instances", json_payload=await request.json())


@app.delete("/api/admin/charts/instances/{instance_id}")
async def admin_charts_delete_instance_relay(instance_id: str) -> JSONResponse:
    return await _relay_collector_json("DELETE", f"/admin/charts/instances/{instance_id}")


@app.get("/api/admin/charts/errors")
async def admin_charts_errors_relay() -> JSONResponse:
    return await _relay_collector_json("GET", "/admin/charts/errors")


@app.get("/api/admin/charts/stream")
async def admin_charts_stream_relay(request: Request) -> StreamingResponse:
    async def event_generator() -> Any:
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "GET",
                    f"{collector_base_url}/admin/charts/stream",
                    timeout=httpx.Timeout(connect=10.0, read=None, write=None, pool=None),
                ) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_text():
                        if await request.is_disconnected():
                            return
                        if chunk:
                            yield chunk
        except Exception as exc:  # noqa: BLE001
            payload = {"error": f"collector charts stream relay failed: {exc}"}
            yield f"event: upstream_error\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/stream")
async def stream(
    request: Request,
    symbol: str = Query(..., min_length=1),
    scope: str | None = Query(None, pattern="^(krx|nxt|total)$"),
    market: str | None = Query(None, pattern="^(krx|nxt|total)$"),
) -> StreamingResponse:
    market_scope = _resolve_market_scope(scope=scope, market=market)

    async def event_generator() -> Any:
        queue: asyncio.Queue[str | BaseException | None] = asyncio.Queue()
        control_owner_id = uuid.uuid4().hex
        consumer_task: asyncio.Task[Any] | None = None
        disconnect_task: asyncio.Task[Any] | None = None

        async def watch_disconnect() -> None:
            while not await request.is_disconnected():
                await asyncio.sleep(0.25)
            await queue.put(None)

        async def pump_dashboard_events() -> None:
            try:
                async with dashboard_broker.open_subscription(
                    topic=DASHBOARD_EVENTS_TOPIC,
                    group_id=_build_dashboard_group_id(symbol=symbol, market_scope=market_scope),
                ) as consumer:
                    await _publish_dashboard_control(
                        action="start",
                        owner_id=control_owner_id,
                        symbol=symbol,
                        market_scope=market_scope,
                    )

                    async for message in consumer:
                        if await request.is_disconnected():
                            return

                        payload = message.value if isinstance(message.value, dict) else None
                        if payload is None:
                            continue
                        if payload.get("symbol") != symbol:
                            continue
                        payload_market_scope = str(payload.get("market_scope") or payload.get("market") or "").lower()
                        if payload_market_scope != market_scope.lower():
                            continue

                        event_name = payload.get("event_name")
                        event_payload = payload.get("payload")
                        if not isinstance(event_name, str) or not isinstance(event_payload, dict):
                            continue

                        await queue.put(
                            _serialize_sse_lines(
                                [
                                    f"event: {event_name}",
                                    f"data: {json.dumps(event_payload, ensure_ascii=False)}",
                                    "",
                                ]
                            )
                        )
            except Exception as exc:
                await queue.put(exc)
            finally:
                with contextlib.suppress(Exception):
                    await _publish_dashboard_control(
                        action="stop",
                        owner_id=control_owner_id,
                        symbol=symbol,
                        market_scope=market_scope,
                    )
                await queue.put(None)

        consumer_task = asyncio.create_task(pump_dashboard_events())
        disconnect_task = asyncio.create_task(watch_disconnect())

        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                if isinstance(item, BaseException):
                    raise item
                yield item
        except Exception as exc:
            if not await request.is_disconnected():
                error_payload = {"error": str(exc)}
                yield f"event: error\ndata: {json.dumps(error_payload, ensure_ascii=False)}\n\n"
        finally:
            if consumer_task is not None:
                consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await consumer_task
            if disconnect_task is not None:
                disconnect_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await disconnect_task

    return StreamingResponse(event_generator(), media_type="text/event-stream")
