#!/usr/bin/env python3
"""Browser DOM verification for /admin/events configured event filters."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import Route, expect, sync_playwright


ROOT = Path(__file__).resolve().parents[1]
TARGET_ID = "events-e2e-binance-spot-btcusdt"
EVENT_TYPES = ["trade", "order_book_snapshot", "ticker", "ohlcv"]


def _event(event_id: str, event_name: str, seq: int) -> dict:
    payload = {
        "event_name": event_name,
        "provider": "ccxt",
        "venue": "binance",
        "symbol": "BTC/USDT",
        "instrument_type": "spot",
        "occurred_at": f"2026-04-26T00:00:{seq:02d}.000Z",
        "received_at": f"2026-04-26T00:00:{seq:02d}.100Z",
        "raw": {
            "bids": [[70000 - seq, 0.1]],
            "asks": [[70001 + seq, 0.2]],
            "timestamp": 1777161600000 + seq * 1000,
            "datetime": f"2026-04-26T00:00:{seq:02d}.000Z",
        },
    }
    if event_name != "order_book_snapshot":
        payload["raw"] = {"price": 70000 + seq, "qty": 0.01}
    return {
        "event_id": event_id,
        "topic_name": "market.dashboard-events.v1",
        "event_name": event_name,
        "symbol": "BTC/USDT",
        "market_scope": "",
        "published_at": f"2026-04-26T00:00:{seq:02d}.200Z",
        "matched_target_ids": [TARGET_ID],
        "payload": payload,
        "provider": "ccxt",
        "canonical_symbol": "ccxt:binance:crypto:spot:BTC/USDT",
        "instrument_type": "spot",
        "raw_symbol": "BTCUSDT",
    }


def _batch(events: list[dict], *, captured_at: str) -> dict:
    counts: dict[str, int] = {}
    last_seen: dict[str, str] = {}
    observed: list[str] = []
    for ev in events:
        name = ev["event_name"]
        counts[name] = counts.get(name, 0) + 1
        last_seen[name] = ev["published_at"]
        if name not in observed:
            observed.append(name)
    return {
        "new_events": events,
        "configured_event_names": EVENT_TYPES,
        "observed_event_names": observed,
        "event_counts": counts,
        "event_last_seen": last_seen,
        "buffer_size": len(events),
        "captured_at": captured_at,
    }


def _sse(batch: dict) -> str:
    return "event: connected\ndata: {}\n\n" + f"event: events\ndata: {json.dumps(batch)}\n\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--out", default="docs/verification/admin-events-e2e-latest")
    args = parser.parse_args()
    out = (ROOT / args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)

    all_events = [
        _event("obs-1", "order_book_snapshot", 1),
        _event("trade-1", "trade", 2),
        _event("ticker-1", "ticker", 3),
    ]
    orderbook_events = [_event("obs-filtered-1", "order_book_snapshot", 4), _event("obs-filtered-2", "order_book_snapshot", 5)]
    requested_streams: list[str] = []

    def route_api(route: Route) -> None:
        req = route.request
        url = req.url
        parsed = urlparse(url)
        if url.endswith("/api/admin/snapshot"):
            route.fulfill(json={
                "captured_at": "2026-04-26T00:00:00.000Z",
                "source_service": "collector",
                "event_type_catalog": [],
                "collection_targets": [{
                    "target_id": TARGET_ID,
                    "instrument": {"symbol": "BTC/USDT", "instrument_id": "BTCUSDT", "venue": "binance", "asset_class": "crypto", "instrument_type": "spot", "provider": "ccxt"},
                    "market_scope": "",
                    "event_types": EVENT_TYPES,
                    "owner_service": "collector",
                    "enabled": True,
                    "provider": "ccxt",
                    "canonical_symbol": "ccxt:binance:crypto:spot:BTC/USDT",
                }],
                "runtime_status": [],
                "collection_target_status": [],
                "source_runtime_status": [],
                "schema_version": "v1",
            })
        elif parsed.path == "/api/admin/events/stream":
            requested_streams.append(url)
            qs = parse_qs(parsed.query)
            name = qs.get("event_name", [""])[0]
            events = orderbook_events if name == "order_book_snapshot" else all_events
            route.fulfill(status=200, headers={"content-type": "text/event-stream"}, body=_sse(_batch(events, captured_at="2026-04-26T00:01:00.000Z")))
        else:
            route.continue_()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 980})
        page.route(re.compile(r".*/api/admin/.*"), route_api)
        target = f"{args.base_url.rstrip()}/admin/events"
        page.goto(target, wait_until="networkidle")

        selects = page.locator(".filter-bar select")
        expect(selects.nth(0)).to_be_visible()
        target_options = selects.nth(0).locator("option").evaluate_all("opts => opts.map(o => ({value: o.value, text: o.textContent}))")
        selects.nth(0).select_option(TARGET_ID)
        event_options = selects.nth(3).locator("option").evaluate_all("opts => opts.map(o => o.value).filter(Boolean)")
        expect(page.locator(".field-block").filter(has_text="Configured Events")).to_contain_text("order_book_snapshot")
        expect(page.locator(".field-block").filter(has_text="Observed Recent Events")).to_contain_text(re.compile("order_book_snapshot|최근 버퍼에 관측된 이벤트 없음"))
        selects.nth(3).select_option("order_book_snapshot")
        page.get_by_role("button", name="적용").click()
        expect(page.locator("tbody tr").filter(has_not=page.locator(".payload-pre"))).to_have_count(2)
        event_cell_text = page.locator("tbody tr:not(.payload-row) td:nth-child(2)").all_text_contents()
        page.get_by_role("button", name="페이로드").first.click()
        payload_text = page.locator(".payload-pre").first.text_content() or ""
        page.reload(wait_until="networkidle")
        selects = page.locator(".filter-bar select")
        selects.nth(0).select_option(TARGET_ID)
        reloaded_event_options = selects.nth(3).locator("option").evaluate_all("opts => opts.map(o => o.value).filter(Boolean)")
        normalized_option_count = page.locator('select option[value^="normalized"]').count()
        normalized_text_count = page.locator("body", has_text=re.compile(r"normalized(?:\.|\b)")).count()
        screenshot = out / "admin-events-e2e.png"
        page.screenshot(path=str(screenshot), full_page=True)
        browser.close()

    checks = {
        "targetSelectorVisible": True,
        "targetOptionsContainTarget": any(o["value"] == TARGET_ID for o in target_options),
        "eventOptionsEqualConfigured": event_options == EVENT_TYPES,
        "orderBookOptionExists": "order_book_snapshot" in event_options,
        "filteredRowsOrderBookOnly": event_cell_text and all(text.strip() == "order_book_snapshot" for text in event_cell_text),
        "payloadRawOrderBookOnly": "raw" in payload_text and "bids" in payload_text and "asks" in payload_text and "normalized" not in payload_text,
        "reloadRecoversConfiguredOptions": reloaded_event_options == EVENT_TYPES,
        "normalizedNotExposed": normalized_option_count == 0 and normalized_text_count == 0,
    }
    evidence = {
        "target": target,
        "target_id": TARGET_ID,
        "configured_event_types": EVENT_TYPES,
        "requested_streams": requested_streams,
        "screenshot": str(screenshot.relative_to(ROOT)),
        "checks": checks,
    }
    (out / "admin-events-e2e-evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    if not all(checks.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
