#!/usr/bin/env python3
"""Live smoke check for raw-only order_book_snapshot admin events."""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]


def fetch_json(url: str) -> dict:
    with urlopen(url, timeout=10) as response:  # noqa: S310 - local/admin URL supplied by operator
        return json.loads(response.read().decode("utf-8"))


def has_normalized(value: object) -> bool:
    if isinstance(value, dict):
        return any(k == "normalized" or k.startswith("normalized.") or has_normalized(v) for k, v in value.items())
    if isinstance(value, list):
        return any(has_normalized(v) for v in value)
    return False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--out", default="docs/verification/live-orderbook-smoke-latest")
    args = parser.parse_args()

    out = (ROOT / args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    base_url = args.base_url.rstrip("/")
    snapshot = fetch_json(f"{base_url}/api/admin/snapshot")
    targets = [t for t in snapshot.get("collection_targets", []) if t.get("enabled") and "order_book_snapshot" in t.get("event_types", [])]
    target_id = args.target_id or (targets[0]["target_id"] if targets else "")
    if not target_id:
        evidence = {"ok": False, "blocker": "no enabled target configured for order_book_snapshot", "snapshot": snapshot}
        (out / "live-orderbook-smoke-evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        print(json.dumps(evidence, indent=2))
        raise SystemExit(2)

    params = {"target_id": target_id, "event_name": "order_book_snapshot", "limit": "50"}
    url = f"{base_url}/api/admin/events?{urlencode(params)}"
    samples: list[dict] = []
    deadline = time.monotonic() + args.seconds
    while True:
        payload = fetch_json(url)
        samples.append({
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "event_count": payload.get("event_counts", {}).get("order_book_snapshot", 0),
            "last_seen": payload.get("event_last_seen", {}).get("order_book_snapshot"),
            "row_count": len(payload.get("recent_events", [])),
            "first_event_id": (payload.get("recent_events") or [{}])[0].get("event_id"),
        })
        if time.monotonic() >= deadline:
            final_payload = payload
            break
        time.sleep(args.interval)

    events = final_payload.get("recent_events", [])
    row_count = len(events)
    first_payload = (events[0] if events else {}).get("payload", {})
    raw = first_payload.get("raw", {}) if isinstance(first_payload, dict) else {}
    counts = [s["event_count"] for s in samples]
    last_seen_values = [s["last_seen"] for s in samples]
    checks = {
        "enabledTargetFound": True,
        "rowCountGreaterThanZero": row_count > 0,
        "countIncreasesOrLastSeenUpdates": (max(counts) > min(counts)) or (len(set(last_seen_values)) > 1),
        "rawBidsAsksPresent": isinstance(raw, dict) and bool(raw.get("bids")) and bool(raw.get("asks")),
        "normalizedAbsent": not has_normalized(first_payload) and "normalized" not in json.dumps(final_payload),
        "allRowsAreOrderBook": row_count > 0 and all(ev.get("event_name") == "order_book_snapshot" for ev in events),
    }
    evidence = {
        "ok": all(checks.values()),
        "target_id": target_id,
        "configured_event_types": next((t.get("event_types") for t in targets if t.get("target_id") == target_id), None),
        "url": url,
        "duration_seconds": args.seconds,
        "samples": samples,
        "checks": checks,
        "final_event_counts": final_payload.get("event_counts", {}),
        "final_event_last_seen": final_payload.get("event_last_seen", {}),
        "first_event_payload_keys": sorted(first_payload.keys()) if isinstance(first_payload, dict) else [],
        "first_raw_keys": sorted(raw.keys()) if isinstance(raw, dict) else [],
    }
    (out / "live-orderbook-smoke-evidence.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    if not evidence["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
