"""Tests for the /admin/charts/stream raw_event extension.

Verifies that raw runtime events (e.g. ``ohlcv``) pushed through the
collector control plane are fanned out on /admin/charts/stream as a
named ``raw_event`` SSE frame — this is the data source that drives
candle panels in the admin charts view.
"""
from __future__ import annotations

import asyncio
import json
import unittest
from datetime import datetime


class FormatRawEventFrameTests(unittest.TestCase):
    def test_frame_carries_symbol_timestamp_and_full_payload(self) -> None:
        from packages.contracts.admin import RecentRuntimeEvent
        from apps.collector.service import _format_admin_charts_raw_event_frame

        published_at = datetime(2026, 4, 21, 12, 0, 0)
        event = RecentRuntimeEvent(
            event_id="e1",
            topic_name="t",
            event_name="ohlcv",
            symbol="BTCUSDT",
            market_scope="total",
            published_at=published_at,
            payload={
                "open": 100.0,
                "high": 110.0,
                "low": 99.0,
                "close": 105.0,
                "volume": 12.5,
                "timeframe": "1m",
                "occurred_at": "2026-04-21T12:00:00+00:00",
            },
        )
        frame = _format_admin_charts_raw_event_frame(event)
        self.assertTrue(frame.startswith("event: raw_event\n"))
        data_line = next(
            line for line in frame.split("\n") if line.startswith("data: ")
        )
        data = json.loads(data_line[len("data: "):])
        self.assertEqual(data["symbol"], "BTCUSDT")
        self.assertEqual(data["event_name"], "ohlcv")
        self.assertEqual(data["timestamp"], "2026-04-21T12:00:00+00:00")
        self.assertIn("published_at", data)
        self.assertEqual(data["payload"]["open"], 100.0)
        self.assertEqual(data["payload"]["close"], 105.0)
        self.assertEqual(data["payload"]["volume"], 12.5)
        self.assertEqual(data["payload"]["timeframe"], "1m")

    def test_crypto_trade_raw_frame_cannot_sample_korean_display_fields(self) -> None:
        from packages.contracts.admin import RecentRuntimeEvent
        from apps.collector.service import _format_admin_charts_raw_event_frame

        event = RecentRuntimeEvent(
            event_id="e2",
            topic_name="t",
            event_name="trade",
            symbol="BTC/USDT",
            market_scope="",
            published_at=datetime(2026, 4, 21, 12, 0, 0),
            provider="ccxt",
            payload={
                "event_name": "trade",
                "provider": "ccxt",
                "occurred_at": "2026-04-21T12:00:00+00:00",
                "normalized": {"price": 100.0, "quantity": 0.01},
                "raw": {"id": "t1", "info": {"foo": "bar"}},
            },
        )
        frame = _format_admin_charts_raw_event_frame(event)
        for forbidden in ("체결", "호가", "잔량", "현재가", "거래량", "매수", "매도"):
            self.assertNotIn(forbidden, frame)
        data_line = next(line for line in frame.split("\n") if line.startswith("data: "))
        data = json.loads(data_line[len("data: "):])
        self.assertEqual(data["payload"]["raw"]["info"]["foo"], "bar")


class AdminChartsStreamRealFanOutTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end via the real control-plane subscribe_events() fan-out.

    We drive the same generator that /admin/charts/stream uses by calling
    the route handler and iterating its StreamingResponse body. This
    avoids mocking the fan-out — ``record_runtime_event`` enqueues to
    the real subscriber queue used by the generator.
    """

    async def test_ohlcv_is_emitted_as_raw_event_sse_frame(self) -> None:
        from apps.collector.service import admin_charts_stream, dashboard_service

        await dashboard_service.start()
        try:
            class _Req:
                async def is_disconnected(self) -> bool:
                    return False

            response = await admin_charts_stream(_Req())  # type: ignore[arg-type]
            iterator = response.body_iterator

            # First chunk: the `connected` preface.
            preface = await asyncio.wait_for(iterator.__anext__(), timeout=5.0)
            if isinstance(preface, bytes):
                preface = preface.decode()
            self.assertIn("event: connected", preface)

            # Start pulling the next chunk so the generator enters its
            # ``async with subscribe_events()`` and registers a real
            # subscriber before we push the event.  Without this, a race
            # would drop the event onto zero subscribers.
            next_chunk_task = asyncio.create_task(iterator.__anext__())
            # Let the generator reach its await inside asyncio.wait().
            for _ in range(3):
                await asyncio.sleep(0)

            # Push a real ohlcv event via the real control plane.
            await dashboard_service._control_plane.record_runtime_event(  # noqa: SLF001
                symbol="BTCUSDT",
                market_scope="total",
                event_name="ohlcv",
                payload={
                    "open": 100.0,
                    "high": 110.0,
                    "low": 99.0,
                    "close": 105.0,
                    "volume": 1.0,
                    "timeframe": "1m",
                    "occurred_at": "2026-04-21T12:00:00+00:00",
                },
            )

            # Drain until we get a raw_event frame.
            raw_frame = None
            first = await asyncio.wait_for(next_chunk_task, timeout=5.0)
            if isinstance(first, bytes):
                first = first.decode()
            chunks = [first]
            for _ in range(4):
                if any(c.startswith("event: raw_event") for c in chunks):
                    break
                more = await asyncio.wait_for(iterator.__anext__(), timeout=5.0)
                if isinstance(more, bytes):
                    more = more.decode()
                chunks.append(more)
            for c in chunks:
                if c.startswith("event: raw_event"):
                    raw_frame = c
                    break

            self.assertIsNotNone(raw_frame, "raw_event frame was not emitted")
            assert raw_frame is not None
            data_line = next(
                ln for ln in raw_frame.split("\n") if ln.startswith("data: ")
            )
            data = json.loads(data_line[len("data: "):])
            self.assertEqual(data["symbol"], "BTCUSDT")
            self.assertEqual(data["event_name"], "ohlcv")
            self.assertEqual(data["timestamp"], "2026-04-21T12:00:00+00:00")
            self.assertEqual(data["payload"]["close"], 105.0)
            self.assertEqual(data["payload"]["timeframe"], "1m")

            # Close the generator cleanly.
            await iterator.aclose()
        finally:
            await dashboard_service.aclose()

    async def test_non_forwarded_event_names_are_suppressed(self) -> None:
        from apps.collector.service import admin_charts_stream, dashboard_service

        await dashboard_service.start()
        try:
            class _Req:
                async def is_disconnected(self) -> bool:
                    return False

            response = await admin_charts_stream(_Req())  # type: ignore[arg-type]
            iterator = response.body_iterator
            preface = await asyncio.wait_for(iterator.__anext__(), timeout=5.0)
            if isinstance(preface, bytes):
                preface = preface.decode()
            self.assertIn("event: connected", preface)

            # Allow subscribe_events() to register before pushing.
            next_chunk_task = asyncio.create_task(iterator.__anext__())
            for _ in range(3):
                await asyncio.sleep(0)

            # Non-chart event name (orderbook); should NOT be forwarded.
            await dashboard_service._control_plane.record_runtime_event(  # noqa: SLF001
                symbol="005930",
                market_scope="total",
                event_name="orderbook",
                payload={"bid": 1, "ask": 2},
            )

            # Next non-heartbeat chunk should NOT be raw_event; we expect
            # only keepalive comments for up to ~1 heartbeat cycle.  To
            # keep the test fast we just assert the first emitted frame
            # within a short window is not a raw_event.
            try:
                chunk = await asyncio.wait_for(next_chunk_task, timeout=1.0)
                if isinstance(chunk, bytes):
                    chunk = chunk.decode()
                self.assertFalse(
                    chunk.startswith("event: raw_event"),
                    f"unexpected raw_event emitted for suppressed name: {chunk!r}",
                )
            except asyncio.TimeoutError:
                next_chunk_task.cancel()  # no emission is also correct

            await iterator.aclose()
        finally:
            await dashboard_service.aclose()


if __name__ == "__main__":
    unittest.main()
