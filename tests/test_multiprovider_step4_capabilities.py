"""Step 4 tests for expanded live capabilities on the CCXT / Binance path.

Covers adapter parsers, runtime dispatch for new dataclasses, and the
control-plane capability matrix.  All tests are offline — no ccxt.pro
WebSocket, no real Binance connections.
"""
from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any


# ---------------------------------------------------------------------------
# Adapter-level: drive generators once against a fake ccxt.pro exchange
# ---------------------------------------------------------------------------


class _FakeExchange:
    def __init__(
        self,
        *,
        ticker: dict | None = None,
        ohlcv: list | None = None,
        mark_price: dict | None = None,
        funding_rate: dict | None = None,
        open_interest: dict | None = None,
    ) -> None:
        self._ticker = ticker
        self._ohlcv = ohlcv
        self._mark_price = mark_price
        self._funding_rate = funding_rate
        self._open_interest = open_interest

    async def watch_ticker(self, symbol):  # noqa: ARG002
        return self._ticker

    async def watchOHLCV(self, symbol, timeframe):  # noqa: ARG002, N802
        return self._ohlcv

    async def watch_mark_price(self, symbol):  # noqa: ARG002
        return self._mark_price

    async def fetch_funding_rate(self, symbol):  # noqa: ARG002
        return self._funding_rate

    async def fetch_open_interest(self, symbol):  # noqa: ARG002
        return self._open_interest

    async def close(self):
        return None


class AdapterCapabilityParserTests(unittest.IsolatedAsyncioTestCase):
    async def _drive_once(self, gen):
        async for value in gen:
            return value
        return None

    async def test_stream_tickers_parses(self) -> None:
        from packages.adapters.ccxt import BinanceLiveAdapter, BinanceTicker

        adapter = BinanceLiveAdapter()
        fake = _FakeExchange(ticker={
            "symbol": "BTC/USDT",
            "timestamp": 1700000000000,
            "last": "100000.0",
            "bid": "99999.0",
            "ask": "100001.0",
            "bidVolume": "1.5",
            "askVolume": "2.0",
            "high": "101000",
            "low": "99000",
            "baseVolume": "10",
            "quoteVolume": "1000000",
        })

        async def _ex(_it):
            return fake

        adapter._exchange = _ex  # type: ignore[assignment]
        event = await self._drive_once(
            adapter.stream_tickers(symbol="BTCUSDT", instrument_type="spot")
        )
        await adapter.aclose()
        assert isinstance(event, BinanceTicker)
        self.assertEqual(event.symbol, "BTC/USDT")
        self.assertEqual(event.instrument_type, "spot")
        self.assertEqual(event.last, Decimal("100000.0"))
        self.assertEqual(event.bid, Decimal("99999.0"))

    async def test_stream_ohlcv_parses_latest_bar(self) -> None:
        from packages.adapters.ccxt import BinanceBar, BinanceLiveAdapter

        adapter = BinanceLiveAdapter()
        bars = [
            [1700000000000, "100", "101", "99", "100.5", "10"],
            [1700000060000, "100.5", "102", "100", "101.5", "12"],
        ]
        fake = _FakeExchange(ohlcv=bars)

        async def _ex(_it):
            return fake

        adapter._exchange = _ex  # type: ignore[assignment]
        event = await self._drive_once(
            adapter.stream_ohlcv(symbol="BTCUSDT", instrument_type="spot", timeframe="1m")
        )
        await adapter.aclose()
        assert isinstance(event, BinanceBar)
        self.assertEqual(event.timeframe, "1m")
        self.assertEqual(event.open_time_ms, 1700000060000)
        self.assertEqual(event.close, Decimal("101.5"))

    async def test_stream_mark_price_requires_perpetual(self) -> None:
        from packages.adapters.ccxt import BinanceLiveAdapter

        adapter = BinanceLiveAdapter()
        with self.assertRaises(ValueError):
            async for _ in adapter.stream_mark_price(symbol="BTCUSDT", instrument_type="spot"):
                break
        await adapter.aclose()

    async def test_stream_mark_price_parses(self) -> None:
        from packages.adapters.ccxt import BinanceLiveAdapter, BinanceMarkPrice

        adapter = BinanceLiveAdapter()
        fake = _FakeExchange(mark_price={
            "symbol": "BTC/USDT:USDT",
            "timestamp": 1700000000000,
            "info": {
                "markPrice": "100123.45",
                "indexPrice": "100100.10",
                "r": "0.00012",
                "T": "1700028800000",
                "E": 1700000000000,
            },
        })

        async def _ex(_it):
            return fake

        adapter._exchange = _ex  # type: ignore[assignment]
        event = await self._drive_once(
            adapter.stream_mark_price(symbol="BTCUSDT", instrument_type="perpetual")
        )
        await adapter.aclose()
        assert isinstance(event, BinanceMarkPrice)
        self.assertEqual(event.instrument_type, "perpetual")
        self.assertEqual(event.mark_price, Decimal("100123.45"))
        self.assertEqual(event.index_price, Decimal("100100.10"))
        self.assertEqual(event.funding_rate, Decimal("0.00012"))
        self.assertEqual(event.next_funding_timestamp_ms, 1700028800000)
        self.assertEqual(event.funding_timestamp_ms, 1700000000000)

    async def test_poll_funding_rate_parses(self) -> None:
        from packages.adapters.ccxt import BinanceFundingRate, BinanceLiveAdapter

        adapter = BinanceLiveAdapter()
        fake = _FakeExchange(funding_rate={
            "symbol": "BTC/USDT:USDT",
            "timestamp": 1700000000000,
            "fundingRate": "0.0001",
            "fundingTimestamp": 1700000000000,
            "nextFundingTimestamp": 1700028800000,
        })

        async def _ex(_it):
            return fake

        adapter._exchange = _ex  # type: ignore[assignment]
        # Use interval=0 to keep the test quick.
        gen = adapter.poll_funding_rate(symbol="BTCUSDT", instrument_type="perpetual", interval=0.0)
        event = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await gen.aclose()
        await adapter.aclose()
        assert isinstance(event, BinanceFundingRate)
        self.assertEqual(event.funding_rate, Decimal("0.0001"))
        self.assertEqual(event.funding_timestamp_ms, 1700000000000)

    async def test_poll_open_interest_parses(self) -> None:
        from packages.adapters.ccxt import BinanceLiveAdapter, BinanceOpenInterest

        adapter = BinanceLiveAdapter()
        fake = _FakeExchange(open_interest={
            "symbol": "BTC/USDT:USDT",
            "timestamp": 1700000000000,
            "openInterestAmount": "12345.6",
            "openInterestValue": "1234567890.0",
        })

        async def _ex(_it):
            return fake

        adapter._exchange = _ex  # type: ignore[assignment]
        gen = adapter.poll_open_interest(symbol="BTCUSDT", instrument_type="perpetual", interval=0.0)
        event = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await gen.aclose()
        await adapter.aclose()
        assert isinstance(event, BinanceOpenInterest)
        self.assertEqual(event.open_interest_amount, Decimal("12345.6"))
        self.assertEqual(event.open_interest_value, Decimal("1234567890.0"))


# ---------------------------------------------------------------------------
# Runtime-level: push dataclasses through the crypto channel and observe
# the published envelope.
# ---------------------------------------------------------------------------


class RuntimeCapabilityDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def _runtime_with_events(self, events_to_yield):
        from apps.collector.runtime import CollectorRuntime

        captured: list[dict[str, Any]] = []

        async def on_event(**kwargs):
            captured.append(kwargs)

        runtime = CollectorRuntime(
            SimpleNamespace(app_key="k", app_secret="s"),
            on_event=on_event,
        )

        async def _noop_wait(**_):
            return None

        runtime._wait_session_ready = _noop_wait  # type: ignore[assignment]

        class _FakeAdapter:
            adapter_id = "fake"

            def __init__(self):
                self.closed = False

            async def stream_trades(self, *, symbol, instrument_type):  # noqa: ARG002
                await asyncio.sleep(10)
                if False:
                    yield

            async def stream_order_book_snapshots(self, *, symbol, instrument_type, limit=10):  # noqa: ARG002
                await asyncio.sleep(10)
                if False:
                    yield

            async def stream_tickers(self, *, symbol, instrument_type):  # noqa: ARG002
                for ev in events_to_yield.get("ticker", []):
                    yield ev
                await asyncio.sleep(10)

            async def stream_ohlcv(self, *, symbol, instrument_type, timeframe="1m"):  # noqa: ARG002
                for ev in events_to_yield.get("ohlcv", []):
                    yield ev
                await asyncio.sleep(10)

            async def stream_mark_price(self, *, symbol, instrument_type):  # noqa: ARG002
                for ev in events_to_yield.get("mark_price", []):
                    yield ev
                await asyncio.sleep(10)

            async def poll_funding_rate(self, *, symbol, instrument_type, interval=30.0):  # noqa: ARG002
                for ev in events_to_yield.get("funding_rate", []):
                    yield ev
                await asyncio.sleep(10)

            async def poll_open_interest(self, *, symbol, instrument_type, interval=30.0):  # noqa: ARG002
                for ev in events_to_yield.get("open_interest", []):
                    yield ev
                await asyncio.sleep(10)

            async def aclose(self):
                self.closed = True

        runtime._crypto_adapter = _FakeAdapter()  # type: ignore[assignment]
        return runtime, captured

    async def _wait_for_event(self, captured, event_name):
        for _ in range(100):
            for ev in captured:
                if ev["event_name"] == event_name:
                    return ev
            await asyncio.sleep(0.02)
        return None

    async def test_spot_ticker_traverses_runtime(self) -> None:
        from packages.adapters.ccxt import BinanceTicker

        tick = BinanceTicker(
            symbol="BTC/USDT",
            instrument_type="spot",
            occurred_at=datetime.now(timezone.utc),
            last=Decimal("100000"),
            bid=Decimal("99999"),
            ask=Decimal("100001"),
            bid_size=Decimal("1"),
            ask_size=Decimal("2"),
            high=Decimal("101000"),
            low=Decimal("99000"),
            base_volume=Decimal("10"),
            quote_volume=Decimal("1000000"),
        )
        runtime, captured = await self._runtime_with_events({"ticker": [tick]})
        try:
            await runtime.register_target(
                owner_id="t-tk",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("ticker",),
                provider="ccxt",
                instrument_type="spot",
            )
            ev = await self._wait_for_event(captured, "ticker")
            self.assertIsNotNone(ev, "expected ticker event to be published")
            self.assertEqual(ev["provider"], "ccxt")
            self.assertEqual(ev["instrument_type"], "spot")
            self.assertEqual(
                ev["canonical_symbol"], "ccxt:binance:crypto:spot:BTC/USDT"
            )
            self.assertEqual(ev["payload"]["last"], 100000)
        finally:
            await runtime.aclose()

    async def test_perpetual_mark_price_traverses_runtime(self) -> None:
        from packages.adapters.ccxt import BinanceMarkPrice

        mark = BinanceMarkPrice(
            symbol="BTC/USDT:USDT",
            instrument_type="perpetual",
            occurred_at=datetime.now(timezone.utc),
            mark_price=Decimal("100123.45"),
            index_price=Decimal("100100.10"),
        )
        runtime, captured = await self._runtime_with_events({"mark_price": [mark]})
        try:
            await runtime.register_target(
                owner_id="t-mp",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("mark_price",),
                provider="ccxt",
                instrument_type="perpetual",
            )
            ev = await self._wait_for_event(captured, "mark_price")
            self.assertIsNotNone(ev, "expected mark_price event to be published")
            self.assertEqual(ev["provider"], "ccxt")
            self.assertEqual(ev["instrument_type"], "perpetual")
            self.assertEqual(
                ev["canonical_symbol"], "ccxt:binance:crypto:perpetual:BTC/USDT:USDT"
            )
            self.assertEqual(ev["payload"]["mark_price"], float(Decimal("100123.45")))
        finally:
            await runtime.aclose()

    async def test_perpetual_ohlcv_traverses_runtime(self) -> None:
        from packages.adapters.ccxt import BinanceBar

        bar = BinanceBar(
            symbol="BTC/USDT:USDT",
            instrument_type="perpetual",
            timeframe="1m",
            open_time_ms=1700000060000,
            occurred_at=datetime.now(timezone.utc),
            open=Decimal("100"),
            high=Decimal("102"),
            low=Decimal("99"),
            close=Decimal("101"),
            volume=Decimal("12.5"),
        )
        runtime, captured = await self._runtime_with_events({"ohlcv": [bar]})
        try:
            await runtime.register_target(
                owner_id="t-bar",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("ohlcv",),
                provider="ccxt",
                instrument_type="perpetual",
            )
            ev = await self._wait_for_event(captured, "ohlcv")
            self.assertIsNotNone(ev)
            self.assertEqual(ev["payload"]["timeframe"], "1m")
            self.assertEqual(ev["payload"]["close"], 101)
        finally:
            await runtime.aclose()

    async def test_perpetual_funding_and_open_interest_traverse_runtime(self) -> None:
        from packages.adapters.ccxt import BinanceMarkPrice, BinanceOpenInterest

        mark_with_funding_fields = BinanceMarkPrice(
            symbol="BTC/USDT:USDT",
            instrument_type="perpetual",
            occurred_at=datetime.now(timezone.utc),
            mark_price=Decimal("100123.45"),
            index_price=Decimal("100100.10"),
            funding_rate=Decimal("0.0001"),
            funding_timestamp_ms=1700000000000,
            next_funding_timestamp_ms=1700028800000,
        )
        oi = BinanceOpenInterest(
            symbol="BTC/USDT:USDT",
            instrument_type="perpetual",
            occurred_at=datetime.now(timezone.utc),
            open_interest_amount=Decimal("12345"),
            open_interest_value=Decimal("1234567890"),
        )
        runtime, captured = await self._runtime_with_events(
            {"mark_price": [mark_with_funding_fields], "open_interest": [oi]}
        )
        try:
            await runtime.register_target(
                owner_id="t-fr",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("funding_rate", "open_interest"),
                provider="ccxt",
                instrument_type="perpetual",
            )
            fr_ev = await self._wait_for_event(captured, "funding_rate")
            oi_ev = await self._wait_for_event(captured, "open_interest")
            self.assertIsNotNone(fr_ev)
            self.assertIsNotNone(oi_ev)
            self.assertEqual(fr_ev["instrument_type"], "perpetual")
            self.assertEqual(fr_ev["payload"]["funding_rate"], float(Decimal("0.0001")))
            self.assertEqual(fr_ev["payload"]["next_funding_timestamp_ms"], 1700028800000)
            self.assertEqual(oi_ev["instrument_type"], "perpetual")
        finally:
            await runtime.aclose()

    async def test_mark_price_and_funding_share_single_mark_price_source(self) -> None:
        from apps.collector.runtime import CollectorRuntime
        from packages.adapters.ccxt import BinanceMarkPrice

        mark_with_funding_fields = BinanceMarkPrice(
            symbol="BTC/USDT:USDT",
            instrument_type="perpetual",
            occurred_at=datetime.now(timezone.utc),
            mark_price=Decimal("100123.45"),
            index_price=Decimal("100100.10"),
            funding_rate=Decimal("0.0001"),
            funding_timestamp_ms=1700000000000,
            next_funding_timestamp_ms=1700028800000,
        )

        captured: list[dict[str, Any]] = []

        async def on_event(**kwargs):
            captured.append(kwargs)

        runtime = CollectorRuntime(
            SimpleNamespace(app_key="k", app_secret="s"),
            on_event=on_event,
        )

        async def _noop_wait(**_):
            return None

        runtime._wait_session_ready = _noop_wait  # type: ignore[assignment]

        stream_call_count = {"n": 0}

        class _CountingAdapter:
            adapter_id = "fake"

            def __init__(self):
                self.closed = False

            async def stream_mark_price(self, *, symbol, instrument_type):  # noqa: ARG002
                stream_call_count["n"] += 1
                yield mark_with_funding_fields
                await asyncio.sleep(10)

            async def aclose(self):
                self.closed = True

        runtime._crypto_adapter = _CountingAdapter()  # type: ignore[assignment]
        try:
            await runtime.register_target(
                owner_id="t-share-1",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("mark_price", "funding_rate"),
                provider="ccxt",
                instrument_type="perpetual",
            )
            mp_ev = await self._wait_for_event(captured, "mark_price")
            fr_ev = await self._wait_for_event(captured, "funding_rate")
            self.assertIsNotNone(mp_ev)
            self.assertIsNotNone(fr_ev)
            self.assertEqual(stream_call_count["n"], 1)

            # Second owner asking only for funding_rate on same canonical must reuse.
            await runtime.register_target(
                owner_id="t-share-2",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("funding_rate",),
                provider="ccxt",
                instrument_type="perpetual",
            )
            # Give the runtime a moment to potentially start another source.
            await asyncio.sleep(0.05)
            self.assertEqual(stream_call_count["n"], 1)
        finally:
            await runtime.aclose()


# ---------------------------------------------------------------------------
# Control-plane capability matrix assertions
# ---------------------------------------------------------------------------


class ControlPlaneMatrixTests(unittest.TestCase):
    def test_binance_perpetual_advertises_mark_and_funding_and_oi(self) -> None:
        from src.collector_control_plane import capability_for

        cap = capability_for(provider="ccxt", venue="binance", instrument_type="perpetual")
        self.assertIsNotNone(cap)
        supported = set(cap.supported_event_types)
        self.assertIn("ticker", supported)
        self.assertIn("ohlcv", supported)
        self.assertIn("mark_price", supported)
        self.assertIn("funding_rate", supported)
        self.assertIn("open_interest", supported)

    def test_binance_spot_advertises_ticker_ohlcv(self) -> None:
        from src.collector_control_plane import capability_for

        cap = capability_for(provider="ccxt", venue="binance", instrument_type="spot")
        self.assertIsNotNone(cap)
        supported = set(cap.supported_event_types)
        self.assertIn("ticker", supported)
        self.assertIn("ohlcv", supported)
        # spot must NOT expose perpetual-only types.
        self.assertNotIn("mark_price", supported)

    def test_event_type_enum_has_all_names(self) -> None:
        from packages.contracts.events import EventType

        names = {e.value for e in EventType}
        for required in (
            "trade",
            "order_book_snapshot",
            "program_trade",
            "ticker",
            "ohlcv",
            "mark_price",
            "funding_rate",
            "open_interest",
        ):
            self.assertIn(required, names)

    def test_event_type_aliases_include_ergonomic_shortcuts(self) -> None:
        from src.collector_control_plane import EVENT_TYPE_ALIASES

        self.assertEqual(EVENT_TYPE_ALIASES["mark"], "mark_price")
        self.assertEqual(EVENT_TYPE_ALIASES["funding"], "funding_rate")
        self.assertEqual(EVENT_TYPE_ALIASES["oi"], "open_interest")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
