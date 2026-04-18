"""Step 2 provider-aware tests for the Binance live (CCXT Pro) path.

These tests do not perform real network I/O; they monkeypatch
``BinanceLiveAdapter`` so the runtime's crypto-channel branch can be
exercised offline.  A separate live verification script (run manually)
covers the actual Binance public WebSocket path.
"""
from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest import mock


class CCXTSymbolNormalisationTests(unittest.TestCase):
    def test_spot_unified_symbol_from_concat(self) -> None:
        from packages.adapters.ccxt import to_unified_symbol

        self.assertEqual(to_unified_symbol("BTCUSDT", "spot"), "BTC/USDT")
        self.assertEqual(to_unified_symbol("ETHBTC", "spot"), "ETH/BTC")

    def test_perpetual_unified_symbol_from_concat(self) -> None:
        from packages.adapters.ccxt import to_unified_symbol

        self.assertEqual(to_unified_symbol("BTCUSDT", "perpetual"), "BTC/USDT:USDT")

    def test_passthrough_when_already_unified(self) -> None:
        from packages.adapters.ccxt import to_unified_symbol

        self.assertEqual(to_unified_symbol("BTC/USDT", "spot"), "BTC/USDT")
        self.assertEqual(to_unified_symbol("BTC/USDT:USDT", "perpetual"), "BTC/USDT:USDT")

    def test_exchange_id_for_instrument_type(self) -> None:
        from packages.adapters.ccxt import exchange_id_for

        self.assertEqual(exchange_id_for("spot"), "binance")
        self.assertEqual(exchange_id_for("perpetual"), "binanceusdm")


class RuntimeCryptoBranchTests(unittest.IsolatedAsyncioTestCase):
    async def _build_runtime_with_fake_adapter(self, trades, order_books):
        from apps.collector.runtime import CollectorRuntime

        events: list[dict[str, Any]] = []

        async def on_event(**kwargs):
            events.append(kwargs)

        runtime = CollectorRuntime(
            SimpleNamespace(app_key="k", app_secret="s"),
            on_event=on_event,
        )

        async def _noop_wait(**_):
            return None

        runtime._wait_session_ready = _noop_wait  # type: ignore[assignment]

        # Build a fake BinanceLiveAdapter that yields the supplied events.
        from packages.adapters.ccxt import BinanceLiveAdapter

        class _FakeAdapter:
            adapter_id = "fake"

            def __init__(self):
                self.closed = False

            async def stream_trades(self, *, symbol, instrument_type):
                for t in trades:
                    yield t
                # Park so the consumer does not race past the event drain.
                await asyncio.sleep(10)

            async def stream_order_book_snapshots(self, *, symbol, instrument_type, limit=10):
                for ob in order_books:
                    yield ob
                await asyncio.sleep(10)

            async def aclose(self):
                self.closed = True

        runtime._crypto_adapter = _FakeAdapter()  # type: ignore[assignment]
        return runtime, events

    async def test_runtime_accepts_ccxt_pro_spot_target_and_emits_event(self) -> None:
        from packages.adapters.ccxt import BinanceTrade
        from packages.domain.enums import Provider

        trade = BinanceTrade(
            symbol="BTC/USDT",
            price=Decimal("100000"),
            quantity=Decimal("0.01"),
            side="buy",
            occurred_at=datetime.now(timezone.utc),
            trade_id="t1",
        )
        runtime, events = await self._build_runtime_with_fake_adapter([trade], [])
        try:
            registration = await runtime.register_target(
                owner_id="t-spot",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("trade",),
                provider="ccxt_pro",  # deprecated alias — must collapse to ccxt
                instrument_type="spot",
            )
            # Externally, ccxt_pro is collapsed to ccxt.
            self.assertEqual(registration.provider, Provider.CCXT)
            self.assertEqual(
                registration.canonical_symbol,
                "ccxt:binance:crypto:spot:BTC/USDT",
            )

            for _ in range(50):
                if events:
                    break
                await asyncio.sleep(0.02)

            self.assertTrue(events, "expected at least one published event")
            ev = events[0]
            self.assertEqual(ev["event_name"], "trade_price")
            # Provider on outgoing payload must be the externally exposed
            # ``ccxt`` value — never ``ccxt_pro``.
            self.assertEqual(ev["provider"], "ccxt")
            self.assertNotEqual(ev["provider"], "ccxt_pro")
            self.assertEqual(
                ev["canonical_symbol"], "ccxt:binance:crypto:spot:BTC/USDT"
            )
            self.assertNotIn("ccxt_pro", ev["canonical_symbol"])
            self.assertEqual(ev["instrument_type"], "spot")
            self.assertIn("raw_symbol", ev)
        finally:
            await runtime.aclose()

    async def test_runtime_distinguishes_spot_and_perpetual_channels(self) -> None:
        from packages.adapters.ccxt import BinanceTrade

        spot_trade = BinanceTrade(
            symbol="BTC/USDT",
            price=Decimal("100000"),
            quantity=Decimal("0.01"),
            side="buy",
            occurred_at=datetime.now(timezone.utc),
            trade_id="s1",
        )
        runtime, events = await self._build_runtime_with_fake_adapter([spot_trade], [])
        try:
            await runtime.register_target(
                owner_id="t-spot",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("trade",),
                provider="ccxt",
                instrument_type="spot",
            )
            await runtime.register_target(
                owner_id="t-perp",
                symbol="BTCUSDT",
                market_scope="",
                event_types=("trade",),
                provider="ccxt",
                instrument_type="perpetual",
            )
            # Both registrations exist independently in the registry.
            self.assertEqual(
                runtime._registrations_by_owner["t-spot"].canonical_symbol,
                "ccxt:binance:crypto:spot:BTC/USDT",
            )
            self.assertEqual(
                runtime._registrations_by_owner["t-perp"].canonical_symbol,
                "ccxt:binance:crypto:perpetual:BTC/USDT:USDT",
            )
            # Two distinct crypto channels — spot/perpetual cannot collide.
            self.assertEqual(len(runtime._crypto_channels), 2)
        finally:
            await runtime.aclose()


class ControlPlaneCryptoMatchingTests(unittest.IsolatedAsyncioTestCase):
    async def _make_service(self):
        from src.collector_control_plane import CollectorControlPlaneService

        async def _noop_start(**_: Any) -> dict[str, object]:
            return {}

        async def _noop_stop(**_: Any) -> dict[str, object]:
            return {}

        return CollectorControlPlaneService(
            service_name="test",
            default_symbol="005930",
            default_market_scope="krx",
            start_publication=_noop_start,
            stop_publication=_noop_stop,
            is_publication_active=lambda _owner_id: False,
        )

    async def test_record_runtime_event_does_not_collide_spot_vs_perpetual(self) -> None:
        svc = await self._make_service()

        spot = await svc.upsert_target(
            target_id=None,
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
            raw_symbol="BTCUSDT",
        )
        perp = await svc.upsert_target(
            target_id=None,
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt_pro",  # legacy alias still accepted
            instrument_type="perpetual",
            raw_symbol="BTCUSDT",
        )
        # Different canonical_symbol → not deduplicated.
        self.assertNotEqual(spot["target"].target_id, perp["target"].target_id)

        await svc.record_runtime_event(
            symbol="BTC/USDT",
            market_scope="",
            event_name="trade",
            payload={"price": 1},
            provider="ccxt_pro",  # legacy alias still accepted on input
            canonical_symbol="ccxt:binance:crypto:perpetual:BTC/USDT:USDT",
        )

        recent = await svc.recent_events(limit=10)
        self.assertEqual(len(recent["recent_events"]), 1)
        ev = recent["recent_events"][0]
        # Only the perpetual target matched.
        self.assertEqual(ev.matched_target_ids, (perp["target"].target_id,))
        self.assertEqual(
            ev.canonical_symbol,
            "ccxt:binance:crypto:perpetual:BTC/USDT:USDT",
        )
        # External provider value must collapse ccxt_pro → ccxt.
        self.assertEqual(ev.provider, "ccxt")
        self.assertNotEqual(ev.provider, "ccxt_pro")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
