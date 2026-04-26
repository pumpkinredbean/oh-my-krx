from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path


class AdminEventsMetadataTests(unittest.IsolatedAsyncioTestCase):
    async def _make_service(self):
        from src.collector_control_plane import CollectorControlPlaneService

        active: set[str] = set()

        async def _start(**kwargs):
            active.add(str(kwargs["owner_id"]))
            return {"subscription_id": kwargs["owner_id"], "status": "started"}

        async def _stop(*, subscription_id: str):
            active.discard(subscription_id)
            return {"subscription_id": subscription_id, "status": "stopped"}

        return CollectorControlPlaneService(
            service_name="collector",
            default_symbol="005930",
            default_market_scope="krx",
            start_publication=_start,
            stop_publication=_stop,
            is_publication_active=lambda owner_id: owner_id in active,
        )

    async def test_configured_target_events_drive_options_even_without_recent_samples(self) -> None:
        svc = await self._make_service()
        upsert = await svc.upsert_target(
            target_id=None,
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade", "order_book_snapshot", "ticker", "ohlcv"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
            raw_symbol="BTCUSDT",
        )
        target_id = upsert["target"].target_id

        await svc.record_runtime_event(
            symbol="BTC/USDT",
            market_scope="",
            event_name="trade",
            payload={"price": "1"},
            provider="ccxt",
            canonical_symbol="ccxt:binance:crypto:spot:BTC/USDT",
            instrument_type="spot",
            raw_symbol="BTCUSDT",
        )

        recent = await svc.recent_events(target_id=target_id, limit=50)

        self.assertEqual(
            recent["configured_event_names"],
            ("trade", "order_book_snapshot", "ticker", "ohlcv"),
        )
        self.assertIn("order_book_snapshot", recent["configured_event_names"])
        self.assertEqual(recent["observed_event_names"], ("trade",))
        self.assertEqual(recent["event_counts"], {"trade": 1})
        self.assertIn("trade", recent["event_last_seen"])
        self.assertNotIn("order_book_snapshot", recent["event_last_seen"])

    async def test_configured_union_uses_enabled_targets_without_target_filter(self) -> None:
        svc = await self._make_service()
        await svc.upsert_target(
            target_id="enabled-target",
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade", "order_book_snapshot"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
        )
        await svc.upsert_target(
            target_id="disabled-target",
            symbol="ETH/USDT",
            market_scope="",
            event_types=["ticker"],
            enabled=False,
            provider="ccxt",
            instrument_type="spot",
        )

        recent = await svc.recent_events(limit=50)

        self.assertEqual(recent["configured_event_names"], ("trade", "order_book_snapshot"))
        self.assertEqual(recent["observed_event_names"], ())

    def test_sse_initial_batch_includes_configured_names_when_no_events(self) -> None:
        from apps.collector.service import _admin_events_batch_from_recent

        batch = _admin_events_batch_from_recent(
            {
                "recent_events": (),
                "available_event_names": (),
                "configured_event_names": ("trade", "order_book_snapshot"),
                "observed_event_names": (),
                "event_counts": {},
                "event_last_seen": {},
                "buffer_size": 0,
                "captured_at": datetime(2026, 1, 1),
            }
        )

        self.assertEqual(batch["new_events"], [])
        self.assertEqual(batch["configured_event_names"], ("trade", "order_book_snapshot"))
        self.assertEqual(batch["observed_event_names"], [])


class AdminEventsFrontendInvariantTests(unittest.TestCase):
    def test_frontend_uses_target_configuration_not_available_names_for_options(self) -> None:
        source = Path("apps/admin_web/src/App.tsx").read_text()

        self.assertIn("target_id", source)
        self.assertIn("configuredOptionNames.map", source)
        self.assertIn("Configured Events", source)
        self.assertIn("Observed Recent Events", source)
        self.assertNotIn("setAvailableNames", source)
        self.assertNotIn("availableNames.map", source)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
