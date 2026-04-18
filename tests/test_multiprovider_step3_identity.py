"""Step 3 contract tests for external identity normalisation and the
capability-driven event catalog.

Pins:

* External provider values are restricted to ``kxt`` and ``ccxt`` —
  ``ccxt_pro`` MUST NOT appear on outward-facing contracts (target,
  snapshot, recent event, canonical_symbol).
* :func:`build_canonical_symbol` produces the new 5-axis format
  ``provider:venue:asset_class:instrument_type:display_symbol[:settle_asset]``.
* ``InstrumentRef.raw_symbol`` is split from the unified/display ``symbol``.
* The control-plane snapshot exposes a ``source_capabilities`` matrix
  keyed by (provider, venue, asset_class, instrument_type) — Binance spot
  must NOT advertise mark_price/funding_rate/open_interest while Binance
  perpetual must, and KXT KRX equity advertises program_trade.
* Capability-gated event-type validation rejects events the source
  cannot expose (e.g. mark_price on Binance spot, program_trade on
  Binance perpetual).
"""
from __future__ import annotations

import unittest
from typing import Any


class ExternalIdentityNormalisationTests(unittest.TestCase):
    def test_no_ccxt_pro_leak_in_canonical(self) -> None:
        from packages.domain.enums import AssetClass, InstrumentType, Provider, Venue
        from packages.domain.models import build_canonical_symbol

        canonical = build_canonical_symbol(
            provider=Provider.CCXT_PRO,
            venue=Venue.BINANCE,
            asset_class=AssetClass.CRYPTO,
            instrument_type=InstrumentType.PERPETUAL,
            display_symbol="BTC/USDT",
            settle_asset="USDT",
        )
        self.assertNotIn("ccxt_pro", canonical)
        self.assertTrue(canonical.startswith("ccxt:"))

    def test_instrument_ref_raw_symbol_field_present(self) -> None:
        from packages.domain.models import InstrumentRef

        ref = InstrumentRef(symbol="BTC/USDT", raw_symbol="BTCUSDT")
        self.assertEqual(ref.symbol, "BTC/USDT")
        self.assertEqual(ref.raw_symbol, "BTCUSDT")


class CapabilityMatrixTests(unittest.IsolatedAsyncioTestCase):
    async def _make_service(self):
        from src.collector_control_plane import CollectorControlPlaneService

        async def _noop(**_: Any) -> dict[str, object]:
            return {}

        return CollectorControlPlaneService(
            service_name="test",
            default_symbol="005930",
            default_market_scope="krx",
            start_publication=_noop,
            stop_publication=_noop,
            is_publication_active=lambda _o: False,
        )

    async def test_snapshot_advertises_source_capabilities(self) -> None:
        svc = await self._make_service()
        snap = await svc.snapshot()
        caps = {(c.provider, c.venue, c.instrument_type): set(c.supported_event_types) for c in snap.source_capabilities}

        # KXT KRX equity exposes program_trade.
        self.assertIn(("kxt", "krx", "spot"), caps)
        self.assertIn("program_trade", caps[("kxt", "krx", "spot")])

        # Binance spot does NOT expose mark_price / funding_rate / open_interest.
        self.assertIn(("ccxt", "binance", "spot"), caps)
        spot_events = caps[("ccxt", "binance", "spot")]
        self.assertNotIn("mark_price", spot_events)
        self.assertNotIn("funding_rate", spot_events)
        self.assertNotIn("open_interest", spot_events)
        # No ccxt_pro provider key in capability matrix.
        self.assertFalse(any(c.provider == "ccxt_pro" for c in snap.source_capabilities))

        # Binance perpetual exposes the derivatives quartet.
        self.assertIn(("ccxt", "binance", "perpetual"), caps)
        perp_events = caps[("ccxt", "binance", "perpetual")]
        for required in ("mark_price", "funding_rate", "open_interest"):
            self.assertIn(required, perp_events)

    async def test_capability_gating_rejects_program_trade_on_binance(self) -> None:
        svc = await self._make_service()
        with self.assertRaises(ValueError) as cm:
            await svc.upsert_target(
                target_id=None,
                symbol="BTC/USDT",
                market_scope="",
                event_types=["program_trade"],
                enabled=False,
                provider="ccxt",
                instrument_type="spot",
            )
        self.assertIn("program_trade", str(cm.exception))

    async def test_capability_gating_rejects_mark_price_on_binance_spot(self) -> None:
        svc = await self._make_service()
        with self.assertRaises(ValueError):
            await svc.upsert_target(
                target_id=None,
                symbol="BTC/USDT",
                market_scope="",
                event_types=["mark_price"],
                enabled=False,
                provider="ccxt",
                instrument_type="spot",
            )

    async def test_capability_allows_mark_price_on_binance_perpetual(self) -> None:
        svc = await self._make_service()
        result = await svc.upsert_target(
            target_id=None,
            symbol="BTC/USDT",
            market_scope="",
            event_types=["mark_price", "funding_rate", "trade"],
            enabled=False,
            provider="ccxt_pro",  # legacy alias must still work and collapse to ccxt
            instrument_type="perpetual",
        )
        target = result["target"]
        # External provider value collapsed.
        self.assertEqual(target.provider.value, "ccxt")
        self.assertNotIn("ccxt_pro", target.canonical_symbol)
        self.assertIn("perpetual", target.canonical_symbol)


class NoCcxtProLeakInRecordedEventTests(unittest.IsolatedAsyncioTestCase):
    async def test_record_runtime_event_externalises_provider(self) -> None:
        from src.collector_control_plane import CollectorControlPlaneService

        async def _noop(**_: Any) -> dict[str, object]:
            return {}

        svc = CollectorControlPlaneService(
            service_name="t",
            default_symbol="005930",
            default_market_scope="krx",
            start_publication=_noop,
            stop_publication=_noop,
            is_publication_active=lambda _: False,
        )
        await svc.record_runtime_event(
            symbol="BTC/USDT",
            market_scope="",
            event_name="trade",
            payload={"price": 1},
            provider="ccxt_pro",  # legacy input
            canonical_symbol="ccxt:binance:crypto:spot:BTC/USDT",
            instrument_type="spot",
            raw_symbol="BTCUSDT",
        )
        recent = await svc.recent_events(limit=10)
        ev = recent["recent_events"][0]
        self.assertEqual(ev.provider, "ccxt")
        self.assertNotEqual(ev.provider, "ccxt_pro")
        self.assertEqual(ev.raw_symbol, "BTCUSDT")
        self.assertEqual(ev.instrument_type, "spot")


class ProviderRegistryNoCcxtProLeakTests(unittest.TestCase):
    def test_registry_does_not_advertise_ccxt_pro(self) -> None:
        from packages.adapters import build_default_registry
        from packages.domain.enums import Provider

        providers = set(build_default_registry().providers())
        self.assertNotIn(Provider.CCXT_PRO, providers)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
