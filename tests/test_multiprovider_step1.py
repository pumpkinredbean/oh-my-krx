"""Smoke tests for step 1 multiprovider plumbing.

Verifies that:

1. :mod:`packages.domain.enums` exposes ``Provider`` and the new
   ``InstrumentType.SPOT`` / ``InstrumentType.PERPETUAL`` members.
2. :func:`packages.domain.models.build_canonical_symbol` produces the
   documented multiprovider identity shape.
3. The provider registry wires KXT + CCXT + CCXT Pro entries.
4. The control-plane defaults KRX targets to ``Provider.KXT`` and
   populates a canonical_symbol on both the instrument and the target.
5. The runtime rejects non-KXT providers with ``NotImplementedError``
   (step 1 scope: KXT path only is wired end-to-end).
"""
from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from typing import Any


class ProviderEnumTests(unittest.TestCase):
    def test_provider_enum_has_expected_members(self) -> None:
        from packages.domain.enums import Provider

        values = {member.value for member in Provider}
        # ccxt_pro stays in the enum as an internal alias but MUST NOT
        # be exposed externally — see external_provider_value().
        self.assertEqual(values, {"kxt", "ccxt", "ccxt_pro", "other"})

    def test_external_provider_value_collapses_ccxt_pro(self) -> None:
        from packages.domain.enums import Provider, external_provider_value

        self.assertEqual(external_provider_value(Provider.CCXT_PRO), "ccxt")
        self.assertEqual(external_provider_value("ccxt_pro"), "ccxt")
        self.assertEqual(external_provider_value(Provider.CCXT), "ccxt")
        self.assertEqual(external_provider_value(Provider.KXT), "kxt")
        self.assertEqual(external_provider_value(None), "kxt")

    def test_instrument_type_has_spot_and_perpetual(self) -> None:
        from packages.domain.enums import InstrumentType

        values = {member.value for member in InstrumentType}
        self.assertIn("spot", values)
        self.assertIn("perpetual", values)
        self.assertIn("future", values)
        self.assertIn("option", values)
        # Legacy labels still present (backwards compatibility).
        self.assertIn("equity", values)


class CanonicalSymbolTests(unittest.TestCase):
    def test_build_canonical_symbol_kxt_equity(self) -> None:
        from packages.domain.enums import AssetClass, InstrumentType, Provider, Venue
        from packages.domain.models import build_canonical_symbol

        canonical = build_canonical_symbol(
            provider=Provider.KXT,
            venue=Venue.KRX,
            asset_class=AssetClass.EQUITY,
            instrument_type=InstrumentType.SPOT,
            display_symbol="005930",
        )
        self.assertEqual(canonical, "kxt:krx:equity:spot:005930")

    def test_build_canonical_symbol_crypto_perpetual_includes_settle(self) -> None:
        from packages.domain.enums import AssetClass, InstrumentType, Provider, Venue
        from packages.domain.models import build_canonical_symbol

        # ccxt_pro input must collapse to ccxt at the canonical boundary.
        canonical = build_canonical_symbol(
            provider=Provider.CCXT_PRO,
            venue=Venue.BINANCE,
            asset_class=AssetClass.CRYPTO,
            instrument_type=InstrumentType.PERPETUAL,
            display_symbol="BTC/USDT",
            settle_asset="USDT",
        )
        self.assertEqual(canonical, "ccxt:binance:crypto:perpetual:BTC/USDT:USDT")
        self.assertNotIn("ccxt_pro", canonical)

    def test_build_canonical_symbol_crypto_spot(self) -> None:
        from packages.domain.enums import AssetClass, InstrumentType, Provider, Venue
        from packages.domain.models import build_canonical_symbol

        canonical = build_canonical_symbol(
            provider=Provider.CCXT,
            venue=Venue.BINANCE,
            asset_class=AssetClass.CRYPTO,
            instrument_type=InstrumentType.SPOT,
            display_symbol="BTC/USDT",
        )
        self.assertEqual(canonical, "ccxt:binance:crypto:spot:BTC/USDT")

    def test_build_canonical_symbol_handles_missing_axes(self) -> None:
        from packages.domain.models import build_canonical_symbol

        canonical = build_canonical_symbol(
            provider=None,
            venue=None,
            instrument_type=None,
            asset_class=None,
            display_symbol="SYM",
        )
        self.assertEqual(canonical, "unknown:unknown:unknown:unknown:SYM")


class ProviderRegistryTests(unittest.TestCase):
    def test_default_registry_only_exposes_external_providers(self) -> None:
        from packages.adapters import build_default_registry
        from packages.domain.enums import Provider

        registry = build_default_registry()
        providers = set(registry.providers())
        # Step 3: external surface is restricted to {kxt, ccxt}.
        # ccxt_pro is internal transport detail and must not appear.
        self.assertEqual(providers, {Provider.KXT, Provider.CCXT})
        self.assertNotIn(Provider.CCXT_PRO, providers)

    def test_registry_factories_build_stubs(self) -> None:
        from packages.adapters import build_default_registry
        from packages.domain.enums import Provider

        registry = build_default_registry()
        kxt = registry.require(Provider.KXT).factory()
        ccxt = registry.require(Provider.CCXT).factory()
        self.assertEqual(kxt.adapter_id, "kxt")
        self.assertEqual(ccxt.adapter_id, "ccxt")
        self.assertTrue(kxt.implemented)
        self.assertTrue(ccxt.implemented)


class ControlPlaneProviderWiringTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_upsert_target_defaults_to_kxt_and_populates_canonical_symbol(self) -> None:
        from packages.domain.enums import Provider

        svc = await self._make_service()
        result = await svc.upsert_target(
            target_id=None,
            symbol="005930",
            market_scope="krx",
            event_types=["trade"],
            enabled=False,
        )
        target = result["target"]
        # provider field defaults to KXT and canonical_symbol is populated.
        self.assertEqual(target.provider, Provider.KXT)
        self.assertEqual(target.canonical_symbol, "kxt:krx:equity:spot:005930")
        self.assertEqual(target.instrument.provider, Provider.KXT)
        self.assertEqual(target.instrument.canonical_symbol, "kxt:krx:equity:spot:005930")
        self.assertEqual(target.instrument.raw_symbol, "005930")

    async def test_upsert_target_accepts_non_krx_provider_with_empty_scope(self) -> None:
        from packages.domain.enums import Provider

        svc = await self._make_service()
        result = await svc.upsert_target(
            target_id=None,
            symbol="BTC/USDT",
            market_scope="",  # not applicable for crypto providers
            event_types=["trade"],
            enabled=False,
            provider="ccxt_pro",  # legacy alias — must collapse to ccxt
            instrument_type="perpetual",
            raw_symbol="BTCUSDT",
        )
        target = result["target"]
        # ccxt_pro input collapses to Provider.CCXT externally.
        self.assertEqual(target.provider, Provider.CCXT)
        self.assertEqual(target.market_scope, "")
        self.assertEqual(
            target.canonical_symbol,
            "ccxt:binance:crypto:perpetual:BTC/USDT:USDT",
        )
        self.assertNotIn("ccxt_pro", target.canonical_symbol)
        self.assertEqual(target.instrument.raw_symbol, "BTCUSDT")


class RuntimeProviderBranchTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_rejects_unknown_provider(self) -> None:
        from apps.collector.runtime import CollectorRuntime

        runtime = CollectorRuntime(SimpleNamespace(app_key="k", app_secret="s"))
        try:
            async def _noop_wait(**_):
                return None

            runtime._wait_session_ready = _noop_wait  # type: ignore[assignment]

            with self.assertRaises(NotImplementedError):
                await runtime.register_target(
                    owner_id="t-other",
                    symbol="BTCUSDT",
                    market_scope="",
                    event_types=("trade",),
                    provider="other",
                )
        finally:
            await runtime.aclose()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
