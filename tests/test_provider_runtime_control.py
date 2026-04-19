"""Provider-level runtime control step1 tests.

Validates that kxt and ccxt are surfaced as first-class logical runtime
units in the control-plane snapshot with an enable/disable affordance
that preserves target rows and keeps the collector container behavior
separate.
"""

from __future__ import annotations

import unittest
from typing import Any


class _RuntimeStub:
    """In-process stand-in for apps.collector.runtime publication callbacks.

    Records which owner_ids are currently "publishing" so tests can
    assert start/stop behavior without standing up the real runtime.
    """

    def __init__(self) -> None:
        self.active: set[str] = set()
        self.start_calls: list[dict[str, Any]] = []
        self.stop_calls: list[dict[str, Any]] = []

    async def start(self, **kwargs: Any) -> dict[str, object]:
        self.start_calls.append(kwargs)
        owner_id = str(kwargs["owner_id"])
        self.active.add(owner_id)
        return {"owner_id": owner_id}

    async def stop(self, *, subscription_id: str) -> dict[str, object]:
        self.stop_calls.append({"subscription_id": subscription_id})
        self.active.discard(subscription_id)
        return {"subscription_id": subscription_id}

    def is_active(self, owner_id: str) -> bool:
        return owner_id in self.active


class ProviderRuntimeControlTests(unittest.IsolatedAsyncioTestCase):
    async def _make(self) -> tuple[Any, _RuntimeStub]:
        from src.collector_control_plane import CollectorControlPlaneService

        runtime = _RuntimeStub()
        svc = CollectorControlPlaneService(
            service_name="test",
            default_symbol="005930",
            default_market_scope="krx",
            start_publication=runtime.start,
            stop_publication=runtime.stop,
            is_publication_active=runtime.is_active,
        )
        return svc, runtime

    async def test_snapshot_exposes_both_providers_default_enabled(self) -> None:
        svc, _ = await self._make()
        snap = await svc.snapshot()
        rows = {row.provider: row for row in snap.source_runtime_status}
        self.assertEqual(set(rows), {"kxt", "ccxt"})
        for row in rows.values():
            self.assertTrue(row.enabled)
            self.assertEqual(row.active_target_count, 0)
            self.assertEqual(row.state, "stopped")
            self.assertEqual(row.schema_version, "v1")
        # runtime_status remains present (additive field check).
        self.assertTrue(len(snap.runtime_status) >= 1)

    async def test_disabling_ccxt_stops_ccxt_targets_preserves_rows(self) -> None:
        svc, runtime = await self._make()
        await svc.upsert_target(
            target_id="k1",
            symbol="005930",
            market_scope="krx",
            event_types=["trade"],
            enabled=True,
            provider="kxt",
        )
        await svc.upsert_target(
            target_id="c1",
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
        )
        self.assertTrue(runtime.is_active("k1"))
        self.assertTrue(runtime.is_active("c1"))

        await svc.set_provider_enabled("ccxt", False)

        # ccxt target publication released, but row preserved with enabled=True.
        self.assertFalse(runtime.is_active("c1"))
        self.assertTrue(runtime.is_active("k1"))  # kxt untouched
        snap = await svc.snapshot()
        targets_by_id = {t.target_id: t for t in snap.collection_targets}
        self.assertIn("c1", targets_by_id)
        self.assertTrue(targets_by_id["c1"].enabled)

        rows = {row.provider: row for row in snap.source_runtime_status}
        self.assertFalse(rows["ccxt"].enabled)
        self.assertEqual(rows["ccxt"].state, "disabled")
        self.assertEqual(rows["ccxt"].active_target_count, 0)
        self.assertTrue(rows["kxt"].enabled)
        self.assertEqual(rows["kxt"].state, "running")
        self.assertEqual(rows["kxt"].active_target_count, 1)

    async def test_reenabling_ccxt_restarts_publication(self) -> None:
        svc, runtime = await self._make()
        await svc.upsert_target(
            target_id="c1",
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
        )
        await svc.set_provider_enabled("ccxt", False)
        self.assertFalse(runtime.is_active("c1"))

        await svc.set_provider_enabled("ccxt", True)
        self.assertTrue(runtime.is_active("c1"))

        snap = await svc.snapshot()
        rows = {row.provider: row for row in snap.source_runtime_status}
        self.assertTrue(rows["ccxt"].enabled)
        self.assertEqual(rows["ccxt"].state, "running")
        self.assertEqual(rows["ccxt"].active_target_count, 1)

    async def test_disabling_kxt_does_not_affect_ccxt(self) -> None:
        svc, runtime = await self._make()
        await svc.upsert_target(
            target_id="k1",
            symbol="005930",
            market_scope="krx",
            event_types=["trade"],
            enabled=True,
            provider="kxt",
        )
        await svc.upsert_target(
            target_id="c1",
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
        )
        await svc.set_provider_enabled("kxt", False)
        self.assertFalse(runtime.is_active("k1"))
        self.assertTrue(runtime.is_active("c1"))

    async def test_upsert_on_disabled_provider_persists_but_does_not_activate(self) -> None:
        svc, runtime = await self._make()
        await svc.set_provider_enabled("ccxt", False)

        result = await svc.upsert_target(
            target_id="c1",
            symbol="BTC/USDT",
            market_scope="",
            event_types=["trade"],
            enabled=True,
            provider="ccxt",
            instrument_type="spot",
        )
        # Target persisted with enabled=True but publication not started.
        self.assertFalse(runtime.is_active("c1"))
        self.assertTrue(result["target"].enabled)
        self.assertTrue(result.get("provider_disabled"))
        snap = await svc.snapshot()
        ids = {t.target_id for t in snap.collection_targets}
        self.assertIn("c1", ids)

        # Re-enabling activates the persisted target.
        await svc.set_provider_enabled("ccxt", True)
        self.assertTrue(runtime.is_active("c1"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
