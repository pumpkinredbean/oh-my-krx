"""Provider adapter registry for multiprovider market data boundary.

Step 1 scope: provide the registry surface and stubs for KXT / CCXT so the
collector control-plane and runtime can branch on provider without yet
implementing full CCXT Pro connectivity.  The registry is a thin mapping
from :class:`~packages.domain.enums.Provider` → adapter factory / instance
reference; it is deliberately dependency-light so it can be imported from
any runtime entry without pulling provider SDKs by default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from packages.domain.enums import Provider


AdapterFactory = Callable[[], object]


@dataclass(slots=True)
class ProviderRegistration:
    """Metadata + factory for a provider registered on the hub."""

    provider: Provider
    description: str
    factory: AdapterFactory
    supports_streaming: bool = True
    notes: str | None = None


@dataclass(slots=True)
class ProviderRegistry:
    """In-memory provider registry used by the collector control-plane.

    ``register`` is idempotent on provider key — re-registration replaces
    the previous entry, which keeps test setup and service composition
    simple.  ``require`` raises ``KeyError`` for unknown providers so the
    runtime can fail loud if asked to branch to a provider that was not
    wired for the current deployment.
    """

    _entries: dict[Provider, ProviderRegistration] = field(default_factory=dict)

    def register(self, registration: ProviderRegistration) -> None:
        self._entries[registration.provider] = registration

    def get(self, provider: Provider) -> ProviderRegistration | None:
        return self._entries.get(provider)

    def require(self, provider: Provider) -> ProviderRegistration:
        registration = self._entries.get(provider)
        if registration is None:
            raise KeyError(f"provider not registered: {provider.value}")
        return registration

    def providers(self) -> tuple[Provider, ...]:
        return tuple(self._entries.keys())

    def __contains__(self, provider: object) -> bool:  # pragma: no cover - trivial
        return provider in self._entries


def build_default_registry() -> ProviderRegistry:
    """Build the default provider registry wired for this hub build.

    Externally exposed providers are limited to ``KXT`` and ``CCXT``.
    The CCXT entry's factory builds the live ``BinanceLiveAdapter``
    backed by ``ccxt.pro`` — that transport detail is intentionally
    hidden behind the CCXT provider so it never leaks through registry
    listings, snapshots, or admin UI.
    """

    # Local imports keep top-level import cost bounded and avoid pulling
    # optional dependencies unless the adapter is actually constructed.
    from .kxt import build_kxt_adapter_stub
    from .ccxt import build_ccxt_adapter_stub

    registry = ProviderRegistry()
    registry.register(
        ProviderRegistration(
            provider=Provider.KXT,
            description="KSXT-backed KIS provider (KRX equity today)",
            factory=build_kxt_adapter_stub,
            supports_streaming=True,
        )
    )
    registry.register(
        ProviderRegistration(
            provider=Provider.CCXT,
            description="CCXT provider (Binance spot + USDT perpetual via ccxt.pro internally)",
            factory=build_ccxt_adapter_stub,
            supports_streaming=True,
            notes="Streaming served by BinanceLiveAdapter (ccxt.pro is internal transport).",
        )
    )
    return registry
