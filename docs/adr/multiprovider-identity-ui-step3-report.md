# Multiprovider Step 3 — External Identity Normalisation, Capability-Driven Event Catalog, Minimal Admin UI Integration

> Mirror of the vault report at `_vaults/market-data-vault/reviews/ksxt-migration-progress/multiprovider-identity-ui-step3-report.md`.
> The vault path was inaccessible from this session's sandbox so the canonical copy lives here in the repo.
> H25 decision is appended to `docs/adr/decisions.md` (this repo) and should be mirrored back into the vault `decisions.md` by an operator with vault write permission.

## Summary

Step 3 of the multiprovider migration completes three structural changes that step 1 (domain skeleton) and step 2 (live Binance via ccxt.pro) deferred:

1. **External identity normalisation** — provider values exposed on `target / snapshot / event / canonical_symbol` are now restricted to `kxt | ccxt`. `ccxt_pro` is retained internally as the transport detail behind `Provider.CCXT` and **never** appears on outward-facing contracts or UI.
2. **canonical_symbol format upgrade** — `provider:venue:asset_class:instrument_type:display_symbol[:settle_asset][:expiry]`, e.g.
   - `kxt:krx:equity:spot:005930`
   - `ccxt:binance:crypto:spot:BTC/USDT`
   - `ccxt:binance:crypto:perpetual:BTC/USDT:USDT`
3. **Capability-driven event catalog** — replaces the prior global static event list. Each `(provider, venue, asset_class, instrument_type)` source advertises the events it can stream; the control plane gates `upsert_target` validation against this matrix.

Plus a minimum admin UI integration so KXT vs Binance spot/perpetual targets can be created and distinguished without redesigning the page layout.

## What Changed

### Domain (`packages/domain`)

- **`enums.py`**
  - Added `external_provider_value(provider)` boundary helper that collapses `Provider.CCXT_PRO` and `"ccxt_pro"` to `"ccxt"`.
  - Documented `Provider.CCXT_PRO` as internal transport detail only.
  - Added `EXTERNAL_PROVIDER_VALUES` constant (`{"kxt", "ccxt"}`) for outward surfaces.
- **`models.py`**
  - `build_canonical_symbol` accepts `asset_class`, `display_symbol`, `settle_asset`, `expiry` and emits the new 5+ axis format. `provider` is run through `external_provider_value()` so the canonical never leaks `ccxt_pro`. Legacy `symbol=` kwarg kept as alias.
  - `InstrumentRef` gains `raw_symbol: str | None` to split the venue-native id (e.g. `BTCUSDT`, `005930`) from the unified/display symbol (`BTC/USDT`).

### Contracts (`packages/contracts`)

- **`events.py`**
  - `EventType` extended with `TICKER`, `OHLCV`, `MARK_PRICE`, `FUNDING_RATE`, `OPEN_INTEREST` (existing `TRADE`, `ORDER_BOOK_SNAPSHOT`, `PROGRAM_TRADE` retained).
  - `DashboardEventEnvelope` and `DashboardControlEnvelope` gain `instrument_type` and `raw_symbol` (additive, optional, default `None`).
- **`admin.py`**
  - New `SourceCapability` dataclass `(provider, venue, asset_class, instrument_type, label, supported_event_types, market_scope_required)`.
  - `ControlPlaneSnapshot.source_capabilities: tuple[SourceCapability, ...]` — the new capability matrix served alongside the existing `event_type_catalog` (kept for backward compat).
  - `RecentRuntimeEvent` gains `instrument_type` and `raw_symbol`.

### Adapters (`packages/adapters`)

- **`registry.py`** — public registry now lists only `Provider.KXT` and `Provider.CCXT`. The `Provider.CCXT_PRO` entry was removed because `ccxt.pro` is an internal transport behind `Provider.CCXT`, not an externally addressable provider.
- **`ccxt.py`** — `CCXTAdapterStub` now describes the externally exposed CCXT provider (`adapter_id="ccxt"`, supports streaming via the `BinanceLiveAdapter` internal transport). `CCXTProAdapterStub` is retained for legacy import compatibility but is no longer registered.

### Control Plane (`src/collector_control_plane.py`)

- Added `SOURCE_CAPABILITIES` (the actual capability matrix):
  - `kxt / krx / equity / spot` → `trade, order_book_snapshot, program_trade`
  - `ccxt / binance / crypto / spot` → `trade, order_book_snapshot, ticker, ohlcv` (no `mark_price/funding_rate/open_interest`)
  - `ccxt / binance / crypto / perpetual` → `trade, order_book_snapshot, ticker, ohlcv, mark_price, funding_rate, open_interest`
- `capability_for(...)` lookup helper.
- `_normalize_event_types` is now capability-aware: when `(provider, instrument_type)` is known, events outside the matrix are rejected with a meaningful error message (e.g. "event_types not supported by Binance Spot (CCXT): mark_price").
- `_normalize_provider` collapses `ccxt_pro` → `Provider.CCXT` at the boundary.
- `_normalize_instrument_type` defaults KXT to `SPOT` and remaps `EQUITY` → `SPOT` (KRX equities trade on the spot market — see canonical example).
- `_build_instrument_ref` builds the new 5-axis canonical and populates `raw_symbol`. For Binance perpetual it derives `settle_asset` from the unified symbol's quote leg.
- `record_runtime_event` emits with `provider=external_provider_value(...)` and forwards `raw_symbol` / `instrument_type`.
- `upsert_target` accepts `raw_symbol`, exposes the capability-validated event list, and calls `start_publication` with the externally normalised provider value.
- `search_instruments` accepts `provider` and `instrument_type`. Crypto path uses a new `_search_crypto_catalog` (BTC/ETH/SOL/XRP USDT seeds + arbitrary symbol injection).
- Snapshot serialises `source_capabilities=SOURCE_CAPABILITIES`.

### Runtime + Service (`apps/collector`)

- **`runtime.py`**
  - `_resolve_provider` collapses `ccxt_pro` → `Provider.CCXT`. The crypto branch now matches on `Provider.CCXT` only (and accepts the deprecated alias on input).
  - `_resolve_instrument_type` defaults KXT to `SPOT` and remaps `EQUITY` → `SPOT`.
  - `register_target` and `_register_kxt_target` / `_register_crypto_target` accept and propagate `raw_symbol`. `RuntimeTargetRegistration` gains `raw_symbol`.
  - Crypto canonical is now built with the new 5-axis helper (with `settle_asset` for perpetual). The unified symbol (e.g. `BTC/USDT`) is the canonical display; the venue-native string (`BTCUSDT`) is the `raw_symbol`.
  - `_publish_event` forwards `raw_symbol`. Crypto consume loop publishes with `provider=external_provider_value(provider)` so no `ccxt_pro` leaks downstream.
- **`service.py`**
  - `AdminTargetUpsertRequest` accepts `provider`, `instrument_type`, `raw_symbol`. Market scope validation skips KRX rules for crypto.
  - `start_dashboard_publication` collapses `ccxt_pro` → `ccxt` at the boundary (`subscription_key` and the response payload always use the external value).
  - `_handle_runtime_event` runs every event through `external_provider_value()` before publishing or recording, eliminating leak paths through both the Kafka envelope and the recent-event buffer.
  - `/admin/instruments` accepts `provider` and `instrument_type` query params.
- **`publisher.py`**
  - `publish_dashboard_event` accepts `instrument_type`, `raw_symbol` and forwards them on the envelope.

### Web Relay (`src/web_app.py`)

- `/api/admin/instruments` accepts `provider` and `instrument_type`. The KRX scope regex was relaxed to optional so crypto requests need not carry `scope`. `ccxt_pro` input is collapsed to `ccxt` before relay so the relay never propagates the internal alias.

### Admin UI (`apps/admin_web/src/App.tsx`)

Minimum integration — no layout redesign, lives inside the existing **Targets / Runtime / Events** views.

- **Types** — added `ProviderId = 'kxt' | 'ccxt'`, `SourceCapability`, plus new optional fields on `InstrumentRef` (`raw_symbol`, `instrument_type`, `venue`, `asset_class`), `CollectionTarget` (`provider`, `canonical_symbol`), `Snapshot` (`source_capabilities`), `RecentRuntimeEvent` (`provider`, `canonical_symbol`, `instrument_type`, `raw_symbol`).
- **TargetsView** —
  - "소스" preset selector populated from `snapshot.source_capabilities` (KRX Equity (KXT) / Binance Spot (CCXT) / Binance USDT Perpetual (CCXT)).
  - Event-type checkboxes are filtered to the selected source's `supported_event_types` (capability-driven, not global).
  - Form sends `provider`, `instrument_type`, and `market_scope=""` for crypto sources. `raw_symbol` field exposed for crypto.
  - Search uses provider + instrument_type query params.
  - Registered-target rows show provider/instrument_type/scope badges and the `canonical_symbol` underneath the symbol.
- **RuntimeView** — per-target labels include `PROVIDER · instrument_type · SCOPE`.
- **EventsView** — events table shows provider badge, instrument_type, and the canonical_symbol on the symbol cell.

### Tests

- `tests/test_multiprovider_step1.py` — updated for new canonical format (`kxt:krx:equity:spot:005930`), capability gating, no-ccxt_pro-leak in canonical/registry. Added `external_provider_value` collapse test.
- `tests/test_multiprovider_step2_ccxt.py` — updated to assert outgoing event `provider == "ccxt"` (never `ccxt_pro`), new canonical format including settle asset for perpetual.
- `tests/test_multiprovider_step3_identity.py` — new file. Pins:
  - canonical never leaks `ccxt_pro`,
  - `InstrumentRef.raw_symbol` field exists,
  - `source_capabilities` matrix is correct (KXT has program_trade, Binance spot has no mark_price/funding/oi, Binance perpetual has all three),
  - capability gating rejects `program_trade` on Binance and `mark_price` on Binance spot,
  - capability allows `mark_price`/`funding_rate` on Binance perpetual,
  - `record_runtime_event` externalises `ccxt_pro` → `ccxt`,
  - registry no longer advertises `Provider.CCXT_PRO`.

## Verification

### Unit Tests

```
pytest tests/test_multiprovider_step1.py
       tests/test_multiprovider_step2_ccxt.py
       tests/test_multiprovider_step3_identity.py
       tests/test_collector_control_plane_search.py
       --deselect <pre-existing kxt env failures>
→ 31 passed, 3 deselected
```

### No-Leak Verification

End-to-end snapshot serialisation through `fastapi.encoders.jsonable_encoder`:

```python
await svc.upsert_target(provider="ccxt_pro", instrument_type="perpetual",
                        symbol="BTC/USDT", raw_symbol="BTCUSDT", ...)
snap = jsonable_encoder(await svc.snapshot())
assert "ccxt_pro" not in json.dumps(snap)   # PASS
# target.canonical_symbol == "ccxt:binance:crypto:perpetual:BTC/USDT:USDT"
# target.provider          == "ccxt"
# target.instrument.raw_symbol == "BTCUSDT"
# source_capabilities listed: kxt/krx/spot, ccxt/binance/spot, ccxt/binance/perpetual
```

`DashboardEventEnvelope._to_transport_value` round-trip with crypto inputs also verified — `ccxt_pro` does not appear anywhere in the JSON.

### Pre-existing Failures (Unrelated)

3 tests touching `apps.collector.runtime` import fail with `ImportError: cannot import name 'Subscription' from 'kxt'` — this is the local kxt v0.1.0 install missing a public export and is **unchanged from `main` HEAD before this work**. Reproduced under `git stash`. Out of scope (constraint forbids touching `/Users/minkyu/workspace/kxt`).

### Live Sanity Check

Not executed in this session — the runtime branch that publishes live Binance events depends on `apps.collector.runtime` importing successfully, which is currently blocked by the kxt env issue noted above. The path is otherwise unchanged from step 2 (validated in H24); only the externalisation layer was added on top, and that layer is covered by the no-leak unit tests above.

## Capability Matrix

| Provider | Venue   | Asset Class | Instrument Type | Events                                                                  | Market Scope |
| -------- | ------- | ----------- | --------------- | ----------------------------------------------------------------------- | ------------ |
| `kxt`    | `krx`   | `equity`    | `spot`          | trade, order_book_snapshot, program_trade                               | required     |
| `ccxt`   | `binance` | `crypto`  | `spot`          | trade, order_book_snapshot, ticker, ohlcv                               | n/a          |
| `ccxt`   | `binance` | `crypto`  | `perpetual`     | trade, order_book_snapshot, ticker, ohlcv, mark_price, funding_rate, open_interest | n/a          |

Binance spot deliberately excludes `mark_price / funding_rate / open_interest` (perpetual-only metrics).

## Backwards Compatibility

- Legacy callers passing `provider="ccxt_pro"` are still accepted but transparently collapsed to `Provider.CCXT` at every boundary (control plane, runtime, service, relay).
- KXT path semantics are unchanged: defaults remain KRX/equity. The internal `instrument_type` axis was migrated `EQUITY → SPOT` only inside the canonical/runtime/normalisation paths so KRX target dedupe and matching keep working. The `AssetClass.EQUITY` axis is still populated for asset-class downstream consumers.
- `event_type_catalog` (global) is still served on the snapshot — only `source_capabilities` is added on top, so any prior consumer keeps working.

## Files Changed

```
packages/domain/enums.py
packages/domain/models.py
packages/contracts/__init__.py
packages/contracts/admin.py
packages/contracts/events.py
packages/adapters/ccxt.py
packages/adapters/registry.py
src/collector_control_plane.py
src/web_app.py
apps/collector/runtime.py
apps/collector/service.py
apps/collector/publisher.py
apps/admin_web/src/App.tsx
tests/test_multiprovider_step1.py
tests/test_multiprovider_step2_ccxt.py
tests/test_multiprovider_step3_identity.py   (new)
docs/adr/decisions.md                         (H25 append)
docs/adr/multiprovider-identity-ui-step3-report.md  (this report)
```

`/Users/minkyu/workspace/kxt` — **0 changes** (constraint honoured).

## Out of Scope (deferred)

- Charts / replay / backtest.
- Multi-exchange expansion beyond Binance.
- Admin UI redesign (presets list, two-column form preserved).
- DB persistence for crypto subscriptions (deferred until storage binding rework).
- Live Binance run during this session (blocked by pre-existing kxt env import error).

## Remaining Risks

1. **kxt v0.1.0 public API gap** — `Subscription` is referenced by `apps/collector/runtime.py` but not re-exported from `kxt.__init__`. Pre-existing on `main`; blocks 3 runtime-touching tests and any in-process live verification today. Should be resolved upstream in `/Users/minkyu/workspace/kxt`.
2. **`InstrumentRef.instrument_type` for KXT changed from `EQUITY` to `SPOT`** as the internal default. Asset class is still `EQUITY`. If any downstream KXT consumer (storage layer, future processor) was branching on `instrument_type == "equity"` it must be updated, but no such consumer exists in-repo today.
3. **Web admin distribution build (`src/web/admin_dist/assets/index-*.js`) is stale** — the bundled JS still encodes the pre-step3 admin UI. The TS source is correct; whoever ships the next collector image should rebuild the admin SPA bundle to surface the new source preset selector.
4. **Vault artifacts not written by this session** — write permission to `/Users/minkyu/workspace/_vaults/...` is denied in this sandbox. The report and H25 decisions live in `docs/adr/` only; an operator must mirror them into the vault.
