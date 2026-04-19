# Decisions Log

Append-only log of explicit hub decisions.  Each entry uses the H-series
(Hub decision) identifier and captures question, decision, rationale, and
impact so later sessions can branch with full context.

---

## H23 — multiprovider 구현 1차(step 1) 결과

- **질문**: multiprovider 구현 1차(step 1) 결과는 어디까지 완료되었는가?
- **결정**: **PARTIAL** — domain/contracts 확장과 provider adapter 경계
  skeleton, control-plane wiring까지는 완료되었으나 CCXT/CCXT Pro의 live
  adapter 구현과 admin UI의 provider/instrument_type 노출은 의도적으로
  이번 세션에서 수행하지 않았다.
- **근거**:
  - `packages/domain/enums.py`: `Provider` StrEnum(KXT/CCXT/CCXT_PRO/OTHER)
    신규 추가. `InstrumentType`에 `SPOT`, `PERPETUAL` 추가 (FUTURE/OPTION
    기존 유지)로 crypto spot + USDT perpetual 구조 준비 완료.
  - `packages/domain/models.py`: `InstrumentRef`, `CollectionTarget`,
    `InstrumentSearchResult`에 `provider`, `canonical_symbol` 추가.
    `build_canonical_symbol()` helper 도입 — 형식 `<provider>:<venue>:<instrument_type>:<symbol>`.
  - `packages/contracts/events.py`, `packages/contracts/admin.py`:
    `DashboardEventEnvelope`, `DashboardControlEnvelope`,
    `RecentRuntimeEvent`에 provider/canonical_symbol 필드를 additive
    optional로 추가하여 기존 KXT 응답 shape를 깨지 않음.
  - `packages/adapters/`: `registry.py`, `kxt.py`, `ccxt.py` 신규.
    `ProviderRegistry` + `build_default_registry()`가 KXT/CCXT/CCXT_PRO 3개
    provider를 등록. CCXT/CCXT_PRO는 `implemented=False` skeleton.
  - `src/collector_control_plane.py`: `upsert_target`, `search_instruments`,
    `record_runtime_event`가 provider/instrument_type를 수용. KXT 기본값
    유지, 비-KXT provider는 market_scope="" 허용 (not-applicable 의미).
  - `apps/collector/runtime.py`: `register_target`이 provider를 수용하고
    provider != KXT일 때 `NotImplementedError`로 실패 loud — silent
    degradation 방지.
  - `apps/collector/service.py`: `start_dashboard_publication`이
    provider/instrument_type/canonical_symbol kwargs를 수용하며
    `build_default_registry()`를 서비스 생성 시 호출.
- **영향**:
  - 다음 세션은 **CCXT/CCXT Pro live adapter 구현** 또는 **admin UI의
    provider/instrument_type 노출 통합** 중 하나로 진행 가능. blocker
    해소 세션은 필요 없음 — 기존 KXT/KRX runtime은 그대로 동작.
  - admin UI에서 provider 필드를 입력·표시하도록 확장하려면 이번
    세션의 additive 필드를 소비하는 frontend 작업이 남아 있음.
  - 운영상 추가 DB 스키마 변경이 필요한 시점은 crypto provider의
    subscription persistence가 붙을 때로 지연됨.

---

## H24 — Binance spot + USDT perpetual live adapter (step 2) 결과

- **질문**: Binance spot + USDT perpetual live adapter step2 결과는?
- **결정**: **PASS** — 최소 라이브 경로(target upsert → provider-aware
  dispatch → BinanceLiveAdapter subscribe → runtime event publish →
  control-plane recent event) 가 라이브 데이터로 검증됨.
- **근거**:
  - `packages/adapters/ccxt.py`에 step1 skeleton 을 대체하는 실제
    `BinanceLiveAdapter` 구현. `ccxt.pro` lazy import 기반,
    spot=`binance` / perpetual=`binanceusdm` 자동 분기.
    `to_unified_symbol` / `exchange_id_for` 헬퍼로 `BTCUSDT` ↔
    `BTC/USDT` ↔ `BTC/USDT:USDT` 정규화.
  - `apps/collector/runtime.py`에 crypto 채널 분기:
    `_CryptoChannelKey(canonical_symbol, event_name)`,
    `_acquire_crypto_channel`/`_release_crypto_channel`/
    `_consume_crypto_channel`. spot/perp BTCUSDT 가 구조적으로 별도
    채널이며 dedupe 충돌 없음.
  - `_publish_event` 가 provider/canonical_symbol/instrument_type 을
    forward (legacy KXT 핸들러는 TypeError fallback).
  - `apps/collector/service.py`/`publisher.py` 가 provider/
    canonical/instrument_type 을 envelope 및 control_plane 으로 전달.
  - `src/collector_control_plane.py`의 `upsert_target` dedupe 가
    instrument_type 포함, `record_runtime_event` 매칭이
    canonical_symbol 우선 — spot/perp 트레이드가 정확히 자기 target
    만 매칭.
  - 신규 `tests/test_multiprovider_step2_ccxt.py` 8 case 통과,
    갱신된 step1 9 case + 기존 KXT 17 case 회귀 0 → 총 34 passed.
  - 라이브 검증: BTC/USDT spot + BTC/USDT:USDT perpetual 양쪽 모두
    1초 이내 트레이드 수신 확인. End-to-end runtime 경로
    (`provider=ccxt_pro`, `canonical_symbol=ccxt_pro:binance:spot:BTCUSDT`,
    `instrument_type=spot`, payload 정상) 라이브 데이터로 PASS.
  - `/Users/minkyu/workspace/kxt` 변경 0건.
  - 상세: `reviews/ksxt-migration-progress/multiprovider-binance-live-step2-report.md`
    (vault).
- **영향**:
  - 다음 세션은 (1) **admin UI 통합** —
    `AdminTargetUpsertRequest`/`DashboardSubscriptionRequest` 에
    provider/instrument_type 필드 추가, frontend crypto 입력 UI; 또는
    (2) **capability/event-catalog 완성** — provider×event_type matrix,
    crypto symbol seed 중 선택. (1) 을 1순위로 권장.
  - blocker 해소 세션 불필요. KXT/KRX 경로 회귀 없음.
  - `git push` 금지(H6) 는 H21 미해결 조건으로 그대로 유지.

---

## H25 — Multiprovider step 3: external identity normalisation, capability-driven event catalog, minimal admin UI 통합 결과

- **질문**: multiprovider step 3 (외부 identity 정규화 + capability-driven event catalog + admin UI 최소 통합) 결과는?
- **결정**: **PASS** — 외부 노출되는 provider 값이 `kxt | ccxt`로 제한되었고 `ccxt_pro`는 내부 transport detail로만 남았다. canonical_symbol은 `provider:venue:asset_class:instrument_type:display_symbol[:settle_asset][:expiry]` 형식으로 마이그레이션되었으며, raw exchange symbol은 `InstrumentRef.raw_symbol`로 분리되었다. event catalog는 `(provider, venue, asset_class, instrument_type)` 기반 capability matrix로 변경되었고, admin UI는 기존 Targets/Runtime/Events 안에서 KXT vs Binance spot/perpetual 구분 및 crypto target 생성을 지원한다.
- **근거**:
  - `packages/domain/enums.py`: `external_provider_value(provider)` boundary helper 추가 — `Provider.CCXT_PRO`/`"ccxt_pro"`를 `"ccxt"`로 collapse. `EXTERNAL_PROVIDER_VALUES = {"kxt","ccxt"}` 상수로 외부 surface 제한.
  - `packages/domain/models.py`: `build_canonical_symbol`이 `asset_class`/`display_symbol`/`settle_asset`/`expiry` 인자를 받아 5+axis 형식 생성. `provider`는 항상 `external_provider_value()`로 정규화. `InstrumentRef.raw_symbol: str | None` 추가.
  - `packages/contracts/events.py`: `EventType`에 `TICKER`/`OHLCV`/`MARK_PRICE`/`FUNDING_RATE`/`OPEN_INTEREST` 추가 (기존 3종 유지). `DashboardEventEnvelope`/`DashboardControlEnvelope`에 `instrument_type`/`raw_symbol` 추가.
  - `packages/contracts/admin.py`: `SourceCapability` 신규 dataclass. `ControlPlaneSnapshot.source_capabilities` 추가. `RecentRuntimeEvent`에 `instrument_type`/`raw_symbol` 추가.
  - `packages/adapters/registry.py`: 외부 노출 provider를 `{KXT, CCXT}`로 한정. `Provider.CCXT_PRO` 등록 제거 — `ccxt.pro`는 `BinanceLiveAdapter` 내부 transport.
  - `packages/adapters/ccxt.py`: `CCXTAdapterStub`이 외부 CCXT 경계 (`adapter_id="ccxt"`, streaming via `BinanceLiveAdapter`). `CCXTProAdapterStub`은 legacy import 호환만.
  - `src/collector_control_plane.py`: `SOURCE_CAPABILITIES` (KXT KRX equity → trade/orderbook/program_trade · Binance spot → trade/orderbook/ticker/ohlcv · Binance perpetual → 위 + mark_price/funding_rate/open_interest), `capability_for(...)` 헬퍼, `_normalize_event_types`가 capability 기반 reject ("event_types not supported by Binance Spot (CCXT): mark_price"). `_normalize_provider`/`_normalize_instrument_type`이 `ccxt_pro`→`ccxt`, KXT `EQUITY`→`SPOT` collapse. `_build_instrument_ref`가 새 5+axis canonical과 `raw_symbol` 생성. `record_runtime_event`/`upsert_target`/`search_instruments`가 raw_symbol/instrument_type/capability 인지.
  - `apps/collector/runtime.py`: `_resolve_provider`/`_resolve_instrument_type` 외부 normalize. `register_target`/`_register_kxt_target`/`_register_crypto_target`이 `raw_symbol` 전달, `RuntimeTargetRegistration.raw_symbol` 추가. crypto canonical을 새 형식으로 빌드 (perpetual은 settle_asset 포함, display=BTC/USDT, raw=BTCUSDT 분리). `_publish_event`가 raw_symbol 전달, crypto consume이 `external_provider_value(provider)`로 publish — 외부에 ccxt_pro 누출 0건.
  - `apps/collector/service.py`: `AdminTargetUpsertRequest`가 provider/instrument_type/raw_symbol 수용. `start_dashboard_publication`이 ccxt_pro→ccxt collapse. `_handle_runtime_event`가 envelope/control_plane 양쪽에 외부 정규화된 provider 전달. `/admin/instruments`에 provider/instrument_type query 파라미터 추가.
  - `apps/collector/publisher.py`: `publish_dashboard_event`가 instrument_type/raw_symbol forward.
  - `src/web_app.py`: `/api/admin/instruments` relay가 provider/instrument_type 수용, ccxt_pro 입력은 ccxt로 collapse, KRX scope regex를 optional로 완화.
  - `apps/admin_web/src/App.tsx`: `SourceCapability` 타입과 source preset selector를 `snapshot.source_capabilities` 기반으로 추가 (KRX Equity (KXT) / Binance Spot (CCXT) / Binance USDT Perpetual (CCXT)). event-type checkbox가 capability-filtered. crypto 입력 시 `raw_symbol` 필드 노출, market_scope=""로 전송. Targets/Runtime/Events 모두 provider/instrument_type/canonical_symbol을 표면화.
  - 신규 `tests/test_multiprovider_step3_identity.py`: canonical no-leak, raw_symbol field, capability matrix 검증, capability gating reject (program_trade on Binance · mark_price on Binance spot), capability allow (mark_price/funding_rate on perpetual), record_runtime_event 외부 정규화, registry no ccxt_pro. `tests/test_multiprovider_step1.py`/`step2_ccxt.py`는 새 canonical/외부 provider 값으로 갱신. step1+step2+step3+search 합산 31 passed.
  - End-to-end 검증: `jsonable_encoder`로 직렬화한 snapshot 전체 JSON에 `"ccxt_pro"` 부재 확인. `DashboardEventEnvelope` 직렬화도 동일 확인. crypto target canonical은 `ccxt:binance:crypto:perpetual:BTC/USDT:USDT`, `provider`는 `ccxt`, `raw_symbol`은 `BTCUSDT`로 분리됨.
  - `/Users/minkyu/workspace/kxt` 변경 0건 (제약 준수).
- **영향**:
  - `git push` 금지(H6) 는 H21 미해결 조건으로 그대로 유지.
  - 다음 세션은 (1) 정규장 시간대 시나리오 1 재실행 (H21 unblock), (2) admin SPA bundle 재빌드 (`src/web/admin_dist/assets/index-*.js`는 step3 이전 UI를 인코딩한 stale 상태 — frontend 변경은 source-only), (3) kxt v0.1.0의 `Subscription` public export 추가 (apps.collector.runtime import 차단 해결) 중 선택 가능.
  - 잔존 위험: KXT 내부 `instrument_type` 기본값을 `EQUITY`→`SPOT`으로 변경했으나 asset_class는 그대로 `EQUITY`. in-repo 다운스트림 소비자에 영향 없음.
  - 본 결정 리포트는 권한 제한으로 vault에 직접 작성 불가 — repo-local `docs/adr/multiprovider-identity-ui-step3-report.md`에 기록되었으며 vault 측 mirror는 별도 운영자 작업으로 미룬다. (mirror는 H26 cleanup 세션에서 완료됨.)

---

## H26 — Cleanup: vault artifact sync + KXT import 환경 정리 결과

- **질문**: vault artifact sync + KXT import 환경 정리 결과
- **결정**: **PASS** — canonical vault에 step3 report mirror 완료, canonical decisions에 H25/H26 반영 완료, `python -m pytest tests/ -q` 44 passed (full green), KXT 변경 0건.
- **근거**:
  - canonical artifact sync: `_vaults/market-data-vault/reviews/ksxt-migration-progress/multiprovider-identity-ui-step3-report.md` 신규 생성 (repo-local과 byte-identical, `diff -q` MATCH).
  - H25 canonical 반영: 기존 vault `decisions.md`에 부재 → 본 세션에서 H25 + H26 append. H25/H26 각 1회 등장.
  - 실패 11건 root cause: `apps/collector/runtime.py:11-27`의 `from kxt import (... Subscription as KSXTSubscription, ...)`가 KXT v0.1.x public top-level surface에 부재 → `ImportError: cannot import name 'Subscription' from 'kxt'`. 11 failures 모두 동일 import-time 실패의 collection-time 전파.
  - 사용처는 type annotation 전용 2개 (`_ChannelEntry.subscription` line 102, `_consume_subscription` line 672 시그니처) — 런타임 동작 의존 0건.
  - hub-only minimum fix: top-level `Subscription` import 제거. `from __future__ import annotations`로 annotation lazy 평가되므로 `TYPE_CHECKING` 가드 하 `from kxt.clients.kis.realtime.subscription import Subscription as KSXTSubscription`, 런타임은 `KSXTSubscription = Any`. KXT 0건 변경 제약 준수.
  - 검증: `python -m pytest tests/ -q` → 44 passed; `python -c "import apps.collector.runtime; import src.collector_control_plane"` → ok.
  - 상세 cleanup report: `_vaults/market-data-vault/reviews/ksxt-migration-progress/multiprovider-cleanup-step4-report.md`.
- **영향**:
  - 다음 세션은 cleanup 종결로 admin UI 확대(SPA bundle 재빌드 포함) 또는 live capability 확장(reconnect/health, 정규장 시나리오 1 재실행 — H21 unblock) 중 선택 가능.
  - `git push` 금지(H6)는 H21 미해결 조건으로 그대로 유지.
  - 잔존 위험: KXT 내부 모듈 경로(`kxt.clients.kis.realtime.subscription`)에 의존하는 import는 런타임 평가는 회피되지만 KXT 내부 구조 변경 시 정적 분석 도구가 깨질 수 있음 — 향후 KXT가 `Subscription`을 public export로 추가하면 `TYPE_CHECKING` 가드를 top-level import로 되돌리는 1줄 정리 권장.

---

## H30 — Funding-rate WS 승격 + shared mark-price 소스 fan-out

- **질문**: Binance perpetual `funding_rate` 를 REST polling 에서 mark-price WS 파생 live 세만틱으로 승격하고, 동일 canonical 에 대해 `mark_price` + `funding_rate` 를 단일 mark-price WS 로 fan-out 할 수 있는가?
- **결정**: **PASS** — parser/runtime/tests 전면 완료, `python -m pytest tests/ -q` 64 passed (full green), KXT 변경 0건.
- **근거**:
  - 파일 변경: `packages/adapters/ccxt.py` (BinanceMarkPrice 확장 + `_parse_ccxt_mark_price` funding 필드 추출), `apps/collector/runtime.py` (`_CryptoMarkPriceSourceEntry` + `_attach/_detach/_run_crypto_mark_price_source` 도입, MP/FR 채널을 shared source 경로로 라우팅, 구 `_crypto_mark_price_stream` / `_crypto_funding_rate_stream` 제거, `_format_crypto_funding_rate_from_mark` 추가, `aclose` 에서 source task 정리), `src/collector_control_plane.py` (FUNDING_RATE 설명 `mark-price WS 파생, live` 로 갱신), `tests/test_multiprovider_step4_capabilities.py` (parser 테스트에 `r`/`T`/`E` 주입 + funding_rate shared-source 재시나리오 + 새 shared-source counter 테스트).
  - Parser enrichment 키: root `fundingRate` / `nextFundingTimestamp`; info `fundingRate` / `lastFundingRate` / `r` / `T` / `nextFundingTime` / `E`. numeric 문자열 허용, non-numeric 문자열 거부.
  - Runtime shared source 설계: `canonical_symbol` 단위 ref-count (active_event_names set). 최초 attach 시 1 task 생성, 마지막 detach 시 cancel. 생성/파기 race 는 task 생성 직후 재검증으로 방어.
  - pytest 결과: `64 passed, 181 warnings in 0.91s` — 신규 `test_mark_price_and_funding_share_single_mark_price_source` 포함하여 shared-source counter 정확히 1 확인.
  - KXT 변경 0건.
- **영향**:
  - `funding_rate` 이제 live WS 파생 — polling 30s 하한 제거, 업스트림 mark-price 틱마다 갱신.
  - `open_interest` 는 변함없이 polling (30s 하한 유지).
  - 외부 event 이름/페이로드 contract 유지: `mark_price` payload 는 funding 필드 미노출, `funding_rate` payload 필드명은 기존 `funding_rate` / `funding_timestamp_ms` / `next_funding_timestamp_ms` 동일.
  - 잔존 위험: Binance 가 `markPrice@arr` 틱에서 funding 필드를 누락하는 구간에서는 `funding_rate` 이벤트 payload 필드가 `None` 으로 발행될 수 있음. `BinanceFundingRate` / `poll_funding_rate` 는 미사용이지만 제거하지 않고 남김.
  - `git push` 금지(H6) 는 H21 미해결 조건으로 유지.
  - 상세 리포트: `_vaults/market-data-vault/reviews/ksxt-migration-progress/funding-rate-ws-step7-report.md`.
