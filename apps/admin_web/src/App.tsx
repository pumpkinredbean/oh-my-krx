import { FormEvent, useCallback, useEffect, useRef, useState } from 'react';
import { NavLink, Navigate, Route, Routes } from 'react-router-dom';

import ChartsView from './views/ChartsView';

// ─── Types ────────────────────────────────────────────────────────────────────

type MarketScope = 'krx' | 'nxt' | 'total';
type ProviderId = 'kxt' | 'ccxt';

interface InstrumentRef {
  symbol: string;
  instrument_id?: string | null;
  raw_symbol?: string | null;
  instrument_type?: string | null;
  venue?: string | null;
  asset_class?: string | null;
  provider?: ProviderId | null;
}

interface EventCatalogEntry {
  event_type: string;
  topic_name: string;
  description: string;
}

interface SourceCapability {
  provider: ProviderId;
  venue: string;
  asset_class: string;
  instrument_type: string;
  label: string;
  supported_event_types: string[];
  market_scope_required: boolean;
}

interface InstrumentSearchResult {
  instrument: InstrumentRef;
  display_name: string;
  market_scope: string;
  is_active?: boolean;
  provider?: ProviderId | null;
  canonical_symbol?: string | null;
}

interface CollectionTarget {
  target_id: string;
  instrument: InstrumentRef;
  market_scope: string;
  event_types: string[];
  enabled: boolean;
  provider?: ProviderId | null;
  canonical_symbol?: string | null;
}

interface CollectionTargetStatus {
  target_id: string;
  state: string;
  observed_at: string;
  last_event_at?: string | null;
  last_error?: string | null;
  permanent_failure?: boolean;
  failure_reason?: string | null;
  failure_rt_cd?: string | null;
  failure_msg?: string | null;
  failure_attempts?: number | null;
}

interface RuntimeStatus {
  component: string;
  state: string;
  observed_at: string;
  active_collection_target_ids: string[];
  last_error?: string | null;
}

interface SourceRuntimeStatus {
  provider: ProviderId;
  state: string;
  enabled: boolean;
  active_target_count: number;
  last_error?: string | null;
  observed_at?: string | null;
}

interface Snapshot {
  captured_at: string | null;
  source_service: string;
  event_type_catalog: EventCatalogEntry[];
  source_capabilities?: SourceCapability[];
  collection_targets: CollectionTarget[];
  runtime_status: RuntimeStatus[];
  collection_target_status: CollectionTargetStatus[];
  /** True when the collector container is intentionally offline. */
  collector_offline?: boolean;
  /** Raw container status string reported by the Docker socket. */
  container_status?: string;
  /** KSXT realtime session state (IDLE/CONNECTING/HEALTHY/DEGRADED/CLOSED). */
  session_state?: string | null;
  /** Provider-level logical runtime rows (kxt, ccxt). */
  source_runtime_status?: SourceRuntimeStatus[];
}

interface TargetMutationResponse {
  target?: CollectionTarget;
  applied?: boolean;
  warning?: string | null;
}

interface RecentRuntimeEvent {
  event_id: string;
  event_name: string;
  symbol: string;
  market_scope: string;
  topic_name: string;
  published_at: string;
  matched_target_ids: string[];
  payload?: Record<string, unknown>;
  provider?: ProviderId | null;
  canonical_symbol?: string | null;
  instrument_type?: string | null;
  raw_symbol?: string | null;
}

// ─── Utils ────────────────────────────────────────────────────────────────────

const MARKET_SCOPES: MarketScope[] = ['krx', 'nxt', 'total'];

function capabilityKey(c: { provider: string; venue: string; instrument_type: string }) {
  return `${c.provider}:${c.venue}:${c.instrument_type}`;
}

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const payload = await response.json().catch(() => ({ error: 'Invalid JSON response' }));
  if (!response.ok) {
    const msg =
      (payload as { error?: string; detail?: string }).error ??
      (payload as { detail?: string }).detail ??
      `Request failed: ${response.status}`;
    throw new Error(msg);
  }
  return payload as T;
}

function fmt(value?: string | null): string {
  if (!value) return '—';
  const d = new Date(value);
  return d.toLocaleString('ko-KR', { hour12: false });
}

function stateTone(value?: string | null): string {
  const v = String(value ?? '').toLowerCase();
  if (['running', 'ready', 'ok', 'active', 'bound', 'enabled'].includes(v)) return 'good';
  if (['error', 'failed', 'degraded'].includes(v)) return 'danger';
  if (['stopped', 'disabled', 'inactive', 'stopping'].includes(v)) return 'muted';
  return 'default';
}

// ─── Shared Components ────────────────────────────────────────────────────────

function Badge({ label, tone }: { label: string; tone?: string }) {
  return <span className={`badge ${tone ?? stateTone(label)}`}>{label}</span>;
}

function Empty({ msg }: { msg: string }) {
  return <div className="empty-row">{msg}</div>;
}

function Banner({ msg, error }: { msg: string; error?: boolean }) {
  return <div className={error ? 'banner error' : 'banner'}>{msg}</div>;
}

// ─── Inline Confirm Button ────────────────────────────────────────────────────
// Replaces window.confirm: first click arms, second click fires.

function ConfirmButton({
  label,
  confirmLabel = '확인 클릭',
  onConfirm,
  disabled,
  className = 'danger-button',
}: {
  label: string;
  confirmLabel?: string;
  onConfirm: () => void;
  disabled?: boolean;
  className?: string;
}) {
  const [armed, setArmed] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function arm() {
    setArmed(true);
    timer.current = setTimeout(() => setArmed(false), 3500);
  }

  function fire() {
    if (timer.current) clearTimeout(timer.current);
    setArmed(false);
    onConfirm();
  }

  return armed ? (
    <button type="button" className="danger-button armed" onClick={fire} disabled={disabled}>
      {confirmLabel}
    </button>
  ) : (
    <button type="button" className={className} onClick={arm} disabled={disabled}>
      {label}
    </button>
  );
}

// ─── Targets View ─────────────────────────────────────────────────────────────

interface TargetDraft {
  targetId: string;
  capabilityKey: string;
  symbol: string;
  rawSymbol: string;
  scope: MarketScope;
  eventTypes: string[];
  enabled: boolean;
  displayName: string;
}

function defaultCapability(caps: SourceCapability[]): SourceCapability | null {
  return caps[0] ?? null;
}

function emptyDraft(caps: SourceCapability[]): TargetDraft {
  const cap = defaultCapability(caps);
  return {
    targetId: '',
    capabilityKey: cap ? capabilityKey(cap) : '',
    symbol: '',
    rawSymbol: '',
    scope: 'total',
    eventTypes: cap ? [...cap.supported_event_types] : [],
    enabled: true,
    displayName: '',
  };
}

function TargetsView({
  snapshot,
  snapshotBusy,
  onRefresh,
}: {
  snapshot: Snapshot | null;
  snapshotBusy: boolean;
  onRefresh: () => Promise<void>;
}) {
  const catalog = snapshot?.event_type_catalog ?? [];
  const capabilities = snapshot?.source_capabilities ?? [];
  const targets = snapshot?.collection_targets ?? [];
  const statusMap = Object.fromEntries(
    (snapshot?.collection_target_status ?? []).map((s) => [s.target_id, s]),
  );
  const capByKey = Object.fromEntries(capabilities.map((c) => [capabilityKey(c), c]));
  const eventDescByName = Object.fromEntries(catalog.map((e) => [e.event_type, e.description]));

  const [draft, setDraft] = useState<TargetDraft>(() => emptyDraft(capabilities));
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<InstrumentSearchResult[]>([]);
  const [searchMsg, setSearchMsg] = useState('');
  const [searchBusy, setSearchBusy] = useState(false);
  const [formBusy, setFormBusy] = useState(false);
  const [formMsg, setFormMsg] = useState('');
  const [formError, setFormError] = useState(false);

  // Initialise capability key once capabilities arrive.
  useEffect(() => {
    if (capabilities.length > 0 && !draft.capabilityKey) {
      const cap = defaultCapability(capabilities)!;
      setDraft((d) => ({
        ...d,
        capabilityKey: capabilityKey(cap),
        eventTypes: [...cap.supported_event_types],
      }));
    }
  }, [capabilities.length]); // eslint-disable-line react-hooks/exhaustive-deps

  const selectedCapability: SourceCapability | null =
    capByKey[draft.capabilityKey] ?? defaultCapability(capabilities);

  function pickTarget(t: CollectionTarget, name = '') {
    // Resolve the source preset that matches the existing target.
    const inst = t.instrument;
    const matchedKey = capabilityKey({
      provider: (t.provider ?? inst.provider ?? 'kxt') as string,
      venue: (inst.venue ?? (t.provider === 'ccxt' ? 'binance' : 'krx')) as string,
      instrument_type: (inst.instrument_type ?? 'spot') as string,
    });
    const supported = capByKey[matchedKey]?.supported_event_types ?? [];
    // Intersect stored event_types with currently-advertised capability
    // so legacy stored values (e.g. program_trade on KXT spot) cannot be
    // re-submitted from the draft.
    const intersected = t.event_types.filter((et) => supported.includes(et));
    setDraft({
      targetId: t.target_id,
      capabilityKey: capByKey[matchedKey] ? matchedKey : draft.capabilityKey,
      symbol: t.instrument.symbol,
      rawSymbol: t.instrument.raw_symbol ?? '',
      scope: (t.market_scope || 'total') as MarketScope,
      eventTypes: intersected.length ? intersected : supported,
      enabled: t.enabled,
      displayName: name || t.instrument.instrument_id || '',
    });
    setFormMsg('');
    setFormError(false);
  }

  function pickSearchResult(r: InstrumentSearchResult) {
    const inst = r.instrument;
    setDraft((d) => ({
      ...d,
      targetId: '',
      symbol: inst.symbol,
      rawSymbol: inst.raw_symbol ?? '',
      displayName: r.display_name,
    }));
    setFormMsg('');
    setFormError(false);
  }

  function resetDraft() {
    setDraft(emptyDraft(capabilities));
    setFormMsg('');
    setFormError(false);
  }

  function changeSourcePreset(key: string) {
    const cap = capByKey[key];
    if (!cap) return;
    setDraft((d) => ({
      ...d,
      capabilityKey: key,
      // Reset event-type checkboxes to the new source's full capability.
      eventTypes: [...cap.supported_event_types],
      // KRX scope only meaningful for kxt.
      scope: cap.market_scope_required ? d.scope || 'total' : 'total',
    }));
  }

  async function doSearch(e: FormEvent) {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    const cap = selectedCapability;
    if (!cap) return;
    setSearchBusy(true);
    setSearchMsg('');
    try {
      const params = new URLSearchParams({ query: searchQuery.trim(), provider: cap.provider });
      params.set('instrument_type', cap.instrument_type);
      if (cap.market_scope_required) params.set('scope', draft.scope);
      const r = await requestJson<{ instrument_results: InstrumentSearchResult[] }>(
        `/api/admin/instruments?${params.toString()}`,
      );
      setSearchResults(r.instrument_results ?? []);
      setSearchMsg(`${r.instrument_results?.length ?? 0}건`);
    } catch (err) {
      setSearchResults([]);
      setSearchMsg(err instanceof Error ? err.message : '검색 실패');
    } finally {
      setSearchBusy(false);
    }
  }

  async function doSave(e: FormEvent) {
    e.preventDefault();
    const cap = selectedCapability;
    if (!cap) {
      setFormMsg('소스 프리셋이 없습니다.');
      setFormError(true);
      return;
    }
    if (!draft.symbol.trim()) {
      setFormMsg('종목/심볼을 입력하세요.');
      setFormError(true);
      return;
    }
    if (draft.eventTypes.length === 0) {
      setFormMsg('이벤트를 최소 1개 선택하세요.');
      setFormError(true);
      return;
    }
    setFormBusy(true);
    setFormMsg('');
    setFormError(false);
    try {
      const body: Record<string, unknown> = {
        target_id: draft.targetId || undefined,
        symbol: draft.symbol.trim(),
        event_types: draft.eventTypes,
        enabled: draft.enabled,
        provider: cap.provider,
        instrument_type: cap.instrument_type,
      };
      if (cap.market_scope_required) {
        body.market_scope = draft.scope;
      } else {
        body.market_scope = '';
      }
      if (draft.rawSymbol.trim()) body.raw_symbol = draft.rawSymbol.trim();
      const r = await requestJson<TargetMutationResponse>('/api/admin/targets', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      setFormMsg(r.warning ? `저장됨 (경고: ${r.warning})` : '저장되었습니다.');
      await onRefresh();
      if (r.target) {
        pickTarget(r.target, draft.displayName);
      }
    } catch (err) {
      setFormMsg(err instanceof Error ? err.message : '저장 실패');
      setFormError(true);
    } finally {
      setFormBusy(false);
    }
  }

  async function doDelete(targetId: string) {
    setFormBusy(true);
    try {
      await requestJson(`/api/admin/targets/${encodeURIComponent(targetId)}`, {
        method: 'DELETE',
      });
      if (draft.targetId === targetId) resetDraft();
      setFormMsg('삭제되었습니다.');
      setFormError(false);
      await onRefresh();
    } catch (err) {
      setFormMsg(err instanceof Error ? err.message : '삭제 실패');
      setFormError(true);
    } finally {
      setFormBusy(false);
    }
  }

  const isUpdate = !!draft.targetId;

  return (
    <div className="view-grid targets-grid">
      {/* Left column: search + target list */}
      <div className="col-stack">
        {/* Search panel */}
        <section className="panel">
          <div className="panel-head">
            <span className="eyebrow">Instrument Search ({selectedCapability?.label ?? 'no source'})</span>
          </div>
          <form className="search-bar" onSubmit={doSearch}>
            <input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={selectedCapability?.provider === 'ccxt' ? 'BTC/USDT · BTCUSDT' : '005930 · 삼성전자'}
            />
            <button type="submit" disabled={searchBusy || !selectedCapability}>
              {searchBusy ? '…' : '검색'}
            </button>
          </form>
          {searchMsg && <div className="hint">{searchMsg}</div>}
          {searchResults.length > 0 && (
            <div className="tbl-wrap">
              <table>
                <thead>
                  <tr>
                    <th>심볼</th>
                    <th>raw</th>
                    <th>등록</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {searchResults.map((r) => {
                    const registered = targets.some(
                      (t) => t.canonical_symbol && t.canonical_symbol === r.canonical_symbol,
                    );
                    return (
                      <tr key={r.canonical_symbol ?? r.instrument.symbol}>
                        <td>
                          <strong>{r.instrument.symbol}</strong>
                          <div className="sub">{r.display_name}</div>
                        </td>
                        <td className="mono small">{r.instrument.raw_symbol ?? '—'}</td>
                        <td>
                          {registered ? (
                            <Badge label="등록됨" tone="good" />
                          ) : (
                            <Badge label="신규" tone="muted" />
                          )}
                        </td>
                        <td>
                          <button
                            type="button"
                            className="sm-btn"
                            onClick={() => pickSearchResult(r)}
                          >
                            선택
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {/* Target list */}
        <section className="panel">
          <div className="panel-head">
            <span className="eyebrow">Registered Targets</span>
            <span className="count-pill">{targets.length}</span>
          </div>
          {targets.length === 0 ? (
            <Empty msg="등록된 수집 대상 없음" />
          ) : (
            <div className="tbl-wrap">
              <table>
                <thead>
                  <tr>
                    <th>소스 · 심볼</th>
                    <th>이벤트</th>
                    <th>상태</th>
                    <th>마지막 이벤트</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {targets.map((t) => {
                    const st = statusMap[t.target_id];
                    const permanent = st?.permanent_failure;
                    const provider = (t.provider ?? 'kxt') as string;
                    const itype = t.instrument.instrument_type ?? '—';
                    const failureSummary = permanent
                      ? `${st?.failure_reason ?? 'permanent_failure'}${st?.failure_rt_cd ? ` (${st.failure_rt_cd})` : ''}${st?.failure_attempts != null ? ` · ${st.failure_attempts}회` : ''}`
                      : '';
                    const failureTooltip = permanent
                      ? [
                          st?.failure_reason && `reason: ${st.failure_reason}`,
                          st?.failure_rt_cd && `rt_cd: ${st.failure_rt_cd}`,
                          st?.failure_msg && `msg: ${st.failure_msg}`,
                          st?.failure_attempts != null && `attempts: ${st.failure_attempts}`,
                        ]
                          .filter(Boolean)
                          .join('\n')
                      : undefined;
                    return (
                      <tr key={t.target_id} className={draft.targetId === t.target_id ? 'row-selected' : ''}>
                        <td>
                          <strong>{t.instrument.symbol}</strong>
                          <div className="sub">
                            <Badge label={provider.toUpperCase()} tone="default" />
                            {' '}
                            <Badge label={itype} tone="default" />
                            {t.market_scope && (
                              <>
                                {' '}
                                <Badge label={t.market_scope.toUpperCase()} tone="default" />
                              </>
                            )}
                            {' '}
                            {t.enabled ? (
                              <Badge label="활성" tone="good" />
                            ) : (
                              <Badge label="비활성" tone="muted" />
                            )}
                          </div>
                          {t.canonical_symbol && (
                            <div className="sub mono small">{t.canonical_symbol}</div>
                          )}
                        </td>
                        <td className="event-pills">
                          {t.event_types.map((ev) => (
                            <Badge key={ev} label={ev} tone="default" />
                          ))}
                        </td>
                        <td>
                          {permanent ? (
                            <div title={failureTooltip} className="perm-failure-cell">
                              <Badge label="영구 실패" tone="danger" />
                              <div className="sub mono small">{failureSummary}</div>
                            </div>
                          ) : (
                            <Badge label={st?.state ?? 'unknown'} tone={stateTone(st?.state)} />
                          )}
                        </td>
                        <td className="mono">{fmt(st?.last_event_at)}</td>
                        <td>
                          <div className="row-actions">
                            <button
                              type="button"
                              className="sm-btn"
                              onClick={() => pickTarget(t)}
                            >
                              편집
                            </button>
                            <ConfirmButton
                              label="삭제"
                              confirmLabel="삭제 확인"
                              onConfirm={() => void doDelete(t.target_id)}
                              disabled={formBusy || snapshotBusy}
                              className="sm-btn danger-sm"
                            />
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>

      {/* Right column: edit form */}
      <section className="panel form-panel">
        <div className="panel-head">
          <span className="eyebrow">{isUpdate ? 'Update Target' : 'New Target'}</span>
          {draft.symbol && (
            <span className="identity-label">
              {draft.symbol}
              {selectedCapability ? ` · ${selectedCapability.label}` : ''}
            </span>
          )}
        </div>

        <form className="col-stack" onSubmit={doSave}>
          {/* Source preset (provider/venue/instrument_type) */}
          <label className="field">
            <span>소스 (provider · venue · instrument_type)</span>
            <select
              value={draft.capabilityKey}
              onChange={(e) => changeSourcePreset(e.target.value)}
            >
              {capabilities.map((c) => (
                <option key={capabilityKey(c)} value={capabilityKey(c)}>
                  {c.label}
                </option>
              ))}
            </select>
          </label>

          <div className="field-row">
            <label className="field">
              <span>심볼 (display)</span>
              <input
                value={draft.symbol}
                onChange={(e) => setDraft((d) => ({ ...d, symbol: e.target.value }))}
                placeholder={selectedCapability?.provider === 'ccxt' ? 'BTC/USDT' : '005930'}
              />
            </label>
            {selectedCapability?.provider === 'ccxt' && (
              <label className="field">
                <span>raw_symbol</span>
                <input
                  value={draft.rawSymbol}
                  onChange={(e) => setDraft((d) => ({ ...d, rawSymbol: e.target.value }))}
                  placeholder="BTCUSDT"
                />
              </label>
            )}
            {selectedCapability?.market_scope_required && (
              <label className="field">
                <span>시장 범위</span>
                <select
                  value={draft.scope}
                  onChange={(e) => setDraft((d) => ({ ...d, scope: e.target.value as MarketScope }))}
                >
                  {MARKET_SCOPES.map((s) => (
                    <option key={s} value={s}>
                      {s.toUpperCase()}
                    </option>
                  ))}
                </select>
              </label>
            )}
          </div>

          <div className="field-block">
            <div className="field-block-head">
              <span>수집 이벤트 (capability-filtered)</span>
              <div className="inline-actions">
                <button
                  type="button"
                  className="sm-btn"
                  onClick={() =>
                    setDraft((d) => ({
                      ...d,
                      eventTypes: selectedCapability ? [...selectedCapability.supported_event_types] : [],
                    }))
                  }
                >
                  전체
                </button>
                <button
                  type="button"
                  className="sm-btn"
                  onClick={() => setDraft((d) => ({ ...d, eventTypes: [] }))}
                >
                  해제
                </button>
              </div>
            </div>
            <div className="check-grid">
              {(selectedCapability?.supported_event_types ?? []).map((evType) => {
                const checked = draft.eventTypes.includes(evType);
                return (
                  <label key={evType} className="check-card">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() =>
                        setDraft((d) => ({
                          ...d,
                          eventTypes: checked
                            ? d.eventTypes.filter((v) => v !== evType)
                            : [...d.eventTypes, evType],
                        }))
                      }
                    />
                    <span>
                      <strong>{evType}</strong>
                      <small>{eventDescByName[evType] ?? ''}</small>
                    </span>
                  </label>
                );
              })}
            </div>
          </div>

          <label className="toggle-row">
            <input
              type="checkbox"
              checked={draft.enabled}
              onChange={(e) => setDraft((d) => ({ ...d, enabled: e.target.checked }))}
            />
            <span>저장 후 즉시 수집 활성화</span>
          </label>

          <div className="form-actions">
            <button type="submit" disabled={formBusy || snapshotBusy}>
              {formBusy ? '처리 중…' : isUpdate ? '업데이트 저장' : '새 대상 저장'}
            </button>
            <button type="button" className="secondary-button" onClick={resetDraft}>
              초기화
            </button>
          </div>

          {formMsg && <Banner msg={formMsg} error={formError} />}
        </form>
      </section>
    </div>
  );
}

// ─── Runtime View ─────────────────────────────────────────────────────────────

interface CollectorContainerStatus {
  status: string;
  container_id: string | null;
  name: string | null;
  error?: string;
}

function RuntimeView({ snapshot, onRefresh }: { snapshot: Snapshot | null; onRefresh?: () => Promise<void> }) {
  const runtime = snapshot?.runtime_status ?? [];
  const providerRuntimes = snapshot?.source_runtime_status ?? [];
  const targetStatuses = snapshot?.collection_target_status ?? [];
  const targets = snapshot?.collection_targets ?? [];

  const [containerStatus, setContainerStatus] = useState<CollectorContainerStatus | null>(null);
  const [containerBusy, setContainerBusy] = useState(false);
  const [containerMsg, setContainerMsg] = useState('');
  const [containerError, setContainerError] = useState(false);
  const [providerBusy, setProviderBusy] = useState<string>('');
  const [providerMsg, setProviderMsg] = useState('');
  const [providerError, setProviderError] = useState(false);

  const fetchContainerStatus = useCallback(async () => {
    try {
      const r = await requestJson<CollectorContainerStatus>('/api/admin/collector/status');
      setContainerStatus(r);
    } catch {
      setContainerStatus({ status: 'error', container_id: null, name: null });
    }
  }, []);

  useEffect(() => {
    void fetchContainerStatus();
  }, [fetchContainerStatus]);

  async function doContainerAction(action: 'start' | 'stop') {
    setContainerBusy(true);
    setContainerMsg('');
    setContainerError(false);
    try {
      const r = await requestJson<{ ok: boolean; status?: string; error?: string }>(
        `/api/admin/collector/${action}`,
        { method: 'POST' },
      );
      if (r.ok) {
        setContainerMsg(action === 'start' ? '컨테이너 시작됨' : '컨테이너 정지됨');
        await fetchContainerStatus();
        // Refresh root snapshot so the app reflects the new collector state.
        if (onRefresh) await onRefresh();
      } else {
        setContainerMsg(r.error ?? `${action} 실패`);
        setContainerError(true);
      }
    } catch (err) {
      setContainerMsg(err instanceof Error ? err.message : `${action} 실패`);
      setContainerError(true);
    } finally {
      setContainerBusy(false);
    }
  }

  async function doProviderAction(provider: string, action: 'start' | 'stop') {
    setProviderBusy(`${provider}:${action}`);
    setProviderMsg('');
    setProviderError(false);
    try {
      const r = await requestJson<{ provider?: string; enabled?: boolean; error?: string }>(
        `/api/admin/providers/${provider}/${action}`,
        { method: 'POST' },
      );
      if (r.error) {
        setProviderMsg(r.error);
        setProviderError(true);
      } else {
        setProviderMsg(`${provider.toUpperCase()} ${action === 'start' ? '시작됨' : '정지됨'}`);
        if (onRefresh) await onRefresh();
      }
    } catch (err) {
      setProviderMsg(err instanceof Error ? err.message : `${action} 실패`);
      setProviderError(true);
    } finally {
      setProviderBusy('');
    }
  }

  const isRunning = containerStatus?.status === 'running';
  const containerTone = containerStatus
    ? containerStatus.status === 'running'
      ? 'good'
      : containerStatus.status === 'exited' || containerStatus.status === 'not_found'
        ? 'muted'
        : 'danger'
    : 'muted';

  // Build symbol+scope label from targets for better UX
  const labelByTargetId = Object.fromEntries(
    targets.map((t) => {
      const provider = (t.provider ?? 'kxt').toUpperCase();
      const itype = t.instrument.instrument_type ?? '—';
      const scope = t.market_scope ? ` · ${t.market_scope.toUpperCase()}` : '';
      return [t.target_id, `${t.instrument.symbol} · ${provider} · ${itype}${scope}`];
    }),
  );

  return (
    <div className="col-stack">
      {/* Collector service lifecycle control */}
      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Collector Service</span>
          {containerStatus && (
            <Badge label={containerStatus.status} tone={containerTone} />
          )}
        </div>
        <div className="service-control-row">
          <div className="service-meta">
            {containerStatus?.name && (
              <span className="meta-key mono">{containerStatus.name}</span>
            )}
            {containerStatus?.container_id && (
              <span className="meta-key mono">#{containerStatus.container_id}</span>
            )}
            {containerStatus?.error && (
              <span className="hint error-hint">{containerStatus.error}</span>
            )}
          </div>
          <div className="row-actions">
            <button
              type="button"
              className="sm-btn"
              onClick={() => void fetchContainerStatus()}
              disabled={containerBusy}
            >
              새로고침
            </button>
            <button
              type="button"
              className="sm-btn"
              onClick={() => void doContainerAction('start')}
              disabled={containerBusy || isRunning}
            >
              시작
            </button>
            <ConfirmButton
              label="정지"
              confirmLabel="정지 확인"
              onConfirm={() => void doContainerAction('stop')}
              disabled={containerBusy || !isRunning}
              className="sm-btn danger-sm"
            />
          </div>
        </div>
        {containerMsg && <Banner msg={containerMsg} error={containerError} />}
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Provider Runtimes</span>
          <span className="count-pill">{providerRuntimes.length}</span>
        </div>
        {providerRuntimes.length === 0 ? (
          <Empty msg="프로바이더 상태 없음" />
        ) : (
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>프로바이더</th>
                  <th>상태</th>
                  <th>활성</th>
                  <th>활성 타깃 수</th>
                  <th>마지막 오류</th>
                  <th>관측 시각</th>
                  <th>작업</th>
                </tr>
              </thead>
              <tbody>
                {providerRuntimes.map((p) => {
                  const startKey = `${p.provider}:start`;
                  const stopKey = `${p.provider}:stop`;
                  return (
                    <tr key={p.provider}>
                      <td>
                        <strong>{p.provider.toUpperCase()}</strong>
                      </td>
                      <td>
                        <Badge label={p.state} tone={stateTone(p.state)} />
                      </td>
                      <td>
                        <Badge
                          label={p.enabled ? 'enabled' : 'disabled'}
                          tone={p.enabled ? 'good' : 'muted'}
                        />
                      </td>
                      <td>{p.active_target_count}</td>
                      <td className="error-cell">{p.last_error ?? '—'}</td>
                      <td className="mono">{fmt(p.observed_at ?? null)}</td>
                      <td>
                        <div className="row-actions">
                          <button
                            type="button"
                            className="sm-btn"
                            disabled={providerBusy === startKey || p.enabled}
                            onClick={() => void doProviderAction(p.provider, 'start')}
                          >
                            시작
                          </button>
                          <button
                            type="button"
                            className="sm-btn danger-sm"
                            disabled={providerBusy === stopKey || !p.enabled}
                            onClick={() => void doProviderAction(p.provider, 'stop')}
                          >
                            정지
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        {providerMsg && <Banner msg={providerMsg} error={providerError} />}
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Service Health</span>
        </div>
        {runtime.length === 0 ? (
          <Empty msg="서비스 상태 없음" />
        ) : (
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>컴포넌트</th>
                  <th>상태</th>
                  <th>활성 타깃 수</th>
                  <th>마지막 오류</th>
                  <th>관측 시각</th>
                </tr>
              </thead>
              <tbody>
                {runtime.map((r) => (
                  <tr key={r.component}>
                    <td>
                      <strong>{r.component}</strong>
                    </td>
                    <td>
                      <Badge label={r.state} tone={stateTone(r.state)} />
                    </td>
                    <td>{r.active_collection_target_ids.length}</td>
                    <td className="error-cell">{r.last_error ?? '—'}</td>
                    <td className="mono">{fmt(r.observed_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Per-Target Status</span>
          <span className="count-pill">{targetStatuses.length}</span>
        </div>
        {targetStatuses.length === 0 ? (
          <Empty msg="대상별 상태 없음" />
        ) : (
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>종목 · 시장</th>
                  <th>상태</th>
                  <th>마지막 이벤트</th>
                  <th>마지막 오류</th>
                  <th>관측 시각</th>
                </tr>
              </thead>
              <tbody>
                {targetStatuses.map((s) => {
                  const permanent = s.permanent_failure;
                  const summary = permanent
                    ? `${s.failure_reason ?? 'permanent_failure'}${s.failure_rt_cd ? ` (${s.failure_rt_cd})` : ''}${s.failure_attempts != null ? ` · ${s.failure_attempts}회` : ''}`
                    : '';
                  const tooltip = permanent
                    ? [
                        s.failure_reason && `reason: ${s.failure_reason}`,
                        s.failure_rt_cd && `rt_cd: ${s.failure_rt_cd}`,
                        s.failure_msg && `msg: ${s.failure_msg}`,
                        s.failure_attempts != null && `attempts: ${s.failure_attempts}`,
                      ]
                        .filter(Boolean)
                        .join('\n')
                    : undefined;
                  return (
                    <tr key={s.target_id}>
                      <td>
                        <strong>{labelByTargetId[s.target_id] ?? s.target_id}</strong>
                      </td>
                      <td>
                        {permanent ? (
                          <div title={tooltip} className="perm-failure-cell">
                            <Badge label="영구 실패" tone="danger" />
                            <div className="sub mono small">{summary}</div>
                          </div>
                        ) : (
                          <Badge label={s.state} tone={stateTone(s.state)} />
                        )}
                      </td>
                      <td className="mono">{fmt(s.last_event_at)}</td>
                      <td className="error-cell">{s.last_error ?? '—'}</td>
                      <td className="mono">{fmt(s.observed_at)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

// ─── Events View ──────────────────────────────────────────────────────────────

function EventsView({ snapshot }: { snapshot: Snapshot | null }) {
  const targets = snapshot?.collection_targets ?? [];

  const [events, setEvents] = useState<RecentRuntimeEvent[]>([]);
  const [bufferSize, setBufferSize] = useState<number>(0);
  const [capturedAt, setCapturedAt] = useState<string>('');
  const [availableNames, setAvailableNames] = useState<string[]>([]);
  const [filterSymbol, setFilterSymbol] = useState('');
  const [filterScope, setFilterScope] = useState('');
  const [filterName, setFilterName] = useState('');
  const [limit, setLimit] = useState(50);
  const [streamStatus, setStreamStatus] = useState<'connecting' | 'live' | 'error'>('connecting');
  const [streamError, setStreamError] = useState('');
  const [expandedId, setExpandedId] = useState<string | null>(null);

  // Pending filter values — applied on "적용" click to avoid reconnecting on each keystroke
  const [pendingSymbol, setPendingSymbol] = useState('');
  const [pendingScope, setPendingScope] = useState('');
  const [pendingName, setPendingName] = useState('');
  const [pendingLimit, setPendingLimit] = useState(50);

  const esRef = useRef<EventSource | null>(null);

  const buildStreamUrl = useCallback(
    (sym: string, scope: string, name: string, lim: number) => {
      const params = new URLSearchParams();
      if (sym.trim()) params.set('symbol', sym.trim());
      if (scope) params.set('scope', scope);
      if (name) params.set('event_name', name);
      params.set('limit', String(lim));
      return `/api/admin/events/stream?${params.toString()}`;
    },
    [],
  );

  const openStream = useCallback(
    (sym: string, scope: string, name: string, lim: number) => {
      // Close any existing stream first
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }

      setStreamStatus('connecting');
      setStreamError('');

      const url = buildStreamUrl(sym, scope, name, lim);
      const es = new EventSource(url);
      esRef.current = es;

      es.addEventListener('connected', () => {
        // Server sends 'connected' immediately on stream open — go live right away.
        setStreamStatus('live');
        setStreamError('');
      });

      es.addEventListener('events', (e: MessageEvent) => {
        try {
          const batch = JSON.parse(e.data) as {
            new_events: RecentRuntimeEvent[];
            available_event_names: string[];
            buffer_size: number;
            captured_at: string;
          };
          setStreamStatus('live');
          if (batch.new_events && batch.new_events.length > 0) {
            setEvents((prev) => {
              // Prepend new events; keep total at most lim entries
              const combined = [...batch.new_events, ...prev];
              return combined.slice(0, lim);
            });
          }
          if (batch.available_event_names) setAvailableNames(batch.available_event_names);
          if (batch.buffer_size !== undefined) setBufferSize(batch.buffer_size);
          if (batch.captured_at) setCapturedAt(batch.captured_at);
        } catch {
          // ignore parse errors
        }
      });

      // 'upstream_error' is a server-side named event signalling that the
      // collector is temporarily unreachable.  It is intentionally distinct
      // from the browser-native EventSource error (transport failure).
      es.addEventListener('upstream_error', (e: Event) => {
        const msgEvt = e as MessageEvent;
        let errMsg = '수집기 연결 오류';
        try {
          const parsed = JSON.parse(msgEvt.data) as { error?: string };
          if (parsed.error) errMsg = parsed.error;
        } catch {
          // ignore
        }
        // Keep streamStatus as 'live' — the SSE transport is still up.
        // Show the upstream error inline without marking the whole stream broken.
        setStreamError(errMsg);
      });

      es.onopen = () => {
        // Transport-level open: mark as connected even before the first data.
        setStreamStatus('live');
        setStreamError('');
      };

      es.onerror = () => {
        // EventSource will auto-reconnect; only flag as error when truly closed.
        if (es.readyState === EventSource.CLOSED) {
          setStreamStatus('error');
          setStreamError('스트림 연결이 끊어졌습니다. 재연결 중…');
        } else {
          // readyState is CONNECTING — browser is auto-reconnecting, stay 'connecting'
          setStreamStatus('connecting');
        }
      };
    },
    [buildStreamUrl],
  );

  // Open stream on mount with default filters
  useEffect(() => {
    openStream(filterSymbol, filterScope, filterName, limit);
    return () => {
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Apply pending filters and reconnect stream
  function applyFilters() {
    setFilterSymbol(pendingSymbol);
    setFilterScope(pendingScope);
    setFilterName(pendingName);
    setLimit(pendingLimit);
    setEvents([]);
    openStream(pendingSymbol, pendingScope, pendingName, pendingLimit);
  }

  // Build symbol options from known targets
  const symbolOptions = [...new Set(targets.map((t) => t.instrument.symbol))];

  const statusLabel =
    streamStatus === 'connecting'
      ? '연결 중…'
      : streamStatus === 'live'
        ? '실시간'
        : '오류';
  const statusClass =
    streamStatus === 'connecting'
      ? 'hint'
      : streamStatus === 'live'
        ? 'hint live-hint'
        : 'hint error-hint';

  return (
    <div className="col-stack">
      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Recent Runtime Events</span>
          <span className={statusClass}>{statusLabel}{capturedAt ? ` · 캡처: ${fmt(capturedAt)}` : ''}</span>
        </div>

        {/* Filters — changes are pending until "적용" is clicked */}
        <div className="filter-bar">
          <select value={pendingSymbol} onChange={(e) => setPendingSymbol(e.target.value)}>
            <option value="">모든 종목</option>
            {symbolOptions.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
          <select value={pendingScope} onChange={(e) => setPendingScope(e.target.value)}>
            <option value="">모든 시장</option>
            {MARKET_SCOPES.map((s) => (
              <option key={s} value={s}>
                {s.toUpperCase()}
              </option>
            ))}
          </select>
          <select value={pendingName} onChange={(e) => setPendingName(e.target.value)}>
            <option value="">모든 이벤트</option>
            {availableNames.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
          <select
            value={pendingLimit}
            onChange={(e) => setPendingLimit(Number(e.target.value))}
            className="narrow-select"
          >
            {[25, 50, 100, 200].map((n) => (
              <option key={n} value={n}>
                {n}건
              </option>
            ))}
          </select>
          <button type="button" onClick={applyFilters}>
            적용
          </button>
        </div>

        {streamStatus === 'error' && streamError && (
          <div className="hint error-hint">{streamError}</div>
        )}
        {streamStatus !== 'error' && streamError && (
          <div className="hint error-hint">수집기 오류: {streamError}</div>
        )}

        {events.length === 0 ? (
          <Empty msg="최근 이벤트 없음 — 수집 대상을 활성화하면 이벤트가 쌓입니다." />
        ) : (
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>시각</th>
                  <th>이벤트</th>
                  <th>종목 · 시장</th>
                  <th>토픽</th>
                  <th>매칭 타깃</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {events.map((ev) => (
                  <>
                    <tr
                      key={ev.event_id}
                      className={expandedId === ev.event_id ? 'row-expanded' : ''}
                    >
                      <td className="mono">{fmt(ev.published_at)}</td>
                      <td>
                        <Badge label={ev.event_name} tone="default" />
                      </td>
                      <td>
                        <strong>{ev.symbol}</strong>
                        {ev.provider && (
                          <>
                            <span className="sub"> · </span>
                            <Badge label={ev.provider.toUpperCase()} tone="default" />
                          </>
                        )}
                        {ev.instrument_type && (
                          <>
                            {' '}
                            <Badge label={ev.instrument_type} tone="default" />
                          </>
                        )}
                        {ev.market_scope && (
                          <span className="sub"> · {ev.market_scope.toUpperCase()}</span>
                        )}
                        {ev.canonical_symbol && (
                          <div className="sub mono small">{ev.canonical_symbol}</div>
                        )}
                      </td>
                      <td className="mono small">{ev.topic_name}</td>
                      <td>{ev.matched_target_ids.length > 0 ? ev.matched_target_ids.length : '—'}</td>
                      <td>
                        {ev.payload && Object.keys(ev.payload).length > 0 && (
                          <button
                            type="button"
                            className="sm-btn"
                            onClick={() =>
                              setExpandedId(expandedId === ev.event_id ? null : ev.event_id)
                            }
                          >
                            {expandedId === ev.event_id ? '닫기' : '페이로드'}
                          </button>
                        )}
                      </td>
                    </tr>
                    {expandedId === ev.event_id && ev.payload && (
                      <tr key={`${ev.event_id}-payload`} className="payload-row">
                        <td colSpan={6}>
                          <pre className="payload-pre">{JSON.stringify(ev.payload, null, 2)}</pre>
                        </td>
                      </tr>
                    )}
                  </>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div className="hint">
          버퍼 크기: {bufferSize} · 최대 표시 {limit}건
        </div>
      </section>
    </div>
  );
}

// ─── Root App ─────────────────────────────────────────────────────────────────

export default function App() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [snapshotError, setSnapshotError] = useState('');
  const [snapshotBusy, setSnapshotBusy] = useState(true);
  const [sessionRecoveredFlash, setSessionRecoveredFlash] = useState(false);
  const recoveryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const refreshSnapshot = useCallback(async () => {
    setSnapshotBusy(true);
    setSnapshotError('');
    try {
      const next = await requestJson<Snapshot>('/api/admin/snapshot');
      setSnapshot(next);
      // collector_offline is a graceful degraded state, not a global error.
      // Leave snapshotError empty so no top-banner is shown.
    } catch (err) {
      // Only surface as a global error if the fetch itself fails (network
      // error, non-200 that wasn't converted to collector_offline, etc.)
      setSnapshotError(err instanceof Error ? err.message : '스냅샷 로드 실패');
    } finally {
      setSnapshotBusy(false);
    }
  }, []);

  useEffect(() => {
    void refreshSnapshot();
  }, [refreshSnapshot]);

  // Subscribe to the admin meta-event SSE channel (session_recovered,
  // session_state_changed) so the top bar can flash a banner without
  // polluting the market-data event table.  We reuse the existing
  // /api/admin/events/stream endpoint because the collector emits meta
  // events on the same stream with distinct SSE event names.
  useEffect(() => {
    const es = new EventSource('/api/admin/events/stream?limit=1');
    es.addEventListener('session_recovered', () => {
      setSessionRecoveredFlash(true);
      if (recoveryTimer.current) clearTimeout(recoveryTimer.current);
      recoveryTimer.current = setTimeout(() => setSessionRecoveredFlash(false), 3000);
      // Refresh snapshot so permanent-failure/error state clears are
      // visible immediately rather than on the next poll.
      void refreshSnapshot();
    });
    es.addEventListener('session_state_changed', () => {
      void refreshSnapshot();
    });
    return () => {
      es.close();
      if (recoveryTimer.current) clearTimeout(recoveryTimer.current);
    };
  }, [refreshSnapshot]);

  const targets = snapshot?.collection_targets ?? [];
  const runtime = snapshot?.runtime_status ?? [];
  const anyError = runtime.some((r) => stateTone(r.state) === 'danger');
  const sessionDegraded = (snapshot?.session_state ?? '').toUpperCase() === 'DEGRADED';

  return (
    <div className="shell">
      {/* Top bar */}
      <header className="topbar">
        <div className="topbar-brand">
          <span className="brand-label">KIS Admin</span>
          <span className="brand-sub">Collector Control Plane</span>
        </div>
        <div className="topbar-meta">
          <span className="meta-stat">
            <span className="meta-key">Targets</span>
            <strong>{targets.length}</strong>
          </span>
          <span className="meta-stat">
            <span className="meta-key">Health</span>
            <Badge
              label={anyError ? 'degraded' : runtime.length > 0 ? 'ok' : 'no data'}
              tone={anyError ? 'danger' : runtime.length > 0 ? 'good' : 'muted'}
            />
          </span>
          {snapshot && (
            <span className="meta-stat">
              <span className="meta-key">Snapshot</span>
              <span className="mono small">{fmt(snapshot.captured_at)}</span>
            </span>
          )}
          <button
            type="button"
            className="secondary-button sm-topbar-btn"
            onClick={() => void refreshSnapshot()}
            disabled={snapshotBusy}
          >
            {snapshotBusy ? '…' : '새로고침'}
          </button>
        </div>
      </header>

      {snapshotError && (
        <div className="global-banner error">{snapshotError}</div>
      )}
      {!snapshotError && snapshot?.collector_offline && (
        <div className="global-banner degraded">
          수집기 컨테이너가 오프라인 상태입니다 (container: {snapshot.container_status ?? 'offline'}) — Runtime 탭에서 시작할 수 있습니다.
        </div>
      )}
      {sessionDegraded && (
        <div className="global-banner session-degraded">
          KSXT 실시간 세션이 DEGRADED 상태입니다 — 재연결 중. 구독 이벤트가 일시적으로 중단될 수 있습니다.
        </div>
      )}
      {sessionRecoveredFlash && (
        <div className="global-banner session-recovered">
          KSXT 실시간 세션 복구 완료 — 재구독이 정상 상태로 돌아왔습니다.
        </div>
      )}

      {/* Body: sidebar + main content */}
      <div className="body-layout">
        <nav className="sidebar">
          <NavLink to="/targets" className={({ isActive }) => `nav-btn${isActive ? ' active' : ''}`}>
            Targets
          </NavLink>
          <NavLink to="/runtime" className={({ isActive }) => `nav-btn${isActive ? ' active' : ''}`}>
            Runtime
          </NavLink>
          <NavLink to="/events" className={({ isActive }) => `nav-btn${isActive ? ' active' : ''}`}>
            Events
          </NavLink>
          <NavLink to="/charts" className={({ isActive }) => `nav-btn${isActive ? ' active' : ''}`}>
            Charts
          </NavLink>
        </nav>

        <main className="main">
          <Routes>
            <Route index element={<Navigate to="/targets" replace />} />
            <Route
              path="targets"
              element={
                <TargetsView
                  snapshot={snapshot}
                  snapshotBusy={snapshotBusy}
                  onRefresh={refreshSnapshot}
                />
              }
            />
            <Route path="runtime" element={<RuntimeView snapshot={snapshot} onRefresh={refreshSnapshot} />} />
            <Route path="events" element={<EventsView snapshot={snapshot} />} />
            <Route
              path="charts"
              element={
                <ChartsView
                  capabilities={(snapshot?.source_capabilities ?? []).map((c) => ({
                    provider: c.provider,
                    venue: c.venue,
                    instrument_type: c.instrument_type,
                    supported_event_types: c.supported_event_types,
                    label: c.label,
                  }))}
                  targets={(snapshot?.collection_targets ?? []).map((t) => ({
                    target_id: t.target_id,
                    instrument: {
                      symbol: t.instrument.symbol,
                      instrument_type: t.instrument.instrument_type,
                    },
                    provider: t.provider,
                  }))}
                />
              }
            />
            <Route path="*" element={<Navigate to="/targets" replace />} />
          </Routes>
        </main>
      </div>
    </div>
  );
}
