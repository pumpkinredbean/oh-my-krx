import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import GridLayout, { Layout } from 'react-grid-layout';
import Editor from '@monaco-editor/react';
import { createChart, IChartApi, ISeriesApi, LineData, CandlestickData, UTCTimestamp } from 'lightweight-charts';

import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

import hubStubSource from '../assets/hub-stub.py?raw';

// ─── API contract types ───────────────────────────────────────────────────

export interface ChartPanelSpec {
  panel_id: string;
  chart_type: 'line' | 'candle';
  symbol: string;
  source: string;
  series_ref: string;
  x: number;
  y: number;
  w: number;
  h: number;
  title?: string | null;
  notes?: string | null;
}

interface IndicatorScriptSpec {
  script_id: string;
  name: string;
  source: string;
  class_name: string;
  builtin: boolean;
  description?: string | null;
}

interface IndicatorInstanceSpec {
  instance_id: string;
  script_id: string;
  symbol: string;
  market_scope: string;
  params: Record<string, unknown>;
  enabled: boolean;
}

interface IndicatorErrorRow {
  instance_id: string;
  script_id: string;
  symbol: string;
  state: string;
  last_error: string | null;
  last_output_at: string | null;
  output_count: number;
}

interface SeriesPoint {
  timestamp: string;
  value: number;
  meta?: Record<string, unknown>;
}

interface IndicatorOutputEnvelope {
  instance_id: string;
  script_id: string;
  name: string;
  symbol: string;
  market_scope: string;
  output_kind: string;
  published_at: string;
  point: SeriesPoint;
}

// ─── LocalStorage keys ───────────────────────────────────────────────────

const LS_PREFIX = 'korea-market-data-hub.admin-charts';
const LS_PREFERRED = `${LS_PREFIX}.preferredLayout.v1`;
const LS_WORKING = `${LS_PREFIX}.workingLayout.v1`;

// Default layout shipped with the SPA — used when neither preferred nor
// working layout is present in localStorage.
const DEFAULT_LAYOUT: Layout[] = [];

// ─── Small helpers ────────────────────────────────────────────────────────

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  const body = await response.json().catch(() => ({ error: 'invalid json' }));
  if (!response.ok) {
    const msg =
      (body as { error?: string; detail?: string }).error ??
      (body as { detail?: string }).detail ??
      `request failed: ${response.status}`;
    const err = new Error(msg) as Error & { payload?: unknown; status?: number };
    err.payload = body;
    err.status = response.status;
    throw err;
  }
  return body as T;
}

function uid(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

function loadLayout(key: string): Layout[] | null {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed as Layout[];
  } catch {
    // ignore
  }
  return null;
}

function saveLayout(key: string, layout: Layout[]): void {
  try {
    localStorage.setItem(key, JSON.stringify(layout));
  } catch {
    // ignore quota
  }
}

// ─── Panel content: line / candle chart ───────────────────────────────────

function ChartPanel({
  spec,
  indicatorOutputs,
  rawEvents,
}: {
  spec: ChartPanelSpec;
  indicatorOutputs: Map<string, SeriesPoint[]>;
  rawEvents: Map<string, unknown[]>;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Line'> | ISeriesApi<'Candlestick'> | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      layout: { background: { color: '#1a1a1a' }, textColor: '#d0d0d0' },
      grid: { vertLines: { color: '#262626' }, horzLines: { color: '#262626' } },
      timeScale: { timeVisible: true, secondsVisible: true },
      autoSize: true,
    });
    chartRef.current = chart;
    if (spec.chart_type === 'candle') {
      seriesRef.current = chart.addCandlestickSeries();
    } else {
      seriesRef.current = chart.addLineSeries({ color: '#4aa3ff', lineWidth: 2 });
    }
    return () => {
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [spec.chart_type]);

  // Feed data.
  useEffect(() => {
    if (!seriesRef.current) return;
    if (spec.chart_type === 'line') {
      const pts = indicatorOutputs.get(spec.series_ref) ?? [];
      const data: LineData[] = pts.map((p) => ({
        time: Math.floor(new Date(p.timestamp).getTime() / 1000) as UTCTimestamp,
        value: p.value,
      }));
      // Sort by time for lightweight-charts contract.
      data.sort((a, b) => (a.time as number) - (b.time as number));
      (seriesRef.current as ISeriesApi<'Line'>).setData(data);
    } else if (spec.chart_type === 'candle') {
      const key = `${spec.symbol}:${spec.series_ref}`;
      const rows = (rawEvents.get(key) ?? []) as Array<Record<string, any>>;
      const data: CandlestickData[] = rows
        .map((r) => {
          const ts = r.timestamp ?? r.published_at ?? r.time;
          return {
            time: Math.floor(new Date(ts).getTime() / 1000) as UTCTimestamp,
            open: Number(r.open),
            high: Number(r.high),
            low: Number(r.low),
            close: Number(r.close),
          };
        })
        .filter((d) => Number.isFinite(d.open) && Number.isFinite(d.close))
        .sort((a, b) => (a.time as number) - (b.time as number));
      (seriesRef.current as ISeriesApi<'Candlestick'>).setData(data);
    }
  }, [spec.chart_type, spec.series_ref, spec.symbol, indicatorOutputs, rawEvents]);

  return <div className="chart-host" ref={containerRef} />;
}

// ─── Main ChartsView ──────────────────────────────────────────────────────

interface ChartsViewProps {
  capabilities: Array<{
    provider: string;
    venue: string;
    instrument_type: string;
    supported_event_types: string[];
    label: string;
  }>;
  targets: Array<{
    target_id: string;
    instrument: { symbol: string; instrument_type?: string | null };
    provider?: string | null;
  }>;
}

export default function ChartsView({ capabilities, targets }: ChartsViewProps) {
  const [panels, setPanels] = useState<ChartPanelSpec[]>([]);
  const [scripts, setScripts] = useState<IndicatorScriptSpec[]>([]);
  const [instances, setInstances] = useState<IndicatorInstanceSpec[]>([]);
  const [errors, setErrors] = useState<IndicatorErrorRow[]>([]);
  const [banner, setBanner] = useState('');
  const [bannerError, setBannerError] = useState(false);

  // Layout state (react-grid-layout)
  const [layout, setLayout] = useState<Layout[]>(
    () => loadLayout(LS_WORKING) ?? loadLayout(LS_PREFERRED) ?? DEFAULT_LAYOUT,
  );

  // Indicator output buffer (keyed by instance_id), bounded per series.
  const [indicatorOutputs, setIndicatorOutputs] = useState<Map<string, SeriesPoint[]>>(new Map());
  const [rawEvents] = useState<Map<string, unknown[]>>(new Map());

  // Script editor state.
  const [editingScriptId, setEditingScriptId] = useState<string | null>(null);
  const [editorSource, setEditorSource] = useState<string>(hubStubSource);
  const [editorName, setEditorName] = useState<string>('');
  const [editorClassName, setEditorClassName] = useState<string>('MyOBI');
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  // Refresh core state.
  const refresh = useCallback(async () => {
    try {
      const [p, s, i, e] = await Promise.all([
        apiJson<{ panels: ChartPanelSpec[] }>('/api/admin/charts/panels'),
        apiJson<{ scripts: IndicatorScriptSpec[] }>('/api/admin/charts/scripts'),
        apiJson<{ instances: IndicatorInstanceSpec[] }>('/api/admin/charts/instances'),
        apiJson<{ instances: IndicatorErrorRow[] }>('/api/admin/charts/errors'),
      ]);
      setPanels(p.panels ?? []);
      setScripts(s.scripts ?? []);
      setInstances(i.instances ?? []);
      setErrors(e.instances ?? []);
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'charts state load failed');
      setBannerError(true);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Subscribe to indicator output SSE.
  useEffect(() => {
    const es = new EventSource('/api/admin/charts/stream');
    es.addEventListener('indicator_output', (evt: MessageEvent) => {
      try {
        const payload = JSON.parse(evt.data) as IndicatorOutputEnvelope;
        setIndicatorOutputs((prev) => {
          const next = new Map(prev);
          const key = payload.instance_id;
          const cur = next.get(key) ?? [];
          const merged = [...cur, payload.point].slice(-500);
          next.set(key, merged);
          return next;
        });
      } catch {
        // ignore
      }
    });
    return () => es.close();
  }, []);

  // Capability lookup for panel-type disabling (candle requires OHLCV).
  const capsBySymbol = useMemo(() => {
    const map = new Map<string, Set<string>>();
    for (const t of targets) {
      const provider = (t.provider ?? 'kxt').toLowerCase();
      const venue = provider === 'kxt' ? 'krx' : 'binance';
      const itype = (t.instrument.instrument_type ?? 'spot').toLowerCase();
      const cap = capabilities.find(
        (c) => c.provider === provider && c.venue === venue && c.instrument_type === itype,
      );
      if (cap) {
        map.set(t.instrument.symbol, new Set(cap.supported_event_types));
      }
    }
    return map;
  }, [capabilities, targets]);

  function panelSupportsCandle(symbol: string): boolean {
    return capsBySymbol.get(symbol)?.has('ohlcv') ?? false;
  }

  // ── layout sync ──
  useEffect(() => {
    // Keep layout rows in sync with panel count: add rows for any panels
    // missing from layout; drop rows referencing deleted panels.
    setLayout((prev) => {
      const byId = new Map(prev.map((l) => [l.i, l] as const));
      let nextY = prev.reduce((m, l) => Math.max(m, l.y + l.h), 0);
      const merged: Layout[] = [];
      for (const panel of panels) {
        const existing = byId.get(panel.panel_id);
        if (existing) {
          merged.push(existing);
        } else {
          merged.push({
            i: panel.panel_id,
            x: panel.x ?? 0,
            y: nextY,
            w: Math.max(2, panel.w ?? 6),
            h: Math.max(3, panel.h ?? 6),
          });
          nextY += Math.max(3, panel.h ?? 6);
        }
      }
      return merged;
    });
  }, [panels]);

  // Persist working layout on every change; preferred layout only on explicit save.
  useEffect(() => {
    saveLayout(LS_WORKING, layout);
  }, [layout]);

  // ── actions ──

  async function addPanel(chartType: 'line' | 'candle') {
    const defaultSymbol = targets[0]?.instrument.symbol ?? '';
    try {
      const resp = await apiJson<{ panel: ChartPanelSpec }>(
        '/api/admin/charts/panels',
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chart_type: chartType,
            symbol: defaultSymbol,
            source: chartType === 'candle' ? 'raw_event' : 'indicator_output',
            series_ref: chartType === 'candle' ? 'ohlcv' : '',
            x: 0,
            y: 0,
            w: 6,
            h: 6,
            title: chartType === 'candle' ? 'OHLCV' : 'Indicator',
          }),
        },
      );
      setPanels((prev) => [...prev, resp.panel]);
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'add panel failed');
      setBannerError(true);
    }
  }

  async function updatePanel(next: ChartPanelSpec) {
    try {
      const resp = await apiJson<{ panel: ChartPanelSpec }>(
        '/api/admin/charts/panels',
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(next),
        },
      );
      setPanels((prev) => prev.map((p) => (p.panel_id === resp.panel.panel_id ? resp.panel : p)));
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'update panel failed');
      setBannerError(true);
    }
  }

  async function removePanel(panelId: string) {
    try {
      await apiJson(`/api/admin/charts/panels/${encodeURIComponent(panelId)}`, {
        method: 'DELETE',
      });
      setPanels((prev) => prev.filter((p) => p.panel_id !== panelId));
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'delete panel failed');
      setBannerError(true);
    }
  }

  function savePreferredLayout() {
    saveLayout(LS_PREFERRED, layout);
    setBanner('선호 레이아웃이 저장되었습니다.');
    setBannerError(false);
  }

  function restorePreferredLayout() {
    const preferred = loadLayout(LS_PREFERRED);
    if (preferred) {
      setLayout(preferred);
      setBanner('선호 레이아웃으로 복원했습니다.');
      setBannerError(false);
    }
  }

  // ── Script editor actions ──

  function startNewScript() {
    setEditingScriptId(uid('script'));
    setEditorSource(hubStubSource);
    setEditorName('새 인디케이터');
    setEditorClassName('MyOBI');
    setValidationErrors([]);
  }

  function editScript(script: IndicatorScriptSpec) {
    setEditingScriptId(script.script_id);
    setEditorSource(script.source || hubStubSource);
    setEditorName(script.name);
    setEditorClassName(script.class_name);
    setValidationErrors([]);
  }

  async function saveScript() {
    if (!editingScriptId) return;
    try {
      setValidationErrors([]);
      const resp = await apiJson<{ script: IndicatorScriptSpec }>(
        '/api/admin/charts/scripts',
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            script_id: editingScriptId,
            name: editorName,
            source: editorSource,
            class_name: editorClassName,
          }),
        },
      );
      setScripts((prev) => {
        const has = prev.some((s) => s.script_id === resp.script.script_id);
        return has
          ? prev.map((s) => (s.script_id === resp.script.script_id ? resp.script : s))
          : [...prev, resp.script];
      });
      setBanner('스크립트 저장 완료.');
      setBannerError(false);
    } catch (err: any) {
      const payload = err?.payload as { errors?: string[] } | undefined;
      if (payload?.errors) {
        setValidationErrors(payload.errors);
      } else {
        setValidationErrors([err instanceof Error ? err.message : 'save failed']);
      }
    }
  }

  async function deleteScript(script: IndicatorScriptSpec) {
    if (script.builtin) return;
    if (!confirm(`'${script.name}' 스크립트를 삭제할까요?`)) return;
    try {
      await apiJson(`/api/admin/charts/scripts/${encodeURIComponent(script.script_id)}`, {
        method: 'DELETE',
      });
      setScripts((prev) => prev.filter((s) => s.script_id !== script.script_id));
      setInstances((prev) => prev.filter((i) => i.script_id !== script.script_id));
      if (editingScriptId === script.script_id) {
        setEditingScriptId(null);
      }
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'delete failed');
      setBannerError(true);
    }
  }

  // ── Instance activate ──

  async function activateInstance(scriptId: string, symbol: string, topN: number) {
    try {
      const resp = await apiJson<{ instance: IndicatorInstanceSpec }>(
        '/api/admin/charts/instances',
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            script_id: scriptId,
            symbol,
            market_scope: '',
            params: scriptId === 'builtin.obi' ? { top_n: topN } : {},
            enabled: true,
          }),
        },
      );
      setInstances((prev) => {
        const has = prev.some((i) => i.instance_id === resp.instance.instance_id);
        return has
          ? prev.map((i) => (i.instance_id === resp.instance.instance_id ? resp.instance : i))
          : [...prev, resp.instance];
      });
      await refresh();
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'activate failed');
      setBannerError(true);
    }
  }

  async function deactivateInstance(id: string) {
    try {
      await apiJson(`/api/admin/charts/instances/${encodeURIComponent(id)}`, {
        method: 'DELETE',
      });
      setInstances((prev) => prev.filter((i) => i.instance_id !== id));
    } catch (err) {
      setBanner(err instanceof Error ? err.message : 'deactivate failed');
      setBannerError(true);
    }
  }

  // ── Render ──

  const symbols = useMemo(
    () => Array.from(new Set(targets.map((t) => t.instrument.symbol))),
    [targets],
  );

  const instancesBySymbol = useMemo(() => {
    const map = new Map<string, IndicatorInstanceSpec[]>();
    for (const inst of instances) {
      const arr = map.get(inst.symbol) ?? [];
      arr.push(inst);
      map.set(inst.symbol, arr);
    }
    return map;
  }, [instances]);

  return (
    <div className="col-stack charts-view">
      {banner && (
        <div className={bannerError ? 'banner error' : 'banner'}>{banner}</div>
      )}

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Charts Layout</span>
          <div className="row-actions">
            <button type="button" className="sm-btn" onClick={() => void addPanel('line')}>
              + Line 패널
            </button>
            <button type="button" className="sm-btn" onClick={() => void addPanel('candle')}>
              + Candle 패널
            </button>
            <button type="button" className="sm-btn" onClick={savePreferredLayout}>
              레이아웃 저장
            </button>
            <button type="button" className="sm-btn" onClick={restorePreferredLayout}>
              레이아웃 복원
            </button>
          </div>
        </div>

        {panels.length === 0 ? (
          <div className="empty-row">
            패널이 없습니다. 위의 버튼으로 Line/Candle 패널을 추가하세요.
          </div>
        ) : (
          <GridLayout
            className="layout"
            cols={12}
            rowHeight={40}
            width={1200}
            layout={layout}
            onLayoutChange={(next) => setLayout(next)}
            draggableHandle=".panel-drag-handle"
            isDraggable
            isResizable
            margin={[8, 8]}
          >
            {panels.map((panel) => {
              const candleDisabled = panel.chart_type === 'candle' && !panelSupportsCandle(panel.symbol);
              return (
                <div key={panel.panel_id} className="chart-wrapper">
                  <div className="chart-wrapper-head panel-drag-handle">
                    <strong>{panel.title || `${panel.chart_type.toUpperCase()} · ${panel.symbol || '—'}`}</strong>
                    <div className="row-actions">
                      <select
                        value={panel.chart_type}
                        onChange={(e) => void updatePanel({ ...panel, chart_type: e.target.value as any })}
                      >
                        <option value="line">line</option>
                        <option value="candle" disabled={!panelSupportsCandle(panel.symbol)}>
                          candle
                        </option>
                      </select>
                      <select
                        value={panel.symbol}
                        onChange={(e) => void updatePanel({ ...panel, symbol: e.target.value })}
                      >
                        <option value="">— symbol —</option>
                        {symbols.map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                      <select
                        value={panel.source}
                        onChange={(e) => void updatePanel({ ...panel, source: e.target.value })}
                      >
                        <option value="indicator_output">indicator</option>
                        <option value="raw_event">raw event</option>
                      </select>
                      <select
                        value={panel.series_ref}
                        onChange={(e) => void updatePanel({ ...panel, series_ref: e.target.value })}
                      >
                        <option value="">— series —</option>
                        {panel.source === 'indicator_output' &&
                          (instancesBySymbol.get(panel.symbol) ?? []).map((inst) => (
                            <option key={inst.instance_id} value={inst.instance_id}>
                              {scripts.find((s) => s.script_id === inst.script_id)?.name ?? inst.instance_id}
                            </option>
                          ))}
                        {panel.source === 'raw_event' && (
                          <>
                            <option value="ohlcv">ohlcv</option>
                            <option value="trade">trade</option>
                          </>
                        )}
                      </select>
                      <button
                        type="button"
                        className="sm-btn danger-sm"
                        onClick={() => void removePanel(panel.panel_id)}
                      >
                        삭제
                      </button>
                    </div>
                  </div>
                  {candleDisabled ? (
                    <div className="empty-row">
                      이 심볼의 소스는 OHLCV 캔들 스트림을 제공하지 않습니다
                      (예: KXT KRX spot). line 타입으로 변경하거나 OHLCV를
                      지원하는 심볼을 선택하세요.
                    </div>
                  ) : (
                    <ChartPanel
                      spec={panel}
                      indicatorOutputs={indicatorOutputs}
                      rawEvents={rawEvents}
                    />
                  )}
                </div>
              );
            })}
          </GridLayout>
        )}
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Indicator Scripts</span>
          <div className="row-actions">
            <button type="button" className="sm-btn" onClick={startNewScript}>
              + 새 스크립트
            </button>
          </div>
        </div>
        <div className="tbl-wrap">
          <table>
            <thead>
              <tr>
                <th>이름</th>
                <th>클래스</th>
                <th>built-in</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {scripts.map((s) => (
                <tr key={s.script_id}>
                  <td><strong>{s.name}</strong></td>
                  <td className="mono small">{s.class_name}</td>
                  <td>{s.builtin ? 'yes' : '—'}</td>
                  <td>
                    <div className="row-actions">
                      <button
                        type="button"
                        className="sm-btn"
                        onClick={() => editScript(s)}
                        disabled={s.builtin}
                      >
                        편집
                      </button>
                      <button
                        type="button"
                        className="sm-btn danger-sm"
                        onClick={() => void deleteScript(s)}
                        disabled={s.builtin}
                      >
                        삭제
                      </button>
                      <button
                        type="button"
                        className="sm-btn"
                        onClick={() => void activateInstance(s.script_id, symbols[0] ?? '', 5)}
                        disabled={symbols.length === 0}
                      >
                        활성화
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {editingScriptId && (
          <div className="script-editor">
            <div className="field-row">
              <label className="field">
                <span>이름</span>
                <input value={editorName} onChange={(e) => setEditorName(e.target.value)} />
              </label>
              <label className="field">
                <span>클래스명</span>
                <input
                  value={editorClassName}
                  onChange={(e) => setEditorClassName(e.target.value)}
                  placeholder="MyOBI"
                />
              </label>
            </div>
            <Editor
              height="420px"
              defaultLanguage="python"
              value={editorSource}
              onChange={(v) => setEditorSource(v ?? '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 13,
                scrollBeyondLastLine: false,
              }}
            />
            {validationErrors.length > 0 && (
              <div className="banner error">
                <strong>검증 실패</strong>
                <ul>
                  {validationErrors.map((err, idx) => (
                    <li key={idx}>{err}</li>
                  ))}
                </ul>
              </div>
            )}
            <div className="form-actions">
              <button type="button" onClick={() => void saveScript()}>저장 + 검증</button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => setEditingScriptId(null)}
              >
                취소
              </button>
            </div>
          </div>
        )}
      </section>

      <section className="panel">
        <div className="panel-head">
          <span className="eyebrow">Indicator Instances</span>
          <span className="count-pill">{instances.length}</span>
        </div>
        {instances.length === 0 ? (
          <div className="empty-row">활성화된 인디케이터 인스턴스가 없습니다.</div>
        ) : (
          <div className="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>instance</th>
                  <th>script</th>
                  <th>symbol</th>
                  <th>params</th>
                  <th>state</th>
                  <th>output</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {instances.map((inst) => {
                  const err = errors.find((e) => e.instance_id === inst.instance_id);
                  return (
                    <tr key={inst.instance_id}>
                      <td className="mono small">{inst.instance_id}</td>
                      <td className="mono small">{inst.script_id}</td>
                      <td>{inst.symbol || '*'}</td>
                      <td className="mono small">{JSON.stringify(inst.params)}</td>
                      <td>
                        <span className={`badge ${err?.state === 'error' ? 'danger' : 'good'}`}>
                          {err?.state ?? (inst.enabled ? 'running' : 'disabled')}
                        </span>
                        {err?.last_error && (
                          <div className="sub mono small error-cell">{err.last_error.slice(0, 140)}</div>
                        )}
                      </td>
                      <td>{err?.output_count ?? 0}</td>
                      <td>
                        <button
                          type="button"
                          className="sm-btn danger-sm"
                          onClick={() => void deactivateInstance(inst.instance_id)}
                        >
                          해제
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
    </div>
  );
}
