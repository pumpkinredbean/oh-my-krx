"""Source-level regressions for the admin charts binding workbench."""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHARTS_VIEW = ROOT / "apps" / "admin_web" / "src" / "views" / "ChartsView.tsx"
STYLES = ROOT / "apps" / "admin_web" / "src" / "styles.css"


def test_field_binding_uses_strict_select_not_free_text_datalist() -> None:
    source = CHARTS_VIEW.read_text()

    assert "<datalist" not in source
    assert "list={`fields-" not in source
    assert "— select target/event first —" in source
    assert "disabled={!slotTarget || !slot.event_name || valueFields.length === 0}" in source


def test_raw_binding_uses_slot_field_before_param_field_and_syncs_param() -> None:
    source = CHARTS_VIEW.read_text()

    assert "slot.field_name ?? binding.param_values.field" in source
    assert "slot.time_field_name ?? binding.param_values.time_field" in source
    assert "syncRawFieldParam(binding, input_bindings)" in source
    assert "time_field: source?.time_field_name" in source


def test_time_field_candidates_are_separate_from_value_fields() -> None:
    source = CHARTS_VIEW.read_text()

    assert "computeAllowedTimeFields" in source
    assert "TIME_FIELD_PRIORITY" in source
    assert "'raw.info.E', 'raw.info.T', 'raw.info.t'" in source
    assert "price" not in source[source.index("const TIME_FIELD_PRIORITY"):source.index("function isTimeLikePath")]
    assert "x/time field" in source
    assert "y/value field" in source
    assert "disabled={!slotTarget || !slot.event_name || timeFields.length === 0}" in source
    assert "normalized." not in source[source.index("const TIME_FIELD_PRIORITY"):source.index("function isTimeLikePath")]


def test_normalized_candidates_are_filtered_from_value_fields() -> None:
    source = CHARTS_VIEW.read_text()

    assert "!f.startsWith('normalized.')" in source
    assert "if (top === 'normalized') continue;" in source


def test_normalize_panel_scrubs_legacy_normalized_bindings() -> None:
    source = CHARTS_VIEW.read_text()

    assert "function scrubLegacyNormalizedBindingValue" in source
    assert "value.startsWith('normalized.')" in source
    assert "time_field_name: bindingText(raw.base_feed.time_field_name)" in source
    assert "time_field_name: bindingText(s.time_field_name)" in source
    assert "field_name: bindingText(s.field_name)" in source
    assert "param_values: sanitizeParamValues(paramValuesFromAny(b.param_values))" in source


def test_layout_storage_is_v4_and_clamped_full_width() -> None:
    source = CHARTS_VIEW.read_text()

    assert "preferredLayout.v4" in source
    assert "workingLayout.v4" in source
    assert "seed.v4.done" in source
    assert "preferredLayout.v1" not in source
    assert "workingLayout.v1" not in source
    assert "seed.v3.done" not in source
    assert "export function clampChartLayoutItem" in source
    assert "x: 0" in source
    assert "w: CHART_LAYOUT_COLS" in source
    assert "h: Math.max(MIN_CHART_LAYOUT_H" in source
    assert "setLayout(clampChartLayout(preferred))" in source
    assert "onLayoutChange={(next) => setLayout(clampChartLayout(next))}" in source


def test_chart_time_extraction_supports_selected_field_and_fallbacks() -> None:
    source = CHARTS_VIEW.read_text()

    assert "export function parseChartTime" in source
    assert "raw < 10_000_000_000 ? raw * 1000 : raw" in source
    assert "extractChartTime(r, spec.base_feed?.time_field_name)" in source
    assert "extractChartTime(r, timeField)" in source


def test_charts_grid_is_not_shrunk_by_inspector_column() -> None:
    css = STYLES.read_text()
    source = CHARTS_VIEW.read_text()

    assert "grid-template-columns: minmax(0, 1fr);" in css
    assert "grid-template-columns: 1fr 340px" not in css
    assert "width={1200}" not in source


def test_raw_event_target_mirror_updates_existing_keys() -> None:
    source = CHARTS_VIEW.read_text()

    assert "const updatedRows = [...cur, row].slice(-500);" in source
    assert "next.set(symKey, updatedRows);" in source
    assert "rawEventMirrorKeysForPanels(" in source
    assert "next.set(tgtKey, updatedRows);" in source
    assert "rawEvents.size" not in source
    assert "next.get(tgtKey) !== rows" in source
