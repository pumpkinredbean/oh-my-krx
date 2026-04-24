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
    assert "disabled={!slotTarget || !slot.event_name || fieldRes.fields.length === 0}" in source


def test_raw_binding_uses_slot_field_before_param_field_and_syncs_param() -> None:
    source = CHARTS_VIEW.read_text()

    assert "slot.field_name ?? binding.param_values.field" in source
    assert "syncRawFieldParam(binding, input_bindings)" in source


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
