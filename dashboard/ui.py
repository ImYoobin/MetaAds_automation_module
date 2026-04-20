"""Streamlit rendering layer for Meta automation dashboard."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.models import (
    SHEET_DISPLAY_ORDER,
    ValidationResult,
    build_activity_id,
)
from dashboard.services.config_service import (
    add_activity,
    add_brand,
    add_sheet_url,
    delete_activity,
    delete_brand,
    delete_sheet_url,
    rename_activity,
    rename_brand,
)
from dashboard.services.url_service import UrlValidationError, clean_report_url


STATUS_LABEL = {
    "waiting": "Waiting",
    "running": "Running",
    "completed": "Completed",
    "failed": "Failed",
    "skipped": "Skipped",
}

STATUS_STYLE = {
    "waiting": "background-color: #f1f5f9; color: #64748b; font-weight: 600;",
    "running": "background-color: #dbeafe; color: #1d4ed8; font-weight: 700;",
    "completed": "background-color: #dcfce7; color: #166534; font-weight: 700;",
    "failed": "background-color: #fee2e2; color: #b91c1c; font-weight: 700;",
    "skipped": "background-color: #f3f4f6; color: #6b7280; font-weight: 600;",
}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_key(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", _safe_text(value))


def _push_management_message(text: str, *, level: str = "info") -> None:
    messages = st.session_state.setdefault("management_messages", [])
    messages.append({"level": level, "text": text})
    st.session_state["management_messages"] = messages[-30:]


def _toast(text: str, *, icon: str = "✅") -> None:
    try:
        st.toast(text, icon=icon)  # type: ignore[attr-defined]
    except Exception:
        st.success(f"{icon} {text}")


def _extract_url(item: Any) -> str:
    if isinstance(item, dict):
        return _safe_text(item.get("url"))
    return _safe_text(item)


def _ensure_sheet_entries(activity: dict[str, Any], sheet_name: str) -> list[Any]:
    reports = activity.setdefault("reports", {sheet: [] for sheet in SHEET_DISPLAY_ORDER})
    entries = reports.setdefault(sheet_name, [])
    if not isinstance(entries, list):
        entries = [entries]
        reports[sheet_name] = entries
    return entries


def _ready_sheet_count(activity: dict[str, Any]) -> int:
    return sum(
        1
        for sheet_name in SHEET_DISPLAY_ORDER
        if any(_extract_url(item) for item in _ensure_sheet_entries(activity, sheet_name))
    )


def _set_brand_selection(brand: dict[str, Any], selected: bool) -> None:
    selected_ids = set(st.session_state.get("selected_activity_ids", set()))
    brand_code = _safe_text(brand.get("code"))
    for activity in brand.get("activities", []):
        activity_name = _safe_text(activity.get("name"))
        activity_id = build_activity_id(brand_code=brand_code, activity_name=activity_name)
        checkbox_key = f"activity_checkbox_{_safe_key(activity_id)}"
        if selected:
            selected_ids.add(activity_id)
            st.session_state[checkbox_key] = True
        else:
            selected_ids.discard(activity_id)
            st.session_state[checkbox_key] = False
    st.session_state["selected_activity_ids"] = selected_ids


def _on_brand_checkbox_change(brand_code: str, checkbox_key: str) -> None:
    checked = bool(st.session_state.get(checkbox_key))
    config_data = st.session_state.get("config_data") or {}
    if not isinstance(config_data, dict):
        return
    for brand in config_data.get("brands", []):
        if _safe_text(brand.get("code")) == _safe_text(brand_code):
            _set_brand_selection(brand, checked)
            return


def _on_activity_checkbox_change(activity_id: str, checkbox_key: str) -> None:
    checked = bool(st.session_state.get(checkbox_key))
    selected_ids = set(st.session_state.get("selected_activity_ids", set()))
    if checked:
        selected_ids.add(activity_id)
    else:
        selected_ids.discard(activity_id)
    st.session_state["selected_activity_ids"] = selected_ids


def _remove_brand_selections(brand_code: str) -> None:
    selected_ids = set(st.session_state.get("selected_activity_ids", set()))
    st.session_state["selected_activity_ids"] = {
        item_id
        for item_id in selected_ids
        if not item_id.startswith(f"{_safe_text(brand_code)}::")
    }


def _rename_activity_selection(brand_code: str, old_name: str, new_name: str) -> None:
    selected_ids = set(st.session_state.get("selected_activity_ids", set()))
    old_id = build_activity_id(brand_code=brand_code, activity_name=old_name)
    new_id = build_activity_id(brand_code=brand_code, activity_name=new_name)
    if old_id in selected_ids:
        selected_ids.discard(old_id)
        selected_ids.add(new_id)
    st.session_state["selected_activity_ids"] = selected_ids


def _toggle_bool_key(key: str) -> None:
    st.session_state[key] = not bool(st.session_state.get(key, False))


def _reset_flag_key(input_key: str) -> str:
    return f"{input_key}__reset"


def _consume_input_reset(input_key: str) -> None:
    if bool(st.session_state.pop(_reset_flag_key(input_key), False)):
        st.session_state[input_key] = ""


def _request_input_reset(input_key: str) -> None:
    st.session_state[_reset_flag_key(input_key)] = True


def _inject_ui_css() -> None:
    st.markdown(
        """
        <style>
        .meta-caption-muted { color: #6b7280; font-size: 0.92rem; margin-top: 0.12rem; }
        .meta-tree-caption { color: #6b7280; font-size: 0.84rem; }
        .meta-main-card-title { font-size: 1.05rem; font-weight: 700; color: #111827; margin-bottom: 0.2rem; }
        .meta-section-title { font-size: 1rem; font-weight: 700; color: #111827; margin: 0.5rem 0 0.35rem 0; }
        .meta-disabled-box {
            border: 1px dashed #d1d5db;
            border-radius: 0.8rem;
            padding: 0.85rem 1rem;
            background: #f8fafc;
            color: #6b7280;
            font-size: 0.9rem;
            margin: 0.35rem 0 0.8rem 0;
        }
        div[class*="st-key-edit_brand_"] button,
        div[class*="st-key-edit_activity_"] button,
        div[class*="st-key-delete_brand_icon_"] button,
        div[class*="st-key-delete_activity_icon_"] button {
            border: none !important;
            background: transparent !important;
            box-shadow: none !important;
            padding: 0.05rem 0.2rem !important;
            min-height: 1.8rem !important;
        }
        div[class*="st-key-edit_brand_"] button,
        div[class*="st-key-edit_activity_"] button {
            color: #4b5563 !important;
        }
        div[class*="st-key-delete_brand_icon_"] button,
        div[class*="st-key-delete_activity_icon_"] button {
            color: #dc2626 !important;
        }
        div[class*="st-key-edit_brand_"] button:hover,
        div[class*="st-key-edit_activity_"] button:hover,
        div[class*="st-key-delete_brand_icon_"] button:hover,
        div[class*="st-key-delete_activity_icon_"] button:hover {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
        }
        div[class*="st-key-confirm_delete_"] button,
        div[class*="st-key-delete_url_"] button {
            background-color: #ff4b4b !important;
            color: #fff !important;
            border: 1px solid #e53e3e !important;
        }
        div[class*="st-key-confirm_delete_"] button:hover,
        div[class*="st-key-delete_url_"] button:hover {
            background-color: #e53e3e !important;
            border: 1px solid #c53030 !important;
        }
        div[class*="st-key-add_brand_btn"] button,
        div[class*="st-key-add_activity_btn_"] button,
        div[class*="st-key-save_url_draft_"] button {
            background-color: #2563eb !important;
            color: #fff !important;
            border: 1px solid #1d4ed8 !important;
        }
        div[class*="st-key-add_brand_btn"] button:hover,
        div[class*="st-key-add_activity_btn_"] button:hover,
        div[class*="st-key-save_url_draft_"] button:hover {
            background-color: #1d4ed8 !important;
            border: 1px solid #1e40af !important;
        }
        div[class*="st-key-toggle_brand_open_"] button,
        div[class*="st-key-toggle_brand_closed_"] button,
        div[class*="st-key-toggle_activity_open_"] button,
        div[class*="st-key-toggle_activity_closed_"] button,
        div[class*="st-key-toggle_sheet_open_"] button,
        div[class*="st-key-toggle_sheet_closed_"] button {
            background-color: #e5e7eb !important;
            border: 1px solid #9ca3af !important;
            color: #111827 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_ui_messages(execution_messages: list[dict[str, str]]) -> None:
    _ = execution_messages


def _ui_phase_key(value: Any) -> str:
    key = _safe_text(value).lower()
    if key in {"pending", "waiting", "ready"}:
        return "waiting"
    if key in {"running", "exporting", "downloading"}:
        return "running"
    if key in {"completed", "downloaded"}:
        return "completed"
    if key == "failed":
        return "failed"
    if key == "skipped":
        return "skipped"
    return "waiting"


def _status_label_text(value: Any) -> str:
    return STATUS_LABEL.get(_ui_phase_key(value), "Waiting")


def _status_style_text(value: Any) -> str:
    return STATUS_STYLE.get(_ui_phase_key(value), "")


def _missing_columns_style_text(value: Any) -> str:
    return "color: #b91c1c; font-weight: 700;" if _safe_text(value) else ""


def _map_styler(styler, func, subset: list[str]):
    if hasattr(styler, "map"):
        return styler.map(func, subset=subset)
    if hasattr(styler, "applymap"):
        return styler.applymap(func, subset=subset)
    return styler


def _style_status_column(df: pd.DataFrame, status_column: str = "상태"):
    styler = df.style
    if status_column in df.columns:
        styler = _map_styler(styler, _status_style_text, subset=[status_column])
    return styler


def _render_disabled_box(text: str) -> None:
    st.markdown(
        f"<div class='meta-disabled-box'>{text}</div>",
        unsafe_allow_html=True,
    )


def _execution_modes_enabled() -> tuple[bool, bool]:
    return (
        bool(st.session_state.get("enable_report_download", True)),
        bool(st.session_state.get("enable_action_log_download", True)),
    )


def _render_execution_options() -> None:
    st.markdown("<div class='meta-section-title'>실행 옵션</div>", unsafe_allow_html=True)
    cols = st.columns(2)
    with cols[0]:
        st.checkbox("캠페인 데이터 다운로드", key="enable_report_download")
    with cols[1]:
        st.checkbox("액션 로그 다운로드", key="enable_action_log_download")

    report_enabled, action_log_enabled = _execution_modes_enabled()
    if not report_enabled and not action_log_enabled:
        _render_disabled_box("최소 한 개의 실행 항목을 선택해야 합니다.")


def _render_sheet_inline_editor(
    *,
    config_data: dict[str, Any],
    brand_name: str,
    brand_code: str,
    activity_name: str,
    activity_id: str,
    sheet_name: str,
    entries: list[Any],
    save_callback: Callable[[], None],
) -> None:
    safe_activity = _safe_key(activity_id)
    safe_sheet = _safe_key(sheet_name)

    for idx, item in enumerate(list(entries)):
        current_url = _extract_url(item)
        readonly_key = f"url_existing_{safe_activity}_{safe_sheet}_{idx}"
        st.session_state.setdefault(readonly_key, current_url)

        row_cols = st.columns([0.45, 5.9, 1.0], vertical_alignment="center")
        row_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
        row_cols[1].text_input(
            f"URL {idx + 1}",
            key=readonly_key,
            label_visibility="collapsed",
            disabled=True,
        )
        if row_cols[2].button(
            "삭제",
            key=f"delete_url_{safe_activity}_{safe_sheet}_{idx}",
            use_container_width=True,
        ):
            ok, message = delete_sheet_url(
                config_data,
                brand_code=brand_code,
                activity_name=activity_name,
                sheet_name=sheet_name,
                index=idx,
            )
            if ok:
                save_callback()
                _toast("URL이 삭제되었습니다.", icon="✅")
                _push_management_message(
                    f"✅ {brand_name} · {activity_name} · {sheet_name} URL을 삭제했습니다.",
                    level="success",
                )
                st.rerun()
            _push_management_message(
                f"⚠️ {brand_name} · {activity_name} · {sheet_name} URL 삭제 실패: {message}",
                level="warning",
            )

    draft_key = f"url_draft_{safe_activity}_{safe_sheet}"
    _consume_input_reset(draft_key)
    draft_cols = st.columns([0.45, 5.9, 1.0], vertical_alignment="center")
    draft_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
    raw_candidate = draft_cols[1].text_input(
        f"{sheet_name} URL 입력",
        key=draft_key,
        placeholder="Meta report URL을 붙여넣으세요.",
        label_visibility="collapsed",
    )
    if _safe_text(raw_candidate):
        try:
            preview = clean_report_url(
                raw_candidate,
                default_event_source=_safe_text(config_data.get("view_event_source")),
            )
            draft_cols[1].caption(f"미리보기: {preview}")
        except UrlValidationError as exc:
            draft_cols[1].error(f"URL 파싱 실패: {exc}")

    if draft_cols[2].button(
        "저장",
        key=f"save_url_draft_{safe_activity}_{safe_sheet}",
        use_container_width=True,
    ):
        ok, message, _ = add_sheet_url(
            config_data,
            brand_code=brand_code,
            activity_name=activity_name,
            sheet_name=sheet_name,
            raw_url=raw_candidate,
        )
        if ok:
            save_callback()
            _request_input_reset(draft_key)
            _push_management_message(
                f"✅ {brand_name} · {activity_name} · {sheet_name} URL을 저장했습니다.",
                level="success",
            )
            st.rerun()
        _push_management_message(
            f"⚠️ {brand_name} · {activity_name} · {sheet_name} URL 저장 실패: {message}",
            level="warning",
        )


def _render_activity_sheets(
    *,
    config_data: dict[str, Any],
    brand_name: str,
    brand_code: str,
    activity_name: str,
    activity_id: str,
    activity: dict[str, Any],
    save_callback: Callable[[], None],
) -> None:
    header_cols = st.columns([0.45, 2.6, 1.6, 0.8], vertical_alignment="center")
    header_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
    header_cols[1].markdown("**시트**")
    header_cols[2].markdown("**URL 현황**")
    header_cols[3].markdown("**편집**")

    for sheet_name in SHEET_DISPLAY_ORDER:
        entries = _ensure_sheet_entries(activity, sheet_name)
        url_count = len(entries)
        status_text = f"🟢 {url_count}개" if url_count > 0 else "⚫ 0개"
        sheet_open_key = f"ui_sheet_open_{_safe_key(activity_id)}_{_safe_key(sheet_name)}"
        is_sheet_open = bool(st.session_state.get(sheet_open_key, False))
        action_label = "▼" if is_sheet_open else "▶"

        row_cols = st.columns([0.45, 2.6, 1.6, 0.8], vertical_alignment="center")
        row_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
        row_cols[1].markdown(f"`{sheet_name}`")
        row_cols[2].markdown(status_text)
        toggle_sheet_key = (
            f"toggle_sheet_open_{_safe_key(activity_id)}_{_safe_key(sheet_name)}"
            if is_sheet_open
            else f"toggle_sheet_closed_{_safe_key(activity_id)}_{_safe_key(sheet_name)}"
        )
        if row_cols[3].button(action_label, key=toggle_sheet_key, use_container_width=True):
            _toggle_bool_key(sheet_open_key)
            st.rerun()

        if bool(st.session_state.get(sheet_open_key, False)):
            _render_sheet_inline_editor(
                config_data=config_data,
                brand_name=brand_name,
                brand_code=brand_code,
                activity_name=activity_name,
                activity_id=activity_id,
                sheet_name=sheet_name,
                entries=entries,
                save_callback=save_callback,
            )


def _render_activity_row(
    *,
    config_data: dict[str, Any],
    save_callback: Callable[[], None],
    brand: dict[str, Any],
    activity: dict[str, Any],
) -> None:
    brand_code = _safe_text(brand.get("code"))
    brand_name = _safe_text(brand.get("name"))
    activity_name = _safe_text(activity.get("name"))
    activity_id = build_activity_id(brand_code=brand_code, activity_name=activity_name)
    safe_activity = _safe_key(activity_id)
    is_selected = activity_id in set(st.session_state.get("selected_activity_ids", set()))

    ready_sheet_count = _ready_sheet_count(activity)
    badge = f"{ready_sheet_count}/{len(SHEET_DISPLAY_ORDER)} 시트 등록"

    checkbox_key = f"activity_checkbox_{safe_activity}"
    st.session_state[checkbox_key] = is_selected
    activity_open_key = f"ui_activity_open_{safe_activity}"
    is_open = bool(st.session_state.get(activity_open_key, False))
    is_editing = _safe_text(st.session_state.get("ui_edit_activity")) == activity_id

    row_cols = st.columns([0.45, 0.55, 4.2, 0.45, 0.45, 1.5, 0.8], vertical_alignment="center")
    row_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
    row_cols[1].checkbox(
        "Activity",
        key=checkbox_key,
        label_visibility="collapsed",
        on_change=_on_activity_checkbox_change,
        args=(activity_id, checkbox_key),
    )
    row_cols[2].markdown(f"**{activity_name}**")
    if row_cols[3].button("✏️", key=f"edit_activity_{safe_activity}", use_container_width=True):
        st.session_state["ui_edit_activity"] = activity_id
        st.session_state[f"ui_edit_activity_value_{safe_activity}"] = activity_name
        st.rerun()
    if row_cols[4].button("🗑️", key=f"delete_activity_icon_{safe_activity}", use_container_width=True):
        st.session_state["pending_activity_delete"] = activity_id
        st.rerun()

    row_cols[5].markdown(
        f"<span class='meta-tree-caption'>{badge}</span>",
        unsafe_allow_html=True,
    )
    toggle_activity_key = f"toggle_activity_open_{safe_activity}" if is_open else f"toggle_activity_closed_{safe_activity}"
    if row_cols[6].button("▼" if is_open else "▶", key=toggle_activity_key, use_container_width=True):
        _toggle_bool_key(activity_open_key)
        st.rerun()

    if is_editing:
        edit_value_key = f"ui_edit_activity_value_{safe_activity}"
        edit_cols = st.columns([0.45, 4.6, 0.9, 0.9, 1.95], vertical_alignment="center")
        edit_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
        edit_cols[1].text_input("액티비티 이름", key=edit_value_key, label_visibility="collapsed")
        if edit_cols[2].button("✅", key=f"apply_activity_rename_{safe_activity}", use_container_width=True):
            new_name = st.session_state.get(edit_value_key, "")
            ok, message = rename_activity(config_data, brand_code, activity_name, new_name)
            if ok:
                _rename_activity_selection(brand_code, activity_name, _safe_text(new_name))
                save_callback()
                _push_management_message(f"✅ {brand_name} · {activity_name} 액티비티 이름을 변경했습니다.", level="success")
                st.session_state["ui_edit_activity"] = ""
                st.rerun()
            _push_management_message(f"⚠️ {brand_name} · {activity_name} 이름 변경 실패: {message}", level="warning")
        if edit_cols[3].button("❌", key=f"cancel_activity_rename_{safe_activity}", use_container_width=True):
            st.session_state["ui_edit_activity"] = ""
            st.rerun()

    if _safe_text(st.session_state.get("pending_activity_delete")) == activity_id:
        st.warning("이 액티비티를 삭제하면 하위 report URL도 함께 삭제됩니다.")
        confirm_cols = st.columns([0.45, 1.2, 1.2, 4.0], vertical_alignment="center")
        confirm_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
        if confirm_cols[1].button("삭제 확인", key=f"confirm_delete_activity_{safe_activity}", use_container_width=True):
            ok, message = delete_activity(config_data, brand_code, activity_name)
            st.session_state["pending_activity_delete"] = ""
            if ok:
                selected_ids = set(st.session_state.get("selected_activity_ids", set()))
                selected_ids.discard(activity_id)
                st.session_state["selected_activity_ids"] = selected_ids
                save_callback()
                _toast(f"'{activity_name}'이(가) 삭제되었습니다.", icon="✅")
                _push_management_message(f"✅ {brand_name} · {activity_name} 액티비티를 삭제했습니다.", level="success")
                st.rerun()
            _push_management_message(f"⚠️ {brand_name} · {activity_name} 삭제 실패: {message}", level="warning")
        if confirm_cols[2].button("취소", key=f"cancel_delete_activity_{safe_activity}", use_container_width=True):
            st.session_state["pending_activity_delete"] = ""
            st.rerun()

    if not is_open:
        return

    _render_activity_sheets(
        config_data=config_data,
        brand_name=brand_name,
        brand_code=brand_code,
        activity_name=activity_name,
        activity_id=activity_id,
        activity=activity,
        save_callback=save_callback,
    )


def _render_brand_card(
    *,
    config_data: dict[str, Any],
    save_callback: Callable[[], None],
    brand: dict[str, Any],
) -> None:
    brand_code = _safe_text(brand.get("code"))
    brand_name = _safe_text(brand.get("name"))
    safe_brand = _safe_key(brand_code)
    activities = brand.get("activities", [])

    selected_ids = set(st.session_state.get("selected_activity_ids", set()))
    activity_ids = [
        build_activity_id(brand_code=brand_code, activity_name=_safe_text(item.get("name")))
        for item in activities
    ]
    selected_count = sum(1 for item_id in activity_ids if item_id in selected_ids)
    total_count = len(activity_ids)
    all_selected = total_count > 0 and selected_count == total_count

    checkbox_key = f"brand_checkbox_{safe_brand}"
    st.session_state[checkbox_key] = all_selected
    brand_open_key = f"ui_brand_open_{safe_brand}"
    is_open = bool(st.session_state.get(brand_open_key, False))
    is_editing = _safe_text(st.session_state.get("ui_edit_brand")) == brand_code

    with st.container(border=True):
        row_cols = st.columns([0.55, 4.1, 0.45, 0.45, 1.6, 0.75], vertical_alignment="center")
        row_cols[0].checkbox(
            "Brand",
            key=checkbox_key,
            label_visibility="collapsed",
            on_change=_on_brand_checkbox_change,
            args=(brand_code, checkbox_key),
        )
        row_cols[1].markdown(f"**{brand_name}**")
        if row_cols[2].button("✏️", key=f"edit_brand_{safe_brand}", use_container_width=True):
            st.session_state["ui_edit_brand"] = brand_code
            st.session_state[f"ui_edit_brand_value_{safe_brand}"] = brand_name
            st.rerun()
        if row_cols[3].button("🗑️", key=f"delete_brand_icon_{safe_brand}", use_container_width=True):
            st.session_state["pending_brand_delete"] = brand_code
            st.rerun()
        row_cols[4].markdown(
            f"<span class='meta-tree-caption'>{selected_count}/{total_count} 액티비티 선택</span>",
            unsafe_allow_html=True,
        )
        toggle_brand_key = f"toggle_brand_open_{safe_brand}" if is_open else f"toggle_brand_closed_{safe_brand}"
        if row_cols[5].button("▼" if is_open else "▶", key=toggle_brand_key, use_container_width=True):
            _toggle_bool_key(brand_open_key)
            st.rerun()

        if is_editing:
            edit_value_key = f"ui_edit_brand_value_{safe_brand}"
            edit_cols = st.columns([4.8, 0.9, 0.9, 1.95], vertical_alignment="center")
            edit_cols[0].text_input("브랜드 이름", key=edit_value_key, label_visibility="collapsed")
            if edit_cols[1].button("✅", key=f"apply_brand_rename_{safe_brand}", use_container_width=True):
                new_name = st.session_state.get(edit_value_key, "")
                ok, message = rename_brand(config_data, brand_code, new_name)
                if ok:
                    save_callback()
                    _push_management_message(f"✅ {brand_name} 브랜드 이름을 변경했습니다.", level="success")
                    st.session_state["ui_edit_brand"] = ""
                    st.rerun()
                _push_management_message(f"⚠️ {brand_name} 이름 변경 실패: {message}", level="warning")
            if edit_cols[2].button("❌", key=f"cancel_brand_rename_{safe_brand}", use_container_width=True):
                st.session_state["ui_edit_brand"] = ""
                st.rerun()

        if _safe_text(st.session_state.get("pending_brand_delete")) == brand_code:
            st.warning("브랜드를 삭제하면 하위 액티비티와 report URL이 모두 삭제됩니다.")
            confirm_cols = st.columns([1.2, 1.2, 3.8], vertical_alignment="center")
            if confirm_cols[0].button("삭제 확인", key=f"confirm_delete_brand_{safe_brand}", use_container_width=True):
                ok, message = delete_brand(config_data, brand_code)
                st.session_state["pending_brand_delete"] = ""
                if ok:
                    _remove_brand_selections(brand_code)
                    save_callback()
                    _toast(f"'{brand_name}'이(가) 삭제되었습니다.", icon="✅")
                    _push_management_message(f"✅ {brand_name} 브랜드를 삭제했습니다.", level="success")
                    st.rerun()
                _push_management_message(f"⚠️ {brand_name} 삭제 실패: {message}", level="warning")
            if confirm_cols[1].button("취소", key=f"cancel_delete_brand_{safe_brand}", use_container_width=True):
                st.session_state["pending_brand_delete"] = ""
                st.rerun()

        if not is_open:
            return

        if activities:
            for activity in activities:
                _render_activity_row(config_data=config_data, save_callback=save_callback, brand=brand, activity=activity)
        else:
            st.info("등록된 액티비티가 없습니다.")

        st.markdown("##### + 액티비티 추가")
        add_key = f"add_activity_input_{safe_brand}"
        _consume_input_reset(add_key)
        add_cols = st.columns([4.6, 1.2], vertical_alignment="center")
        add_cols[0].text_input("액티비티 추가", key=add_key, placeholder="예: Activity 1", label_visibility="collapsed")
        if add_cols[1].button("추가", key=f"add_activity_btn_{safe_brand}", use_container_width=True):
            ok, message = add_activity(config_data, brand_code, st.session_state.get(add_key, ""))
            if ok:
                save_callback()
                _request_input_reset(add_key)
                _push_management_message(f"✅ {brand_name}에 액티비티를 추가했습니다.", level="success")
                st.rerun()
            _push_management_message(f"⚠️ {brand_name} 액티비티 추가 실패: {message}", level="warning")


def _build_report_df(execution_snapshot: dict[str, Any]) -> pd.DataFrame:
    report_rows = execution_snapshot.get("rows") or []
    if not report_rows:
        return pd.DataFrame()

    visible_rows = [
        row
        for row in report_rows
        if int(getattr(row, "url_count", 0) or 0) > 0
    ]
    if not visible_rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "브랜드": row.brand,
                "액티비티": row.activity,
                "시트": row.sheet,
                "상태": _status_label_text(row.status),
                "메시지": row.message,
                "누락 컬럼": row.missing_columns_text,
                "최종 갱신": row.last_updated,
            }
            for row in visible_rows
        ]
    )


def _build_activity_progress_map(execution_snapshot: dict[str, Any]) -> dict[str, dict[str, int]]:
    progress_map: dict[str, dict[str, int]] = {}
    for row in execution_snapshot.get("rows") or []:
        key = f"{_safe_text(row.brand)}::{_safe_text(row.activity)}"
        if key not in progress_map:
            progress_map[key] = {"processed": 0, "total": 0}
        if int(getattr(row, "url_count", 0) or 0) <= 0 and _ui_phase_key(row.status) == "skipped":
            continue
        progress_map[key]["total"] += 1
        if _ui_phase_key(row.status) in {"completed", "failed", "skipped"}:
            progress_map[key]["processed"] += 1
    return progress_map


def _build_activity_result_df(execution_snapshot: dict[str, Any]) -> pd.DataFrame:
    summaries_raw = execution_snapshot.get("activity_results") or []
    if not isinstance(summaries_raw, list) or not summaries_raw:
        return pd.DataFrame()

    progress_map = _build_activity_progress_map(execution_snapshot)
    rows: list[dict[str, Any]] = []
    for item in summaries_raw:
        if not isinstance(item, dict):
            continue
        brand = _safe_text(item.get("brand"))
        activity = _safe_text(item.get("activity"))
        status = _safe_text(item.get("status"))
        if not status:
            status = "Completed" if _safe_text(item.get("workbook_path")) else "Failed"
        progress = progress_map.get(f"{brand}::{activity}", {"processed": 0, "total": 0})
        rows.append(
            {
                "브랜드": brand,
                "액티비티": activity,
                "상태": _status_label_text(status),
                "처리 시트 수": f"{progress['processed']}/{progress['total']}",
                "메시지": _safe_text(item.get("message")),
                "시간": _safe_text(item.get("updated_at")),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "시간" in df.columns:
        df = df.sort_values(by=["시간"], ascending=False)
    return df


def _build_history_log_df(execution_snapshot: dict[str, Any]) -> pd.DataFrame:
    history_rows = execution_snapshot.get("history_rows") or []
    if not history_rows:
        return pd.DataFrame()

    rows = [
        {
            "브랜드": row.brand,
            "액티비티": row.activity,
            "계정 수": row.account_count,
            "상태": _status_label_text(row.status),
            "메시지": row.message,
            "시간": row.last_updated,
        }
        for row in history_rows
    ]
    df = pd.DataFrame(rows)
    if "시간" in df.columns:
        df = df.sort_values(by=["시간"], ascending=False)
    return df


def _render_start_help_box(*, validation: ValidationResult, is_running: bool, selected_count: int) -> None:
    del is_running
    report_enabled, action_log_enabled = _execution_modes_enabled()
    if selected_count <= 0:
        st.markdown(
            "<p class='meta-caption-muted'>다운로드할 액티비티를 선택하세요.</p>",
            unsafe_allow_html=True,
        )
        return
    if not report_enabled and not action_log_enabled:
        _render_disabled_box("최소 한 개의 실행 항목을 선택해야 합니다.")
        return
    if not validation.can_run and validation.reasons:
        _render_disabled_box(" / ".join(validation.reasons))


def render_top_section(
    *,
    config_data: dict[str, Any],
    save_callback: Callable[[], None],
    validation: ValidationResult,
    execution_snapshot: dict[str, Any],
    on_start_execution: Callable[[], None],
) -> None:
    _inject_ui_css()

    is_running = bool(execution_snapshot.get("is_running"))
    report_enabled, action_log_enabled = _execution_modes_enabled()
    export_disabled = is_running or (not validation.can_run)
    selected_count = len(set(st.session_state.get("selected_activity_ids", set())))

    with st.container(border=True):
        st.markdown("<div class='meta-main-card-title'>액티비티 선택</div>", unsafe_allow_html=True)
        st.markdown(
            "<p class='meta-caption-muted'>선택한 액티비티의 캠페인 데이터/액션 로그를 다운로드합니다.</p>",
            unsafe_allow_html=True,
        )

        brands = config_data.get("brands", [])
        if brands:
            for brand in brands:
                _render_brand_card(config_data=config_data, save_callback=save_callback, brand=brand)
        else:
            _render_disabled_box("등록된 브랜드가 없습니다. 먼저 브랜드를 추가하세요.")

        st.markdown("##### + 브랜드 추가")
        _consume_input_reset("new_brand_name_input")
        add_cols = st.columns([4.8, 1.1], vertical_alignment="center")
        add_cols[0].text_input(
            "브랜드 추가",
            key="new_brand_name_input",
            placeholder="예: Brand A",
            label_visibility="collapsed",
        )
        if add_cols[1].button("추가", key="add_brand_btn", use_container_width=True):
            ok, message = add_brand(config_data, st.session_state.get("new_brand_name_input", ""))
            if ok:
                save_callback()
                _request_input_reset("new_brand_name_input")
                _push_management_message("✅ 브랜드를 추가했습니다.", level="success")
                st.rerun()
            _push_management_message(f"⚠️ 브랜드 추가 실패: {message}", level="warning")

        _render_execution_options()
        _render_start_help_box(
            validation=validation,
            is_running=is_running,
            selected_count=selected_count,
        )

        if st.button(
            "다운로드하기",
            type="primary",
            disabled=export_disabled or (not report_enabled and not action_log_enabled),
            use_container_width=True,
            key="meta_start_btn",
        ):
            on_start_execution()
        st.markdown(
            "<p class='meta-caption-muted'>버튼을 누르고 실행되는 브라우저 창에서 Meta Ads 로그인을 완료해주세요.</p>",
            unsafe_allow_html=True,
        )


def render_sidebar_execution_section(
    *,
    preview_paths: dict[str, Any],
    is_running: bool,
    on_base_parent_dir_change: Callable[[], None] | None = None,
) -> None:
    del preview_paths, is_running
    with st.sidebar:
        st.subheader("⚙️ Run Settings")
        st.selectbox(
            "브라우저",
            options=["msedge", "chrome"],
            key="browser",
        )
        st.text_input(
            "저장 부모 경로",
            key="base_parent_dir_input",
            on_change=on_base_parent_dir_change,
        )


def render_bottom_section(
    *,
    validation: ValidationResult,
    execution_snapshot: dict[str, Any],
    activity_label_by_id: dict[str, str],
) -> None:
    del validation, activity_label_by_id

    st.subheader("📊 진행 상황")
    run_report_enabled = bool(
        st.session_state.get(
            "run_enable_report_download",
            st.session_state.get("enable_report_download", True),
        )
    )
    run_action_log_enabled = bool(
        st.session_state.get(
            "run_enable_action_log_download",
            st.session_state.get("enable_action_log_download", True),
        )
    )

    st.markdown("#### 캠페인 데이터 다운로드")
    report_df = _build_report_df(execution_snapshot)
    if not run_report_enabled:
        _render_disabled_box("캠페인 데이터 다운로드를 켜면 진행 상태가 표시됩니다.")
    elif report_df.empty:
        _render_disabled_box("캠페인 데이터 다운로드 이력이 없습니다.")
    else:
        styled_report_df = _style_status_column(report_df, "상태")
        styled_report_df = _map_styler(styled_report_df, _missing_columns_style_text, subset=["누락 컬럼"])
        st.dataframe(styled_report_df, use_container_width=True, hide_index=True)

    st.markdown("#### 캠페인 데이터 통합본 생성")
    activity_result_df = _build_activity_result_df(execution_snapshot)
    if not run_report_enabled:
        _render_disabled_box("캠페인 데이터 다운로드를 켜면 진행 상태가 표시됩니다.")
    elif activity_result_df.empty:
        _render_disabled_box("다운로드 후 캠페인 데이터 통합본 생성 상태가 표시됩니다.")
    else:
        styled_activity_result_df = _style_status_column(activity_result_df, "상태")
        st.dataframe(styled_activity_result_df, use_container_width=True, hide_index=True)

    st.markdown("#### 액션 로그 다운로드")
    history_log_df = _build_history_log_df(execution_snapshot)
    if not run_action_log_enabled:
        _render_disabled_box("액션 로그 다운로드를 켜면 진행 상태가 표시됩니다.")
    elif history_log_df.empty:
        _render_disabled_box("실행 후 액션 로그 다운로드 상태가 표시됩니다.")
    else:
        styled_history_log_df = _style_status_column(history_log_df, "상태")
        st.dataframe(styled_history_log_df, use_container_width=True, hide_index=True)

    log_file = _safe_text(execution_snapshot.get("log_file"))
    run_id = _safe_text(execution_snapshot.get("run_id"))
    if log_file or run_id:
        st.caption(f"Run ID: {run_id} | Log: {log_file}")
