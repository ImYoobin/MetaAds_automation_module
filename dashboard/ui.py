"""Streamlit rendering layer for Meta automation dashboard."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.models import (
    INTERNAL_TO_WORKBOOK_SHEET_NAME,
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
    update_sheet_url,
)
from dashboard.services.url_service import UrlValidationError, clean_report_url


RESULT_SHEET_COLUMNS: tuple[str, ...] = (
    INTERNAL_TO_WORKBOOK_SHEET_NAME["overall"],
    INTERNAL_TO_WORKBOOK_SHEET_NAME["demo"],
    INTERNAL_TO_WORKBOOK_SHEET_NAME["overall_bof"],
    INTERNAL_TO_WORKBOOK_SHEET_NAME["demo_bof"],
    INTERNAL_TO_WORKBOOK_SHEET_NAME["time"],
    INTERNAL_TO_WORKBOOK_SHEET_NAME["time_bof"],
)


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
        .meta-tree-caption { color: #6b7280; font-size: 0.84rem; }
        .meta-sub-caption { color: #6b7280; font-size: 0.82rem; margin-top: 0.15rem; }
        div[class*="st-key-edit_brand_"] button,
        div[class*="st-key-edit_activity_"] button {
            border: none !important; background: transparent !important; box-shadow: none !important;
            color: #4b5563 !important; padding: 0.05rem 0.3rem !important; min-height: 1.65rem !important;
        }
        div[class*="st-key-edit_brand_"] button:hover,
        div[class*="st-key-edit_activity_"] button:hover {
            color: #111827 !important; background: transparent !important; border: none !important; box-shadow: none !important;
        }
        div[class*="st-key-delete_"] button, div[class*="st-key-confirm_delete_"] button {
            background-color: #ff4b4b !important; color: #fff !important; border: 1px solid #e53e3e !important;
        }
        div[class*="st-key-delete_"] button:hover, div[class*="st-key-confirm_delete_"] button:hover {
            background-color: #e53e3e !important; color: #fff !important; border: 1px solid #c53030 !important;
        }
        div[class*="st-key-add_brand_btn"] button,
        div[class*="st-key-add_activity_btn_"] button,
        div[class*="st-key-add_url_"] button {
            background-color: #2563eb !important; color: #fff !important; border: 1px solid #1d4ed8 !important;
        }
        div[class*="st-key-add_brand_btn"] button:hover,
        div[class*="st-key-add_activity_btn_"] button:hover,
        div[class*="st-key-add_url_"] button:hover {
            background-color: #1d4ed8 !important; border: 1px solid #1e40af !important;
        }
        div[class*="st-key-toggle_brand_open_"] button,
        div[class*="st-key-toggle_brand_closed_"] button,
        div[class*="st-key-toggle_activity_open_"] button,
        div[class*="st-key-toggle_activity_closed_"] button,
        div[class*="st-key-toggle_sheet_open_"] button,
        div[class*="st-key-toggle_sheet_closed_"] button {
            background-color: #e5e7eb !important; border: 1px solid #9ca3af !important; color: #111827 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_ui_messages(execution_messages: list[dict[str, str]]) -> None:
    # 상단 로그/알림 블록은 사용자 요청으로 비노출 처리.
    # 함수 시그니처는 유지해 기존 호출부 호환성을 보장한다.
    _ = execution_messages


def _render_management_messages() -> None:
    # 제목 아래 레이아웃 흔들림 방지를 위해 관리 메시지 블록도 비노출 처리.
    return


def _status_style(status: str) -> str:
    normalized = _safe_text(status).lower()
    if normalized == "completed":
        return "background-color: #dcfce7; color: #166534; font-weight: 600;"
    if normalized == "failed":
        return "background-color: #fee2e2; color: #991b1b; font-weight: 600;"
    if normalized == "running":
        return "background-color: #dbeafe; color: #1d4ed8; font-weight: 600;"
    if normalized == "pending":
        return "background-color: #fef3c7; color: #92400e; font-weight: 600;"
    if normalized == "skipped":
        return "background-color: #f3f4f6; color: #6b7280; font-weight: 500;"
    return ""


def _build_activity_result_df(execution_snapshot: dict[str, Any]) -> pd.DataFrame:
    summaries_raw = execution_snapshot.get("activity_results") or execution_snapshot.get("activity_summaries") or []
    if not isinstance(summaries_raw, list) or not summaries_raw:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for item in summaries_raw:
        if not isinstance(item, dict):
            continue
        brand = _safe_text(item.get("brand"))
        activity = _safe_text(item.get("activity"))
        workbook_path = _safe_text(item.get("workbook_path"))
        workbook_name = Path(workbook_path).name if workbook_path else ""
        rows_by_sheet = item.get("rows_by_sheet") or {}
        failed_sheets = item.get("failed_sheets") or []
        failed_sheet_text = ", ".join(str(s) for s in failed_sheets if _safe_text(s))
        message = _safe_text(item.get("message"))

        if workbook_name:
            result_message = "전체 시트 처리 완료"
        elif failed_sheet_text:
            result_message = f"문제된 시트({failed_sheet_text}) export에 실패해 통합파일이 생성되지 않았습니다."
        elif message:
            result_message = message
        else:
            result_message = "통합파일이 생성되지 않았습니다."

        row: dict[str, Any] = {
            "브랜드": brand,
            "액티비티": activity,
            "통합파일": workbook_name,
            "결과": result_message,
        }
        for sheet_name in RESULT_SHEET_COLUMNS:
            if workbook_name:
                row[f"{sheet_name} 행수"] = int((rows_by_sheet or {}).get(sheet_name, 0))
            else:
                row[f"{sheet_name} 행수"] = ""
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

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

    if entries:
        for idx, item in enumerate(list(entries)):
            current_url = _extract_url(item)
            edit_key = f"url_edit_{safe_activity}_{safe_sheet}_{idx}"
            st.session_state.setdefault(edit_key, current_url)

            row_cols = st.columns([0.45, 5.6, 0.9, 0.9], vertical_alignment="center")
            row_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
            row_cols[1].text_input(f"URL {idx + 1}", key=edit_key, label_visibility="collapsed")
            if row_cols[2].button("💾", key=f"save_url_{safe_activity}_{safe_sheet}_{idx}", use_container_width=True):
                ok, message, _ = update_sheet_url(
                    config_data,
                    brand_code=brand_code,
                    activity_name=activity_name,
                    sheet_name=sheet_name,
                    index=idx,
                    raw_url=st.session_state.get(edit_key, ""),
                )
                if ok:
                    save_callback()
                    _push_management_message(
                        f"✅ {brand_name} · {activity_name} · {sheet_name} URL을 수정했습니다.",
                        level="success",
                    )
                    st.rerun()
                _push_management_message(
                    f"⚠️ {brand_name} · {activity_name} · {sheet_name} URL 수정 실패: {message}",
                    level="warning",
                )

            if row_cols[3].button("🗑️", key=f"delete_url_{safe_activity}_{safe_sheet}_{idx}", use_container_width=True):
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

    add_input_key = f"url_add_{safe_activity}_{safe_sheet}"
    _consume_input_reset(add_input_key)
    add_cols = st.columns([0.45, 5.6, 0.9, 0.9], vertical_alignment="center")
    add_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
    raw_candidate = add_cols[1].text_input(
        f"{sheet_name} 새 URL",
        key=add_input_key,
        placeholder="Meta report URL을 붙여넣으세요.",
        label_visibility="collapsed",
    )
    if _safe_text(raw_candidate):
        try:
            preview = clean_report_url(raw_candidate, default_event_source=_safe_text(config_data.get("view_event_source")))
            add_cols[1].caption(f"미리보기: {preview}")
        except UrlValidationError as exc:
            add_cols[1].error(f"URL 파싱 실패: {exc}")

    if add_cols[2].button("저장", key=f"add_url_{safe_activity}_{safe_sheet}", use_container_width=True):
        ok, message, _ = add_sheet_url(
            config_data,
            brand_code=brand_code,
            activity_name=activity_name,
            sheet_name=sheet_name,
            raw_url=raw_candidate,
        )
        if ok:
            save_callback()
            _request_input_reset(add_input_key)
            _push_management_message(
                f"✅ {brand_name} · {activity_name} · {sheet_name} URL을 추가했습니다.",
                level="success",
            )
            st.rerun()
        _push_management_message(
            f"⚠️ {brand_name} · {activity_name} · {sheet_name} URL 추가 실패: {message}",
            level="warning",
        )
    if add_cols[3].button("삭제", key=f"delete_new_url_{safe_activity}_{safe_sheet}", use_container_width=True):
        _request_input_reset(add_input_key)
        st.rerun()


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
    header_cols[3].markdown("**열기**")

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


def _render_activity_row(*, config_data: dict[str, Any], save_callback: Callable[[], None], brand: dict[str, Any], activity: dict[str, Any]) -> None:
    brand_code = _safe_text(brand.get("code"))
    brand_name = _safe_text(brand.get("name"))
    activity_name = _safe_text(activity.get("name"))
    activity_id = build_activity_id(brand_code=brand_code, activity_name=activity_name)
    safe_activity = _safe_key(activity_id)
    is_selected = activity_id in set(st.session_state.get("selected_activity_ids", set()))

    ready_sheet_count = _ready_sheet_count(activity)
    if ready_sheet_count > 0:
        badge = f"🟢 {ready_sheet_count}/6 시트 준비"
    elif is_selected:
        badge = "🔴 0/6 시트 준비"
    else:
        badge = "⚫ 0/6 시트 준비"

    checkbox_key = f"activity_checkbox_{safe_activity}"
    st.session_state[checkbox_key] = is_selected
    activity_open_key = f"ui_activity_open_{safe_activity}"
    is_open = bool(st.session_state.get(activity_open_key, False))
    is_editing = _safe_text(st.session_state.get("ui_edit_activity")) == activity_id

    row_cols = st.columns([0.45, 0.55, 3.9, 0.35, 1.7, 0.8], vertical_alignment="center")
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

    row_cols[4].markdown(f"<span class='meta-tree-caption'>{badge}</span>", unsafe_allow_html=True)
    toggle_activity_key = f"toggle_activity_open_{safe_activity}" if is_open else f"toggle_activity_closed_{safe_activity}"
    if row_cols[5].button("▼" if is_open else "▶", key=toggle_activity_key, use_container_width=True):
        _toggle_bool_key(activity_open_key)
        st.rerun()

    if is_editing:
        edit_value_key = f"ui_edit_activity_value_{safe_activity}"
        edit_cols = st.columns([0.45, 4.5, 0.9, 0.9, 2.0], vertical_alignment="center")
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

    delete_cols = st.columns([0.45, 1.7, 4.3], vertical_alignment="center")
    delete_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
    if delete_cols[1].button("🗑️ 액티비티 삭제", key=f"delete_activity_{safe_activity}", use_container_width=True):
        st.session_state["pending_activity_delete"] = activity_id

    if _safe_text(st.session_state.get("pending_activity_delete")) == activity_id:
        st.warning("이 액티비티를 삭제하면 하위 report URL도 함께 삭제됩니다.")
        confirm_cols = st.columns([0.45, 1.2, 1.2, 3.6], vertical_alignment="center")
        confirm_cols[0].markdown("&nbsp;", unsafe_allow_html=True)
        if confirm_cols[1].button("삭제 확인", key=f"confirm_delete_activity_{safe_activity}", use_container_width=True):
            ok, message = delete_activity(config_data, brand_code, activity_name)
            st.session_state["pending_activity_delete"] = ""
            if ok:
                selected_ids = set(st.session_state.get("selected_activity_ids", set()))
                selected_ids.discard(activity_id)
                st.session_state["selected_activity_ids"] = selected_ids
                save_callback()
                _toast(f"🗑️ '{activity_name}'이(가) 삭제되었습니다.", icon="✅")
                _push_management_message(f"✅ {brand_name} · {activity_name} 액티비티를 삭제했습니다.", level="success")
                st.rerun()
            _push_management_message(f"⚠️ {brand_name} · {activity_name} 삭제 실패: {message}", level="warning")
        if confirm_cols[2].button("취소", key=f"cancel_delete_activity_{safe_activity}", use_container_width=True):
            st.session_state["pending_activity_delete"] = ""
            st.rerun()

def _render_brand_card(*, config_data: dict[str, Any], save_callback: Callable[[], None], brand: dict[str, Any]) -> None:
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
        row_cols = st.columns([0.55, 3.9, 0.35, 1.6, 0.75], vertical_alignment="center")
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

        row_cols[3].markdown(
            f"<span class='meta-tree-caption'>{selected_count}/{total_count} 액티비티 선택</span>",
            unsafe_allow_html=True,
        )

        toggle_brand_key = f"toggle_brand_open_{safe_brand}" if is_open else f"toggle_brand_closed_{safe_brand}"
        if row_cols[4].button("▼" if is_open else "▶", key=toggle_brand_key, use_container_width=True):
            _toggle_bool_key(brand_open_key)
            st.rerun()

        if is_editing:
            edit_value_key = f"ui_edit_brand_value_{safe_brand}"
            edit_cols = st.columns([4.7, 0.9, 0.9, 2.0], vertical_alignment="center")
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

        if not is_open:
            return

        if activities:
            for activity in activities:
                _render_activity_row(config_data=config_data, save_callback=save_callback, brand=brand, activity=activity)
        else:
            st.info("등록된 액티비티가 없습니다.")

        st.markdown("###### + 액티비티 추가")
        add_key = f"add_activity_input_{safe_brand}"
        _consume_input_reset(add_key)
        add_cols = st.columns([4.5, 1.3], vertical_alignment="center")
        add_cols[0].text_input("액티비티 추가", key=add_key, placeholder="예: Activity 1", label_visibility="collapsed")
        if add_cols[1].button("추가", key=f"add_activity_btn_{safe_brand}", use_container_width=True):
            ok, message = add_activity(config_data, brand_code, st.session_state.get(add_key, ""))
            if ok:
                save_callback()
                _request_input_reset(add_key)
                _push_management_message(f"✅ {brand_name}에 액티비티를 추가했습니다.", level="success")
                st.rerun()
            _push_management_message(f"⚠️ {brand_name} 액티비티 추가 실패: {message}", level="warning")

        delete_cols = st.columns([1.6, 4.2], vertical_alignment="center")
        if delete_cols[0].button("🗑️ 브랜드 삭제", key=f"delete_brand_{safe_brand}", use_container_width=True):
            st.session_state["pending_brand_delete"] = brand_code

        if _safe_text(st.session_state.get("pending_brand_delete")) == brand_code:
            st.warning("브랜드를 삭제하면 하위 액티비티와 report URL이 모두 삭제됩니다.")
            confirm_cols = st.columns([1.2, 1.2, 3.4], vertical_alignment="center")
            if confirm_cols[0].button("삭제 확인", key=f"confirm_delete_brand_{safe_brand}", use_container_width=True):
                ok, message = delete_brand(config_data, brand_code)
                st.session_state["pending_brand_delete"] = ""
                if ok:
                    _remove_brand_selections(brand_code)
                    save_callback()
                    _toast(f"🗑️ '{brand_name}'이(가) 삭제되었습니다.", icon="✅")
                    _push_management_message(f"✅ {brand_name} 브랜드를 삭제했습니다.", level="success")
                    st.rerun()
                _push_management_message(f"⚠️ {brand_name} 삭제 실패: {message}", level="warning")
            if confirm_cols[1].button("취소", key=f"cancel_delete_brand_{safe_brand}", use_container_width=True):
                st.session_state["pending_brand_delete"] = ""
                st.rerun()


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
    selected_count = len(set(st.session_state.get("selected_activity_ids", set())))
    if is_running:
        help_text = "현재 실행 중입니다."
    elif selected_count <= 0:
        help_text = "아래에서 Export할 Report를 선택하세요."
    elif not validation.can_run:
        help_text = "선택한 Report의 준비 상태를 확인하세요."
    else:
        help_text = "Meta에 로그인하고 Report를 다운로드합니다."
    export_disabled = is_running or (not validation.can_run)

    header_cols = st.columns([3.7, 1.6], vertical_alignment="top")
    header_cols[0].subheader("📋 Export할 Report 선택하기")
    header_cols[1].markdown(
        "<div class='meta-sub-caption'>Meta에 로그인하고 Report를 다운로드합니다.</div>",
        unsafe_allow_html=True,
    )
    if header_cols[1].button(
        "Export",
        type="primary",
        disabled=export_disabled,
        use_container_width=True,
        help=help_text,
    ):
        on_start_execution()

    brands = config_data.get("brands", [])
    if brands:
        for brand in brands:
            _render_brand_card(config_data=config_data, save_callback=save_callback, brand=brand)
    else:
        st.info("등록된 브랜드가 없습니다. 먼저 브랜드를 추가하세요.")

    st.markdown("##### + 브랜드 추가")
    _consume_input_reset("new_brand_name_input")
    add_cols = st.columns([4.5, 1.3], vertical_alignment="center")
    add_cols[0].text_input("브랜드 추가", key="new_brand_name_input", placeholder="예: Brand A", label_visibility="collapsed")
    if add_cols[1].button("추가", key="add_brand_btn", use_container_width=True):
        ok, message = add_brand(config_data, st.session_state.get("new_brand_name_input", ""))
        if ok:
            save_callback()
            _request_input_reset("new_brand_name_input")
            _push_management_message("✅ 브랜드를 추가했습니다.", level="success")
            st.rerun()
        _push_management_message(f"⚠️ 브랜드 추가 실패: {message}", level="warning")


def render_sidebar_execution_section(
    *,
    on_output_dir_change: Callable[[], None] | None = None,
    on_downloads_dir_change: Callable[[], None] | None = None,
    on_logs_dir_change: Callable[[], None] | None = None,
) -> None:
    with st.sidebar:
        st.subheader("⚙️ Run Settings")
        st.selectbox(
            "브라우저",
            options=["msedge", "chrome"],
            index=0 if _safe_text(st.session_state.get("browser")) != "chrome" else 1,
            key="browser",
        )
        st.text_input(
            "결과 저장 경로",
            key="output_dir_input",
            on_change=on_output_dir_change,
        )
        st.text_input(
            "다운로드 경로",
            key="downloads_dir_input",
            on_change=on_downloads_dir_change,
        )
        st.text_input(
            "로그 경로",
            key="logs_dir_input",
            on_change=on_logs_dir_change,
        )


def render_bottom_section(
    *,
    validation: ValidationResult,
    execution_snapshot: dict[str, Any],
    activity_label_by_id: dict[str, str],
) -> None:
    st.subheader("📊 진행 상황")

    st.markdown("#### 실행 준비 상태")
    if validation.readiness_rows:
        readiness_df = pd.DataFrame(
            [{"브랜드": r.brand, "액티비티": r.activity, "준비 시트 수": r.sheet_count, "URL 수": r.url_count} for r in validation.readiness_rows]
        )
        st.dataframe(readiness_df, use_container_width=True, hide_index=True)
    else:
        st.info("선택된 액티비티가 없습니다.")

    if validation.missing_url_activity_ids:
        labels = [activity_label_by_id.get(item_id, item_id) for item_id in validation.missing_url_activity_ids]
        st.warning("아래 액티비티는 report URL이 없어 export할 수 없습니다: " + ", ".join(labels))

    st.markdown("#### 실행 로그")
    rows = execution_snapshot.get("rows", [])
    if rows:
        log_df = pd.DataFrame(
            [
                {
                    "브랜드": row.brand,
                    "액티비티": row.activity,
                    "시트": row.sheet,
                    "URL 수": row.url_count,
                    "상태": row.status,
                    "메시지": row.message,
                    "최종 갱신": row.last_updated,
                }
                for row in rows
            ]
        )
        filtered_df = log_df[log_df["상태"] != "Skipped"].copy()
        if filtered_df.empty:
            st.caption("표시할 로그가 없습니다. (Skipped 제외)")
        else:
            styled = filtered_df.style.map(_status_style, subset=["상태"])
            st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("실행 로그가 없습니다.")

    result_df = _build_activity_result_df(execution_snapshot)
    if not result_df.empty:
        st.markdown("#### 처리 완료 요약 / 생성된 통합 파일")
        st.dataframe(result_df, use_container_width=True, hide_index=True)

    log_file = _safe_text(execution_snapshot.get("log_file"))
    run_id = _safe_text(execution_snapshot.get("run_id"))
    if log_file or run_id:
        st.caption(f"Run ID: {run_id} | Log: {log_file}")
