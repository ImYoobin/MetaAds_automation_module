"""Streamlit dashboard app entrypoint."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import streamlit as st

from dashboard.models import build_activity_id
from dashboard.services.config_service import DEFAULT_CONFIG_PATH, load_config, save_config
from dashboard.services.execution_service import create_execution_store, start_execution
from dashboard.services.validation_service import build_execution_plan, validate_run_selection
from dashboard.ui import (
    render_bottom_section,
    render_sidebar_execution_section,
    render_top_section,
)
from meta_core.constants import (
    DEFAULT_BROWSER,
    DEFAULT_USER_DOWNLOADS_DIR,
    DEFAULT_USER_LOGS_DIR,
    DEFAULT_USER_OUTPUT_DIR,
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _init_state() -> None:
    if "config_path" not in st.session_state:
        st.session_state["config_path"] = str(DEFAULT_CONFIG_PATH)

    if "config_data" not in st.session_state:
        result = load_config(st.session_state["config_path"])
        st.session_state["config_data"] = result.config
        st.session_state["ui_messages"] = [
            {"level": "warning", "text": message}
            for message in result.messages
        ]
    else:
        st.session_state.setdefault("ui_messages", [])

    st.session_state.setdefault("management_messages", [])
    st.session_state.setdefault("selected_activity_ids", set())
    st.session_state.setdefault("pending_brand_delete", None)
    st.session_state.setdefault("pending_activity_delete", None)
    st.session_state.setdefault("execution_store", create_execution_store())
    st.session_state.setdefault("browser", DEFAULT_BROWSER)
    st.session_state.setdefault("output_dir", str(DEFAULT_USER_OUTPUT_DIR))
    st.session_state.setdefault("downloads_dir", str(DEFAULT_USER_DOWNLOADS_DIR))
    st.session_state.setdefault("logs_dir", str(DEFAULT_USER_LOGS_DIR))
    st.session_state.setdefault("opened_output_for_run", "")


def _activity_label_map(config_data: dict[str, Any]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for brand in config_data.get("brands", []):
        brand_code = _safe_text(brand.get("code"))
        brand_name = _safe_text(brand.get("name"))
        for activity in brand.get("activities", []):
            activity_name = _safe_text(activity.get("name"))
            item_id = build_activity_id(brand_code=brand_code, activity_name=activity_name)
            labels[item_id] = f"{brand_name} / {activity_name}"
    return labels


def _cleanup_selection(config_data: dict[str, Any]) -> None:
    labels = _activity_label_map(config_data)
    valid_ids = set(labels.keys())
    selected = set(st.session_state.get("selected_activity_ids", set()))
    st.session_state["selected_activity_ids"] = selected.intersection(valid_ids)


def _persist_config() -> None:
    save_config(
        st.session_state["config_data"],
        st.session_state["config_path"],
    )


def _open_output_folder_for_completed_run(execution_snapshot: dict[str, Any]) -> None:
    run_status = _safe_text(execution_snapshot.get("run_status"))
    run_id = _safe_text(execution_snapshot.get("run_id"))
    if run_status not in {"Completed", "Completed (With Failures)"} or not run_id:
        return
    if _safe_text(st.session_state.get("opened_output_for_run")) == run_id:
        return

    target_dir: Path | None = None
    outputs = execution_snapshot.get("outputs") or []
    if isinstance(outputs, list) and outputs:
        first_output = outputs[0]
        if isinstance(first_output, dict):
            workbook_path = _safe_text(first_output.get("workbook_path"))
            if workbook_path:
                target_dir = Path(workbook_path).expanduser().resolve().parent

    if target_dir is None:
        output_dir = _safe_text(st.session_state.get("output_dir"))
        if output_dir:
            target_dir = Path(output_dir).expanduser().resolve()

    if target_dir is None:
        st.session_state["opened_output_for_run"] = run_id
        return

    try:
        os.startfile(str(target_dir))  # type: ignore[attr-defined]
        st.session_state.setdefault("ui_messages", []).append(
            {
                "level": "success",
                "text": f"결과 파일 폴더를 열었습니다: {target_dir}",
            }
        )
    except Exception as exc:
        st.session_state.setdefault("ui_messages", []).append(
            {
                "level": "warning",
                "text": f"결과 파일 폴더를 자동으로 열지 못했습니다: {exc}",
            }
        )
    finally:
        st.session_state["opened_output_for_run"] = run_id


def main() -> None:
    st.set_page_config(
        page_title="Meta Ads Auto Export",
        page_icon="M",
        layout="wide",
    )
    _init_state()
    config_data = st.session_state["config_data"]
    _cleanup_selection(config_data)

    execution_store = st.session_state["execution_store"]
    execution_store.drain_events()
    execution_snapshot = execution_store.snapshot()
    _open_output_folder_for_completed_run(execution_snapshot)

    validation = validate_run_selection(
        config_data,
        set(st.session_state.get("selected_activity_ids", set())),
    )
    activity_labels = _activity_label_map(config_data)

    def _start_run() -> None:
        current_validation = validate_run_selection(
            config_data,
            set(st.session_state.get("selected_activity_ids", set())),
        )
        if not current_validation.can_run:
            st.session_state.setdefault("ui_messages", []).append(
                {
                    "level": "warning",
                    "text": " / ".join(current_validation.reasons),
                }
            )
            return

        _persist_config()
        plan = build_execution_plan(
            config_data,
            set(st.session_state.get("selected_activity_ids", set())),
        )
        ok, message = start_execution(
            store=execution_store,
            plan=plan,
            view_event_source=_safe_text(config_data.get("view_event_source")),
            export_event_source=_safe_text(config_data.get("export_event_source")),
            browser=_safe_text(st.session_state.get("browser")) or DEFAULT_BROWSER,
            output_dir=Path(_safe_text(st.session_state.get("output_dir"))),
            downloads_dir=Path(_safe_text(st.session_state.get("downloads_dir"))),
            logs_dir=Path(_safe_text(st.session_state.get("logs_dir"))),
        )
        st.session_state.setdefault("ui_messages", []).append(
            {
                "level": "success" if ok else "warning",
                "text": message,
            }
        )
        if ok:
            st.session_state["opened_output_for_run"] = ""
        st.rerun()

    st.title("Meta Ads Auto Export")
    st.markdown(
        "📋 상단에서 Export할 Report를 선택합니다.<br>"
        "📊 하단에서 실행 준비 상태와 진행 로그를 확인합니다.<br>"
        "⚙️ 좌측 사이드바에서 Run Settings를 설정합니다.",
        unsafe_allow_html=True,
    )

    render_sidebar_execution_section()
    render_top_section(
        config_data=config_data,
        save_callback=_persist_config,
        validation=validation,
        execution_snapshot=execution_snapshot,
        on_start_execution=_start_run,
    )

    validation = validate_run_selection(
        config_data,
        set(st.session_state.get("selected_activity_ids", set())),
    )

    st.divider()
    render_bottom_section(
        validation=validation,
        execution_snapshot=execution_snapshot,
        activity_label_by_id=activity_labels,
    )

    if execution_snapshot.get("is_running"):
        st.caption("Run is in progress. Auto-refresh every 2 seconds.")
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
