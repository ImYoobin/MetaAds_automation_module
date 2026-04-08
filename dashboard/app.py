"""Streamlit dashboard app entrypoint."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path, PureWindowsPath
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

RUNTIME_SETTINGS_PATH = Path("config/meta/runtime_settings.json")
RUNTIME_PATH_KEYS: tuple[str, ...] = ("output_dir", "downloads_dir", "logs_dir")
RUNTIME_INPUT_KEY_BY_PATH_KEY: dict[str, str] = {
    "output_dir": "output_dir_input",
    "downloads_dir": "downloads_dir_input",
    "logs_dir": "logs_dir_input",
}
INVALID_RUNTIME_PATH_MESSAGE = "올바르지 않은 경로입니다. 로컬 PC 경로를 입력해주세요."
_WINDOWS_ABS_DRIVE_RE = re.compile(r"^[A-Za-z]:\\")
_WINDOWS_DRIVE_TOKEN_RE = re.compile(r"[A-Za-z]:\\")
_WINDOWS_USER_PROFILE_RE = re.compile(r"^[A-Za-z]:\\Users\\[^\\]+(?:\\|$)", re.IGNORECASE)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _default_runtime_settings() -> dict[str, str]:
    return {
        "browser": DEFAULT_BROWSER,
        "output_dir": str(DEFAULT_USER_OUTPUT_DIR),
        "downloads_dir": str(DEFAULT_USER_DOWNLOADS_DIR),
        "logs_dir": str(DEFAULT_USER_LOGS_DIR),
    }


def _to_portable_runtime_path(value: str) -> str:
    normalized = _safe_text(value).replace("/", "\\").rstrip("\\")
    if not normalized:
        return normalized

    home_path = _safe_text(str(Path.home())).replace("/", "\\").rstrip("\\")
    if not home_path:
        return normalized

    lowered = normalized.lower()
    home_lowered = home_path.lower()
    if lowered == home_lowered:
        return "%USERPROFILE%"

    prefix = f"{home_lowered}\\"
    if lowered.startswith(prefix):
        suffix = normalized[len(home_path) :].lstrip("\\/")
        if suffix:
            return f"%USERPROFILE%\\{suffix}"
        return "%USERPROFILE%"

    return normalized


def _is_foreign_windows_profile_path(value: str) -> bool:
    normalized = _safe_text(value).replace("/", "\\").rstrip("\\")
    if not normalized:
        return False

    home_path = _safe_text(str(Path.home())).replace("/", "\\").rstrip("\\")
    if not home_path:
        return False

    lowered = normalized.lower()
    home_lowered = home_path.lower()
    if lowered == home_lowered or lowered.startswith(f"{home_lowered}\\"):
        return False

    return bool(_WINDOWS_USER_PROFILE_RE.match(normalized))


def _validate_runtime_path(value: Any, *, check_writable: bool) -> tuple[bool, str]:
    raw_value = _safe_text(value)
    if not raw_value:
        return False, INVALID_RUNTIME_PATH_MESSAGE

    expanded = _safe_text(os.path.expandvars(raw_value))
    candidate = expanded.replace("/", "\\")

    if not _WINDOWS_ABS_DRIVE_RE.match(candidate):
        return False, INVALID_RUNTIME_PATH_MESSAGE

    drive_tokens = _WINDOWS_DRIVE_TOKEN_RE.findall(candidate)
    if len(drive_tokens) != 1:
        return False, INVALID_RUNTIME_PATH_MESSAGE

    lowered = candidate.lower()
    if "\\onedrive" in lowered:
        return False, INVALID_RUNTIME_PATH_MESSAGE

    invalid_chars = set('<>:"|?*')
    for part in PureWindowsPath(candidate).parts[1:]:
        segment = _safe_text(part).rstrip("\\/")
        if not segment:
            continue
        if any(char in invalid_chars for char in segment):
            return False, INVALID_RUNTIME_PATH_MESSAGE

    normalized = str(Path(candidate).expanduser())
    if check_writable:
        try:
            target_dir = Path(normalized)
            target_dir.mkdir(parents=True, exist_ok=True)
            probe_path = target_dir / f".path_probe_{time.time_ns()}.tmp"
            probe_path.write_text("ok", encoding="utf-8")
            probe_path.unlink(missing_ok=True)
        except Exception:
            return False, INVALID_RUNTIME_PATH_MESSAGE

    return True, normalized


def _sanitize_loaded_runtime_settings(runtime_settings: dict[str, str]) -> tuple[dict[str, str], bool]:
    defaults = _default_runtime_settings()
    sanitized = dict(defaults)
    has_invalid = False

    raw_browser = _safe_text(runtime_settings.get("browser"))
    if raw_browser:
        if raw_browser in {"msedge", "chrome"}:
            sanitized["browser"] = raw_browser
        else:
            has_invalid = True

    for key in RUNTIME_PATH_KEYS:
        raw_value = _safe_text(runtime_settings.get(key))
        if not raw_value:
            if key in runtime_settings:
                has_invalid = True
            continue
        is_valid, normalized_or_message = _validate_runtime_path(raw_value, check_writable=False)
        if is_valid:
            if _is_foreign_windows_profile_path(normalized_or_message):
                has_invalid = True
                continue
            sanitized[key] = normalized_or_message
        else:
            has_invalid = True

    return sanitized, has_invalid


def _push_runtime_path_warning() -> None:
    st.session_state.setdefault("ui_messages", []).append(
        {
            "level": "warning",
            "text": INVALID_RUNTIME_PATH_MESSAGE,
        }
    )


def _on_runtime_path_input_change(path_key: str) -> None:
    input_key = RUNTIME_INPUT_KEY_BY_PATH_KEY[path_key]
    candidate = _safe_text(st.session_state.get(input_key))
    is_valid, normalized_or_message = _validate_runtime_path(candidate, check_writable=False)
    if is_valid:
        st.session_state[path_key] = normalized_or_message
        st.session_state[f"_runtime_valid_{path_key}"] = normalized_or_message
        st.session_state[input_key] = normalized_or_message
        return

    fallback = _safe_text(st.session_state.get(f"_runtime_valid_{path_key}"))
    if not fallback:
        fallback = _default_runtime_settings()[path_key]
    st.session_state[path_key] = fallback
    st.session_state[input_key] = fallback
    st.session_state[f"_runtime_valid_{path_key}"] = fallback
    _push_runtime_path_warning()


def _on_output_dir_input_change() -> None:
    _on_runtime_path_input_change("output_dir")


def _on_downloads_dir_input_change() -> None:
    _on_runtime_path_input_change("downloads_dir")


def _on_logs_dir_input_change() -> None:
    _on_runtime_path_input_change("logs_dir")


def _validate_runtime_paths_before_run() -> tuple[bool, dict[str, str]]:
    normalized_paths: dict[str, str] = {}
    for path_key in RUNTIME_PATH_KEYS:
        is_valid, normalized_or_message = _validate_runtime_path(
            st.session_state.get(path_key),
            check_writable=True,
        )
        if not is_valid:
            _push_runtime_path_warning()
            return False, {}
        normalized_paths[path_key] = normalized_or_message

    for path_key, normalized in normalized_paths.items():
        st.session_state[path_key] = normalized
        st.session_state[f"_runtime_valid_{path_key}"] = normalized

    return True, normalized_paths


def _load_runtime_settings(path: str | Path = RUNTIME_SETTINGS_PATH) -> tuple[dict[str, str], list[str]]:
    settings_path = Path(path).expanduser().resolve()
    if not settings_path.exists():
        return {}, []

    try:
        parsed = json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {}, [f"Run settings 파일을 읽지 못했습니다: {settings_path} ({exc})"]

    if not isinstance(parsed, dict):
        return {}, [f"Run settings 형식이 올바르지 않습니다: {settings_path}"]

    out: dict[str, str] = {}
    for key in ("browser", "output_dir", "downloads_dir", "logs_dir"):
        value = _safe_text(parsed.get(key))
        if value:
            out[key] = value
    return out, []


def _runtime_settings_payload() -> dict[str, str]:
    defaults = _default_runtime_settings()
    browser = _safe_text(st.session_state.get("browser")) or defaults["browser"]
    if browser not in {"msedge", "chrome"}:
        browser = defaults["browser"]
    output_dir = _safe_text(st.session_state.get("output_dir")) or defaults["output_dir"]
    downloads_dir = _safe_text(st.session_state.get("downloads_dir")) or defaults["downloads_dir"]
    logs_dir = _safe_text(st.session_state.get("logs_dir")) or defaults["logs_dir"]
    return {
        "browser": browser,
        "output_dir": _to_portable_runtime_path(output_dir),
        "downloads_dir": _to_portable_runtime_path(downloads_dir),
        "logs_dir": _to_portable_runtime_path(logs_dir),
    }


def _persist_runtime_settings(*, force: bool = False) -> None:
    payload = _runtime_settings_payload()
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if (not force) and (_safe_text(st.session_state.get("_runtime_settings_last_saved")) == serialized):
        return

    settings_path = Path(st.session_state.get("runtime_settings_path", str(RUNTIME_SETTINGS_PATH))).expanduser().resolve()
    try:
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = settings_path.with_suffix(settings_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(settings_path)
        st.session_state["_runtime_settings_last_saved"] = serialized
        st.session_state["_runtime_settings_last_error"] = ""
        st.session_state["_runtime_settings_needs_heal"] = False
    except Exception as exc:  # noqa: BLE001
        error_text = f"Run settings 저장 실패: {exc}"
        if _safe_text(st.session_state.get("_runtime_settings_last_error")) != error_text:
            st.session_state.setdefault("ui_messages", []).append(
                {
                    "level": "warning",
                    "text": error_text,
                }
            )
        st.session_state["_runtime_settings_last_error"] = error_text
        st.session_state["_runtime_settings_needs_heal"] = True


def _init_state() -> None:
    if "config_path" not in st.session_state:
        st.session_state["config_path"] = str(DEFAULT_CONFIG_PATH)

    runtime_settings_raw, runtime_messages = _load_runtime_settings(RUNTIME_SETTINGS_PATH)
    runtime_settings, has_invalid_runtime_settings = _sanitize_loaded_runtime_settings(runtime_settings_raw)
    st.session_state.setdefault("runtime_settings_path", str(RUNTIME_SETTINGS_PATH))
    st.session_state.setdefault("_runtime_settings_needs_heal", False)
    if has_invalid_runtime_settings:
        st.session_state["_runtime_settings_needs_heal"] = True

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
    st.session_state.setdefault("browser", runtime_settings["browser"])
    if _safe_text(st.session_state.get("browser")) not in {"msedge", "chrome"}:
        st.session_state["browser"] = runtime_settings["browser"]
        st.session_state["_runtime_settings_needs_heal"] = True

    for path_key in RUNTIME_PATH_KEYS:
        st.session_state.setdefault(path_key, runtime_settings[path_key])
        is_valid, normalized_or_message = _validate_runtime_path(
            st.session_state.get(path_key),
            check_writable=False,
        )
        if not is_valid:
            normalized = runtime_settings[path_key]
            st.session_state["_runtime_settings_needs_heal"] = True
        else:
            normalized = normalized_or_message
        st.session_state[path_key] = normalized
        st.session_state[f"_runtime_valid_{path_key}"] = normalized
        input_key = RUNTIME_INPUT_KEY_BY_PATH_KEY[path_key]
        st.session_state.setdefault(input_key, normalized)

    st.session_state.setdefault("opened_output_for_run", "")

    if has_invalid_runtime_settings:
        runtime_messages = [*runtime_messages, INVALID_RUNTIME_PATH_MESSAGE]

    if runtime_messages:
        existing = st.session_state.setdefault("ui_messages", [])
        existing.extend({"level": "warning", "text": message} for message in runtime_messages)
        st.session_state["ui_messages"] = existing[-20:]

    st.session_state.setdefault(
        "_runtime_settings_last_saved",
        json.dumps(_runtime_settings_payload(), ensure_ascii=False, sort_keys=True),
    )
    st.session_state.setdefault("_runtime_settings_last_error", "")


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
        paths_valid, normalized_paths = _validate_runtime_paths_before_run()
        if not paths_valid:
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
            output_dir=Path(normalized_paths["output_dir"]),
            downloads_dir=Path(normalized_paths["downloads_dir"]),
            logs_dir=Path(normalized_paths["logs_dir"]),
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

    render_sidebar_execution_section(
        on_output_dir_change=_on_output_dir_input_change,
        on_downloads_dir_change=_on_downloads_dir_input_change,
        on_logs_dir_change=_on_logs_dir_input_change,
    )
    _persist_runtime_settings(force=bool(st.session_state.get("_runtime_settings_needs_heal")))
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
