"""Streamlit dashboard app entrypoint."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import time
from pathlib import Path, PureWindowsPath
from typing import Any

import streamlit as st

from dashboard.models import build_activity_id
from dashboard.services.config_service import DEFAULT_CONFIG_PATH, load_config, save_config
from dashboard.services.execution_service import create_execution_store, start_execution
from dashboard.services.validation_service import (
    build_execution_plan,
    build_history_execution_plan,
    validate_run_selection,
)
from dashboard.ui import (
    render_bottom_section,
    render_sidebar_execution_section,
    render_top_section,
)
from meta_core.constants import DEFAULT_BROWSER
from meta_core.pathing import build_meta_shared_user_data_dir


RUNTIME_SETTINGS_PATH = Path("config/meta/runtime_settings.json")
LEGACY_RUNTIME_PATH_KEYS: tuple[str, ...] = ("output_dir", "downloads_dir", "logs_dir")
BASE_PARENT_INPUT_KEY = "base_parent_dir_input"
EXPORT_ROOT_DIRNAME = "MetaAdsExport"
DEFAULT_USER_PARENT_DIR = Path.home()
DEFAULT_USER_PARENT_DIR_TOKEN = "%USERPROFILE%"
INVALID_RUNTIME_PATH_MESSAGE = "올바르지 않은 부모 경로입니다. 로컬 PC의 폴더 경로를 입력해주세요."
_WINDOWS_ABS_DRIVE_RE = re.compile(r"^[A-Za-z]:\\")
_WINDOWS_DRIVE_TOKEN_RE = re.compile(r"[A-Za-z]:\\")
_LEGACY_EXPORT_ROOT_NAMES = {EXPORT_ROOT_DIRNAME.lower(), "googleadsexport"}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_path(path_text: str) -> Path:
    expanded = _safe_text(os.path.expandvars(path_text))
    return Path(expanded).expanduser().resolve()


def _default_runtime_settings() -> dict[str, str]:
    browser = _safe_text(DEFAULT_BROWSER).lower()
    if browser not in {"msedge", "chrome"}:
        browser = "msedge"
    return {
        "browser": browser,
        "base_parent_dir": DEFAULT_USER_PARENT_DIR_TOKEN,
    }


def _current_run_date_token() -> str:
    return _safe_text(st.session_state.get("run_date_folder")) or dt.datetime.now().strftime("%Y%m%d")


def _build_storage_roots(base_parent_dir_text: str = "") -> dict[str, Path]:
    parent_dir = _safe_path(
        _safe_text(base_parent_dir_text)
        or _safe_text(st.session_state.get("base_parent_dir"))
        or str(DEFAULT_USER_PARENT_DIR)
    )
    export_root = (parent_dir / EXPORT_ROOT_DIRNAME).resolve()
    output_root = (export_root / "output").resolve()
    return {
        "base": export_root,
        "raw_root": (export_root / "raw").resolve(),
        "trace_root": (export_root / "trace").resolve(),
        "output_root": output_root,
        "action_log_root": (output_root / "action_log").resolve(),
    }


def _build_run_storage_paths(base_parent_dir_text: str = "", run_date: str = "") -> dict[str, Path]:
    roots = _build_storage_roots(base_parent_dir_text)
    effective_run_date = _safe_text(run_date) or _current_run_date_token()
    return {
        "run_date": effective_run_date,
        "base": roots["base"],
        "raw_root": roots["raw_root"],
        "trace_root": roots["trace_root"],
        "output_root": roots["output_root"],
        "action_log_root": roots["action_log_root"],
        "raw_dir": (roots["raw_root"] / effective_run_date).resolve(),
        "trace_dir": (roots["trace_root"] / effective_run_date).resolve(),
        "output_dir": (roots["output_root"] / effective_run_date).resolve(),
        "action_log_dir": (roots["action_log_root"] / effective_run_date).resolve(),
    }


def _build_shared_user_data_dir(base_parent_dir_text: str = "", browser: str = "") -> Path:
    parent_dir = _safe_path(
        _safe_text(base_parent_dir_text)
        or _safe_text(st.session_state.get("base_parent_dir"))
        or str(DEFAULT_USER_PARENT_DIR)
    )
    selected_browser = (
        _safe_text(browser)
        or _safe_text(st.session_state.get("browser"))
        or _default_runtime_settings()["browser"]
    )
    return build_meta_shared_user_data_dir(parent_dir, selected_browser)


def _serialize_base_parent_dir_for_settings(base_parent_dir_text: str) -> str:
    normalized = _safe_path(base_parent_dir_text)
    if normalized == DEFAULT_USER_PARENT_DIR.resolve():
        return DEFAULT_USER_PARENT_DIR_TOKEN
    return str(normalized)


def _infer_base_parent_dir_from_legacy_settings(runtime_settings: dict[str, str]) -> str:
    for key in LEGACY_RUNTIME_PATH_KEYS:
        raw_value = _safe_text(runtime_settings.get(key))
        if not raw_value:
            continue
        try:
            candidate_path = _safe_path(raw_value)
        except Exception:  # noqa: BLE001
            continue
        for ancestor in (candidate_path, *candidate_path.parents):
            if ancestor.name.lower() in _LEGACY_EXPORT_ROOT_NAMES:
                return str(ancestor.parent)
    return ""


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
    sanitized = {
        "browser": _default_runtime_settings()["browser"],
        "base_parent_dir": str(DEFAULT_USER_PARENT_DIR),
    }
    has_invalid = False

    raw_browser = _safe_text(runtime_settings.get("browser")).lower()
    if raw_browser:
        if raw_browser in {"msedge", "chrome"}:
            sanitized["browser"] = raw_browser
        else:
            has_invalid = True

    raw_parent_dir = _safe_text(runtime_settings.get("base_parent_dir")) or _infer_base_parent_dir_from_legacy_settings(
        runtime_settings
    )
    if raw_parent_dir:
        is_valid, normalized_or_message = _validate_runtime_path(raw_parent_dir, check_writable=False)
        if is_valid:
            sanitized["base_parent_dir"] = normalized_or_message
        else:
            has_invalid = True
    elif any(_safe_text(runtime_settings.get(key)) for key in LEGACY_RUNTIME_PATH_KEYS):
        has_invalid = True

    return sanitized, has_invalid


def _push_runtime_path_warning() -> None:
    st.session_state["_runtime_path_error"] = INVALID_RUNTIME_PATH_MESSAGE


def _on_base_parent_dir_input_change() -> None:
    candidate = _safe_text(st.session_state.get(BASE_PARENT_INPUT_KEY))
    is_valid, normalized_or_message = _validate_runtime_path(candidate, check_writable=False)
    if is_valid:
        st.session_state["base_parent_dir"] = normalized_or_message
        st.session_state["_runtime_valid_base_parent_dir"] = normalized_or_message
        st.session_state[BASE_PARENT_INPUT_KEY] = normalized_or_message
        return

    fallback = _safe_text(st.session_state.get("_runtime_valid_base_parent_dir"))
    if not fallback:
        fallback = str(DEFAULT_USER_PARENT_DIR)
    st.session_state["base_parent_dir"] = fallback
    st.session_state["_runtime_valid_base_parent_dir"] = fallback
    st.session_state[BASE_PARENT_INPUT_KEY] = fallback
    _push_runtime_path_warning()


def _validate_runtime_paths_before_run() -> tuple[bool, dict[str, str]]:
    is_valid, normalized_or_message = _validate_runtime_path(
        st.session_state.get("base_parent_dir"),
        check_writable=True,
    )
    if not is_valid:
        _push_runtime_path_warning()
        return False, {}

    st.session_state["base_parent_dir"] = normalized_or_message
    st.session_state["_runtime_valid_base_parent_dir"] = normalized_or_message
    return True, {"base_parent_dir": normalized_or_message}


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
    for key in ("browser", "base_parent_dir", *LEGACY_RUNTIME_PATH_KEYS):
        value = _safe_text(parsed.get(key))
        if value:
            out[key] = value
    return out, []


def _runtime_settings_payload() -> dict[str, str]:
    defaults = _default_runtime_settings()
    browser = _safe_text(st.session_state.get("browser")) or defaults["browser"]
    if browser not in {"msedge", "chrome"}:
        browser = defaults["browser"]
    base_parent_dir = _safe_text(st.session_state.get("base_parent_dir")) or str(DEFAULT_USER_PARENT_DIR)
    return {
        "browser": browser,
        "base_parent_dir": _serialize_base_parent_dir_for_settings(base_parent_dir),
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
        st.session_state["_runtime_settings_last_error"] = error_text
        st.session_state["_runtime_settings_needs_heal"] = True


def _prepare_run_directories() -> dict[str, str]:
    run_date = dt.datetime.now().strftime("%Y%m%d")
    run_paths = _build_run_storage_paths(run_date=run_date)
    run_output_dir = run_paths["output_dir"]
    run_raw_dir = run_paths["raw_dir"]
    run_trace_dir = run_paths["trace_dir"]
    run_action_log_dir = run_paths["action_log_dir"]

    run_output_dir.mkdir(parents=True, exist_ok=True)
    run_raw_dir.mkdir(parents=True, exist_ok=True)
    run_trace_dir.mkdir(parents=True, exist_ok=True)
    run_action_log_dir.mkdir(parents=True, exist_ok=True)

    st.session_state["run_output_root_dir"] = str(run_paths["output_root"])
    st.session_state["run_output_dir"] = str(run_output_dir)
    st.session_state["run_raw_dir"] = str(run_raw_dir)
    st.session_state["run_trace_dir"] = str(run_trace_dir)
    st.session_state["run_downloads_dir"] = str(run_raw_dir)
    st.session_state["run_logs_dir"] = str(run_trace_dir)
    st.session_state["run_action_log_dir"] = str(run_action_log_dir)
    st.session_state["run_date_folder"] = run_date

    return {
        "run_date": run_date,
        "output_dir": str(run_output_dir),
        "output_root_dir": str(run_paths["output_root"]),
        "raw_dir": str(run_raw_dir),
        "trace_dir": str(run_trace_dir),
        "action_log_dir": str(run_action_log_dir),
    }


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
    st.session_state.setdefault("enable_report_download", True)
    st.session_state.setdefault("enable_action_log_download", True)
    st.session_state.setdefault("run_enable_report_download", True)
    st.session_state.setdefault("run_enable_action_log_download", True)
    st.session_state.setdefault("execution_store", create_execution_store())

    browser = _safe_text(st.session_state.get("browser")) or runtime_settings["browser"]
    if browser not in {"msedge", "chrome"}:
        browser = runtime_settings["browser"]
        st.session_state["_runtime_settings_needs_heal"] = True
    st.session_state["browser"] = browser

    base_parent_dir = _safe_text(st.session_state.get("base_parent_dir")) or runtime_settings["base_parent_dir"]
    is_valid, normalized_or_message = _validate_runtime_path(base_parent_dir, check_writable=False)
    if is_valid:
        normalized_parent = normalized_or_message
    else:
        normalized_parent = runtime_settings["base_parent_dir"]
        st.session_state["_runtime_settings_needs_heal"] = True
    st.session_state["base_parent_dir"] = normalized_parent
    st.session_state["_runtime_valid_base_parent_dir"] = normalized_parent
    st.session_state.setdefault(BASE_PARENT_INPUT_KEY, normalized_parent)
    st.session_state[BASE_PARENT_INPUT_KEY] = _safe_text(st.session_state.get(BASE_PARENT_INPUT_KEY)) or normalized_parent

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
    st.session_state.setdefault("_page_notice", {})


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


def _execution_modes_enabled() -> tuple[bool, bool]:
    return (
        bool(st.session_state.get("enable_report_download", True)),
        bool(st.session_state.get("enable_action_log_download", True)),
    )


def _open_output_folder_for_completed_run(execution_snapshot: dict[str, Any]) -> None:
    run_status = _safe_text(execution_snapshot.get("run_status"))
    run_id = _safe_text(execution_snapshot.get("run_id"))
    outputs = execution_snapshot.get("outputs") or []
    history_outputs = execution_snapshot.get("history_outputs") or []
    has_any_output = bool(outputs or history_outputs)
    if run_status not in {"Completed", "Completed (With Failures)", "Failed"} or not run_id or not has_any_output:
        return
    if _safe_text(st.session_state.get("opened_output_for_run")) == run_id:
        return

    output_root_dir = _safe_text(st.session_state.get("run_output_root_dir"))
    if not output_root_dir:
        output_root_dir = str(_build_storage_roots()["output_root"])

    try:
        subprocess.Popen(["explorer", output_root_dir])  # noqa: S603
    except Exception:
        pass

    st.session_state["opened_output_for_run"] = run_id


def main() -> None:
    st.set_page_config(
        page_title="Meta Ads Auto Download",
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

    report_enabled, action_log_enabled = _execution_modes_enabled()
    validation = validate_run_selection(
        config_data,
        set(st.session_state.get("selected_activity_ids", set())),
        enable_report_download=report_enabled,
        enable_action_log_download=action_log_enabled,
    )
    activity_labels = _activity_label_map(config_data)

    def _start_run() -> None:
        report_enabled, action_log_enabled = _execution_modes_enabled()
        current_validation = validate_run_selection(
            config_data,
            set(st.session_state.get("selected_activity_ids", set())),
            enable_report_download=report_enabled,
            enable_action_log_download=action_log_enabled,
        )
        if not current_validation.can_run:
            st.session_state["_page_notice"] = {
                "level": "warning",
                "text": " / ".join(current_validation.reasons),
            }
            st.rerun()
            return

        paths_valid, _ = _validate_runtime_paths_before_run()
        if not paths_valid:
            st.rerun()
            return

        run_dirs = _prepare_run_directories()
        _persist_config()
        user_data_dir = _build_shared_user_data_dir(
            browser=_safe_text(st.session_state.get("browser")) or _default_runtime_settings()["browser"],
        )
        report_plan = build_execution_plan(
            config_data,
            set(st.session_state.get("selected_activity_ids", set())),
        )
        history_plan = build_history_execution_plan(
            config_data,
            set(st.session_state.get("selected_activity_ids", set())),
        )
        ok, message = start_execution(
            store=execution_store,
            report_plan=report_plan,
            history_plan=history_plan,
            enable_report_download=report_enabled,
            enable_action_log_download=action_log_enabled,
            view_event_source=_safe_text(config_data.get("view_event_source")),
            export_event_source=_safe_text(config_data.get("export_event_source")),
            browser=_safe_text(st.session_state.get("browser")) or _default_runtime_settings()["browser"],
            output_dir=Path(run_dirs["output_dir"]),
            raw_dir=Path(run_dirs["raw_dir"]),
            trace_dir=Path(run_dirs["trace_dir"]),
            action_log_dir=Path(run_dirs["action_log_dir"]),
            user_data_dir=user_data_dir,
        )
        if ok:
            st.session_state["run_enable_report_download"] = report_enabled
            st.session_state["run_enable_action_log_download"] = action_log_enabled
            st.session_state["opened_output_for_run"] = ""
            st.session_state["run_user_data_dir"] = str(user_data_dir)
            st.session_state["_page_notice"] = {}
        else:
            st.session_state["_page_notice"] = {
                "level": "warning",
                "text": message,
            }
        st.rerun()

    st.title("Meta Ads Auto Download")
    st.markdown(
        "광고 계정에 세팅된 리포트 URL을 붙여넣어 주세요.<br>"
        "시트별로 등록된 URL을 기반으로 액티비티별 통합 파일이 생성됩니다.",
        unsafe_allow_html=True,
    )

    notice = st.session_state.pop("_page_notice", {})
    if isinstance(notice, dict):
        notice_text = _safe_text(notice.get("text"))
        notice_level = _safe_text(notice.get("level")).lower()
        if notice_text:
            if notice_level == "success":
                st.success(notice_text)
            elif notice_level == "error":
                st.error(notice_text)
            else:
                st.warning(notice_text)

    preview_paths = _build_run_storage_paths()
    render_sidebar_execution_section(
        preview_paths=preview_paths,
        is_running=bool(execution_snapshot.get("is_running")),
        on_base_parent_dir_change=_on_base_parent_dir_input_change,
    )
    runtime_path_error = _safe_text(st.session_state.pop("_runtime_path_error", ""))
    if runtime_path_error:
        st.warning(runtime_path_error)
    _persist_runtime_settings(force=bool(st.session_state.get("_runtime_settings_needs_heal")))

    render_top_section(
        config_data=config_data,
        save_callback=_persist_config,
        validation=validation,
        execution_snapshot=execution_snapshot,
        on_start_execution=_start_run,
    )

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
