"""Background execution worker and thread-safe progress store."""

from __future__ import annotations

import datetime as dt
import queue
import threading
from pathlib import Path
from typing import Any, Callable

from dashboard.models import (
    ActivityExecutionPlan,
    HistoryExecutionPlan,
    HistoryLogRow,
    LogRow,
)
from meta_core.pathing import prepare_meta_user_data_dir


ProgressCallback = Callable[[dict[str, Any]], None]

HISTORY_WAITING_FOR_LOGIN_MESSAGE = "\ub85c\uadf8\uc778 \ub300\uae30\uc911\uc785\ub2c8\ub2e4."
HISTORY_WAITING_FOR_REPORT_MESSAGE = (
    "\ucea0\ud398\uc778 \ub370\uc774\ud130 \ub2e4\uc6b4\ub85c\ub4dc \uc9c4\ud589\uc911\uc785\ub2c8\ub2e4."
)
HISTORY_WAITING_FOR_PRIOR_ACTIVITY_MESSAGE = (
    "\uc55e\uc120 \uc561\ud2f0\ube44\ud2f0 \ucc98\ub9ac \ub300\uae30\uc911\uc785\ub2c8\ub2e4."
)
HISTORY_PREPARING_RUN_MESSAGE = "\uc561\uc158 \ub85c\uadf8 \ub2e4\uc6b4\ub85c\ub4dc \uc900\ube44\uc911\uc785\ub2c8\ub2e4."
REPORT_WAITING_FOR_LOGIN_MESSAGE = HISTORY_WAITING_FOR_LOGIN_MESSAGE
REPORT_WAITING_FOR_PRIOR_SHEET_MESSAGE = (
    "\uc55e\uc120 \uc2dc\ud2b8 \ucc98\ub9ac \ub300\uae30\uc911\uc785\ub2c8\ub2e4."
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _build_report_row_id(
    *,
    brand_code: str,
    activity_name: str,
    sheet_name: str,
) -> str:
    return f"{brand_code}::{activity_name}::{sheet_name}"


def _build_history_row_id(
    *,
    brand_code: str,
    activity_name: str,
) -> str:
    return f"{brand_code}::{activity_name}::history"


def _activity_key(brand: str, activity: str) -> str:
    return f"{brand}::{activity}"


def _load_report_runner() -> Any:
    from dashboard.services.meta_adapter import run_meta_export_with_plan

    return run_meta_export_with_plan


def _load_history_runner() -> Any:
    from meta_history_log import run_meta_history_with_plan

    return run_meta_history_with_plan


def _phase_progress_bridge(
    callback: ProgressCallback,
    *,
    allow_run_started: bool,
    phase_label: str,
) -> ProgressCallback:
    def _inner(event: dict[str, Any]) -> None:
        payload = dict(event)
        event_type = _safe_text(payload.get("type"))
        if event_type == "run_completed":
            return
        if event_type == "run_started" and not allow_run_started:
            return
        if event_type == "login_status":
            message = _safe_text(payload.get("message"))
            if message:
                payload["message"] = f"{phase_label}: {message}"
        callback(payload)

    return _inner


class ExecutionStateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._rows: dict[str, LogRow] = {}
        self._row_order: list[str] = []
        self._messages: list[dict[str, str]] = []
        self._activity_results: dict[str, dict[str, Any]] = {}
        self._history_rows: dict[str, HistoryLogRow] = {}
        self._history_row_order: list[str] = []
        self._history_outputs: dict[str, dict[str, Any]] = {}
        self._thread: threading.Thread | None = None
        self._running = False
        self.run_status = "Idle"
        self.login_status = "Not Started"
        self.run_id = ""
        self.log_file = ""
        self.last_error = ""

    def push_event(self, event: dict[str, Any]) -> None:
        self._events.put(dict(event))

    def is_running(self) -> bool:
        with self._lock:
            alive = bool(self._thread and self._thread.is_alive())
            return bool(self._running and alive)

    def initialize_rows(
        self,
        *,
        report_plan: list[ActivityExecutionPlan],
        history_plan: list[HistoryExecutionPlan],
        enable_report_download: bool,
        enable_action_log_download: bool,
    ) -> None:
        with self._lock:
            self._rows = {}
            self._row_order = []
            self._messages = []
            self._activity_results = {}
            self._history_rows = {}
            self._history_row_order = []
            self._history_outputs = {}
            self.last_error = ""
            self.run_status = "Idle"
            self.login_status = "Not Started"
            self.run_id = ""
            self.log_file = ""

            if enable_report_download:
                for activity in report_plan:
                    has_report_rows = False
                    for sheet in activity.sheets:
                        if len(sheet.urls) <= 0:
                            continue
                        row_id = _build_report_row_id(
                            brand_code=activity.brand_code,
                            activity_name=activity.activity_name,
                            sheet_name=sheet.sheet_display_name,
                        )
                        self._rows[row_id] = LogRow(
                            row_id=row_id,
                            brand=activity.brand_name,
                            activity=activity.activity_name,
                            sheet=sheet.sheet_display_name,
                            url_count=len(sheet.urls),
                            status="Pending",
                            message="Ready",
                            last_updated=_now_text(),
                            missing_columns_text="",
                        )
                        self._row_order.append(row_id)
                        has_report_rows = True
                    if has_report_rows:
                        self._upsert_activity_result(
                            brand=activity.brand_name,
                            activity=activity.activity_name,
                            status="Waiting",
                            workbook_path="",
                            rows_by_sheet={},
                            failed_sheets=[],
                            message="캠페인 데이터 다운로드 후 통합본을 생성합니다.",
                        )

            if enable_action_log_download:
                history_waiting = enable_report_download and bool(report_plan)
                for activity in history_plan:
                    row_id = _build_history_row_id(
                        brand_code=activity.brand_code,
                        activity_name=activity.activity_name,
                    )
                    account_count = len(activity.account_targets)
                    if account_count <= 0:
                        status = "Skipped"
                        message = "Report URL에서 실행 대상 계정을 찾지 못했습니다."
                    elif history_waiting:
                        status = "Waiting"
                        message = HISTORY_WAITING_FOR_REPORT_MESSAGE
                    else:
                        status = "Pending"
                        message = "Ready"
                    self._history_rows[row_id] = HistoryLogRow(
                        row_id=row_id,
                        brand=activity.brand_name,
                        activity=activity.activity_name,
                        account_count=account_count,
                        status=status,
                        message=message,
                        last_updated=_now_text(),
                    )
                    self._history_row_order.append(row_id)

    def start_thread(self, thread: threading.Thread) -> None:
        with self._lock:
            self._thread = thread
            self._running = True
            self.run_status = "Running"
            self.login_status = "Waiting Login"
            report_login_wait_assigned = False
            for row_id in list(self._row_order):
                row = self._rows.get(row_id)
                if not row or row.status not in {"Pending", "Waiting"}:
                    continue
                if not report_login_wait_assigned:
                    self._update_row(
                        row_id=row_id,
                        status="Waiting",
                        message=REPORT_WAITING_FOR_LOGIN_MESSAGE,
                    )
                    report_login_wait_assigned = True
                else:
                    self._update_row(
                        row_id=row_id,
                        status="Waiting",
                        message=REPORT_WAITING_FOR_PRIOR_SHEET_MESSAGE,
                    )
            if not self._rows:
                for row_id, row in list(self._history_rows.items()):
                    if row.status == "Pending":
                        self._update_history_row(
                            row_id=row_id,
                            message=HISTORY_WAITING_FOR_LOGIN_MESSAGE,
                        )

    def mark_finished(self) -> None:
        with self._lock:
            self._running = False
            self._thread = None

    def _append_message(self, *, level: str, text: str) -> None:
        message = _safe_text(text)
        if not message:
            return
        self._messages.append(
            {
                "level": level,
                "text": message,
                "time": _now_text(),
            }
        )
        self._messages = self._messages[-14:]

    def _update_row(
        self,
        *,
        row_id: str,
        status: str | None = None,
        message: str | None = None,
        missing_columns_text: str | None = None,
    ) -> None:
        existing = self._rows.get(row_id)
        if not existing:
            return
        self._rows[row_id] = LogRow(
            row_id=existing.row_id,
            brand=existing.brand,
            activity=existing.activity,
            sheet=existing.sheet,
            url_count=existing.url_count,
            status=_safe_text(status) or existing.status,
            message=existing.message if message is None else _safe_text(message),
            last_updated=_now_text(),
            missing_columns_text=(
                existing.missing_columns_text
                if missing_columns_text is None
                else _safe_text(missing_columns_text)
            ),
        )

    def _update_history_row(
        self,
        *,
        row_id: str,
        status: str | None = None,
        message: str | None = None,
    ) -> None:
        existing = self._history_rows.get(row_id)
        if not existing:
            return
        self._history_rows[row_id] = HistoryLogRow(
            row_id=existing.row_id,
            brand=existing.brand,
            activity=existing.activity,
            account_count=existing.account_count,
            status=_safe_text(status) or existing.status,
            message=existing.message if message is None else _safe_text(message),
            last_updated=_now_text(),
        )

    def _upsert_activity_result(
        self,
        *,
        brand: str,
        activity: str,
        status: str = "",
        workbook_path: str = "",
        rows_by_sheet: dict[str, int] | None = None,
        failed_sheets: list[str] | None = None,
        message: str = "",
    ) -> None:
        key = _activity_key(brand, activity)
        existing = self._activity_results.get(
            key,
            {
                "brand": brand,
                "activity": activity,
                "status": "Waiting",
                "workbook_path": "",
                "rows_by_sheet": {},
                "failed_sheets": [],
                "message": "",
                "updated_at": "",
            },
        )
        if _safe_text(status):
            existing["status"] = _safe_text(status)
        if workbook_path:
            existing["workbook_path"] = workbook_path
        if isinstance(rows_by_sheet, dict):
            merged = dict(existing.get("rows_by_sheet") or {})
            for sheet_name, row_count in rows_by_sheet.items():
                merged[str(sheet_name)] = int(row_count or 0)
            existing["rows_by_sheet"] = merged
        if isinstance(failed_sheets, list):
            existing["failed_sheets"] = [str(item) for item in failed_sheets if _safe_text(item)]
        if message is not None:
            existing["message"] = _safe_text(message)
        existing["updated_at"] = _now_text()
        self._activity_results[key] = existing

    def _upsert_history_output(
        self,
        *,
        brand: str,
        activity: str,
        file_path: str = "",
        row_count: int = 0,
        failed_accounts: list[str] | None = None,
        message: str = "",
    ) -> None:
        key = _activity_key(brand, activity)
        existing = self._history_outputs.get(
            key,
            {
                "brand": brand,
                "activity": activity,
                "file_path": "",
                "row_count": 0,
                "failed_accounts": [],
                "message": "",
                "updated_at": "",
            },
        )
        if file_path:
            existing["file_path"] = file_path
        existing["row_count"] = int(row_count or 0)
        if isinstance(failed_accounts, list):
            existing["failed_accounts"] = [str(item) for item in failed_accounts if _safe_text(item)]
        if message is not None:
            existing["message"] = _safe_text(message)
        existing["updated_at"] = _now_text()
        self._history_outputs[key] = existing

    def mark_pending_report_rows(self, *, status: str, message: str) -> None:
        with self._lock:
            for row_id, row in list(self._rows.items()):
                if row.status in {"Pending", "Waiting", "Running"}:
                    self._update_row(row_id=row_id, status=status, message=message)

    def mark_pending_history_rows(self, *, status: str, message: str) -> None:
        with self._lock:
            for row_id, row in list(self._history_rows.items()):
                if row.status in {"Pending", "Waiting", "Running"}:
                    self._update_history_row(row_id=row_id, status=status, message=message)

    def mark_pending_activity_results(self, *, status: str, message: str) -> None:
        with self._lock:
            for key, item in list(self._activity_results.items()):
                current_status = _safe_text(item.get("status"))
                if current_status in {"", "Pending", "Waiting", "Running"}:
                    updated = dict(item)
                    updated["status"] = _safe_text(status) or current_status or "Failed"
                    updated["message"] = _safe_text(message)
                    updated["updated_at"] = _now_text()
                    self._activity_results[key] = updated

    def drain_events(self) -> None:
        changed = False
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break

            changed = True
            event_type = _safe_text(event.get("type"))
            with self._lock:
                if event_type == "run_started":
                    run_id = _safe_text(event.get("run_id"))
                    log_file = _safe_text(event.get("log_file"))
                    if run_id:
                        self.run_id = run_id
                    if log_file:
                        self.log_file = log_file
                elif event_type == "login_status":
                    status = _safe_text(event.get("status"))
                    if status:
                        self.login_status = status
                    self._append_message(level="info", text=_safe_text(event.get("message")))
                elif event_type == "run_message":
                    self._append_message(
                        level=_safe_text(event.get("level")) or "info",
                        text=_safe_text(event.get("message")),
                    )
                elif event_type == "row_update":
                    self._update_row(
                        row_id=_safe_text(event.get("row_id")),
                        status=_safe_text(event.get("status")) or None,
                        message=event.get("message") if "message" in event else None,
                        missing_columns_text=(
                            event.get("missing_columns_text")
                            if "missing_columns_text" in event
                            else None
                        ),
                    )
                elif event_type == "history_row_update":
                    self._update_history_row(
                        row_id=_safe_text(event.get("row_id")),
                        status=_safe_text(event.get("status")) or None,
                        message=event.get("message") if "message" in event else None,
                    )
                elif event_type == "activity_result":
                    self._upsert_activity_result(
                        brand=_safe_text(event.get("brand")),
                        activity=_safe_text(event.get("activity")),
                        status=_safe_text(event.get("status")),
                        workbook_path=_safe_text(event.get("workbook_path")),
                        rows_by_sheet=(
                            event.get("rows_by_sheet")
                            if isinstance(event.get("rows_by_sheet"), dict)
                            else {}
                        ),
                        failed_sheets=(
                            event.get("failed_sheets")
                            if isinstance(event.get("failed_sheets"), list)
                            else []
                        ),
                        message=_safe_text(event.get("message")),
                    )
                elif event_type == "activity_output":
                    self._upsert_activity_result(
                        brand=_safe_text(event.get("brand")),
                        activity=_safe_text(event.get("activity")),
                        workbook_path=_safe_text(event.get("workbook_path")),
                    )
                elif event_type == "activity_summary":
                    rows_by_sheet: dict[str, int] = {}
                    rows_raw = event.get("rows_by_sheet")
                    if isinstance(rows_raw, dict):
                        for sheet_name, row_count in rows_raw.items():
                            rows_by_sheet[str(sheet_name)] = int(row_count or 0)
                    self._upsert_activity_result(
                        brand=_safe_text(event.get("brand")),
                        activity=_safe_text(event.get("activity")),
                        workbook_path=_safe_text(event.get("workbook_path")),
                        rows_by_sheet=rows_by_sheet,
                        message=_safe_text(event.get("message")),
                    )
                elif event_type == "history_result":
                    self._upsert_history_output(
                        brand=_safe_text(event.get("brand")),
                        activity=_safe_text(event.get("activity")),
                        file_path=_safe_text(event.get("file_path")),
                        row_count=int(event.get("row_count") or 0),
                        failed_accounts=(
                            event.get("failed_accounts")
                            if isinstance(event.get("failed_accounts"), list)
                            else []
                        ),
                        message=_safe_text(event.get("message")),
                    )
                elif event_type == "run_warning":
                    self._append_message(level="warning", text=_safe_text(event.get("message")))
                elif event_type == "run_completed":
                    self.run_status = _safe_text(event.get("run_status")) or "Completed"
                    self.login_status = "Done"
                    self._append_message(level="success", text=_safe_text(event.get("message")))
                    self._running = False
                elif event_type == "run_failed":
                    self.run_status = "Failed"
                    self.login_status = "Error"
                    self.last_error = _safe_text(event.get("error")) or "Unknown error"
                    self._append_message(level="error", text=self.last_error)
                    self._running = False

        if changed:
            with self._lock:
                has_report_failures = any(row.status == "Failed" for row in self._rows.values())
                has_history_failures = any(row.status == "Failed" for row in self._history_rows.values())
                has_activity_failures = any(
                    _safe_text(item.get("status")) == "Failed"
                    for item in self._activity_results.values()
                )
                has_any_output = bool(
                    any(_safe_text(item.get("workbook_path")) for item in self._activity_results.values())
                    or self._history_outputs
                )
                if (
                    not self._running
                    and self.run_status == "Completed"
                    and (has_report_failures or has_history_failures or has_activity_failures)
                ):
                    self.run_status = "Completed (With Failures)"
                elif not self._running and self.run_status == "Failed" and has_any_output:
                    self.run_status = "Completed (With Failures)"
                    if self.login_status == "Error":
                        self.login_status = "Done"

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rows = [self._rows[row_id] for row_id in self._row_order if row_id in self._rows]
            history_rows = [
                self._history_rows[row_id]
                for row_id in self._history_row_order
                if row_id in self._history_rows
            ]
            activity_results = list(self._activity_results.values())
            outputs = [
                {
                    "brand": item.get("brand", ""),
                    "activity": item.get("activity", ""),
                    "workbook_path": item.get("workbook_path", ""),
                }
                for item in activity_results
                if _safe_text(item.get("workbook_path"))
            ]
            history_outputs = list(self._history_outputs.values())
            return {
                "run_status": self.run_status,
                "login_status": self.login_status,
                "run_id": self.run_id,
                "log_file": self.log_file,
                "last_error": self.last_error,
                "rows": rows,
                "messages": list(self._messages),
                "outputs": outputs,
                "activity_results": activity_results,
                "history_rows": history_rows,
                "history_outputs": history_outputs,
                "is_running": self._running,
            }


def _build_completion_message(
    *,
    enable_report_download: bool,
    enable_action_log_download: bool,
    report_output_count: int,
    history_output_count: int,
) -> str:
    if enable_report_download and enable_action_log_download:
        return (
            f"캠페인 데이터 {report_output_count}건과 "
            f"액션 로그 {history_output_count}건 처리를 완료했습니다."
        )
    if enable_report_download:
        return f"캠페인 데이터 {report_output_count}건 처리를 완료했습니다."
    return f"액션 로그 {history_output_count}건 처리를 완료했습니다."


def _build_partial_failure_message(
    *,
    report_output_count: int,
    history_output_count: int,
    report_phase_completed: bool,
    history_phase_started: bool,
) -> str:
    if report_phase_completed and history_phase_started:
        return (
            f"캠페인 데이터 {report_output_count}건은 저장되었지만 "
            "액션 로그 다운로드 중 오류가 발생했습니다."
        )
    if report_output_count > 0 and history_output_count > 0:
        return (
            f"캠페인 데이터 {report_output_count}건과 액션 로그 {history_output_count}건은 "
            "저장되었지만 일부 단계에서 오류가 발생했습니다."
        )
    if report_output_count > 0:
        return f"캠페인 데이터 {report_output_count}건은 저장되었지만 일부 단계에서 오류가 발생했습니다."
    if history_output_count > 0:
        return f"액션 로그 {history_output_count}건은 저장되었지만 일부 단계에서 오류가 발생했습니다."
    return "일부 단계에서 오류가 발생했습니다."


def _worker(
    *,
    store: ExecutionStateStore,
    report_plan: list[ActivityExecutionPlan],
    history_plan: list[HistoryExecutionPlan],
    enable_report_download: bool,
    enable_action_log_download: bool,
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: Path,
    raw_dir: Path,
    trace_dir: Path,
    action_log_dir: Path,
    user_data_dir: Path,
) -> None:
    report_result = None
    history_result = None
    report_phase_completed = False
    history_phase_started = False
    try:
        prepared_user_data_dir = prepare_meta_user_data_dir(requested_dir=user_data_dir)
        active_user_data_dir = prepared_user_data_dir.effective_dir
        if prepared_user_data_dir.migration_mode == "legacy_fallback":
            store.push_event(
                {
                    "type": "run_warning",
                    "message": (
                        "Meta login profile migration failed; "
                        "reusing the legacy action-log profile for this run."
                    ),
                }
            )
        elif prepared_user_data_dir.migration_mode in {"move", "copy"}:
            store.push_event(
                {
                    "type": "run_message",
                    "level": "info",
                    "message": "Reused the existing Meta login profile.",
                }
            )

        if enable_report_download:
            run_meta_export_with_plan = _load_report_runner()
            report_result = run_meta_export_with_plan(
                plan=report_plan,
                view_event_source=view_event_source,
                export_event_source=export_event_source,
                browser=browser,
                output_dir=output_dir,
                raw_dir=raw_dir,
                trace_dir=trace_dir,
                user_data_dir=active_user_data_dir,
                progress_cb=_phase_progress_bridge(
                    store.push_event,
                    allow_run_started=True,
                    phase_label="캠페인 데이터 다운로드",
                ),
            )
            report_phase_completed = True
            if enable_action_log_download:
                store.push_event(
                    {
                        "type": "run_message",
                        "level": "info",
                        "message": HISTORY_PREPARING_RUN_MESSAGE,
                    }
                )

        if enable_action_log_download:
            store.mark_pending_history_rows(
                status="Waiting",
                message=HISTORY_WAITING_FOR_LOGIN_MESSAGE,
            )
            run_meta_history_with_plan = _load_history_runner()
            history_phase_started = True
            history_result = run_meta_history_with_plan(
                plan=history_plan,
                browser=browser,
                action_log_dir=action_log_dir,
                trace_dir=trace_dir,
                user_data_dir=active_user_data_dir,
                progress_cb=_phase_progress_bridge(
                    store.push_event,
                    allow_run_started=not enable_report_download,
                    phase_label="액션 로그 다운로드",
                ),
                emit_run_started=not enable_report_download,
            )

        store.push_event(
            {
                "type": "run_completed",
                "message": _build_completion_message(
                    enable_report_download=enable_report_download,
                    enable_action_log_download=enable_action_log_download,
                    report_output_count=len(getattr(report_result, "outputs", []) or []),
                    history_output_count=len(getattr(history_result, "outputs", []) or []),
                ),
            }
        )
    except Exception as exc:  # noqa: BLE001
        error_text = f"실행 중 오류가 발생했습니다: {exc}"
        report_output_count = len(getattr(report_result, "outputs", []) or [])
        history_output_count = len(getattr(history_result, "outputs", []) or [])
        has_any_output = report_output_count > 0 or history_output_count > 0

        if enable_report_download and not report_phase_completed:
            store.mark_pending_report_rows(status="Failed", message=error_text)
            store.mark_pending_activity_results(status="Failed", message=error_text)
        if enable_action_log_download:
            if enable_report_download and not report_phase_completed:
                store.mark_pending_history_rows(
                    status="Skipped",
                    message="캠페인 데이터 다운로드 실패로 액션 로그 다운로드를 시작하지 못했습니다.",
                )
            elif not history_phase_started:
                store.mark_pending_history_rows(
                    status="Failed",
                    message=f"액션 로그 다운로드를 시작하지 못했습니다: {exc}",
                )
            else:
                store.mark_pending_history_rows(
                    status="Failed",
                    message=f"액션 로그 다운로드 중 오류가 발생했습니다: {exc}",
                )

        if has_any_output:
            store.push_event({"type": "run_warning", "message": error_text})
            store.push_event(
                {
                    "type": "run_completed",
                    "run_status": "Completed (With Failures)",
                    "message": _build_partial_failure_message(
                        report_output_count=report_output_count,
                        history_output_count=history_output_count,
                        report_phase_completed=report_phase_completed,
                        history_phase_started=history_phase_started,
                    ),
                }
            )
        else:
            store.push_event({"type": "run_failed", "error": error_text})
    finally:
        store.mark_finished()


def start_execution(
    *,
    store: ExecutionStateStore,
    report_plan: list[ActivityExecutionPlan],
    history_plan: list[HistoryExecutionPlan],
    enable_report_download: bool,
    enable_action_log_download: bool,
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: str | Path,
    raw_dir: str | Path,
    trace_dir: str | Path,
    action_log_dir: str | Path,
    user_data_dir: str | Path,
) -> tuple[bool, str]:
    if store.is_running():
        return False, "이미 실행 중입니다."
    if not enable_report_download and not enable_action_log_download:
        return False, "최소 한 개의 실행 옵션을 선택해주세요."
    if enable_report_download and not report_plan:
        return False, "캠페인 데이터 다운로드 대상이 없습니다."
    if enable_action_log_download and not history_plan:
        return False, "액션 로그 다운로드 대상이 없습니다."

    store.initialize_rows(
        report_plan=report_plan,
        history_plan=history_plan,
        enable_report_download=enable_report_download,
        enable_action_log_download=enable_action_log_download,
    )

    thread = threading.Thread(
        target=_worker,
        kwargs={
            "store": store,
            "report_plan": report_plan,
            "history_plan": history_plan,
            "enable_report_download": enable_report_download,
            "enable_action_log_download": enable_action_log_download,
            "view_event_source": _safe_text(view_event_source),
            "export_event_source": _safe_text(export_event_source),
            "browser": browser,
            "output_dir": Path(output_dir),
            "raw_dir": Path(raw_dir),
            "trace_dir": Path(trace_dir),
            "action_log_dir": Path(action_log_dir),
            "user_data_dir": Path(user_data_dir),
        },
        daemon=True,
    )
    store.start_thread(thread)
    thread.start()
    return True, "실행을 시작했습니다."


def create_execution_store() -> ExecutionStateStore:
    return ExecutionStateStore()
