"""Background execution worker and thread-safe progress store."""

from __future__ import annotations

import datetime as dt
import queue
import threading
from pathlib import Path
from typing import Any

from dashboard.models import ActivityExecutionPlan, LogRow
from dashboard.services.meta_adapter import run_meta_export_with_plan


def _now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _build_row_id(
    *,
    brand_code: str,
    activity_name: str,
    sheet_name: str,
) -> str:
    return f"{brand_code}::{activity_name}::{sheet_name}"


def _activity_key(brand: str, activity: str) -> str:
    return f"{brand}::{activity}"


class ExecutionStateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: queue.Queue[dict[str, Any]] = queue.Queue()
        self._rows: dict[str, LogRow] = {}
        self._row_order: list[str] = []
        self._messages: list[dict[str, str]] = []
        self._activity_results: dict[str, dict[str, Any]] = {}
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

    def initialize_rows(self, plan: list[ActivityExecutionPlan]) -> None:
        with self._lock:
            self._rows = {}
            self._row_order = []
            self._messages = []
            self._activity_results = {}
            self.last_error = ""
            for activity in plan:
                for sheet in activity.sheets:
                    row_id = _build_row_id(
                        brand_code=activity.brand_code,
                        activity_name=activity.activity_name,
                        sheet_name=sheet.sheet_display_name,
                    )
                    has_urls = len(sheet.urls) > 0
                    self._rows[row_id] = LogRow(
                        row_id=row_id,
                        brand=activity.brand_name,
                        activity=activity.activity_name,
                        sheet=sheet.sheet_display_name,
                        url_count=len(sheet.urls),
                        status="Pending" if has_urls else "Skipped",
                        message="Ready" if has_urls else "No URL registered",
                        last_updated=_now_text(),
                    )
                    self._row_order.append(row_id)

    def start_thread(self, thread: threading.Thread) -> None:
        with self._lock:
            self._thread = thread
            self._running = True
            self.run_status = "Running"
            self.login_status = "Waiting Login"

    def mark_finished(self) -> None:
        with self._lock:
            self._running = False
            self._thread = None

    def _update_row(self, *, row_id: str, status: str, message: str) -> None:
        existing = self._rows.get(row_id)
        if not existing:
            return
        self._rows[row_id] = LogRow(
            row_id=existing.row_id,
            brand=existing.brand,
            activity=existing.activity,
            sheet=existing.sheet,
            url_count=existing.url_count,
            status=status,
            message=message,
            last_updated=_now_text(),
        )

    def _upsert_activity_result(
        self,
        *,
        brand: str,
        activity: str,
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
                "workbook_path": "",
                "rows_by_sheet": {},
                "failed_sheets": [],
                "message": "",
            },
        )
        if workbook_path:
            existing["workbook_path"] = workbook_path
        if isinstance(rows_by_sheet, dict):
            merged = dict(existing.get("rows_by_sheet") or {})
            for sheet_name, row_count in rows_by_sheet.items():
                merged[str(sheet_name)] = int(row_count or 0)
            existing["rows_by_sheet"] = merged
        if isinstance(failed_sheets, list):
            existing["failed_sheets"] = [str(item) for item in failed_sheets if str(item).strip()]
        if str(message or "").strip():
            existing["message"] = str(message)
        self._activity_results[key] = existing

    def drain_events(self) -> None:
        changed = False
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break
            changed = True
            event_type = str(event.get("type") or "").strip()
            with self._lock:
                if event_type == "run_started":
                    self.run_id = str(event.get("run_id") or "")
                    self.log_file = str(event.get("log_file") or "")
                elif event_type == "login_status":
                    self.login_status = str(event.get("status") or self.login_status)
                    message = str(event.get("message") or "").strip()
                    if message:
                        self._messages.append(
                            {
                                "level": "info",
                                "text": message,
                                "time": _now_text(),
                            }
                        )
                elif event_type == "row_update":
                    self._update_row(
                        row_id=str(event.get("row_id") or ""),
                        status=str(event.get("status") or "Running"),
                        message=str(event.get("message") or ""),
                    )
                elif event_type == "activity_result":
                    self._upsert_activity_result(
                        brand=str(event.get("brand") or ""),
                        activity=str(event.get("activity") or ""),
                        workbook_path=str(event.get("workbook_path") or ""),
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
                        message=str(event.get("message") or ""),
                    )
                elif event_type == "activity_output":
                    self._upsert_activity_result(
                        brand=str(event.get("brand") or ""),
                        activity=str(event.get("activity") or ""),
                        workbook_path=str(event.get("workbook_path") or ""),
                    )
                elif event_type == "activity_summary":
                    rows_by_sheet: dict[str, int] = {}
                    rows_raw = event.get("rows_by_sheet")
                    if isinstance(rows_raw, dict):
                        for sheet_name, row_count in rows_raw.items():
                            rows_by_sheet[str(sheet_name)] = int(row_count or 0)
                    self._upsert_activity_result(
                        brand=str(event.get("brand") or ""),
                        activity=str(event.get("activity") or ""),
                        workbook_path=str(event.get("workbook_path") or ""),
                        rows_by_sheet=rows_by_sheet,
                        message=str(event.get("message") or ""),
                    )
                elif event_type == "run_warning":
                    message = str(event.get("message") or "").strip()
                    if message:
                        self._messages.append(
                            {
                                "level": "warning",
                                "text": message,
                                "time": _now_text(),
                            }
                        )
                elif event_type == "run_completed":
                    self.run_status = "Completed"
                    self.login_status = "Done"
                    completion_message = str(event.get("message") or "").strip()
                    if completion_message:
                        self._messages.append(
                            {
                                "level": "success",
                                "text": completion_message,
                                "time": _now_text(),
                            }
                        )
                    self._running = False
                elif event_type == "run_failed":
                    self.run_status = "Failed"
                    self.login_status = "Error"
                    self.last_error = str(event.get("error") or "Unknown error")
                    self._messages.append(
                        {
                            "level": "error",
                            "text": self.last_error,
                            "time": _now_text(),
                        }
                    )
                    self._running = False

        if changed:
            with self._lock:
                has_failures = any(row.status == "Failed" for row in self._rows.values())
                if not self._running and self.run_status == "Completed" and has_failures:
                    self.run_status = "Completed (With Failures)"

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rows = [self._rows[row_id] for row_id in self._row_order if row_id in self._rows]
            activity_results = list(self._activity_results.values())
            outputs = [
                {
                    "brand": item.get("brand", ""),
                    "activity": item.get("activity", ""),
                    "workbook_path": item.get("workbook_path", ""),
                }
                for item in activity_results
                if str(item.get("workbook_path") or "").strip()
            ]
            return {
                "run_status": self.run_status,
                "login_status": self.login_status,
                "run_id": self.run_id,
                "log_file": self.log_file,
                "last_error": self.last_error,
                "rows": rows,
                "messages": list(self._messages[-12:]),
                "outputs": outputs,
                "activity_results": activity_results,
                "is_running": self._running,
            }


def _worker(
    *,
    store: ExecutionStateStore,
    plan: list[ActivityExecutionPlan],
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: Path,
    downloads_dir: Path,
    logs_dir: Path,
) -> None:
    try:
        result = run_meta_export_with_plan(
            plan=plan,
            view_event_source=view_event_source,
            export_event_source=export_event_source,
            browser=browser,
            output_dir=output_dir,
            downloads_dir=downloads_dir,
            logs_dir=logs_dir,
            progress_cb=store.push_event,
        )
        store.push_event(
            {
                "type": "run_completed",
                "message": (
                    f"Export completed (run_id={result.run_id}, "
                    f"workbook={len(result.outputs)})"
                ),
            }
        )
    except Exception as exc:  # noqa: BLE001
        store.push_event(
            {
                "type": "run_failed",
                "error": f"Execution failed: {str(exc)}",
            }
        )
    finally:
        store.mark_finished()


def start_execution(
    *,
    store: ExecutionStateStore,
    plan: list[ActivityExecutionPlan],
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: str | Path,
    downloads_dir: str | Path,
    logs_dir: str | Path,
) -> tuple[bool, str]:
    if store.is_running():
        return False, "이미 실행 중입니다."
    if not plan:
        return False, "실행 대상이 없습니다."

    store.initialize_rows(plan)
    thread = threading.Thread(
        target=_worker,
        kwargs={
            "store": store,
            "plan": plan,
            "view_event_source": str(view_event_source or "").strip(),
            "export_event_source": str(export_event_source or "").strip(),
            "browser": browser,
            "output_dir": Path(output_dir),
            "downloads_dir": Path(downloads_dir),
            "logs_dir": Path(logs_dir),
        },
        daemon=True,
    )
    store.start_thread(thread)
    thread.start()
    return True, "실행을 시작했습니다."


def create_execution_store() -> ExecutionStateStore:
    return ExecutionStateStore()
