from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import threading

from dashboard.models import (
    ActivityExecutionPlan,
    HistoryAccountTarget,
    HistoryExecutionPlan,
    SheetExecutionPlan,
)
from dashboard.services import execution_service as execution_service_module
from dashboard.services.execution_service import ExecutionStateStore
from meta_core.pathing import PreparedMetaUserDataDir


def _sample_report_plan(
    *,
    include_empty_sheet: bool = False,
    include_second_sheet_with_url: bool = False,
) -> list[ActivityExecutionPlan]:
    sheets = [
        SheetExecutionPlan(
            sheet_display_name="Overall",
            sheet_key="overall",
            urls=["https://example.com/1"],
        )
    ]
    if include_second_sheet_with_url:
        sheets.append(
            SheetExecutionPlan(
                sheet_display_name="Placement",
                sheet_key="placement",
                urls=["https://example.com/2"],
            )
        )
    if include_empty_sheet:
        sheets.append(
            SheetExecutionPlan(
                sheet_display_name="Demo",
                sheet_key="demo",
                urls=[],
            )
        )
    return [
        ActivityExecutionPlan(
            brand_code="brand_a",
            brand_name="Brand A",
            activity_name="Activity 1",
            sheets=sheets,
        )
    ]


def _sample_history_plan(with_account_targets: bool = True) -> list[HistoryExecutionPlan]:
    return [
        HistoryExecutionPlan(
            brand_code="brand_a",
            brand_name="Brand A",
            activity_name="Activity 1",
            account_targets=(
                [HistoryAccountTarget(act="111", business_id="999")]
                if with_account_targets
                else []
            ),
        )
    ]


class ExecutionServiceTests(unittest.TestCase):
    def test_initialize_rows_sets_history_to_waiting_when_both_modes_enabled(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=_sample_report_plan(),
            history_plan=_sample_history_plan(),
            enable_report_download=True,
            enable_action_log_download=True,
        )

        snapshot = store.snapshot()
        self.assertEqual(len(snapshot["rows"]), 1)
        self.assertEqual(snapshot["rows"][0].status, "Pending")
        self.assertEqual(len(snapshot["activity_results"]), 1)
        self.assertEqual(snapshot["activity_results"][0]["status"], "Waiting")
        self.assertEqual(
            snapshot["activity_results"][0]["message"],
            "캠페인 데이터 다운로드 후 통합본을 생성합니다.",
        )
        self.assertEqual(len(snapshot["history_rows"]), 1)
        self.assertEqual(snapshot["history_rows"][0].status, "Waiting")
        self.assertEqual(
            snapshot["history_rows"][0].message,
            "캠페인 데이터 다운로드 진행중입니다.",
        )

    def test_initialize_rows_sets_history_to_pending_for_history_only_runs(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=[],
            history_plan=_sample_history_plan(),
            enable_report_download=False,
            enable_action_log_download=True,
        )

        snapshot = store.snapshot()
        self.assertEqual(snapshot["rows"], [])
        self.assertEqual(len(snapshot["history_rows"]), 1)
        self.assertEqual(snapshot["history_rows"][0].status, "Pending")
        self.assertEqual(snapshot["activity_results"], [])

    def test_start_thread_marks_history_only_rows_as_waiting_for_login(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=[],
            history_plan=_sample_history_plan(),
            enable_report_download=False,
            enable_action_log_download=True,
        )
        store.start_thread(threading.Thread(target=lambda: None))

        snapshot = store.snapshot()
        self.assertEqual(snapshot["history_rows"][0].message, "로그인 대기중입니다.")

    def test_initialize_rows_skips_report_rows_without_urls(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=_sample_report_plan(include_empty_sheet=True),
            history_plan=[],
            enable_report_download=True,
            enable_action_log_download=False,
        )

        snapshot = store.snapshot()
        self.assertEqual(len(snapshot["rows"]), 1)
        self.assertEqual(snapshot["rows"][0].sheet, "Overall")

    def test_start_thread_marks_report_rows_as_waiting_for_login(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=_sample_report_plan(),
            history_plan=[],
            enable_report_download=True,
            enable_action_log_download=False,
        )
        store.start_thread(threading.Thread(target=lambda: None))

        snapshot = store.snapshot()
        self.assertEqual(snapshot["rows"][0].message, "로그인 대기중입니다.")

    def test_start_thread_distinguishes_report_login_wait_and_prior_sheet_wait(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=_sample_report_plan(include_second_sheet_with_url=True),
            history_plan=[],
            enable_report_download=True,
            enable_action_log_download=False,
        )
        store.start_thread(threading.Thread(target=lambda: None))

        snapshot = store.snapshot()
        self.assertEqual(len(snapshot["rows"]), 2)
        self.assertEqual(snapshot["rows"][0].status, "Waiting")
        self.assertEqual(snapshot["rows"][0].message, "로그인 대기중입니다.")
        self.assertEqual(snapshot["rows"][1].status, "Waiting")
        self.assertEqual(snapshot["rows"][1].message, "앞선 시트 처리 대기중입니다.")

    def test_failed_run_with_output_becomes_completed_with_failures(self) -> None:
        store = ExecutionStateStore()

        store.initialize_rows(
            report_plan=_sample_report_plan(),
            history_plan=[],
            enable_report_download=True,
            enable_action_log_download=False,
        )
        store.push_event(
            {
                "type": "activity_result",
                "brand": "Brand A",
                "activity": "Activity 1",
                "status": "Completed",
                "workbook_path": r"C:\temp\Brand_A.xlsx",
                "rows_by_sheet": {"Overall": 10},
                "failed_sheets": [],
                "message": "통합본 생성완료:Brand_A.xlsx",
            }
        )
        store.push_event({"type": "run_failed", "error": "실행 중 오류가 발생했습니다: boom"})

        store.drain_events()
        snapshot = store.snapshot()

        self.assertEqual(snapshot["run_status"], "Completed (With Failures)")
        self.assertEqual(snapshot["activity_results"][0]["workbook_path"], r"C:\temp\Brand_A.xlsx")

    def test_report_success_then_history_failure_keeps_report_completed(self) -> None:
        store = ExecutionStateStore()
        report_plan = _sample_report_plan()
        history_plan = _sample_history_plan()
        store.initialize_rows(
            report_plan=report_plan,
            history_plan=history_plan,
            enable_report_download=True,
            enable_action_log_download=True,
        )

        def _fake_report_runner(**kwargs):
            progress_cb = kwargs["progress_cb"]
            progress_cb({"type": "run_started", "run_id": "run-1", "log_file": "trace.log"})
            progress_cb(
                {
                    "type": "row_update",
                    "row_id": "brand_a::Activity 1::Overall",
                    "status": "Completed",
                    "message": "1/1 URL 완료",
                }
            )
            progress_cb(
                {
                    "type": "activity_result",
                    "brand": "Brand A",
                    "activity": "Activity 1",
                    "status": "Completed",
                    "workbook_path": r"C:\temp\report.xlsx",
                    "rows_by_sheet": {"Overall": 10},
                    "failed_sheets": [],
                    "message": "통합본 생성완료:report.xlsx",
                }
            )
            return SimpleNamespace(outputs=[SimpleNamespace(workbook_path=r"C:\temp\report.xlsx")])

        def _fake_history_runner(**kwargs):
            raise RuntimeError("history boom")

        with (
            patch.object(
                execution_service_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=Path("requested-profile"),
                    effective_dir=Path("requested-profile"),
                    legacy_dir=Path("legacy-profile"),
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(execution_service_module, "_load_report_runner", return_value=_fake_report_runner),
            patch.object(execution_service_module, "_load_history_runner", return_value=_fake_history_runner),
        ):
            execution_service_module._worker(
                store=store,
                report_plan=report_plan,
                history_plan=history_plan,
                enable_report_download=True,
                enable_action_log_download=True,
                view_event_source="1",
                export_event_source="1",
                browser="msedge",
                output_dir=Path("."),
                raw_dir=Path("."),
                trace_dir=Path("."),
                action_log_dir=Path("."),
                user_data_dir=Path("requested-profile"),
            )

        store.drain_events()
        snapshot = store.snapshot()

        self.assertEqual(snapshot["run_status"], "Completed (With Failures)")
        self.assertEqual(snapshot["rows"][0].status, "Completed")
        self.assertEqual(snapshot["activity_results"][0]["status"], "Completed")
        self.assertEqual(snapshot["history_rows"][0].status, "Failed")
        self.assertIn("액션 로그 다운로드 중 오류가 발생했습니다", snapshot["history_rows"][0].message)


    def test_worker_passes_prepared_user_data_dir_to_both_runners(self) -> None:
        store = ExecutionStateStore()
        report_plan = _sample_report_plan()
        history_plan = _sample_history_plan()
        store.initialize_rows(
            report_plan=report_plan,
            history_plan=history_plan,
            enable_report_download=True,
            enable_action_log_download=True,
        )

        resolved_profile = Path("resolved-profile")
        report_calls: list[Path] = []
        history_calls: list[Path] = []

        def _fake_report_runner(**kwargs):
            report_calls.append(kwargs["user_data_dir"])
            return SimpleNamespace(outputs=[])

        def _fake_history_runner(**kwargs):
            history_calls.append(kwargs["user_data_dir"])
            return SimpleNamespace(outputs=[])

        with (
            patch.object(
                execution_service_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=Path("requested-profile"),
                    effective_dir=resolved_profile,
                    legacy_dir=Path("legacy-profile"),
                    migration_mode="move",
                    warning="",
                ),
            ),
            patch.object(execution_service_module, "_load_report_runner", return_value=_fake_report_runner),
            patch.object(execution_service_module, "_load_history_runner", return_value=_fake_history_runner),
        ):
            execution_service_module._worker(
                store=store,
                report_plan=report_plan,
                history_plan=history_plan,
                enable_report_download=True,
                enable_action_log_download=True,
                view_event_source="1",
                export_event_source="1",
                browser="msedge",
                output_dir=Path("."),
                raw_dir=Path("."),
                trace_dir=Path("."),
                action_log_dir=Path("."),
                user_data_dir=Path("requested-profile"),
            )

        self.assertEqual(report_calls, [resolved_profile])
        self.assertEqual(history_calls, [resolved_profile])


if __name__ == "__main__":
    unittest.main()
