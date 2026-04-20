from __future__ import annotations

import sys
import unittest
from pathlib import Path
import shutil
from types import SimpleNamespace
import uuid
from unittest.mock import patch

from dashboard.models import HistoryAccountTarget, HistoryExecutionPlan
from meta_core.pathing import PreparedMetaUserDataDir
from meta_history_log import runtime as runtime_module
from meta_history_log.main import (
    NO_ACTION_LOG_ROWS_MESSAGE,
    NO_TARGET_ACCOUNTS_MESSAGE,
    RunnerOptions,
)


class _DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def info(self, msg: str, *args) -> None:
        self.records.append(("info", msg % args if args else msg))

    def warning(self, msg: str, *args) -> None:
        self.records.append(("warning", msg % args if args else msg))

    def error(self, msg: str, *args) -> None:
        self.records.append(("error", msg % args if args else msg))

    def exception(self, msg: str, *args) -> None:
        self.records.append(("exception", msg % args if args else msg))


class _FakePage:
    def __init__(self) -> None:
        self.url = "https://business.facebook.com/"
        self.goto_calls: list[tuple[str, str]] = []

    def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        self.goto_calls.append((url, wait_until))
        self.url = url


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self.pages = [page]
        self.closed = False

    def new_page(self) -> _FakePage:
        return self.pages[0]

    def close(self) -> None:
        self.closed = True


class _FakeSyncPlaywright:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class HistoryRuntimeTests(unittest.TestCase):
    def _temp_root(self) -> Path:
        temp_parent = (Path.cwd() / ".tmp_history_runtime_tests").resolve()
        temp_parent.mkdir(parents=True, exist_ok=True)
        root = (temp_parent / f"history_runtime_test_{uuid.uuid4().hex}").resolve()
        root.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        return root

    def _build_options(self, root: Path) -> RunnerOptions:
        return RunnerOptions(
            browser="msedge",
            headless=False,
            user_data_dir=(root / "user_data").resolve(),
            output_dir=(root / "output").resolve(),
            log_dir=(root / "trace").resolve(),
            screenshot_dir=(root / "trace" / "screenshots").resolve(),
            login_timeout_sec=300,
            action_timeout_ms=15_000,
            table_load_timeout_sec=20,
            lazy_scroll_pause_sec=1.0,
            lazy_scroll_max_rounds=5,
            lazy_scroll_no_new_rounds=2,
            step_retry_count=1,
        )

    def test_launch_context_with_fallback_uses_isolated_profile_after_primary_failure(self) -> None:
        root = self._temp_root()
        options = self._build_options(root)
        logger = _DummyLogger()
        expected_context = object()

        with patch.object(
            runtime_module,
            "_launch_context_for_profile",
            side_effect=[RuntimeError("profile locked"), expected_context],
        ) as launch_mock:
            result = runtime_module._launch_context_with_fallback(
                object(),
                options=options,
                logger=logger,
            )

        self.assertIs(result, expected_context)
        self.assertEqual(launch_mock.call_count, 2)
        self.assertEqual(launch_mock.call_args_list[0].kwargs["browser"], "msedge")
        self.assertEqual(
            launch_mock.call_args_list[0].kwargs["user_data_dir"],
            options.user_data_dir,
        )
        self.assertEqual(launch_mock.call_args_list[1].kwargs["browser"], "msedge")
        self.assertNotEqual(
            launch_mock.call_args_list[1].kwargs["user_data_dir"],
            options.user_data_dir,
        )
        self.assertIn(
            "_runtime_profiles",
            str(launch_mock.call_args_list[1].kwargs["user_data_dir"]),
        )

    def test_launch_context_with_fallback_reports_all_attempts(self) -> None:
        root = self._temp_root()
        options = self._build_options(root)
        logger = _DummyLogger()

        with patch.object(
            runtime_module,
            "_launch_context_for_profile",
            side_effect=[
                RuntimeError("first"),
                RuntimeError("second"),
                RuntimeError("third"),
            ],
        ):
            with self.assertRaises(RuntimeError) as raised:
                runtime_module._launch_context_with_fallback(
                    object(),
                    options=options,
                    logger=logger,
                )

        message = str(raised.exception)
        self.assertIn("primary/msedge: first", message)
        self.assertIn("isolated-profile/msedge: second", message)
        self.assertIn("isolated-profile/chromium: third", message)

    def test_format_exception_message_uses_repr_when_string_is_empty(self) -> None:
        class _BlankError(Exception):
            def __str__(self) -> str:
                return ""

        message = runtime_module._format_exception_message(_BlankError())

        self.assertIn("_BlankError", message)

    def test_resolve_startup_bootstrap_url_uses_first_account_target(self) -> None:
        url = runtime_module._resolve_startup_bootstrap_url(
            [
                HistoryExecutionPlan(
                    brand_code="brand_a",
                    brand_name="Brand A",
                    activity_name="ACT_A",
                    account_targets=[],
                ),
                HistoryExecutionPlan(
                    brand_code="brand_b",
                    brand_name="Brand B",
                    activity_name="ACT_B",
                    account_targets=[HistoryAccountTarget(act="111", business_id="222")],
                ),
            ]
        )

        self.assertIn("https://adsmanager.facebook.com/adsmanager/manage/campaigns?", url)
        self.assertIn("act=111", url)
        self.assertIn("business_id=222", url)

    def test_run_meta_history_with_plan_continues_after_failures_and_skips_zero_rows(self) -> None:
        root = self._temp_root()
        page = _FakePage()
        context = _FakeContext(page)
        events: list[dict[str, object]] = []
        saved_outputs: list[tuple[list[list[str]], Path]] = []
        logger = _DummyLogger()
        plan = [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_NO_TARGET",
                account_targets=[],
            ),
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_FAIL",
                account_targets=[HistoryAccountTarget(act="111", business_id="999")],
            ),
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_EMPTY",
                account_targets=[HistoryAccountTarget(act="222", business_id="999")],
            ),
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_OK",
                account_targets=[HistoryAccountTarget(act="333", business_id="999")],
            ),
        ]

        def _fake_collect_for_account(**kwargs):
            activity_prefix = kwargs["activity_prefix"]
            if activity_prefix == "ACT_FAIL":
                raise RuntimeError("boom")
            if activity_prefix == "ACT_EMPTY":
                return []
            if activity_prefix == "ACT_OK":
                return [["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]]
            raise AssertionError(f"Unexpected activity: {activity_prefix}")

        def _fake_save_activity_xlsx(*, rows, output_path):
            saved_outputs.append((list(rows), output_path))

        with (
            patch.dict(
                sys.modules,
                {"playwright.sync_api": SimpleNamespace(sync_playwright=lambda: _FakeSyncPlaywright())},
            ),
            patch.object(
                runtime_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=root / "requested-profile",
                    effective_dir=root / "resolved-profile",
                    legacy_dir=root / "legacy-profile",
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(runtime_module, "_setup_logger", return_value=logger),
            patch.object(runtime_module, "_launch_context_with_fallback", return_value=context),
            patch.object(runtime_module, "_wait_for_login_context"),
            patch.object(runtime_module, "_collect_for_account", side_effect=_fake_collect_for_account),
            patch.object(runtime_module, "_save_activity_xlsx", side_effect=_fake_save_activity_xlsx),
        ):
            result = runtime_module.run_meta_history_with_plan(
                plan=plan,
                browser="msedge",
                action_log_dir=root / "output",
                trace_dir=root / "trace",
                user_data_dir=root / "user_data",
                progress_cb=events.append,
            )

        final_updates = {
            event["row_id"]: event
            for event in events
            if event.get("type") == "history_row_update"
        }
        history_results = [event for event in events if event.get("type") == "history_result"]

        self.assertEqual(
            final_updates["brand_a::ACT_NO_TARGET::history"]["message"],
            NO_TARGET_ACCOUNTS_MESSAGE,
        )
        self.assertEqual(final_updates["brand_a::ACT_NO_TARGET::history"]["status"], "Skipped")
        self.assertEqual(final_updates["brand_a::ACT_FAIL::history"]["status"], "Failed")
        self.assertIn(runtime_module.HISTORY_FAILED_PREFIX, final_updates["brand_a::ACT_FAIL::history"]["message"])
        self.assertEqual(final_updates["brand_a::ACT_EMPTY::history"]["status"], "Skipped")
        self.assertEqual(
            final_updates["brand_a::ACT_EMPTY::history"]["message"],
            NO_ACTION_LOG_ROWS_MESSAGE,
        )
        self.assertEqual(final_updates["brand_a::ACT_OK::history"]["status"], "Completed")
        self.assertEqual(len(history_results), 1)
        self.assertEqual(history_results[0]["activity"], "ACT_OK")
        self.assertEqual(len(saved_outputs), 1)
        self.assertEqual(len(result.outputs), 1)
        self.assertEqual(result.outputs[0].activity_name, "ACT_OK")
        self.assertTrue(context.closed)

    def test_run_meta_history_with_plan_saves_partial_output_when_some_accounts_fail(self) -> None:
        root = self._temp_root()
        page = _FakePage()
        context = _FakeContext(page)
        events: list[dict[str, object]] = []
        saved_outputs: list[tuple[list[list[str]], Path]] = []
        logger = _DummyLogger()
        plan = [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_PARTIAL",
                account_targets=[
                    HistoryAccountTarget(act="111", business_id="999"),
                    HistoryAccountTarget(act="222", business_id="999"),
                ],
            )
        ]

        def _fake_collect_for_account(**kwargs):
            account = kwargs["account"]
            if account.act == "111":
                return [["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]]
            raise RuntimeError("late boom")

        def _fake_save_activity_xlsx(*, rows, output_path):
            saved_outputs.append((list(rows), output_path))

        with (
            patch.dict(
                sys.modules,
                {"playwright.sync_api": SimpleNamespace(sync_playwright=lambda: _FakeSyncPlaywright())},
            ),
            patch.object(
                runtime_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=root / "requested-profile",
                    effective_dir=root / "resolved-profile",
                    legacy_dir=root / "legacy-profile",
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(runtime_module, "_setup_logger", return_value=logger),
            patch.object(runtime_module, "_launch_context_with_fallback", return_value=context),
            patch.object(runtime_module, "_wait_for_login_context"),
            patch.object(runtime_module, "_collect_for_account", side_effect=_fake_collect_for_account),
            patch.object(runtime_module, "_save_activity_xlsx", side_effect=_fake_save_activity_xlsx),
        ):
            result = runtime_module.run_meta_history_with_plan(
                plan=plan,
                browser="msedge",
                action_log_dir=root / "output",
                trace_dir=root / "trace",
                user_data_dir=root / "user_data",
                progress_cb=events.append,
            )

        final_updates = [
            event for event in events if event.get("type") == "history_row_update"
        ]
        history_results = [event for event in events if event.get("type") == "history_result"]

        self.assertEqual(final_updates[-1]["status"], "Failed")
        self.assertIn(runtime_module.HISTORY_PARTIAL_SAVED_PREFIX, final_updates[-1]["message"])
        self.assertEqual(len(history_results), 1)
        self.assertIn(runtime_module.HISTORY_PARTIAL_SAVED_PREFIX, history_results[0]["message"])
        self.assertEqual(len(saved_outputs), 1)
        self.assertEqual(len(result.outputs), 1)
        self.assertEqual(result.outputs[0].failed_accounts, ["222/999"])
        self.assertTrue(context.closed)

    def test_run_meta_history_with_plan_marks_queued_rows_as_waiting_for_prior_activity(self) -> None:
        root = self._temp_root()
        page = _FakePage()
        context = _FakeContext(page)
        events: list[dict[str, object]] = []
        logger = _DummyLogger()
        plan = [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_FIRST",
                account_targets=[HistoryAccountTarget(act="111", business_id="999")],
            ),
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_SECOND",
                account_targets=[HistoryAccountTarget(act="222", business_id="999")],
            ),
        ]

        with (
            patch.dict(
                sys.modules,
                {"playwright.sync_api": SimpleNamespace(sync_playwright=lambda: _FakeSyncPlaywright())},
            ),
            patch.object(
                runtime_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=root / "requested-profile",
                    effective_dir=root / "resolved-profile",
                    legacy_dir=root / "legacy-profile",
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(runtime_module, "_setup_logger", return_value=logger),
            patch.object(runtime_module, "_launch_context_with_fallback", return_value=context),
            patch.object(runtime_module, "_wait_for_login_context"),
            patch.object(
                runtime_module,
                "_collect_for_account",
                side_effect=[
                    [["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]],
                    [["Activity", "Details", "Item", "User", "2026-04-20 12:05:00"]],
                ],
            ),
            patch.object(runtime_module, "_save_activity_xlsx"),
        ):
            runtime_module.run_meta_history_with_plan(
                plan=plan,
                browser="msedge",
                action_log_dir=root / "output",
                trace_dir=root / "trace",
                user_data_dir=root / "user_data",
                progress_cb=events.append,
            )

        queued_updates = [
            event
            for event in events
            if event.get("type") == "history_row_update"
            and event.get("row_id") == "brand_a::ACT_SECOND::history"
            and event.get("status") == "Waiting"
        ]
        self.assertTrue(queued_updates)
        self.assertIn(
            runtime_module.HISTORY_WAITING_FOR_PRIOR_ACTIVITY_MESSAGE,
            [event["message"] for event in queued_updates],
        )

    def test_run_meta_history_with_plan_reveals_window_when_relogin_is_required(self) -> None:
        root = self._temp_root()
        page = _FakePage()
        context = _FakeContext(page)
        events: list[dict[str, object]] = []
        logger = _DummyLogger()
        plan = [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_LOGIN",
                account_targets=[HistoryAccountTarget(act="111", business_id="999")],
            )
        ]

        def _fake_wait_for_login_context(*args, **kwargs):
            status_callback = kwargs["status_callback"]
            status_callback(
                "login_page",
                "https://business.facebook.com/business/loginpage/?next=https://business.facebook.com/",
            )

        with (
            patch.dict(
                sys.modules,
                {"playwright.sync_api": SimpleNamespace(sync_playwright=lambda: _FakeSyncPlaywright())},
            ),
            patch.object(
                runtime_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=root / "requested-profile",
                    effective_dir=root / "resolved-profile",
                    legacy_dir=root / "legacy-profile",
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(runtime_module, "_setup_logger", return_value=logger),
            patch.object(runtime_module, "_launch_context_with_fallback", return_value=context),
            patch.object(runtime_module, "_minimize_history_window") as minimize_mock,
            patch.object(runtime_module, "_maximize_history_window") as maximize_mock,
            patch.object(runtime_module, "_bring_history_window_to_front") as focus_mock,
            patch.object(runtime_module, "_wait_for_login_context", side_effect=_fake_wait_for_login_context),
            patch.object(
                runtime_module,
                "_collect_for_account",
                return_value=[["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]],
            ),
            patch.object(runtime_module, "_save_activity_xlsx"),
        ):
            result = runtime_module.run_meta_history_with_plan(
                plan=plan,
                browser="msedge",
                action_log_dir=root / "output",
                trace_dir=root / "trace",
                user_data_dir=root / "user_data",
                progress_cb=events.append,
            )

        self.assertEqual(len(result.outputs), 1)
        minimize_mock.assert_called_once()
        maximize_mock.assert_called_once()
        focus_mock.assert_called_once()

    def test_run_meta_history_with_plan_recovers_after_dead_page_once(self) -> None:
        root = self._temp_root()
        first_page = _FakePage()
        second_page = _FakePage()
        first_context = _FakeContext(first_page)
        second_context = _FakeContext(second_page)
        events: list[dict[str, object]] = []
        logger = _DummyLogger()
        plan = [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="ACT_RECOVER",
                account_targets=[HistoryAccountTarget(act="111", business_id="999")],
            )
        ]
        seen_pages: list[_FakePage] = []

        def _fake_collect_for_account(**kwargs):
            seen_pages.append(kwargs["page"])
            if len(seen_pages) == 1:
                raise RuntimeError("Target page, context or browser has been closed")
            return [["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]]

        with (
            patch.dict(
                sys.modules,
                {"playwright.sync_api": SimpleNamespace(sync_playwright=lambda: _FakeSyncPlaywright())},
            ),
            patch.object(
                runtime_module,
                "prepare_meta_user_data_dir",
                return_value=PreparedMetaUserDataDir(
                    requested_dir=root / "requested-profile",
                    effective_dir=root / "resolved-profile",
                    legacy_dir=root / "legacy-profile",
                    migration_mode="none",
                    warning="",
                ),
            ),
            patch.object(runtime_module, "_setup_logger", return_value=logger),
            patch.object(
                runtime_module,
                "_launch_context_with_fallback",
                side_effect=[first_context, second_context],
            ),
            patch.object(runtime_module, "_wait_for_login_context"),
            patch.object(runtime_module, "_collect_for_account", side_effect=_fake_collect_for_account),
            patch.object(runtime_module, "_save_activity_xlsx"),
        ):
            result = runtime_module.run_meta_history_with_plan(
                plan=plan,
                browser="msedge",
                action_log_dir=root / "output",
                trace_dir=root / "trace",
                user_data_dir=root / "user_data",
                progress_cb=events.append,
            )

        self.assertEqual(seen_pages, [first_page, second_page])
        self.assertTrue(first_context.closed)
        self.assertTrue(second_context.closed)
        self.assertEqual(len(result.outputs), 1)
        final_updates = [
            event for event in events if event.get("type") == "history_row_update"
        ]
        self.assertEqual(final_updates[-1]["status"], "Completed")


if __name__ == "__main__":
    unittest.main()
