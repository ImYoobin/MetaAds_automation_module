from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from meta_history_log import main as history_main_module
from meta_history_log.main import AccountTarget, RunnerOptions


class _FakePage:
    def __init__(self, *, initial_url: str) -> None:
        self.url = initial_url
        self.goto_calls: list[tuple[str, str]] = []
        self.wait_calls: list[int] = []

    def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        self.goto_calls.append((url, wait_until))
        self.url = url

    def wait_for_timeout(self, timeout: int) -> None:
        self.wait_calls.append(timeout)


class _FakeTable:
    def wait_for(self, *, state: str, timeout: int) -> None:
        _ = (state, timeout)


class HistoryMainTests(unittest.TestCase):
    def _build_options(self) -> RunnerOptions:
        root = Path.cwd()
        return RunnerOptions(
            browser="msedge",
            headless=False,
            user_data_dir=(root / "user_data").resolve(),
            output_dir=(root / "output").resolve(),
            log_dir=(root / "trace").resolve(),
            screenshot_dir=(root / "trace" / "screenshots").resolve(),
            login_timeout_sec=300,
            action_timeout_ms=15_000,
            table_load_timeout_sec=30,
            lazy_scroll_pause_sec=1.0,
            lazy_scroll_max_rounds=5,
            lazy_scroll_no_new_rounds=2,
            step_retry_count=1,
        )

    def test_wait_for_login_context_redirects_from_business_home_to_ready_url(self) -> None:
        ready_url = (
            "https://adsmanager.facebook.com/adsmanager/manage/campaigns?"
            "act=111&business_id=222"
        )
        page = _FakePage(
            initial_url="https://business.facebook.com/latest/home?business_id=222&asset_id=333"
        )

        with patch.object(
            history_main_module.time,
            "time",
            side_effect=[0, 0, 3, 4],
        ):
            history_main_module._wait_for_login_context(
                page,
                timeout_sec=5,
                ready_url=ready_url,
            )

        self.assertEqual(page.goto_calls, [(ready_url, "domcontentloaded")])
        self.assertEqual(page.url, ready_url)

    def test_wait_for_login_context_reports_login_page_state(self) -> None:
        page = _FakePage(
            initial_url=(
                "https://business.facebook.com/business/loginpage/?"
                "next=https://business.facebook.com/"
            )
        )
        seen_states: list[tuple[str, str]] = []

        with patch.object(
            history_main_module.time,
            "time",
            side_effect=[0, 0, 2, 6],
        ):
            with self.assertRaises(history_main_module.AutomationError):
                history_main_module._wait_for_login_context(
                    page,
                    timeout_sec=5,
                    status_callback=lambda state, url: seen_states.append((state, url)),
                )

        self.assertTrue(seen_states)
        self.assertEqual(seen_states[0][0], "login_page")
        self.assertIn("business/loginpage", seen_states[0][1])

    def test_goto_campaigns_with_bootstrap_filter_passes_ready_url(self) -> None:
        page = _FakePage(initial_url="https://business.facebook.com/")
        account = AccountTarget(act="111", business_id="222")
        options = self._build_options()

        with patch.object(history_main_module, "_wait_for_login_context") as wait_mock:
            history_main_module._goto_campaigns_with_bootstrap_filter(
                page,
                account,
                activity_prefix="ACTIVITY",
                options=options,
            )

        self.assertEqual(len(page.goto_calls), 1)
        goto_url, wait_until = page.goto_calls[0]
        self.assertEqual(wait_until, "domcontentloaded")
        self.assertIn("https://adsmanager.facebook.com/adsmanager/manage/campaigns?", goto_url)
        self.assertIn("act=111", goto_url)
        self.assertIn("business_id=222", goto_url)
        wait_mock.assert_called_once_with(
            page,
            timeout_sec=options.login_timeout_sec,
            ready_url=goto_url,
        )

    def test_collect_for_account_uses_last_14_days_step_and_js_extractor(self) -> None:
        page = _FakePage(initial_url="https://adsmanager.facebook.com/adsmanager/manage/campaigns")
        account = AccountTarget(act="111", business_id="222")
        options = self._build_options()
        step_names: list[str] = []

        def _record_run_step(**kwargs):
            step_names.append(kwargs["step_name"])
            return kwargs["fn"]()

        with (
            patch.object(history_main_module, "_run_step", side_effect=_record_run_step),
            patch.object(history_main_module, "ensure_campaign_name_filter"),
            patch.object(history_main_module, "_select_all_adsets"),
            patch.object(history_main_module, "_open_history_panel"),
            patch.object(history_main_module, "_ensure_last_14_days"),
            patch.object(history_main_module, "_ensure_scope_adsets"),
            patch.object(history_main_module, "_extract_rows_js_accumulated", return_value=[]),
            patch.object(history_main_module, "_extract_rows_clipboard_fallback") as clipboard_mock,
        ):
            history_main_module._collect_for_account(
                page=page,
                logger=SimpleNamespace(
                    info=lambda *args, **kwargs: None,
                    warning=lambda *args, **kwargs: None,
                    exception=lambda *args, **kwargs: None,
                ),
                options=options,
                activity_prefix="ACTIVITY",
                account=account,
            )

        self.assertIn("set_last_14_days", step_names)
        self.assertNotIn("set_last_7_days", step_names)
        self.assertIn("extract_rows_js_accumulated", step_names)
        self.assertNotIn("extract_rows_clipboard_fallback", step_names)
        clipboard_mock.assert_not_called()

    def test_collect_for_account_returns_js_rows_without_clipboard_fallback(self) -> None:
        page = _FakePage(initial_url="https://adsmanager.facebook.com/adsmanager/manage/campaigns")
        account = AccountTarget(act="111", business_id="222")
        options = self._build_options()
        step_names: list[str] = []

        def _record_run_step(**kwargs):
            step_names.append(kwargs["step_name"])
            return kwargs["fn"]()

        with (
            patch.object(history_main_module, "_run_step", side_effect=_record_run_step),
            patch.object(history_main_module, "ensure_campaign_name_filter"),
            patch.object(history_main_module, "_select_all_adsets"),
            patch.object(history_main_module, "_open_history_panel"),
            patch.object(history_main_module, "_ensure_last_14_days"),
            patch.object(history_main_module, "_ensure_scope_adsets"),
            patch.object(
                history_main_module,
                "_extract_rows_js_accumulated",
                return_value=[["Activity", "Details", "Item", "User", "2026-04-20 12:00:00"]],
            ),
            patch.object(history_main_module, "_extract_rows_clipboard_fallback") as clipboard_mock,
        ):
            rows = history_main_module._collect_for_account(
                page=page,
                logger=SimpleNamespace(
                    info=lambda *args, **kwargs: None,
                    warning=lambda *args, **kwargs: None,
                    exception=lambda *args, **kwargs: None,
                ),
                options=options,
                activity_prefix="ACTIVITY",
                account=account,
            )

        self.assertEqual(len(rows), 1)
        self.assertIn("extract_rows_js_accumulated", step_names)
        self.assertNotIn("extract_rows_clipboard_fallback", step_names)
        clipboard_mock.assert_not_called()

    def test_extract_rows_js_accumulated_returns_empty_without_scrolling_when_empty_state(self) -> None:
        options = self._build_options()
        page = _FakePage(initial_url="https://business.facebook.com/")
        table = _FakeTable()
        empty_snapshot = {
            "header_cells": list(history_main_module.HISTORY_COLUMNS),
            "rows": [],
            "scroll_top": 0,
            "scroll_height": 0,
            "client_height": 0,
            "progress_visible": 0,
            "empty_state_visible": True,
            "empty_state_text": "No results found",
        }

        with (
            patch.object(history_main_module, "_table_locator", return_value=table),
            patch.object(history_main_module, "_wait_for_history_table_initial_snapshot", return_value=empty_snapshot),
            patch.object(history_main_module, "_scroll_history_table_once") as scroll_mock,
        ):
            rows = history_main_module._extract_rows_js_accumulated(
                page=page,
                options=options,
                logger=SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None),
            )

        self.assertEqual(rows, [])
        scroll_mock.assert_not_called()

    def test_normalize_history_row_preserves_multiline_item_changed(self) -> None:
        raw_row = [
            "Ad delivered",
            "Started delivery",
            "FCAS_Meta_General_BOF_Naver_ROAS_Retargetting_QV_Collection_cheil_T1A1_0-10sec_260418_RF02E5B \n ad ID: 120246785302610558",
            "Meta",
            "Apr 19 at 8:18 PM",
        ]

        normalized = history_main_module._normalize_history_row(raw_row)

        self.assertEqual(
            normalized[2],
            "FCAS_Meta_General_BOF_Naver_ROAS_Retargetting_QV_Collection_cheil_T1A1_0-10sec_260418_RF02E5B\nad ID: 120246785302610558",
        )

    def test_save_activity_xlsx_preserves_multiline_item_changed_as_text(self) -> None:
        rows = [
            [
                "Ad delivered",
                "Started delivery",
                "FCAS_Meta_General_BOF_Naver_ROAS_Retargetting_QV_Collection_cheil_T1A1_0-10sec_260418_RF02E5B\nad ID: 120246785302610558",
                "Meta",
                "Apr 19 at 8:18 PM",
            ]
        ]

        output_path = (Path.cwd() / "tests" / ".tmp" / "history_main_activity.xlsx").resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            history_main_module._save_activity_xlsx(rows=rows, output_path=output_path)

            from openpyxl import load_workbook  # type: ignore

            workbook = load_workbook(output_path)
            worksheet = workbook.active
            cell = worksheet["C2"]
            self.assertEqual(
                cell.value,
                "FCAS_Meta_General_BOF_Naver_ROAS_Retargetting_QV_Collection_cheil_T1A1_0-10sec_260418_RF02E5B\nad ID: 120246785302610558",
            )
            self.assertEqual(cell.number_format, "@")
            workbook.close()
        finally:
            if output_path.exists():
                output_path.unlink()


if __name__ == "__main__":
    unittest.main()
