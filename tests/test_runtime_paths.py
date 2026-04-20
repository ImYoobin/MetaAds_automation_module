from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import dashboard.app as app_module


class MetaRuntimePathTests(unittest.TestCase):
    def test_build_run_storage_paths_from_parent_dir(self) -> None:
        paths = app_module._build_run_storage_paths(r"C:\Users\tester", "20260416")

        self.assertEqual(paths["base"], Path(r"C:\Users\tester\MetaAdsExport"))
        self.assertEqual(paths["raw_dir"], Path(r"C:\Users\tester\MetaAdsExport\raw\20260416"))
        self.assertEqual(paths["trace_dir"], Path(r"C:\Users\tester\MetaAdsExport\trace\20260416"))
        self.assertEqual(paths["output_dir"], Path(r"C:\Users\tester\MetaAdsExport\output\20260416"))
        self.assertEqual(paths["action_log_dir"], Path(r"C:\Users\tester\MetaAdsExport\output\action_log\20260416"))

    def test_build_shared_user_data_dir_uses_browser_scoped_profile(self) -> None:
        fake_st = SimpleNamespace(
            session_state={
                "base_parent_dir": r"C:\Users\tester",
                "browser": "chrome",
            }
        )

        with patch.object(app_module, "st", fake_st):
            path = app_module._build_shared_user_data_dir()

        self.assertEqual(path, Path(r"C:\Users\tester\MetaAdsExport\user_data\meta\chrome"))

    def test_sanitize_loaded_runtime_settings_migrates_legacy_output_dir(self) -> None:
        sanitized, has_invalid = app_module._sanitize_loaded_runtime_settings(
            {
                "browser": "msedge",
                "output_dir": r"%USERPROFILE%\MetaAdsExport\output",
                "downloads_dir": r"%USERPROFILE%\MetaAdsExport\raw",
                "logs_dir": r"%USERPROFILE%\MetaAdsExport\trace",
            }
        )

        self.assertFalse(has_invalid)
        self.assertEqual(sanitized["base_parent_dir"], str(Path.home()))

    def test_runtime_settings_payload_serializes_home_as_userprofile(self) -> None:
        fake_st = SimpleNamespace(
            session_state={
                "browser": "msedge",
                "base_parent_dir": str(Path.home()),
            }
        )

        with patch.object(app_module, "st", fake_st):
            payload = app_module._runtime_settings_payload()

        self.assertEqual(payload["base_parent_dir"], "%USERPROFILE%")

    def test_open_output_folder_for_completed_run_opens_output_root_once(self) -> None:
        fake_st = SimpleNamespace(
            session_state={
                "opened_output_for_run": "",
                "run_output_root_dir": r"C:\Users\tester\MetaAdsExport\output",
                "run_output_dir": r"C:\Users\tester\MetaAdsExport\output\20260416",
                "base_parent_dir": r"C:\Users\tester",
            }
        )
        snapshot = {
            "run_status": "Completed",
            "run_id": "run-1",
            "outputs": [{"brand": "Brand A", "activity": "Activity 1", "workbook_path": "x.xlsx"}],
            "history_outputs": [],
        }

        with (
            patch.object(app_module, "st", fake_st),
            patch("dashboard.app.subprocess.Popen") as popen_mock,
        ):
            app_module._open_output_folder_for_completed_run(snapshot)
            app_module._open_output_folder_for_completed_run(snapshot)

        popen_mock.assert_called_once_with(["explorer", r"C:\Users\tester\MetaAdsExport\output"])
        self.assertEqual(fake_st.session_state["opened_output_for_run"], "run-1")

    def test_open_output_folder_for_failed_run_when_history_output_exists(self) -> None:
        fake_st = SimpleNamespace(
            session_state={
                "opened_output_for_run": "",
                "run_output_root_dir": r"C:\Users\tester\MetaAdsExport\output",
                "base_parent_dir": r"C:\Users\tester",
            }
        )
        snapshot = {
            "run_status": "Failed",
            "run_id": "run-2",
            "outputs": [],
            "history_outputs": [{"brand": "Brand A", "activity": "Activity 1", "file_path": "y.xlsx"}],
        }

        with (
            patch.object(app_module, "st", fake_st),
            patch("dashboard.app.subprocess.Popen") as popen_mock,
        ):
            app_module._open_output_folder_for_completed_run(snapshot)

        popen_mock.assert_called_once_with(["explorer", r"C:\Users\tester\MetaAdsExport\output"])
        self.assertEqual(fake_st.session_state["opened_output_for_run"], "run-2")

    def test_open_output_folder_does_not_open_without_any_outputs(self) -> None:
        fake_st = SimpleNamespace(
            session_state={
                "opened_output_for_run": "",
                "run_output_root_dir": r"C:\Users\tester\MetaAdsExport\output",
                "base_parent_dir": r"C:\Users\tester",
            }
        )
        snapshot = {
            "run_status": "Completed (With Failures)",
            "run_id": "run-3",
            "outputs": [],
            "history_outputs": [],
        }

        with (
            patch.object(app_module, "st", fake_st),
            patch("dashboard.app.subprocess.Popen") as popen_mock,
        ):
            app_module._open_output_folder_for_completed_run(snapshot)

        popen_mock.assert_not_called()
        self.assertEqual(fake_st.session_state["opened_output_for_run"], "")


if __name__ == "__main__":
    unittest.main()
