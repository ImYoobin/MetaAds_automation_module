from __future__ import annotations

import io
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from dashboard.models import HistoryAccountTarget, HistoryExecutionPlan
from meta_history_log import subprocess_bridge as bridge_module


class _FakeProcess:
    def __init__(self, *, stdout_text: str, stderr_text: str, returncode: int) -> None:
        self.stdout = io.StringIO(stdout_text)
        self.stderr = io.StringIO(stderr_text)
        self._returncode = returncode

    def wait(self) -> int:
        return self._returncode


class HistorySubprocessBridgeTests(unittest.TestCase):
    def _temp_root(self) -> Path:
        temp_parent = (Path.cwd() / ".tmp_history_bridge_tests").resolve()
        temp_parent.mkdir(parents=True, exist_ok=True)
        root = (temp_parent / f"history_bridge_test_{uuid.uuid4().hex}").resolve()
        root.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        return root

    def _sample_plan(self) -> list[HistoryExecutionPlan]:
        return [
            HistoryExecutionPlan(
                brand_code="brand_a",
                brand_name="Brand A",
                activity_name="Activity 1",
                account_targets=[HistoryAccountTarget(act="111", business_id="222")],
            )
        ]

    def test_bridge_forwards_events_and_parses_result(self) -> None:
        root = self._temp_root()
        trace_dir = (root / "trace").resolve()
        action_log_dir = (root / "output" / "action_log").resolve()
        events: list[dict[str, str]] = []
        fake_process = _FakeProcess(
            stdout_text=(
                '{"bridge_kind":"event","payload":{"type":"login_status","status":"Waiting Login","message":"Waiting"}}\n'
                '{"bridge_kind":"result","payload":{"run_id":"run-1","log_file":"trace.log","outputs":[{"brand_name":"Brand A","activity_name":"Activity 1","file_path":"out.xlsx","row_count":3,"failed_accounts":["111/222"]}]}}\n'
            ),
            stderr_text="",
            returncode=0,
        )

        with patch.object(bridge_module.subprocess, "Popen", return_value=fake_process) as popen_mock:
            result = bridge_module.run_meta_history_with_plan(
                plan=self._sample_plan(),
                browser="msedge",
                action_log_dir=action_log_dir,
                trace_dir=trace_dir,
                user_data_dir=(root / "user_data"),
                progress_cb=events.append,
                emit_run_started=True,
            )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "login_status")
        self.assertEqual(result.run_id, "run-1")
        self.assertEqual(result.log_file, "trace.log")
        self.assertEqual(len(result.outputs), 1)
        self.assertEqual(result.outputs[0].file_path, "out.xlsx")
        self.assertEqual(result.outputs[0].row_count, 3)
        popen_args = popen_mock.call_args.args[0]
        self.assertEqual(popen_args[1:5], ["-X", "utf8", "-m", "meta_history_log.subprocess_runner"])
        popen_kwargs = popen_mock.call_args.kwargs
        self.assertEqual(popen_kwargs["encoding"], "utf-8")
        self.assertEqual(popen_kwargs["env"]["PYTHONIOENCODING"], "utf-8")
        self.assertEqual(popen_kwargs["env"]["PYTHONUTF8"], "1")
        self.assertEqual(list(trace_dir.glob("history_bridge_request_*.json")), [])

    def test_bridge_raises_runtime_error_with_child_error_payload(self) -> None:
        root = self._temp_root()
        trace_dir = (root / "trace").resolve()
        action_log_dir = (root / "output" / "action_log").resolve()
        fake_process = _FakeProcess(
            stdout_text=(
                '{"bridge_kind":"error","payload":{"error":"NotImplementedError()","error_type":"NotImplementedError"}}\n'
            ),
            stderr_text="child stderr line",
            returncode=1,
        )

        with patch.object(bridge_module.subprocess, "Popen", return_value=fake_process):
            with self.assertRaises(RuntimeError) as raised:
                bridge_module.run_meta_history_with_plan(
                    plan=self._sample_plan(),
                    browser="msedge",
                    action_log_dir=action_log_dir,
                    trace_dir=trace_dir,
                    user_data_dir=(root / "user_data"),
                )

        message = str(raised.exception)
        self.assertIn("NotImplementedError()", message)
        self.assertIn("stderr=child stderr line", message)


if __name__ == "__main__":
    unittest.main()
