"""Parent-process bridge for running Meta history in a separate Python process."""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Callable

from dashboard.models import (
    HistoryAdapterExecutionResult,
    HistoryExecutionOutput,
    HistoryExecutionPlan,
)


ProgressCallback = Callable[[dict[str, Any]], None]


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _emit(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback:
        callback(dict(payload))


def _module_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _build_request_path(trace_dir: Path) -> Path:
    token = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return (trace_dir.expanduser().resolve() / f"history_bridge_request_{token}.json").resolve()


def _serialize_plan(plan: list[HistoryExecutionPlan]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for item in plan:
        serialized.append(
            {
                "brand_code": item.brand_code,
                "brand_name": item.brand_name,
                "activity_name": item.activity_name,
                "account_targets": [
                    {
                        "act": target.act,
                        "business_id": target.business_id,
                    }
                    for target in item.account_targets
                ],
            }
        )
    return serialized


def _build_request_payload(
    *,
    plan: list[HistoryExecutionPlan],
    browser: str,
    action_log_dir: str | Path,
    trace_dir: str | Path,
    user_data_dir: str | Path,
    emit_run_started: bool,
) -> dict[str, Any]:
    return {
        "plan": _serialize_plan(plan),
        "browser": _safe_text(browser),
        "action_log_dir": str(Path(action_log_dir).expanduser().resolve()),
        "trace_dir": str(Path(trace_dir).expanduser().resolve()),
        "user_data_dir": str(Path(user_data_dir).expanduser().resolve()),
        "emit_run_started": bool(emit_run_started),
    }


def _parse_result_payload(payload: dict[str, Any]) -> HistoryAdapterExecutionResult:
    outputs_raw = payload.get("outputs")
    outputs: list[HistoryExecutionOutput] = []
    if isinstance(outputs_raw, list):
        for item in outputs_raw:
            if not isinstance(item, dict):
                continue
            outputs.append(
                HistoryExecutionOutput(
                    brand_name=_safe_text(item.get("brand_name")),
                    activity_name=_safe_text(item.get("activity_name")),
                    file_path=_safe_text(item.get("file_path")),
                    row_count=int(item.get("row_count") or 0),
                    failed_accounts=[
                        _safe_text(account)
                        for account in item.get("failed_accounts", [])
                        if _safe_text(account)
                    ],
                )
            )
    return HistoryAdapterExecutionResult(
        run_id=_safe_text(payload.get("run_id")),
        log_file=_safe_text(payload.get("log_file")),
        outputs=outputs,
    )


def _drain_stream(stream: Any, sink: list[str]) -> None:
    try:
        for raw_line in stream:
            line = _safe_text(raw_line).rstrip("\n")
            if line:
                sink.append(line)
    except Exception as exc:  # noqa: BLE001
        sink.append(f"[bridge stderr read failed] {exc}")


def _build_process_error(
    *,
    error_payload: dict[str, Any] | None,
    stderr_lines: list[str],
    stdout_noise: list[str],
    returncode: int,
) -> str:
    parts: list[str] = []
    if isinstance(error_payload, dict):
        error_text = _safe_text(error_payload.get("error"))
        if error_text:
            parts.append(error_text)
        error_type = _safe_text(error_payload.get("error_type"))
        if error_type and error_type not in error_text:
            parts.append(f"type={error_type}")
    if stderr_lines:
        parts.append("stderr=" + " | ".join(stderr_lines[-6:]))
    if stdout_noise:
        parts.append("stdout=" + " | ".join(stdout_noise[-4:]))
    if not parts:
        parts.append(f"Meta history subprocess exited with code {returncode}.")
    return " ".join(parts)


def run_meta_history_with_plan(
    *,
    plan: list[HistoryExecutionPlan],
    browser: str,
    action_log_dir: str | Path,
    trace_dir: str | Path,
    user_data_dir: str | Path,
    progress_cb: ProgressCallback | None = None,
    emit_run_started: bool = True,
) -> HistoryAdapterExecutionResult:
    if not plan:
        raise ValueError("No selected activities to run.")

    trace_path = Path(trace_dir).expanduser().resolve()
    trace_path.mkdir(parents=True, exist_ok=True)
    request_path = _build_request_path(trace_path)
    request_path.write_text(
        json.dumps(
            _build_request_payload(
                plan=plan,
                browser=browser,
                action_log_dir=action_log_dir,
                trace_dir=trace_path,
                user_data_dir=user_data_dir,
                emit_run_started=emit_run_started,
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    command = [
        sys.executable,
        "-X",
        "utf8",
        "-m",
        "meta_history_log.subprocess_runner",
        "--request-json",
        str(request_path),
    ]
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    process = subprocess.Popen(  # noqa: S603
        command,
        cwd=str(_module_root()),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        creationflags=creation_flags,
    )

    stderr_lines: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:
        stderr_thread = threading.Thread(
            target=_drain_stream,
            args=(process.stderr, stderr_lines),
            daemon=True,
        )
        stderr_thread.start()

    result_payload: dict[str, Any] | None = None
    error_payload: dict[str, Any] | None = None
    stdout_noise: list[str] = []
    try:
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = _safe_text(raw_line)
                if not line:
                    continue
                try:
                    envelope = json.loads(line)
                except Exception:  # noqa: BLE001
                    stdout_noise.append(line)
                    continue
                kind = _safe_text(envelope.get("bridge_kind"))
                payload = envelope.get("payload")
                if kind == "event" and isinstance(payload, dict):
                    _emit(progress_cb, payload)
                elif kind == "result" and isinstance(payload, dict):
                    result_payload = payload
                elif kind == "error" and isinstance(payload, dict):
                    error_payload = payload

        returncode = int(process.wait())
        if stderr_thread is not None:
            stderr_thread.join(timeout=2)

        if returncode == 0 and isinstance(result_payload, dict):
            return _parse_result_payload(result_payload)

        raise RuntimeError(
            _build_process_error(
                error_payload=error_payload,
                stderr_lines=stderr_lines,
                stdout_noise=stdout_noise,
                returncode=returncode,
            )
        )
    finally:
        request_path.unlink(missing_ok=True)
