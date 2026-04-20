"""Child-process entrypoint for integrated Meta history execution."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path
from typing import Any, TextIO

from dashboard.models import HistoryExecutionPlan, HistoryAccountTarget

from .runtime import run_meta_history_with_plan as run_meta_history_with_plan_in_process


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _emit(stdout: TextIO, *, kind: str, payload: dict[str, Any]) -> None:
    stdout.write(
        json.dumps(
            {
                "bridge_kind": kind,
                "payload": payload,
            },
            ensure_ascii=False,
        )
        + "\n"
    )
    stdout.flush()


def _reconfigure_stream(stream: TextIO | Any) -> None:
    try:
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        return


def _build_plan(raw_plan: Any) -> list[HistoryExecutionPlan]:
    plans: list[HistoryExecutionPlan] = []
    if not isinstance(raw_plan, list):
        return plans
    for item in raw_plan:
        if not isinstance(item, dict):
            continue
        targets: list[HistoryAccountTarget] = []
        for target in item.get("account_targets", []):
            if not isinstance(target, dict):
                continue
            targets.append(
                HistoryAccountTarget(
                    act=_safe_text(target.get("act")),
                    business_id=_safe_text(target.get("business_id")),
                )
            )
        plans.append(
            HistoryExecutionPlan(
                brand_code=_safe_text(item.get("brand_code")),
                brand_name=_safe_text(item.get("brand_name")),
                activity_name=_safe_text(item.get("activity_name")),
                account_targets=targets,
            )
        )
    return plans


def _serialize_result(result: Any) -> dict[str, Any]:
    return {
        "run_id": _safe_text(getattr(result, "run_id", "")),
        "log_file": _safe_text(getattr(result, "log_file", "")),
        "outputs": [
            {
                "brand_name": _safe_text(item.brand_name),
                "activity_name": _safe_text(item.activity_name),
                "file_path": _safe_text(item.file_path),
                "row_count": int(item.row_count or 0),
                "failed_accounts": [_safe_text(account) for account in item.failed_accounts if _safe_text(account)],
            }
            for item in getattr(result, "outputs", []) or []
        ],
    }


def _load_request(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Integrated Meta history subprocess runner")
    parser.add_argument("--request-json", required=True, help="Path to the bridge request JSON file.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    request = _load_request(args.request_json)
    original_stdout = sys.stdout
    _reconfigure_stream(original_stdout)
    _reconfigure_stream(sys.stderr)
    progress_stream_broken = False

    def _progress_cb(event: dict[str, Any]) -> None:
        nonlocal progress_stream_broken
        if progress_stream_broken:
            return
        try:
            _emit(original_stdout, kind="event", payload=dict(event))
        except OSError as exc:
            progress_stream_broken = True
            try:
                sys.stderr.write(f"[history progress emit skipped] {exc}\n")
                sys.stderr.flush()
            except Exception:
                pass

    try:
        sys.stdout = sys.stderr
        result = run_meta_history_with_plan_in_process(
            plan=_build_plan(request.get("plan")),
            browser=_safe_text(request.get("browser")),
            action_log_dir=_safe_text(request.get("action_log_dir")),
            trace_dir=_safe_text(request.get("trace_dir")),
            user_data_dir=_safe_text(request.get("user_data_dir")),
            progress_cb=_progress_cb,
            emit_run_started=bool(request.get("emit_run_started", True)),
        )
    except Exception as exc:  # noqa: BLE001
        _emit(
            original_stdout,
            kind="error",
            payload={
                "error": _safe_text(exc) or exc.__class__.__name__,
                "error_type": exc.__class__.__name__,
                "traceback": traceback.format_exc(),
            },
        )
        return 1
    finally:
        sys.stdout = original_stdout

    _emit(original_stdout, kind="result", payload=_serialize_result(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
