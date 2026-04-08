"""Adapter layer to reuse existing Meta automation internals."""

from __future__ import annotations

import datetime as dt
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from dashboard.models import (
    ActivityExecutionOutput,
    ActivityExecutionPlan,
    AdapterExecutionResult,
    INTERNAL_TO_WORKBOOK_SHEET_NAME,
)
from dashboard.services.url_service import parse_cleaned_url
from meta_core.engine.file_naming import format_name
from meta_core.engine.meta_automation import MetaAutomation
from meta_core.runtime import (
    build_sb_kwargs,
    configure_insecure_https_bootstrap,
    configure_logger,
    ensure_browser_driver_ready,
    load_embedded_engine_config,
    verify_download_context,
)
from meta_core.transformer import (
    MetaExportTransformer,
    parse_meta_export_payload_to_dataframe,
)


ProgressCallback = Callable[[dict[str, Any]], None]


def _now_run_id() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _now_yymmdd() -> str:
    return dt.datetime.now().strftime("%y%m%d_%H%M%S")


def _run_date_folder_name(run_id: str) -> str:
    token = str(run_id or "").strip().split("_")[0]
    if len(token) == 8 and token.isdigit():
        return f"{token[0:4]}-{token[4:6]}-{token[6:8]}"
    return dt.datetime.now().strftime("%Y-%m-%d")


def _with_run_date_dir(base_dir: Path, run_date_folder: str) -> Path:
    resolved = base_dir.expanduser().resolve()
    if resolved.name == run_date_folder:
        return resolved
    return (resolved / run_date_folder).resolve()


def _normalize_engine_browser(browser: str) -> str:
    normalized = str(browser or "").strip().lower()
    if normalized == "msedge":
        return "edge"
    if normalized == "chrome":
        return "chrome"
    raise RuntimeError("`browser` must be one of: msedge, chrome.")


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if getattr(df.columns, "has_duplicates", False):
        return df.loc[:, ~df.columns.duplicated()].copy()
    return df


def _emit(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback:
        callback(payload)


def _minimize_browser_window(sb: Any, logger: Any) -> bool:
    driver = getattr(sb, "driver", None)
    if driver is None:
        return False

    try:
        window_info = driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
        window_id = int(window_info.get("windowId") or 0)
        if window_id > 0:
            driver.execute_cdp_cmd(
                "Browser.setWindowBounds",
                {"windowId": window_id, "bounds": {"windowState": "minimized"}},
            )
            logger.info("browser_window_minimized strategy=cdp")
            return True
    except Exception as exc:  # noqa: BLE001
        logger.info("browser_minimize_cdp_failed reason=%s", exc)

    with suppress(Exception):
        driver.minimize_window()
        logger.info("browser_window_minimized strategy=webdriver")
        return True
    return False


def _emit_row(
    callback: ProgressCallback | None,
    *,
    row_id: str,
    status: str,
    message: str,
) -> None:
    _emit(
        callback,
        {
            "type": "row_update",
            "row_id": row_id,
            "status": status,
            "message": message,
        },
    )


def _emit_activity_result(
    callback: ProgressCallback | None,
    *,
    brand: str,
    activity: str,
    workbook_path: str = "",
    rows_by_sheet: dict[str, int] | None = None,
    failed_sheets: list[str] | None = None,
    message: str = "",
) -> None:
    _emit(
        callback,
        {
            "type": "activity_result",
            "brand": brand,
            "activity": activity,
            "workbook_path": workbook_path,
            "rows_by_sheet": dict(rows_by_sheet or {}),
            "failed_sheets": list(failed_sheets or []),
            "message": str(message or ""),
        },
    )


def _resolve_unified_output_pattern(naming_config: dict[str, Any]) -> str:
    default_pattern = "{brand}_{activity}_{yyMMdd}_Unified.xlsx"
    configured = str(naming_config.get("final_file_name_pattern") or "").strip()
    if not configured:
        return default_pattern
    # Guard against legacy/hardcoded patterns that collapse multiple activities into one file.
    if "{brand}" not in configured or "{activity}" not in configured:
        return default_pattern
    return configured


def _build_workbook_for_activity(
    *,
    raw_files_by_sheet: dict[str, list[str]],
    output_dir: Path,
    brand_name: str,
    activity_name: str,
    yymmdd: str,
    naming_config: dict[str, Any],
) -> tuple[str, dict[str, int]]:
    source_df_by_sheet: dict[str, pd.DataFrame] = {}
    rows_by_sheet: dict[str, int] = {}
    for sheet_key, raw_files in raw_files_by_sheet.items():
        frames: list[pd.DataFrame] = []
        for path in raw_files:
            payload = Path(path).read_bytes()
            df = parse_meta_export_payload_to_dataframe(
                payload=payload,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                sheet_key=sheet_key,
            )
            frames.append(df)
        if not frames:
            continue
        merged_df = pd.concat(frames, ignore_index=True, sort=False)
        normalized = _dedupe_columns(merged_df.fillna(""))
        source_df_by_sheet[sheet_key] = normalized
        display_sheet = INTERNAL_TO_WORKBOOK_SHEET_NAME.get(sheet_key, sheet_key)
        rows_by_sheet[display_sheet] = int(len(normalized.index))

    transformer = MetaExportTransformer()
    workbook_bytes, _ = transformer.build_unified_workbook(source_df_by_sheet)
    output_pattern = _resolve_unified_output_pattern(naming_config)
    output_filename = format_name(
        pattern=output_pattern,
        brand=brand_name,
        activity=activity_name,
        yymmdd=yymmdd,
        sheet="Unified",
    )
    output_path = (output_dir / output_filename).resolve()
    output_path.write_bytes(workbook_bytes)
    return str(output_path), rows_by_sheet


def run_meta_export_with_plan(
    *,
    plan: list[ActivityExecutionPlan],
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: Path,
    downloads_dir: Path,
    logs_dir: Path,
    progress_cb: ProgressCallback | None = None,
) -> AdapterExecutionResult:
    if not plan:
        raise ValueError("No selected activities to run.")

    run_id = _now_run_id()
    run_date_folder = _run_date_folder_name(run_id)
    logs_dir = _with_run_date_dir(logs_dir, run_date_folder)
    output_dir = _with_run_date_dir(output_dir, run_date_folder)
    downloads_dir = _with_run_date_dir(downloads_dir, run_date_folder)
    logs_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    logger = configure_logger(logs_dir=logs_dir, run_id=run_id)
    log_file = str((logs_dir / f"run_{run_id}.log").resolve())
    _emit(
        progress_cb,
        {
            "type": "run_started",
            "run_id": run_id,
            "log_file": log_file,
        },
    )

    engine_browser = _normalize_engine_browser(browser)
    engine_config = load_embedded_engine_config()
    meta_config = dict(engine_config.get("meta") or {})
    naming_config = dict(engine_config.get("naming") or {})
    meta_config["home_url"] = "https://business.facebook.com/"

    configure_insecure_https_bootstrap()
    meta = MetaAutomation(
        meta_config=meta_config,
        naming_config=naming_config,
        download_dir=str(downloads_dir),
        headless=False,
    )

    try:
        from seleniumbase import SB
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("seleniumbase is required for META automation") from exc

    sb_kwargs = build_sb_kwargs(meta, engine_browser)
    ensure_browser_driver_ready(browser=engine_browser, logger=logger)
    outputs: list[ActivityExecutionOutput] = []
    overall_errors: list[str] = []

    with SB(**sb_kwargs) as sb:
        meta._enable_download_behavior(sb)  # noqa: SLF001
        verify_download_context(meta, watcher_dir=downloads_dir, logger=logger)
        last_minimize_monotonic = 0.0

        def _ensure_minimized(*, force: bool = False) -> None:
            nonlocal last_minimize_monotonic
            now = time.monotonic()
            if not force and (now - last_minimize_monotonic) < 0.8:
                return
            if _minimize_browser_window(sb, logger):
                last_minimize_monotonic = now

        _emit(
            progress_cb,
            {
                "type": "login_status",
                "status": "Waiting Login",
                "message": "Waiting for Meta login in browser.",
            },
        )
        sb.open("https://business.facebook.com/")
        meta._wait_for_meta_login(sb)  # noqa: SLF001
        _emit(
            progress_cb,
            {
                "type": "login_status",
                "status": "Logged In",
                "message": "Meta login confirmed.",
            },
        )
        _ensure_minimized(force=True)

        for activity_plan in plan:
            _ensure_minimized()
            yymmdd = _now_yymmdd()
            raw_files_by_sheet: dict[str, list[str]] = {}
            activity_errors: list[str] = []
            failed_sheet_names: list[str] = []

            for sheet_plan in activity_plan.sheets:
                _ensure_minimized()
                row_id = (
                    f"{activity_plan.brand_code}::{activity_plan.activity_name}::{sheet_plan.sheet_display_name}"
                )
                urls = list(sheet_plan.urls)
                url_count = len(urls)
                if url_count <= 0:
                    _emit_row(
                        progress_cb,
                        row_id=row_id,
                        status="Skipped",
                        message="No URL registered for this sheet.",
                    )
                    continue

                report_name = INTERNAL_TO_WORKBOOK_SHEET_NAME.get(
                    sheet_plan.sheet_key,
                    sheet_plan.sheet_display_name,
                )
                report_to_sheet = {report_name: report_name}
                success_files: list[str] = []
                first_failures: list[tuple[int, str, str]] = []

                def _export_one(cleaned_url: str, *, index: int, attempt: int) -> str:
                    _ensure_minimized()
                    parsed = parse_cleaned_url(
                        cleaned_url,
                        default_event_source=view_event_source,
                    )
                    brand_cfg = {
                        "brand_ko": activity_plan.brand_name,
                        "meta_act_id": parsed.act_id,
                        "meta_ad_account_id": parsed.act_id,
                        "meta_business_id": parsed.business_id,
                        "meta_global_scope_id": parsed.global_scope_id,
                        "meta_view_event_source": parsed.event_source or view_event_source,
                        "meta_export_event_source": export_event_source,
                    }

                    def _progress_bridge(event: dict[str, Any]) -> None:
                        phase = str(event.get("phase") or "").strip().lower()
                        if phase not in {"failed", "downloading", "waiting_ready", "exporting"}:
                            return
                        if phase == "failed":
                            msg = f"URL {index}/{url_count}: issue detected (attempt {attempt}/2)"
                        elif phase == "downloading":
                            msg = f"URL {index}/{url_count}: downloading (attempt {attempt}/2)"
                        elif phase == "exporting":
                            msg = f"URL {index}/{url_count}: export requested (attempt {attempt}/2)"
                        else:
                            msg = f"URL {index}/{url_count}: waiting (attempt {attempt}/2)"
                        _emit_row(progress_cb, row_id=row_id, status="Running", message=msg)

                    return meta._export_report_via_view_id(  # noqa: SLF001
                        sb=sb,
                        brand_cfg=brand_cfg,
                        report_name=report_name,
                        report_id=parsed.report_id,
                        activity_for_filename=activity_plan.activity_name,
                        yymmdd=yymmdd,
                        report_to_sheet=report_to_sheet,
                        sheet_key=sheet_plan.sheet_key,
                        progress_event_cb=_progress_bridge,
                    )

                for index, cleaned_url in enumerate(urls, start=1):
                    _emit_row(
                        progress_cb,
                        row_id=row_id,
                        status="Running",
                        message=f"URL {index}/{url_count}: first attempt running",
                    )
                    try:
                        success_files.append(_export_one(cleaned_url, index=index, attempt=1))
                        _emit_row(
                            progress_cb,
                            row_id=row_id,
                            status="Running",
                            message=f"URL {index}/{url_count}: first attempt succeeded",
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.exception(
                            "sheet_url_export_failed brand=%s activity=%s sheet=%s index=%s attempt=%s",
                            activity_plan.brand_name,
                            activity_plan.activity_name,
                            sheet_plan.sheet_display_name,
                            index,
                            1,
                        )
                        first_failures.append((index, cleaned_url, str(exc)))
                        _emit_row(
                            progress_cb,
                            row_id=row_id,
                            status="Running",
                            message=f"URL {index}/{url_count}: first attempt failed, retry queued",
                        )

                remaining_failures: list[tuple[int, str]] = []
                if first_failures:
                    retry_total = len(first_failures)
                    for retry_idx, (orig_index, cleaned_url, _) in enumerate(first_failures, start=1):
                        _emit_row(
                            progress_cb,
                            row_id=row_id,
                            status="Running",
                            message=(
                                f"Retrying failed URL {retry_idx}/{retry_total} "
                                f"(original {orig_index}/{url_count})"
                            ),
                        )
                        try:
                            success_files.append(_export_one(cleaned_url, index=orig_index, attempt=2))
                            _emit_row(
                                progress_cb,
                                row_id=row_id,
                                status="Running",
                                message=f"URL {orig_index}/{url_count}: retry succeeded",
                            )
                        except Exception as retry_exc:  # noqa: BLE001
                            logger.exception(
                                "sheet_url_export_failed brand=%s activity=%s sheet=%s index=%s attempt=%s",
                                activity_plan.brand_name,
                                activity_plan.activity_name,
                                sheet_plan.sheet_display_name,
                                orig_index,
                                2,
                            )
                            remaining_failures.append((orig_index, str(retry_exc)))

                if success_files:
                    raw_files_by_sheet.setdefault(sheet_plan.sheet_key, []).extend(success_files)

                success_count = len(success_files)
                final_fail_count = len(remaining_failures)
                if final_fail_count > 0:
                    sheet_display = INTERNAL_TO_WORKBOOK_SHEET_NAME.get(
                        sheet_plan.sheet_key,
                        sheet_plan.sheet_display_name,
                    )
                    failed_sheet_names.append(sheet_display)
                    error_hint = remaining_failures[0][1][:140] if remaining_failures else ""
                    row_message = (
                        f"{success_count}/{url_count} URL succeeded, "
                        f"{final_fail_count} failed after retry"
                    )
                    if error_hint:
                        row_message = f"{row_message} ({error_hint})"
                    activity_errors.append(f"{sheet_display}: {row_message}")
                    _emit_row(progress_cb, row_id=row_id, status="Failed", message=row_message)
                else:
                    retry_success = success_count - (url_count - len(first_failures))
                    if retry_success > 0:
                        row_message = (
                            f"{success_count}/{url_count} URL completed "
                            f"(retry recovered {retry_success})"
                        )
                    else:
                        row_message = f"{success_count}/{url_count} URL completed"
                    _emit_row(progress_cb, row_id=row_id, status="Completed", message=row_message)

            if failed_sheet_names:
                failed_sheet_text = ", ".join(dict.fromkeys(failed_sheet_names))
                summary_message = (
                    f"문제된 시트({failed_sheet_text}) export에 실패해 통합파일이 생성되지 않았습니다."
                )
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    workbook_path="",
                    rows_by_sheet={},
                    failed_sheets=list(dict.fromkeys(failed_sheet_names)),
                    message=summary_message,
                )
                overall_errors.append(
                    f"[{activity_plan.brand_name}/{activity_plan.activity_name}] {summary_message}"
                )
                overall_errors.extend(
                    f"[{activity_plan.brand_name}/{activity_plan.activity_name}] {item}"
                    for item in activity_errors
                )
                continue

            if not any(raw_files_by_sheet.values()):
                no_file_message = "다운로드된 파일이 없어 통합파일을 생성하지 않았습니다."
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    workbook_path="",
                    rows_by_sheet={},
                    failed_sheets=[],
                    message=no_file_message,
                )
                overall_errors.append(
                    f"[{activity_plan.brand_name}/{activity_plan.activity_name}] {no_file_message}"
                )
                continue

            try:
                workbook_path, rows_by_sheet = _build_workbook_for_activity(
                    raw_files_by_sheet=raw_files_by_sheet,
                    output_dir=output_dir,
                    brand_name=activity_plan.brand_name,
                    activity_name=activity_plan.activity_name,
                    yymmdd=yymmdd,
                    naming_config=naming_config,
                )
                outputs.append(
                    ActivityExecutionOutput(
                        brand_name=activity_plan.brand_name,
                        activity_name=activity_plan.activity_name,
                        workbook_path=workbook_path,
                        raw_files_by_sheet=raw_files_by_sheet,
                    )
                )
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    workbook_path=workbook_path,
                    rows_by_sheet=rows_by_sheet,
                    failed_sheets=[],
                    message="전체 시트 처리 완료",
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "workbook_build_failed brand=%s activity=%s",
                    activity_plan.brand_name,
                    activity_plan.activity_name,
                )
                reason = f"통합파일 생성 실패: {str(exc)[:200]}"
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    workbook_path="",
                    rows_by_sheet={},
                    failed_sheets=[],
                    message=reason,
                )
                overall_errors.append(
                    f"[{activity_plan.brand_name}/{activity_plan.activity_name}] {reason}"
                )
                for sheet_plan in activity_plan.sheets:
                    row_id = (
                        f"{activity_plan.brand_code}::{activity_plan.activity_name}::{sheet_plan.sheet_display_name}"
                    )
                    if len(sheet_plan.urls) <= 0:
                        continue
                    _emit_row(progress_cb, row_id=row_id, status="Failed", message=reason)

    if overall_errors:
        _emit(
            progress_cb,
            {
                "type": "run_warning",
                "message": "\n".join(overall_errors),
            },
        )

    return AdapterExecutionResult(
        run_id=run_id,
        log_file=log_file,
        outputs=outputs,
    )
