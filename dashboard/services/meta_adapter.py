"""Adapter layer to reuse existing Meta automation internals."""

from __future__ import annotations

import datetime as dt
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


def _get_browser_window_state(sb: Any, logger: Any) -> str:
    driver = getattr(sb, "driver", None)
    if driver is None:
        return "unknown"
    with suppress(Exception):
        window_info = driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
        bounds = dict(window_info.get("bounds") or {})
        state = str(bounds.get("windowState") or "").strip().lower()
        if state in {"normal", "minimized", "maximized", "fullscreen"}:
            return state
    with suppress(Exception):
        if driver.execute_script("return document.hidden === true;"):
            return "minimized"
    with suppress(Exception):
        logger.debug("browser_window_state_unknown")
    return "unknown"


def _emit_row(
    callback: ProgressCallback | None,
    *,
    row_id: str,
    status: str | None = None,
    message: str | None = None,
    missing_columns_text: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "type": "row_update",
        "row_id": row_id,
    }
    if status is not None:
        payload["status"] = status
    if message is not None:
        payload["message"] = message
    if missing_columns_text is not None:
        payload["missing_columns_text"] = missing_columns_text
    _emit(callback, payload)


def _emit_activity_result(
    callback: ProgressCallback | None,
    *,
    brand: str,
    activity: str,
    status: str,
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
            "status": status,
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
) -> tuple[str, dict[str, int], dict[str, list[str]]]:
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
    workbook_bytes, missing_by_sheet = transformer.build_unified_workbook(source_df_by_sheet)
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
    return str(output_path), rows_by_sheet, missing_by_sheet


def run_meta_export_with_plan(
    *,
    plan: list[ActivityExecutionPlan],
    view_event_source: str,
    export_event_source: str,
    browser: str,
    output_dir: Path,
    raw_dir: Path,
    trace_dir: Path,
    user_data_dir: Path,
    progress_cb: ProgressCallback | None = None,
) -> AdapterExecutionResult:
    if not plan:
        raise ValueError("No selected activities to run.")

    run_id = _now_run_id()
    output_dir = output_dir.expanduser().resolve()
    raw_dir = raw_dir.expanduser().resolve()
    trace_dir = trace_dir.expanduser().resolve()
    user_data_dir = user_data_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    trace_dir.mkdir(parents=True, exist_ok=True)
    user_data_dir.mkdir(parents=True, exist_ok=True)

    logger = configure_logger(logs_dir=trace_dir, run_id=run_id)
    log_file = str((trace_dir / f"run_{run_id}.log").resolve())
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
        download_dir=str(raw_dir),
        headless=False,
    )

    try:
        from seleniumbase import SB
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("seleniumbase is required for Meta automation.") from exc

    logger.info("shared_user_data_dir=%s", user_data_dir)
    sb_kwargs = build_sb_kwargs(meta, engine_browser, user_data_dir=user_data_dir)
    ensure_browser_driver_ready(browser=engine_browser, logger=logger)
    outputs: list[ActivityExecutionOutput] = []
    overall_errors: list[str] = []

    with SB(**sb_kwargs) as sb:
        meta._enable_download_behavior(sb)  # noqa: SLF001
        verify_download_context(meta, watcher_dir=raw_dir, logger=logger)
        auto_minimized_once = False
        user_window_override = False
        minimize_skip_logged = False
        logger.info("minimize_policy=post_login_once")

        def _apply_minimize_policy(*, on_login: bool = False) -> None:
            nonlocal auto_minimized_once, user_window_override, minimize_skip_logged
            if not auto_minimized_once:
                if not on_login:
                    return
                if _minimize_browser_window(sb, logger):
                    auto_minimized_once = True
                return

            state = _get_browser_window_state(sb, logger)
            if state in {"normal", "maximized", "fullscreen"} and not user_window_override:
                user_window_override = True
                logger.info("user_window_override_detected state=%s", state)
            if user_window_override and not minimize_skip_logged:
                logger.info("minimize_skipped_due_to_user_override")
                minimize_skip_logged = True

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
        _apply_minimize_policy(on_login=True)

        for activity_plan in plan:
            _apply_minimize_policy()
            yymmdd = _now_yymmdd()
            raw_files_by_sheet: dict[str, list[str]] = {}
            activity_errors: list[str] = []
            failed_sheet_names: list[str] = []

            for sheet_plan in activity_plan.sheets:
                _apply_minimize_policy()
                row_id = (
                    f"{activity_plan.brand_code}::{activity_plan.activity_name}::{sheet_plan.sheet_display_name}"
                )
                urls = list(sheet_plan.urls)
                url_count = len(urls)
                if url_count <= 0:
                    continue

                report_name = INTERNAL_TO_WORKBOOK_SHEET_NAME.get(
                    sheet_plan.sheet_key,
                    sheet_plan.sheet_display_name,
                )
                report_to_sheet = {report_name: report_name}
                success_files: list[str] = []
                first_failures: list[tuple[int, str, str]] = []

                def _export_one(cleaned_url: str, *, index: int, attempt: int) -> str:
                    _apply_minimize_policy()
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
                            msg = f"URL {index}/{url_count}: 문제 감지 (시도 {attempt}/2)"
                        elif phase == "downloading":
                            msg = f"URL {index}/{url_count}: 다운로드 중 (시도 {attempt}/2)"
                        elif phase == "exporting":
                            msg = f"URL {index}/{url_count}: 내보내기 요청 중 (시도 {attempt}/2)"
                        else:
                            msg = f"URL {index}/{url_count}: 대기 중 (시도 {attempt}/2)"
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
                        message=f"URL {index}/{url_count}: 1차 시도 중입니다.",
                    )
                    try:
                        success_files.append(_export_one(cleaned_url, index=index, attempt=1))
                        _emit_row(
                            progress_cb,
                            row_id=row_id,
                            status="Running",
                            message=f"URL {index}/{url_count}: 1차 시도 완료",
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
                            message=f"URL {index}/{url_count}: 1차 시도 실패, 재시도를 예약했습니다.",
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
                                f"재시도 {retry_idx}/{retry_total} 진행 중 "
                                f"(원본 URL {orig_index}/{url_count})"
                            ),
                        )
                        try:
                            success_files.append(_export_one(cleaned_url, index=orig_index, attempt=2))
                            _emit_row(
                                progress_cb,
                                row_id=row_id,
                                status="Running",
                                message=f"URL {orig_index}/{url_count}: 재시도 성공",
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
                        f"{success_count}/{url_count} URL 완료, "
                        f"{final_fail_count}개 재시도 후 실패"
                    )
                    if error_hint:
                        row_message = f"{row_message} ({error_hint})"
                    activity_errors.append(f"{sheet_display}: {row_message}")
                    _emit_row(progress_cb, row_id=row_id, status="Failed", message=row_message)
                else:
                    retry_success = success_count - (url_count - len(first_failures))
                    if retry_success > 0:
                        row_message = (
                            f"{success_count}/{url_count} URL 완료 "
                            f"(재시도 복구 {retry_success})"
                        )
                    else:
                        row_message = f"{success_count}/{url_count} URL 완료"
                    _emit_row(progress_cb, row_id=row_id, status="Completed", message=row_message)

            if failed_sheet_names:
                failed_sheet_text = ", ".join(dict.fromkeys(failed_sheet_names))
                summary_message = (
                    f"문제된 시트({failed_sheet_text}) export에 실패해 통합본을 생성하지 않았습니다."
                )
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    status="Failed",
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
                no_file_message = "다운로드된 파일이 없어 통합본을 생성하지 않았습니다."
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    status="Failed",
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
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    status="Running",
                    workbook_path="",
                    rows_by_sheet={},
                    failed_sheets=[],
                    message="통합본 생성 중입니다.",
                )
                workbook_path, rows_by_sheet, missing_by_sheet = _build_workbook_for_activity(
                    raw_files_by_sheet=raw_files_by_sheet,
                    output_dir=output_dir,
                    brand_name=activity_plan.brand_name,
                    activity_name=activity_plan.activity_name,
                    yymmdd=yymmdd,
                    naming_config=naming_config,
                )
                for sheet_plan in activity_plan.sheets:
                    if len(sheet_plan.urls) <= 0:
                        continue
                    row_id = (
                        f"{activity_plan.brand_code}::{activity_plan.activity_name}::{sheet_plan.sheet_display_name}"
                    )
                    display_sheet = INTERNAL_TO_WORKBOOK_SHEET_NAME.get(
                        sheet_plan.sheet_key,
                        sheet_plan.sheet_display_name,
                    )
                    missing_text = ", ".join(missing_by_sheet.get(display_sheet, []))
                    _emit_row(
                        progress_cb,
                        row_id=row_id,
                        missing_columns_text=missing_text,
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
                    status="Completed",
                    workbook_path=workbook_path,
                    rows_by_sheet=rows_by_sheet,
                    failed_sheets=[],
                    message=f"통합본 생성완료:{Path(workbook_path).name}",
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "workbook_build_failed brand=%s activity=%s",
                    activity_plan.brand_name,
                    activity_plan.activity_name,
                )
                reason = f"통합본 생성 실패: {str(exc)[:200]}"
                _emit_activity_result(
                    progress_cb,
                    brand=activity_plan.brand_name,
                    activity=activity_plan.activity_name,
                    status="Failed",
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
