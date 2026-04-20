"""Importable runtime API for integrated Meta action-log execution."""

from __future__ import annotations

import datetime as dt
import logging
import tempfile
from pathlib import Path
from typing import Any, Callable

from dashboard.models import (
    HistoryAdapterExecutionResult,
    HistoryExecutionOutput,
    HistoryExecutionPlan,
)
from meta_core.pathing import prepare_meta_user_data_dir

from .main import (
    AccountTarget,
    DEFAULT_ACTION_TIMEOUT_MS,
    DEFAULT_LAZY_SCROLL_MAX_ROUNDS,
    DEFAULT_LAZY_SCROLL_NO_NEW_ROUNDS,
    DEFAULT_LAZY_SCROLL_PAUSE_SEC,
    DEFAULT_LOGIN_TIMEOUT_SEC,
    DEFAULT_STEP_RETRY_COUNT,
    DEFAULT_TABLE_LOAD_TIMEOUT_SEC,
    ManualInterventionRequired,
    NO_ACTION_LOG_ROWS_MESSAGE,
    NO_TARGET_ACCOUNTS_MESSAGE,
    RunnerOptions,
    _build_campaigns_bootstrap_url,
    _build_output_file_path,
    _collect_for_account,
    _dedupe_rows,
    _save_activity_xlsx,
    _setup_logger,
    _wait_for_login_context,
)


ProgressCallback = Callable[[dict[str, Any]], None]

HISTORY_WAITING_FOR_PRIOR_ACTIVITY_MESSAGE = (
    "\uc55e\uc120 \uc561\ud2f0\ube44\ud2f0 \ucc98\ub9ac \ub300\uae30\uc911\uc785\ub2c8\ub2e4."
)
HISTORY_RUNNING_ACCOUNT_TEMPLATE = "\uacc4\uc815 {current}/{total} \uc218\uc9d1 \uc911\uc785\ub2c8\ub2e4."
HISTORY_FAILED_PREFIX = "\uc561\uc158 \ub85c\uadf8 \ub2e4\uc6b4\ub85c\ub4dc \uc2e4\ud328"
HISTORY_PARTIAL_SAVED_PREFIX = "\ubd80\ubd84 \uc800\uc7a5\uc644\ub8cc"
HISTORY_SAVED_PREFIX = "\uc561\uc158 \ub85c\uadf8 \uc800\uc7a5\uc644\ub8cc"


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _now_run_id() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def _history_row_id(*, brand_code: str, activity_name: str) -> str:
    return f"{brand_code}::{activity_name}::history"


def _emit(callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if callback:
        callback(dict(payload))


def _resolve_log_file(logger: logging.Logger) -> str:
    for handler in getattr(logger, "handlers", []):
        filename = _safe_text(getattr(handler, "baseFilename", ""))
        if filename:
            return filename
    return ""


def _normalize_browser(browser: str) -> str:
    normalized = _safe_text(browser).lower()
    if normalized in {"msedge", "chrome", "chromium"}:
        return normalized
    return "msedge"


def _format_exception_message(exc: BaseException) -> str:
    message = _safe_text(exc)
    if message:
        return message
    message = _safe_text(repr(exc))
    if message:
        return message
    return exc.__class__.__name__


def _fallback_output_root() -> Path:
    return (Path(__file__).resolve().parent / "_local_output").resolve()


def _build_runner_options(
    *,
    browser: str,
    action_log_dir: Path,
    trace_dir: Path,
    user_data_dir: Path,
) -> RunnerOptions:
    output_dir = action_log_dir.expanduser().resolve()
    log_dir = trace_dir.expanduser().resolve()
    return RunnerOptions(
        browser=_normalize_browser(browser),
        headless=False,
        user_data_dir=user_data_dir.expanduser().resolve(),
        output_dir=output_dir,
        log_dir=log_dir,
        screenshot_dir=(log_dir / "screenshots").resolve(),
        login_timeout_sec=DEFAULT_LOGIN_TIMEOUT_SEC,
        action_timeout_ms=DEFAULT_ACTION_TIMEOUT_MS,
        table_load_timeout_sec=DEFAULT_TABLE_LOAD_TIMEOUT_SEC,
        lazy_scroll_pause_sec=DEFAULT_LAZY_SCROLL_PAUSE_SEC,
        lazy_scroll_max_rounds=DEFAULT_LAZY_SCROLL_MAX_ROUNDS,
        lazy_scroll_no_new_rounds=DEFAULT_LAZY_SCROLL_NO_NEW_ROUNDS,
        step_retry_count=DEFAULT_STEP_RETRY_COUNT,
    )


def _build_isolated_user_data_dir(*, log_dir: Path, browser: str) -> Path:
    profile_root = (log_dir.expanduser().resolve() / "_runtime_profiles").resolve()
    profile_root.mkdir(parents=True, exist_ok=True)
    return Path(
        tempfile.mkdtemp(
            prefix=f"{_normalize_browser(browser)}_",
            dir=str(profile_root),
        )
    ).resolve()


def _ensure_output_roots(options: RunnerOptions, *, run_date_token: str) -> RunnerOptions:
    try:
        options.output_dir.mkdir(parents=True, exist_ok=True)
        options.log_dir.mkdir(parents=True, exist_ok=True)
        options.screenshot_dir.mkdir(parents=True, exist_ok=True)
        return options
    except Exception:
        fallback_root = _fallback_output_root()
        fallback_options = RunnerOptions(
            browser=options.browser,
            headless=options.headless,
            user_data_dir=options.user_data_dir,
            output_dir=(fallback_root / "output" / "action_log" / run_date_token).resolve(),
            log_dir=(fallback_root / "trace" / run_date_token).resolve(),
            screenshot_dir=(fallback_root / "trace" / run_date_token / "screenshots").resolve(),
            login_timeout_sec=options.login_timeout_sec,
            action_timeout_ms=options.action_timeout_ms,
            table_load_timeout_sec=options.table_load_timeout_sec,
            lazy_scroll_pause_sec=options.lazy_scroll_pause_sec,
            lazy_scroll_max_rounds=options.lazy_scroll_max_rounds,
            lazy_scroll_no_new_rounds=options.lazy_scroll_no_new_rounds,
            step_retry_count=options.step_retry_count,
        )
        fallback_options.output_dir.mkdir(parents=True, exist_ok=True)
        fallback_options.log_dir.mkdir(parents=True, exist_ok=True)
        fallback_options.screenshot_dir.mkdir(parents=True, exist_ok=True)
        return fallback_options


def _launch_context_for_profile(
    playwright: Any,
    *,
    browser: str,
    user_data_dir: Path,
    headless: bool,
    action_timeout_ms: int,
) -> Any:
    channel: str | None = None
    normalized_browser = _normalize_browser(browser)
    if normalized_browser in {"msedge", "chrome"}:
        channel = normalized_browser

    context = playwright.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        channel=channel,
        headless=headless,
        viewport={"width": 1600, "height": 920},
        args=["--disable-blink-features=AutomationControlled"],
    )
    context.set_default_timeout(action_timeout_ms)
    return context


def _launch_context_with_fallback(
    playwright: Any,
    *,
    options: RunnerOptions,
    logger: logging.Logger,
) -> Any:
    attempts: list[tuple[str, str, Path]] = [
        ("primary", options.browser, options.user_data_dir.expanduser().resolve()),
        (
            "isolated-profile",
            options.browser,
            _build_isolated_user_data_dir(log_dir=options.log_dir, browser=options.browser),
        ),
    ]
    if _normalize_browser(options.browser) in {"msedge", "chrome"}:
        attempts.append(
            (
                "isolated-profile",
                "chromium",
                _build_isolated_user_data_dir(log_dir=options.log_dir, browser="chromium"),
            )
        )

    attempt_errors: list[str] = []
    for strategy, browser_name, user_data_dir in attempts:
        try:
            logger.info(
                "launch_context_attempt strategy=%s browser=%s user_data_dir=%s",
                strategy,
                browser_name,
                user_data_dir,
            )
            context = _launch_context_for_profile(
                playwright,
                browser=browser_name,
                user_data_dir=user_data_dir,
                headless=options.headless,
                action_timeout_ms=options.action_timeout_ms,
            )
            logger.info(
                "launch_context_ready strategy=%s browser=%s user_data_dir=%s",
                strategy,
                browser_name,
                user_data_dir,
            )
            return context
        except Exception as exc:  # noqa: BLE001
            error_text = _format_exception_message(exc)
            attempt_errors.append(f"{strategy}/{browser_name}: {error_text}")
            logger.exception(
                "launch_context_attempt_failed strategy=%s browser=%s user_data_dir=%s error=%s",
                strategy,
                browser_name,
                user_data_dir,
                error_text,
            )

    raise RuntimeError(
        "\ube0c\ub77c\uc6b0\uc800 \ucee8\ud14d\uc2a4\ud2b8 \uc2e4\ud589\uc5d0 \uc2e4\ud328\ud588\uc2b5\ub2c8\ub2e4. "
        + " | ".join(attempt_errors)
    )


def _resolve_startup_bootstrap_url(plan: list[HistoryExecutionPlan]) -> str:
    for activity_plan in plan:
        if not activity_plan.account_targets:
            continue
        first_target = activity_plan.account_targets[0]
        return _build_campaigns_bootstrap_url(
            account=AccountTarget(
                act=first_target.act,
                business_id=first_target.business_id,
            ),
            activity_prefix=activity_plan.activity_name,
        )
    return ""


def _is_dead_browser_exception(exc: BaseException) -> bool:
    text = _format_exception_message(exc).lower()
    return (
        "targetclosederror" in text
        or "target page, context or browser has been closed" in text
        or "target page, context or browser is already closed" in text
        or "browser has been closed" in text
    )


def _close_context_safely(context: Any, *, logger: logging.Logger) -> None:
    if context is None:
        return
    try:
        context.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("history_context_close_ignored error=%s", _format_exception_message(exc))


def _get_runtime_page(context: Any) -> Any:
    return context.pages[0] if getattr(context, "pages", None) else context.new_page()


def _recreate_runtime_context(
    playwright: Any,
    *,
    context: Any,
    options: RunnerOptions,
    logger: logging.Logger,
) -> tuple[Any, Any]:
    _close_context_safely(context, logger=logger)
    new_context = _launch_context_with_fallback(
        playwright,
        options=options,
        logger=logger,
    )
    new_page = _get_runtime_page(new_context)
    logger.info(
        "history_context_recreated browser=%s user_data_dir=%s",
        options.browser,
        options.user_data_dir,
    )
    return new_context, new_page


def _emit_waiting_history_rows(
    *,
    progress_cb: ProgressCallback | None,
    plan: list[HistoryExecutionPlan],
    start_index: int,
) -> None:
    for queued_activity in plan[start_index + 1 :]:
        if not queued_activity.account_targets:
            continue
        _emit(
            progress_cb,
            {
                "type": "history_row_update",
                "row_id": _history_row_id(
                    brand_code=queued_activity.brand_code,
                    activity_name=queued_activity.activity_name,
                ),
                "status": "Waiting",
                "message": HISTORY_WAITING_FOR_PRIOR_ACTIVITY_MESSAGE,
            },
        )


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

    run_id = _now_run_id()
    run_date_token = dt.datetime.now().strftime("%Y%m%d")
    prepared_user_data_dir = prepare_meta_user_data_dir(requested_dir=user_data_dir)
    options = _ensure_output_roots(
        _build_runner_options(
            browser=browser,
            action_log_dir=Path(action_log_dir),
            trace_dir=Path(trace_dir),
            user_data_dir=prepared_user_data_dir.effective_dir,
        ),
        run_date_token=run_date_token,
    )
    logger = _setup_logger(options.log_dir, verbose=False)
    log_file = _resolve_log_file(logger)
    logger.info(
        "shared_user_data_dir requested=%s effective=%s migration_mode=%s",
        prepared_user_data_dir.requested_dir,
        prepared_user_data_dir.effective_dir,
        prepared_user_data_dir.migration_mode,
    )
    if prepared_user_data_dir.warning:
        logger.warning("shared_user_data_dir_warning %s", prepared_user_data_dir.warning)

    if emit_run_started:
        _emit(
            progress_cb,
            {
                "type": "run_started",
                "run_id": run_id,
                "log_file": log_file,
            },
        )

    _emit(
        progress_cb,
        {
            "type": "login_status",
            "status": "Waiting Login",
            "message": "Waiting for Meta login in browser.",
        },
    )

    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        logger.exception("playwright_import_failed")
        raise RuntimeError("playwright is required. Run: pip install -r requirements.txt") from exc

    outputs: list[HistoryExecutionOutput] = []

    try:
        logger.info(
            "playwright_runtime_bootstrap_start browser=%s headless=%s user_data_dir=%s",
            options.browser,
            options.headless,
            options.user_data_dir,
        )
        with sync_playwright() as playwright:
            logger.info("playwright_runtime_bootstrap_ready")
            context = _launch_context_with_fallback(
                playwright,
                options=options,
                logger=logger,
            )
            try:
                page = _get_runtime_page(context)
                startup_url = _resolve_startup_bootstrap_url(plan)
                if startup_url:
                    page.goto(startup_url, wait_until="domcontentloaded")
                    logger.info(
                        "Opened Ads Manager bootstrap for integrated action-log phase. url=%s",
                        startup_url,
                    )
                    _wait_for_login_context(
                        page,
                        timeout_sec=options.login_timeout_sec,
                        ready_url=startup_url,
                    )
                else:
                    page.goto("https://business.facebook.com/", wait_until="domcontentloaded")
                    logger.info("Opened Meta Business home for integrated action-log phase.")
                _emit(
                    progress_cb,
                    {
                        "type": "login_status",
                        "status": "Logged In",
                        "message": "Meta login confirmed.",
                    },
                )

                for activity_index, activity_plan in enumerate(plan):
                    row_id = _history_row_id(
                        brand_code=activity_plan.brand_code,
                        activity_name=activity_plan.activity_name,
                    )
                    accounts = [
                        AccountTarget(
                            act=target.act,
                            business_id=target.business_id,
                        )
                        for target in activity_plan.account_targets
                    ]
                    account_count = len(accounts)
                    if account_count <= 0:
                        _emit(
                            progress_cb,
                            {
                                "type": "history_row_update",
                                "row_id": row_id,
                                "status": "Skipped",
                                "message": NO_TARGET_ACCOUNTS_MESSAGE,
                            },
                        )
                        continue

                    _emit_waiting_history_rows(
                        progress_cb=progress_cb,
                        plan=plan,
                        start_index=activity_index,
                    )

                    collected_rows: list[list[str]] = []
                    failed_accounts: list[str] = []
                    failure_messages: list[str] = []
                    account_index = 0

                    while account_index < account_count:
                        account = accounts[account_index]
                        _emit(
                            progress_cb,
                            {
                                "type": "history_row_update",
                                "row_id": row_id,
                                "status": "Running",
                                "message": HISTORY_RUNNING_ACCOUNT_TEMPLATE.format(
                                    current=account_index + 1,
                                    total=account_count,
                                ),
                            },
                        )
                        recovered_once = False
                        while True:
                            try:
                                rows = _collect_for_account(
                                    page=page,
                                    logger=logger,
                                    options=options,
                                    activity_prefix=activity_plan.activity_name,
                                    account=account,
                                    force_ui_fallback=False,
                                )
                                logger.info(
                                    "account_collect_done activity=%s account=%s/%s rows=%s",
                                    activity_plan.activity_name,
                                    account.act,
                                    account.business_id,
                                    len(rows),
                                )
                                collected_rows.extend(rows)
                                break
                            except ManualInterventionRequired as exc:
                                failed_accounts.append(f"{account.act}/{account.business_id}")
                                logger.exception(
                                    "account_collect_failed activity=%s account=%s/%s error=%s",
                                    activity_plan.activity_name,
                                    account.act,
                                    account.business_id,
                                    exc,
                                )
                                failure_messages.append(
                                    f"{account.act}/{account.business_id}: {_format_exception_message(exc)}"
                                )
                                break
                            except Exception as exc:  # noqa: BLE001
                                if _is_dead_browser_exception(exc) and not recovered_once:
                                    logger.warning(
                                        "account_collect_recovering_context activity=%s account=%s/%s error=%s",
                                        activity_plan.activity_name,
                                        account.act,
                                        account.business_id,
                                        _format_exception_message(exc),
                                    )
                                    context, page = _recreate_runtime_context(
                                        playwright,
                                        context=context,
                                        options=options,
                                        logger=logger,
                                    )
                                    recovered_once = True
                                    continue

                                failed_accounts.append(f"{account.act}/{account.business_id}")
                                logger.exception(
                                    "account_collect_failed activity=%s account=%s/%s error=%s",
                                    activity_plan.activity_name,
                                    account.act,
                                    account.business_id,
                                    exc,
                                )
                                failure_messages.append(
                                    f"{account.act}/{account.business_id}: {_format_exception_message(exc)}"
                                )
                                if _is_dead_browser_exception(exc):
                                    try:
                                        context, page = _recreate_runtime_context(
                                            playwright,
                                            context=context,
                                            options=options,
                                            logger=logger,
                                        )
                                    except Exception as recovery_exc:  # noqa: BLE001
                                        logger.warning(
                                            "account_collect_post_failure_recovery_failed activity=%s account=%s/%s error=%s",
                                            activity_plan.activity_name,
                                            account.act,
                                            account.business_id,
                                            _format_exception_message(recovery_exc),
                                        )
                                break
                        account_index += 1

                    deduped = _dedupe_rows(collected_rows)
                    if not deduped:
                        if failed_accounts:
                            failure_message = (
                                f"{HISTORY_FAILED_PREFIX}: {failure_messages[0]}"
                                if failure_messages
                                else HISTORY_FAILED_PREFIX
                            )
                            logger.error(
                                "activity_failed_without_rows brand=%s activity=%s failed_accounts=%s message=%s",
                                activity_plan.brand_name,
                                activity_plan.activity_name,
                                failed_accounts,
                                failure_message,
                            )
                            _emit(
                                progress_cb,
                                {
                                    "type": "history_row_update",
                                    "row_id": row_id,
                                    "status": "Failed",
                                    "message": failure_message,
                                },
                            )
                        else:
                            logger.warning(
                                "activity_skipped_without_rows brand=%s activity=%s message=%s",
                                activity_plan.brand_name,
                                activity_plan.activity_name,
                                NO_ACTION_LOG_ROWS_MESSAGE,
                            )
                            _emit(
                                progress_cb,
                                {
                                    "type": "history_row_update",
                                    "row_id": row_id,
                                    "status": "Skipped",
                                    "message": NO_ACTION_LOG_ROWS_MESSAGE,
                                },
                            )
                        continue

                    output_path = _build_output_file_path(
                        options=options,
                        activity_prefix=activity_plan.activity_name,
                    )
                    _save_activity_xlsx(rows=deduped, output_path=output_path)

                    outputs.append(
                        HistoryExecutionOutput(
                            brand_name=activity_plan.brand_name,
                            activity_name=activity_plan.activity_name,
                            file_path=str(output_path),
                            row_count=len(deduped),
                            failed_accounts=failed_accounts,
                        )
                    )

                    if failed_accounts:
                        failure_message = (
                            f"{HISTORY_PARTIAL_SAVED_PREFIX}:{Path(output_path).name} / "
                            f"\uccab \uc2e4\ud328:{failure_messages[0]}"
                            if failure_messages
                            else f"{HISTORY_PARTIAL_SAVED_PREFIX}:{Path(output_path).name}"
                        )
                        logger.warning(
                            "activity_output_saved_with_failures brand=%s activity=%s rows=%s path=%s failed_accounts=%s",
                            activity_plan.brand_name,
                            activity_plan.activity_name,
                            len(deduped),
                            output_path,
                            failed_accounts,
                        )
                        _emit(
                            progress_cb,
                            {
                                "type": "history_row_update",
                                "row_id": row_id,
                                "status": "Failed",
                                "message": failure_message,
                            },
                        )
                        _emit(
                            progress_cb,
                            {
                                "type": "history_result",
                                "brand": activity_plan.brand_name,
                                "activity": activity_plan.activity_name,
                                "file_path": str(output_path),
                                "row_count": len(deduped),
                                "failed_accounts": failed_accounts,
                                "message": failure_message,
                            },
                        )
                        continue

                    success_message = f"{HISTORY_SAVED_PREFIX}:{Path(output_path).name}"
                    _emit(
                        progress_cb,
                        {
                            "type": "history_row_update",
                            "row_id": row_id,
                            "status": "Completed",
                            "message": success_message,
                        },
                    )
                    _emit(
                        progress_cb,
                        {
                            "type": "history_result",
                            "brand": activity_plan.brand_name,
                            "activity": activity_plan.activity_name,
                            "file_path": str(output_path),
                            "row_count": len(deduped),
                            "failed_accounts": failed_accounts,
                            "message": success_message,
                        },
                    )
            finally:
                _close_context_safely(context, logger=logger)
    except Exception as exc:  # noqa: BLE001
        error_text = _format_exception_message(exc)
        logger.exception("integrated_history_runtime_failed error=%s", error_text)
        raise RuntimeError(error_text) from exc

    return HistoryAdapterExecutionResult(
        run_id=run_id,
        log_file=log_file,
        outputs=outputs,
    )
