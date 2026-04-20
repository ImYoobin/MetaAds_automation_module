"""Selection validation and execution-plan builders."""

from __future__ import annotations

from typing import Any

from dashboard.models import (
    ActivityExecutionPlan,
    DISPLAY_TO_INTERNAL_SHEET_KEY,
    HistoryAccountTarget,
    HistoryExecutionPlan,
    ReadinessRow,
    SHEET_DISPLAY_ORDER,
    SheetExecutionPlan,
    ValidationResult,
    build_activity_id,
)
from dashboard.services.url_service import is_cleaned_url_valid, parse_cleaned_url


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _iter_brands(config: dict[str, Any]) -> list[dict[str, Any]]:
    brands = config.get("brands", [])
    return brands if isinstance(brands, list) else []


def _iter_activities(brand: dict[str, Any]) -> list[dict[str, Any]]:
    activities = brand.get("activities", [])
    return activities if isinstance(activities, list) else []


def _extract_valid_urls(
    activity: dict[str, Any],
    *,
    default_view_event_source: str,
) -> dict[str, list[str]]:
    reports = activity.get("reports")
    if not isinstance(reports, dict):
        return {sheet: [] for sheet in SHEET_DISPLAY_ORDER}

    out: dict[str, list[str]] = {}
    for sheet in SHEET_DISPLAY_ORDER:
        raw_entries = reports.get(sheet, [])
        if not isinstance(raw_entries, list):
            raw_entries = [raw_entries]

        valid_urls: list[str] = []
        for item in raw_entries:
            url = _safe_text(item.get("url")) if isinstance(item, dict) else _safe_text(item)
            if not url:
                continue
            ok, _ = is_cleaned_url_valid(url, default_event_source=default_view_event_source)
            if ok:
                valid_urls.append(url)
        out[sheet] = valid_urls

    return out


def _extract_history_account_targets(
    activity: dict[str, Any],
    *,
    default_view_event_source: str,
) -> list[HistoryAccountTarget]:
    valid_by_sheet = _extract_valid_urls(
        activity,
        default_view_event_source=default_view_event_source,
    )
    deduped_pairs: set[tuple[str, str]] = set()
    targets: list[HistoryAccountTarget] = []

    for urls in valid_by_sheet.values():
        for url in urls:
            parsed = parse_cleaned_url(url, default_event_source=default_view_event_source)
            pair = (parsed.act_id, parsed.business_id)
            if pair in deduped_pairs:
                continue
            deduped_pairs.add(pair)
            targets.append(
                HistoryAccountTarget(
                    act=parsed.act_id,
                    business_id=parsed.business_id,
                )
            )

    return targets


def validate_execution_modes(
    *,
    enable_report_download: bool,
    enable_action_log_download: bool,
) -> list[str]:
    if enable_report_download or enable_action_log_download:
        return []
    return ["최소 한 개의 실행 옵션을 선택해주세요."]


def build_readiness_rows(
    config: dict[str, Any],
    selected_activity_ids: set[str],
) -> list[ReadinessRow]:
    default_view_event_source = _safe_text(config.get("view_event_source"))
    rows: list[ReadinessRow] = []

    for brand in _iter_brands(config):
        brand_code = _safe_text(brand.get("code"))
        brand_name = _safe_text(brand.get("name"))
        for activity in _iter_activities(brand):
            activity_name = _safe_text(activity.get("name"))
            activity_id = build_activity_id(
                brand_code=brand_code,
                activity_name=activity_name,
            )
            if activity_id not in selected_activity_ids:
                continue

            valid_by_sheet = _extract_valid_urls(
                activity,
                default_view_event_source=default_view_event_source,
            )
            url_count = sum(len(items) for items in valid_by_sheet.values())
            sheet_count = sum(1 for items in valid_by_sheet.values() if items)
            rows.append(
                ReadinessRow(
                    brand=brand_name,
                    activity=activity_name,
                    sheet_count=sheet_count,
                    url_count=url_count,
                )
            )

    return rows


def validate_run_selection(
    config: dict[str, Any],
    selected_activity_ids: set[str],
    *,
    enable_report_download: bool = True,
    enable_action_log_download: bool = True,
) -> ValidationResult:
    mode_reasons = validate_execution_modes(
        enable_report_download=enable_report_download,
        enable_action_log_download=enable_action_log_download,
    )
    if mode_reasons:
        return ValidationResult(
            can_run=False,
            reasons=mode_reasons,
            readiness_rows=[],
        )

    if not selected_activity_ids:
        return ValidationResult(
            can_run=False,
            reasons=["선택된 액티비티가 없습니다."],
            readiness_rows=[],
        )

    readiness_rows = build_readiness_rows(config, selected_activity_ids)
    if not readiness_rows:
        return ValidationResult(
            can_run=False,
            reasons=["선택한 액티비티를 찾을 수 없습니다."],
            readiness_rows=[],
        )

    missing_url_ids: list[str] = []
    default_view_event_source = _safe_text(config.get("view_event_source"))
    for brand in _iter_brands(config):
        brand_code = _safe_text(brand.get("code"))
        for activity in _iter_activities(brand):
            activity_name = _safe_text(activity.get("name"))
            activity_id = build_activity_id(
                brand_code=brand_code,
                activity_name=activity_name,
            )
            if activity_id not in selected_activity_ids:
                continue

            valid_by_sheet = _extract_valid_urls(
                activity,
                default_view_event_source=default_view_event_source,
            )
            total_url_count = sum(len(items) for items in valid_by_sheet.values())
            if total_url_count <= 0:
                missing_url_ids.append(activity_id)

    if missing_url_ids:
        return ValidationResult(
            can_run=False,
            reasons=[
                "선택한 액티비티에 등록된 report URL이 없습니다.",
                "Report URL을 추가한 뒤 다시 시도해주세요.",
            ],
            missing_url_activity_ids=missing_url_ids,
            readiness_rows=readiness_rows,
        )

    return ValidationResult(
        can_run=True,
        reasons=[],
        readiness_rows=readiness_rows,
    )


def build_execution_plan(
    config: dict[str, Any],
    selected_activity_ids: set[str],
) -> list[ActivityExecutionPlan]:
    default_view_event_source = _safe_text(config.get("view_event_source"))
    plan: list[ActivityExecutionPlan] = []

    for brand in _iter_brands(config):
        brand_code = _safe_text(brand.get("code"))
        brand_name = _safe_text(brand.get("name"))
        for activity in _iter_activities(brand):
            activity_name = _safe_text(activity.get("name"))
            activity_id = build_activity_id(
                brand_code=brand_code,
                activity_name=activity_name,
            )
            if activity_id not in selected_activity_ids:
                continue

            valid_by_sheet = _extract_valid_urls(
                activity,
                default_view_event_source=default_view_event_source,
            )
            sheets: list[SheetExecutionPlan] = []
            for sheet_display in SHEET_DISPLAY_ORDER:
                sheets.append(
                    SheetExecutionPlan(
                        sheet_display_name=sheet_display,
                        sheet_key=DISPLAY_TO_INTERNAL_SHEET_KEY[sheet_display],
                        urls=list(valid_by_sheet.get(sheet_display, [])),
                    )
                )

            plan.append(
                ActivityExecutionPlan(
                    brand_code=brand_code,
                    brand_name=brand_name,
                    activity_name=activity_name,
                    sheets=sheets,
                )
            )

    return plan


def build_history_execution_plan(
    config: dict[str, Any],
    selected_activity_ids: set[str],
) -> list[HistoryExecutionPlan]:
    default_view_event_source = _safe_text(config.get("view_event_source"))
    plan: list[HistoryExecutionPlan] = []

    for brand in _iter_brands(config):
        brand_code = _safe_text(brand.get("code"))
        brand_name = _safe_text(brand.get("name"))
        for activity in _iter_activities(brand):
            activity_name = _safe_text(activity.get("name"))
            activity_id = build_activity_id(
                brand_code=brand_code,
                activity_name=activity_name,
            )
            if activity_id not in selected_activity_ids:
                continue

            plan.append(
                HistoryExecutionPlan(
                    brand_code=brand_code,
                    brand_name=brand_name,
                    activity_name=activity_name,
                    account_targets=_extract_history_account_targets(
                        activity,
                        default_view_event_source=default_view_event_source,
                    ),
                )
            )

    return plan
