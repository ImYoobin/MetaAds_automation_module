"""Shared models for Streamlit dashboard orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


SHEET_DISPLAY_ORDER: tuple[str, ...] = (
    "Overall",
    "Demo",
    "Overall_BoF",
    "Demo_BoF",
    "Time",
    "Time_BoF",
)

DISPLAY_TO_INTERNAL_SHEET_KEY: dict[str, str] = {
    "Overall": "overall",
    "Demo": "demo",
    "Overall_BoF": "overall_bof",
    "Demo_BoF": "demo_bof",
    "Time": "time",
    "Time_BoF": "time_bof",
}

INTERNAL_TO_DISPLAY_SHEET_KEY: dict[str, str] = {
    value: key for key, value in DISPLAY_TO_INTERNAL_SHEET_KEY.items()
}

INTERNAL_TO_WORKBOOK_SHEET_NAME: dict[str, str] = {
    "overall": "Overall",
    "demo": "Demo",
    "overall_bof": "Overall-BoF",
    "demo_bof": "Demo-BoF",
    "time": "Time",
    "time_bof": "Time-BoF",
}


def build_activity_id(*, brand_code: str, activity_name: str) -> str:
    return f"{str(brand_code or '').strip()}::{str(activity_name or '').strip()}"


@dataclass(frozen=True)
class ParsedReportUrl:
    raw_url: str
    cleaned_url: str
    act_id: str
    business_id: str
    global_scope_id: str
    report_id: str
    event_source: str = ""


@dataclass(frozen=True)
class SheetExecutionPlan:
    sheet_display_name: str
    sheet_key: str
    urls: list[str]


@dataclass(frozen=True)
class ActivityExecutionPlan:
    brand_code: str
    brand_name: str
    activity_name: str
    sheets: list[SheetExecutionPlan]


@dataclass(frozen=True)
class HistoryAccountTarget:
    act: str
    business_id: str


@dataclass(frozen=True)
class HistoryExecutionPlan:
    brand_code: str
    brand_name: str
    activity_name: str
    account_targets: list[HistoryAccountTarget]


@dataclass(frozen=True)
class ReadinessRow:
    brand: str
    activity: str
    sheet_count: int
    url_count: int


@dataclass(frozen=True)
class ValidationResult:
    can_run: bool
    reasons: list[str] = field(default_factory=list)
    missing_url_activity_ids: list[str] = field(default_factory=list)
    readiness_rows: list[ReadinessRow] = field(default_factory=list)


@dataclass(frozen=True)
class LogRow:
    row_id: str
    brand: str
    activity: str
    sheet: str
    url_count: int
    status: str
    message: str
    last_updated: str
    missing_columns_text: str = ""


@dataclass(frozen=True)
class HistoryLogRow:
    row_id: str
    brand: str
    activity: str
    account_count: int
    status: str
    message: str
    last_updated: str


@dataclass(frozen=True)
class ActivityExecutionOutput:
    brand_name: str
    activity_name: str
    workbook_path: str
    raw_files_by_sheet: dict[str, list[str]]


@dataclass(frozen=True)
class AdapterExecutionResult:
    run_id: str
    log_file: str
    outputs: list[ActivityExecutionOutput]


@dataclass(frozen=True)
class HistoryExecutionOutput:
    brand_name: str
    activity_name: str
    file_path: str
    row_count: int
    failed_accounts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class HistoryAdapterExecutionResult:
    run_id: str
    log_file: str
    outputs: list[HistoryExecutionOutput]


@dataclass(frozen=True)
class ConfigLoadResult:
    config: dict
    messages: list[str]
    path: Path
