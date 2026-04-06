"""Config schema + validation for standalone Meta export."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import (
    ALL_SHEETS,
    DEFAULT_ACTIVITY_NAME,
    DEFAULT_BRAND_CODE,
    DEFAULT_BRAND_NAME,
    DEFAULT_EXPORT_EVENT_SOURCE,
    DEFAULT_VIEW_EVENT_SOURCE,
    REQUIRED_SHEETS,
)


class ConfigValidationError(ValueError):
    """Raised when standalone config is invalid."""


_SHEET_ALIASES: dict[str, str] = {
    "overall": "overall",
    "demo": "demo",
    "time": "time",
    "overall_bof": "overall_bof",
    "overall-bof": "overall_bof",
    "overallbof": "overall_bof",
    "demo_bof": "demo_bof",
    "demo-bof": "demo_bof",
    "demobof": "demo_bof",
    "time_bof": "time_bof",
    "time-bof": "time_bof",
    "timebof": "time_bof",
}


@dataclass(frozen=True)
class AccountGroups:
    """Reserved for future expansion (multi account select/concat)."""

    primary: tuple[str, ...] = ()
    bof: tuple[str, ...] = ()


@dataclass(frozen=True)
class SheetConfig:
    act_id: str
    business_id: str
    global_scope_id: str
    report_id: str
    enabled: bool = True


@dataclass(frozen=True)
class StandaloneMetaExportConfig:
    brand_code: str
    brand_name: str
    activity_name: str
    view_event_source: str
    export_event_source: str
    selection_mode: str
    account_groups: AccountGroups
    sheet_config_by_key: dict[str, SheetConfig]


def normalize_sheet_key(sheet_key: str) -> str:
    raw = str(sheet_key or "").strip().lower().replace("-", "_")
    raw = "_".join(part for part in raw.split("_") if part)
    return _SHEET_ALIASES.get(raw, raw)


def _as_string(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return default


def _coerce_sheet_config(raw: dict[str, Any]) -> SheetConfig:
    act_id = _as_string(raw.get("act_id") or raw.get("meta_act_id") or raw.get("act"))
    business_id = _as_string(
        raw.get("business_id") or raw.get("meta_business_id") or raw.get("business")
    )
    global_scope_id = _as_string(
        raw.get("global_scope_id")
        or raw.get("meta_global_scope_id")
        or raw.get("global_scope")
        or business_id
    )
    report_id = _as_string(
        raw.get("report_id") or raw.get("selected_report_id") or raw.get("meta_report_id")
    )
    enabled = _as_bool(raw.get("enabled"), True)
    return SheetConfig(
        act_id=act_id,
        business_id=business_id,
        global_scope_id=global_scope_id or business_id,
        report_id=report_id,
        enabled=enabled,
    )


def _validate_required_sheets(sheet_config_by_key: dict[str, SheetConfig]) -> None:
    missing_required_sheet: list[str] = []
    missing_fields: list[str] = []

    for sheet_key in REQUIRED_SHEETS:
        config = sheet_config_by_key.get(sheet_key)
        if not config:
            missing_required_sheet.append(sheet_key)
            continue
        if not config.enabled:
            continue
        fields = []
        if not config.act_id:
            fields.append("act_id")
        if not config.business_id:
            fields.append("business_id")
        if not config.report_id:
            fields.append("report_id")
        if fields:
            missing_fields.append(f"{sheet_key}[{', '.join(fields)}]")

    if missing_required_sheet or missing_fields:
        chunks = []
        if missing_required_sheet:
            chunks.append("missing sheets: " + ", ".join(missing_required_sheet))
        if missing_fields:
            chunks.append("missing fields: " + "; ".join(missing_fields))
        raise ConfigValidationError(
            "Invalid standalone Meta config (" + " | ".join(chunks) + ")."
        )


def parse_config(raw: Any) -> StandaloneMetaExportConfig:
    if not isinstance(raw, dict):
        raise ConfigValidationError("Root config JSON must be an object.")

    brand_raw = raw.get("brand")
    if not isinstance(brand_raw, dict):
        brand_raw = {}

    brand_code = _as_string(brand_raw.get("code")) or DEFAULT_BRAND_CODE
    brand_name = _as_string(brand_raw.get("name")) or DEFAULT_BRAND_NAME
    activity_name = _as_string(raw.get("activity_name")) or DEFAULT_ACTIVITY_NAME

    view_event_source = (
        _as_string(raw.get("view_event_source")) or DEFAULT_VIEW_EVENT_SOURCE
    )
    export_event_source = (
        _as_string(raw.get("export_event_source")) or DEFAULT_EXPORT_EVENT_SOURCE
    )
    selection_mode = _as_string(raw.get("selection_mode")) or "single"

    account_groups_raw = raw.get("account_groups")
    if not isinstance(account_groups_raw, dict):
        account_groups_raw = {}
    account_groups = AccountGroups(
        primary=tuple(
            _as_string(item)
            for item in account_groups_raw.get("primary", [])
            if _as_string(item)
        ),
        bof=tuple(
            _as_string(item) for item in account_groups_raw.get("bof", []) if _as_string(item)
        ),
    )

    sheet_config_raw = raw.get("sheet_config_by_key")
    if not isinstance(sheet_config_raw, dict):
        raise ConfigValidationError("`sheet_config_by_key` must be a JSON object.")

    sheet_config_by_key: dict[str, SheetConfig] = {}
    for key, value in sheet_config_raw.items():
        normalized_key = normalize_sheet_key(str(key))
        if normalized_key not in ALL_SHEETS:
            continue
        if not isinstance(value, dict):
            value = {"report_id": value}
        sheet_config_by_key[normalized_key] = _coerce_sheet_config(value)

    _validate_required_sheets(sheet_config_by_key)

    return StandaloneMetaExportConfig(
        brand_code=brand_code,
        brand_name=brand_name,
        activity_name=activity_name,
        view_event_source=view_event_source,
        export_event_source=export_event_source,
        selection_mode=selection_mode,
        account_groups=account_groups,
        sheet_config_by_key=sheet_config_by_key,
    )


def load_config(path: str | Path) -> StandaloneMetaExportConfig:
    config_path = Path(path).expanduser().resolve()
    if not config_path.exists():
        raise ConfigValidationError(f"Config file not found: {config_path}")

    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(f"Invalid JSON config: {config_path}") from exc

    return parse_config(raw)
