"""Activity catalog parser for standalone Meta Ads export runner."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import (
    ConfigValidationError,
    StandaloneMetaExportConfig,
    normalize_sheet_key,
    parse_config,
)
from .constants import ALL_SHEETS, DEFAULT_EXPORT_EVENT_SOURCE, DEFAULT_VIEW_EVENT_SOURCE


class CatalogValidationError(ValueError):
    """Raised when activity catalog JSON is invalid."""


@dataclass(frozen=True)
class CatalogActivity:
    brand_code: str
    brand_name: str
    activity_name: str
    config: StandaloneMetaExportConfig
    enabled: bool = True


@dataclass(frozen=True)
class CatalogBrand:
    brand_code: str
    brand_name: str
    activities: tuple[CatalogActivity, ...]
    enabled: bool = True


@dataclass(frozen=True)
class StandaloneActivityCatalog:
    brands: tuple[CatalogBrand, ...]


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


def _normalize_sheet_map(
    *,
    raw_report_map: dict[str, Any],
    default_act_id: str,
    default_business_id: str,
    default_global_scope_id: str,
) -> dict[str, dict[str, Any]]:
    """
    Normalize catalog sheet map to config-compatible `sheet_config_by_key`.

    Supported values per sheet key:
    - string: report_id only (uses activity/brand-level account IDs)
    - object: report_id + optional per-sheet act/business/global overrides
    """
    normalized: dict[str, dict[str, str | bool]] = {}
    for sheet_key, sheet_value in raw_report_map.items():
        normalized_key = normalize_sheet_key(str(sheet_key))
        if normalized_key not in ALL_SHEETS:
            continue

        if isinstance(sheet_value, dict):
            enabled = _as_bool(sheet_value.get("enabled"), True)
            report_id = _as_string(
                sheet_value.get("report_id")
                or sheet_value.get("selected_report_id")
                or sheet_value.get("meta_report_id")
                or sheet_value.get("id")
            )
            act_id = _as_string(sheet_value.get("act_id") or sheet_value.get("act")) or default_act_id
            business_id = (
                _as_string(
                    sheet_value.get("business_id")
                    or sheet_value.get("meta_business_id")
                    or sheet_value.get("business")
                )
                or default_business_id
            )
            global_scope_id = (
                _as_string(
                    sheet_value.get("global_scope_id")
                    or sheet_value.get("meta_global_scope_id")
                    or sheet_value.get("global_scope")
                )
                or default_global_scope_id
                or business_id
            )
        else:
            enabled = True
            report_id = _as_string(sheet_value)
            act_id = default_act_id
            business_id = default_business_id
            global_scope_id = default_global_scope_id or business_id

        normalized[normalized_key] = {
            "act_id": act_id,
            "business_id": business_id,
            "global_scope_id": global_scope_id,
            "report_id": report_id,
            "enabled": enabled,
        }
    return normalized


def _parse_activity_config(
    *,
    brand_code: str,
    brand_name: str,
    brand_act_id: str,
    brand_business_id: str,
    brand_global_scope_id: str,
    view_event_source: str,
    export_event_source: str,
    activity_raw: dict[str, Any],
) -> StandaloneMetaExportConfig:
    activity_name = _as_string(activity_raw.get("name") or activity_raw.get("activity_name"))
    if not activity_name:
        raise CatalogValidationError("Each activity must include `name` or `activity_name`.")

    activity_act_id = _as_string(activity_raw.get("act_id") or activity_raw.get("act")) or brand_act_id
    activity_business_id = _as_string(activity_raw.get("business_id")) or brand_business_id
    activity_global_scope_id = (
        _as_string(activity_raw.get("global_scope_id")) or brand_global_scope_id or activity_business_id
    )

    report_map_raw = activity_raw.get("report_id_by_key")
    if not isinstance(report_map_raw, dict):
        raise CatalogValidationError(
            f"Activity `{activity_name}` must include `report_id_by_key` as an object."
        )
    sheet_config_by_key = _normalize_sheet_map(
        raw_report_map=report_map_raw,
        default_act_id=activity_act_id,
        default_business_id=activity_business_id,
        default_global_scope_id=activity_global_scope_id,
    )

    raw_single_config = {
        "brand": {
            "code": brand_code,
            "name": brand_name,
        },
        "activity_name": activity_name,
        "view_event_source": view_event_source,
        "export_event_source": export_event_source,
        "selection_mode": "single",
        "sheet_config_by_key": sheet_config_by_key,
    }
    try:
        return parse_config(raw_single_config)
    except ConfigValidationError as exc:
        raise CatalogValidationError(
            f"Invalid sheet mapping for brand `{brand_code}` activity `{activity_name}`: {exc}"
        ) from exc


def parse_activity_catalog(raw: Any) -> StandaloneActivityCatalog:
    if not isinstance(raw, dict):
        raise CatalogValidationError("Root catalog JSON must be an object.")

    root_view_event_source = _as_string(raw.get("view_event_source")) or DEFAULT_VIEW_EVENT_SOURCE
    root_export_event_source = _as_string(raw.get("export_event_source")) or DEFAULT_EXPORT_EVENT_SOURCE

    brands_raw = raw.get("brands")
    if not isinstance(brands_raw, list) or not brands_raw:
        raise CatalogValidationError("`brands` must be a non-empty array.")

    brands: list[CatalogBrand] = []
    for brand_raw in brands_raw:
        if not isinstance(brand_raw, dict):
            raise CatalogValidationError("Each item in `brands` must be an object.")

        brand_enabled = _as_bool(brand_raw.get("enabled"), True)
        if not brand_enabled:
            continue

        brand_code = _as_string(brand_raw.get("code"))
        brand_name = _as_string(brand_raw.get("name"))
        brand_act_id = _as_string(brand_raw.get("act_id") or brand_raw.get("act"))
        brand_business_id = _as_string(brand_raw.get("business_id"))
        brand_global_scope_id = _as_string(brand_raw.get("global_scope_id")) or brand_business_id
        brand_view_event_source = (
            _as_string(brand_raw.get("view_event_source")) or root_view_event_source
        )
        brand_export_event_source = (
            _as_string(brand_raw.get("export_event_source")) or root_export_event_source
        )

        missing: list[str] = []
        if not brand_code:
            missing.append("code")
        if not brand_name:
            missing.append("name")
        if not brand_act_id:
            missing.append("act_id")
        if not brand_business_id:
            missing.append("business_id")
        if missing:
            raise CatalogValidationError(
                f"Brand catalog entry is missing required fields: {', '.join(missing)}."
            )

        activities_raw = brand_raw.get("activities")
        if not isinstance(activities_raw, list) or not activities_raw:
            raise CatalogValidationError(f"Brand `{brand_code}` must include non-empty `activities`.")

        activities: list[CatalogActivity] = []
        for activity_raw in activities_raw:
            if not isinstance(activity_raw, dict):
                raise CatalogValidationError(
                    f"Brand `{brand_code}` contains a non-object activity entry."
                )
            activity_enabled = _as_bool(activity_raw.get("enabled"), True)
            if not activity_enabled:
                continue
            config = _parse_activity_config(
                brand_code=brand_code,
                brand_name=brand_name,
                brand_act_id=brand_act_id,
                brand_business_id=brand_business_id,
                brand_global_scope_id=brand_global_scope_id,
                view_event_source=brand_view_event_source,
                export_event_source=brand_export_event_source,
                activity_raw=activity_raw,
            )
            activities.append(
                CatalogActivity(
                    brand_code=brand_code,
                    brand_name=brand_name,
                    activity_name=config.activity_name,
                    config=config,
                    enabled=True,
                )
            )
        if not activities:
            continue
        brands.append(
            CatalogBrand(
                brand_code=brand_code,
                brand_name=brand_name,
                activities=tuple(activities),
                enabled=True,
            )
        )

    return StandaloneActivityCatalog(brands=tuple(brands))


def load_activity_catalog(path: str | Path) -> StandaloneActivityCatalog:
    catalog_path = Path(path).expanduser().resolve()
    if not catalog_path.exists():
        raise CatalogValidationError(f"Catalog file not found: {catalog_path}")

    try:
        raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CatalogValidationError(f"Invalid JSON catalog: {catalog_path}") from exc

    return parse_activity_catalog(raw)
