"""Config load/save and CRUD helpers for dashboard."""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

from dashboard.models import (
    ConfigLoadResult,
    DISPLAY_TO_INTERNAL_SHEET_KEY,
    INTERNAL_TO_DISPLAY_SHEET_KEY,
    SHEET_DISPLAY_ORDER,
)
from dashboard.services.url_service import (
    UrlValidationError,
    build_cleaned_url_from_parts,
    clean_report_url,
)
from meta_core.config import normalize_sheet_key
from meta_core.constants import (
    DEFAULT_EXPORT_EVENT_SOURCE,
    DEFAULT_VIEW_EVENT_SOURCE,
)


DEFAULT_CONFIG_PATH = Path("config/meta/activity_catalog.json")
DEFAULT_CONFIG_EXAMPLE_NAME = "activity_catalog.example.json"


def _default_reports() -> dict[str, list[dict[str, str]]]:
    return {sheet: [] for sheet in SHEET_DISPLAY_ORDER}


def default_config() -> dict[str, Any]:
    return {
        "view_event_source": DEFAULT_VIEW_EVENT_SOURCE,
        "export_event_source": DEFAULT_EXPORT_EVENT_SOURCE,
        "brands": [],
    }


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _slugify(value: str) -> str:
    text = _safe_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "brand"


def _is_truthy(value: Any, *, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = _safe_text(value).lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return default


def _unique_code(*, seed: str, existing: set[str]) -> str:
    base = _slugify(seed)
    if base not in existing:
        return base
    suffix = 2
    while f"{base}_{suffix}" in existing:
        suffix += 1
    return f"{base}_{suffix}"


def _iter_sheet_aliases(sheet_display_name: str) -> tuple[str, ...]:
    internal = DISPLAY_TO_INTERNAL_SHEET_KEY[sheet_display_name]
    compact = internal.replace("_", "")
    hyphen = internal.replace("_", "-")
    return (
        sheet_display_name,
        internal,
        internal.lower(),
        compact,
        hyphen,
        sheet_display_name.replace("_", "-"),
    )


def _normalize_reports_from_new(
    reports_raw: Any,
    *,
    default_view_event_source: str,
) -> dict[str, list[dict[str, str]]]:
    out = _default_reports()
    if not isinstance(reports_raw, dict):
        return out

    for sheet_name in SHEET_DISPLAY_ORDER:
        raw_entries: Any = None
        for alias in _iter_sheet_aliases(sheet_name):
            if alias in reports_raw:
                raw_entries = reports_raw.get(alias)
                break
        if raw_entries is None:
            continue
        if not isinstance(raw_entries, list):
            raw_entries = [raw_entries]

        cleaned_entries: list[dict[str, str]] = []
        for item in raw_entries:
            if isinstance(item, dict):
                raw_url = _safe_text(item.get("url"))
            else:
                raw_url = _safe_text(item)
            if not raw_url:
                continue
            try:
                cleaned = clean_report_url(
                    raw_url,
                    default_event_source=default_view_event_source,
                )
            except UrlValidationError:
                continue
            cleaned_entries.append({"url": cleaned})
        out[sheet_name] = cleaned_entries
    return out


def _extract_report_id(raw_value: Any) -> str:
    if isinstance(raw_value, dict):
        return _safe_text(
            raw_value.get("report_id")
            or raw_value.get("selected_report_id")
            or raw_value.get("meta_report_id")
            or raw_value.get("id")
        )
    return _safe_text(raw_value)


def _normalize_reports_from_legacy(
    *,
    brand_raw: dict[str, Any],
    activity_raw: dict[str, Any],
    default_view_event_source: str,
) -> dict[str, list[dict[str, str]]]:
    out = _default_reports()
    source_map = activity_raw.get("report_id_by_key") or activity_raw.get("sheet_config_by_key")
    if not isinstance(source_map, dict):
        return out

    activity_act_id = _safe_text(activity_raw.get("act_id") or activity_raw.get("act"))
    activity_business_id = _safe_text(activity_raw.get("business_id"))
    activity_scope_id = _safe_text(activity_raw.get("global_scope_id"))
    brand_act_id = _safe_text(brand_raw.get("act_id") or brand_raw.get("act"))
    brand_business_id = _safe_text(brand_raw.get("business_id"))
    brand_scope_id = _safe_text(brand_raw.get("global_scope_id"))
    effective_event_source = (
        _safe_text(activity_raw.get("view_event_source"))
        or _safe_text(brand_raw.get("view_event_source"))
        or default_view_event_source
    )

    for key, raw_value in source_map.items():
        sheet_key = normalize_sheet_key(str(key or ""))
        sheet_display = INTERNAL_TO_DISPLAY_SHEET_KEY.get(sheet_key)
        if not sheet_display:
            continue
        if isinstance(raw_value, dict) and not _is_truthy(raw_value.get("enabled"), default=True):
            continue

        report_id = _extract_report_id(raw_value)
        if not report_id:
            continue

        if isinstance(raw_value, dict):
            act_id = _safe_text(raw_value.get("act_id") or raw_value.get("act"))
            business_id = _safe_text(
                raw_value.get("business_id")
                or raw_value.get("meta_business_id")
                or raw_value.get("business")
            )
            scope_id = _safe_text(
                raw_value.get("global_scope_id")
                or raw_value.get("meta_global_scope_id")
                or raw_value.get("global_scope")
            )
        else:
            act_id = ""
            business_id = ""
            scope_id = ""

        act_value = act_id or activity_act_id or brand_act_id
        business_value = business_id or activity_business_id or brand_business_id
        scope_value = scope_id or activity_scope_id or brand_scope_id or business_value
        if not act_value or not business_value:
            continue
        cleaned_url = build_cleaned_url_from_parts(
            act_id=act_value,
            business_id=business_value,
            global_scope_id=scope_value,
            report_id=report_id,
            event_source=effective_event_source,
        )
        out[sheet_display].append({"url": cleaned_url})
    return out


def _normalize_activity(
    *,
    activity_raw: Any,
    brand_raw: dict[str, Any],
    default_view_event_source: str,
) -> dict[str, Any] | None:
    if not isinstance(activity_raw, dict):
        return None
    activity_name = _safe_text(activity_raw.get("name") or activity_raw.get("activity_name"))
    if not activity_name:
        return None

    reports = _normalize_reports_from_new(
        activity_raw.get("reports"),
        default_view_event_source=default_view_event_source,
    )
    if all(len(items) == 0 for items in reports.values()):
        reports = _normalize_reports_from_legacy(
            brand_raw=brand_raw,
            activity_raw=activity_raw,
            default_view_event_source=default_view_event_source,
        )

    return {
        "name": activity_name,
        "enabled": _is_truthy(activity_raw.get("enabled"), default=True),
        "reports": reports,
    }


def _normalize_brand(
    *,
    brand_raw: Any,
    used_codes: set[str],
    default_view_event_source: str,
) -> dict[str, Any] | None:
    if not isinstance(brand_raw, dict):
        return None
    brand_name = _safe_text(brand_raw.get("name"))
    if not brand_name:
        return None

    requested_code = _safe_text(brand_raw.get("code")) or _slugify(brand_name)
    brand_code = _unique_code(seed=requested_code, existing=used_codes)
    used_codes.add(brand_code)

    activities_raw = brand_raw.get("activities")
    if not isinstance(activities_raw, list):
        activities_raw = []

    activities: list[dict[str, Any]] = []
    used_activity_names: set[str] = set()
    for raw_activity in activities_raw:
        normalized_activity = _normalize_activity(
            activity_raw=raw_activity,
            brand_raw=brand_raw,
            default_view_event_source=default_view_event_source,
        )
        if not normalized_activity:
            continue
        name_key = normalized_activity["name"].strip().lower()
        if name_key in used_activity_names:
            continue
        used_activity_names.add(name_key)
        activities.append(normalized_activity)

    return {
        "code": brand_code,
        "name": brand_name,
        "enabled": _is_truthy(brand_raw.get("enabled"), default=True),
        "activities": activities,
    }


def normalize_config(raw: Any) -> dict[str, Any]:
    base = default_config()
    if not isinstance(raw, dict):
        return base

    view_event_source = _safe_text(raw.get("view_event_source")) or DEFAULT_VIEW_EVENT_SOURCE
    export_event_source = _safe_text(raw.get("export_event_source")) or DEFAULT_EXPORT_EVENT_SOURCE
    brands_raw = raw.get("brands")
    if not isinstance(brands_raw, list):
        brands_raw = []

    used_codes: set[str] = set()
    brands: list[dict[str, Any]] = []
    for raw_brand in brands_raw:
        normalized_brand = _normalize_brand(
            brand_raw=raw_brand,
            used_codes=used_codes,
            default_view_event_source=view_event_source,
        )
        if normalized_brand:
            brands.append(normalized_brand)

    return {
        "view_event_source": view_event_source,
        "export_event_source": export_event_source,
        "brands": brands,
    }


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _seed_config_from_example_if_missing(
    *,
    config_path: Path,
    messages: list[str],
) -> dict[str, Any] | None:
    """
    Bootstrap active config from bundled example when activity_catalog.json is missing.
    This supports lean release bundles that ship example only.
    """
    candidate_paths = [config_path.with_name(DEFAULT_CONFIG_EXAMPLE_NAME)]
    module_root = Path(__file__).resolve().parents[2]
    candidate_paths.append((module_root / "config" / "meta" / DEFAULT_CONFIG_EXAMPLE_NAME).resolve())

    example_path: Path | None = None
    for candidate in candidate_paths:
        if candidate.exists():
            example_path = candidate
            break
    if example_path is None:
        return None

    try:
        raw = json.loads(example_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        messages.append(f"Example config is invalid JSON: {example_path}")
        return None

    normalized = normalize_config(raw)
    try:
        _ensure_parent(config_path)
        payload = json.dumps(normalized, ensure_ascii=False, indent=2)
        config_path.write_text(payload, encoding="utf-8")
        messages.append(
            f"Created active config from example: {config_path.name} (source={example_path.name})"
        )
    except Exception as exc:  # noqa: BLE001
        messages.append(
            f"Failed to write active config from example; using in-memory config only: {exc}"
        )
    return normalized


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> ConfigLoadResult:
    config_path = Path(path).expanduser().resolve()
    messages: list[str] = []
    if not config_path.exists():
        seeded = _seed_config_from_example_if_missing(config_path=config_path, messages=messages)
        if seeded is not None:
            return ConfigLoadResult(config=seeded, messages=messages, path=config_path)
        return ConfigLoadResult(config=default_config(), messages=messages, path=config_path)

    raw: Any = {}
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = config_path.with_suffix(config_path.suffix + f".broken_{timestamp}")
        try:
            config_path.replace(backup_path)
            messages.append(f"깨진 config를 백업했습니다: {backup_path.name}")
        except Exception:
            messages.append("깨진 config를 감지했습니다. 기본 스키마로 복구합니다.")
        return ConfigLoadResult(config=default_config(), messages=messages, path=config_path)

    normalized = normalize_config(raw)
    return ConfigLoadResult(config=normalized, messages=messages, path=config_path)


def save_config(config: dict[str, Any], path: str | Path = DEFAULT_CONFIG_PATH) -> None:
    config_path = Path(path).expanduser().resolve()
    _ensure_parent(config_path)
    normalized = normalize_config(config)
    payload = json.dumps(normalized, ensure_ascii=False, indent=2)
    temp_path = config_path.with_suffix(config_path.suffix + ".tmp")
    temp_path.write_text(payload, encoding="utf-8")
    temp_path.replace(config_path)


def _find_brand(config: dict[str, Any], brand_code: str) -> dict[str, Any] | None:
    code = _safe_text(brand_code)
    for brand in config.get("brands", []):
        if _safe_text(brand.get("code")) == code:
            return brand
    return None


def _find_activity(brand: dict[str, Any], activity_name: str) -> dict[str, Any] | None:
    name = _safe_text(activity_name)
    for activity in brand.get("activities", []):
        if _safe_text(activity.get("name")) == name:
            return activity
    return None


def add_brand(config: dict[str, Any], name: str) -> tuple[bool, str]:
    brand_name = _safe_text(name)
    if not brand_name:
        return False, "브랜드 이름을 입력해주세요."
    exists = {
        _safe_text(item.get("name")).lower()
        for item in config.get("brands", [])
    }
    if brand_name.lower() in exists:
        return False, "동일한 브랜드 이름이 이미 존재합니다."
    used_codes = {_safe_text(item.get("code")) for item in config.get("brands", [])}
    brand_code = _unique_code(seed=brand_name, existing=used_codes)
    config.setdefault("brands", []).append(
        {
            "code": brand_code,
            "name": brand_name,
            "enabled": True,
            "activities": [],
        }
    )
    return True, "브랜드를 추가했습니다."


def rename_brand(config: dict[str, Any], brand_code: str, new_name: str) -> tuple[bool, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다."
    candidate = _safe_text(new_name)
    if not candidate:
        return False, "브랜드 이름을 입력해주세요."
    used = {
        _safe_text(item.get("name")).lower()
        for item in config.get("brands", [])
        if item is not brand
    }
    if candidate.lower() in used:
        return False, "동일한 브랜드 이름이 이미 존재합니다."
    brand["name"] = candidate
    return True, "브랜드 이름을 변경했습니다."


def delete_brand(config: dict[str, Any], brand_code: str) -> tuple[bool, str]:
    code = _safe_text(brand_code)
    brands = config.get("brands", [])
    for idx, brand in enumerate(brands):
        if _safe_text(brand.get("code")) == code:
            brands.pop(idx)
            return True, "브랜드를 삭제했습니다."
    return False, "브랜드를 찾을 수 없습니다."


def add_activity(config: dict[str, Any], brand_code: str, name: str) -> tuple[bool, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다."
    activity_name = _safe_text(name)
    if not activity_name:
        return False, "액티비티 이름을 입력해주세요."
    exists = {
        _safe_text(item.get("name")).lower()
        for item in brand.get("activities", [])
    }
    if activity_name.lower() in exists:
        return False, "같은 브랜드 내에 동일한 액티비티가 있습니다."
    brand.setdefault("activities", []).append(
        {
            "name": activity_name,
            "enabled": True,
            "reports": _default_reports(),
        }
    )
    return True, "액티비티를 추가했습니다."


def rename_activity(
    config: dict[str, Any],
    brand_code: str,
    old_name: str,
    new_name: str,
) -> tuple[bool, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다."
    activity = _find_activity(brand, old_name)
    if not activity:
        return False, "액티비티를 찾을 수 없습니다."
    candidate = _safe_text(new_name)
    if not candidate:
        return False, "액티비티 이름을 입력해주세요."
    used = {
        _safe_text(item.get("name")).lower()
        for item in brand.get("activities", [])
        if item is not activity
    }
    if candidate.lower() in used:
        return False, "같은 브랜드 내에 동일한 액티비티가 있습니다."
    activity["name"] = candidate
    return True, "액티비티 이름을 변경했습니다."


def delete_activity(config: dict[str, Any], brand_code: str, activity_name: str) -> tuple[bool, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다."
    activities = brand.get("activities", [])
    name = _safe_text(activity_name)
    for idx, activity in enumerate(activities):
        if _safe_text(activity.get("name")) == name:
            activities.pop(idx)
            return True, "액티비티를 삭제했습니다."
    return False, "액티비티를 찾을 수 없습니다."


def add_sheet_url(
    config: dict[str, Any],
    *,
    brand_code: str,
    activity_name: str,
    sheet_name: str,
    raw_url: str,
) -> tuple[bool, str, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다.", ""
    activity = _find_activity(brand, activity_name)
    if not activity:
        return False, "액티비티를 찾을 수 없습니다.", ""
    sheet = str(sheet_name or "").strip()
    if sheet not in SHEET_DISPLAY_ORDER:
        return False, "유효하지 않은 시트입니다.", ""
    try:
        cleaned = clean_report_url(
            raw_url,
            default_event_source=_safe_text(config.get("view_event_source")),
        )
    except UrlValidationError as exc:
        return False, str(exc), ""
    reports = activity.setdefault("reports", _default_reports())
    reports.setdefault(sheet, [])
    reports[sheet].append({"url": cleaned})
    return True, "URL을 추가했습니다.", cleaned


def update_sheet_url(
    config: dict[str, Any],
    *,
    brand_code: str,
    activity_name: str,
    sheet_name: str,
    index: int,
    raw_url: str,
) -> tuple[bool, str, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다.", ""
    activity = _find_activity(brand, activity_name)
    if not activity:
        return False, "액티비티를 찾을 수 없습니다.", ""
    reports = activity.setdefault("reports", _default_reports())
    items = reports.setdefault(sheet_name, [])
    if index < 0 or index >= len(items):
        return False, "수정할 URL을 찾을 수 없습니다.", ""
    try:
        cleaned = clean_report_url(
            raw_url,
            default_event_source=_safe_text(config.get("view_event_source")),
        )
    except UrlValidationError as exc:
        return False, str(exc), ""
    items[index] = {"url": cleaned}
    return True, "URL을 수정했습니다.", cleaned


def delete_sheet_url(
    config: dict[str, Any],
    *,
    brand_code: str,
    activity_name: str,
    sheet_name: str,
    index: int,
) -> tuple[bool, str]:
    brand = _find_brand(config, brand_code)
    if not brand:
        return False, "브랜드를 찾을 수 없습니다."
    activity = _find_activity(brand, activity_name)
    if not activity:
        return False, "액티비티를 찾을 수 없습니다."
    reports = activity.setdefault("reports", _default_reports())
    items = reports.setdefault(sheet_name, [])
    if index < 0 or index >= len(items):
        return False, "삭제할 URL을 찾을 수 없습니다."
    items.pop(index)
    return True, "URL을 삭제했습니다."
