#!/usr/bin/env python3
"""Meta Ads Manager Ad Sets Activity History collector.

This script intentionally runs as an independent utility under `history_source`
while sharing config format with existing Meta report-export automation:

- config/meta/activity_catalog.json
- config/meta/runtime_settings.json

Core behavior:
- Extract (act, business_id) from report URLs in activity_catalog.json.
- For each enabled activity_prefix(activity.name), open Ad Sets page per account.
- Apply filter: Campaign name contains all of {activity_prefix}_
- Select all ad sets -> open Activity History
- Force Last 7 days and scope=Ad Sets via UI
- Collect table rows (with lazy-load scrolling)
- Save one xlsx per activity_prefix
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse


HISTORY_COLUMNS: list[str] = [
    "Activity",
    "Activity details",
    "Item changed",
    "Changed by",
    "Date and time",
]

DEFAULT_LOGIN_TIMEOUT_SEC = 300
DEFAULT_ACTION_TIMEOUT_MS = 15_000
DEFAULT_TABLE_LOAD_TIMEOUT_SEC = 30
DEFAULT_LAZY_SCROLL_PAUSE_SEC = 1.2
DEFAULT_LAZY_SCROLL_MAX_ROUNDS = 120
DEFAULT_LAZY_SCROLL_NO_NEW_ROUNDS = 3
DEFAULT_STEP_RETRY_COUNT = 2
DEFAULT_FILTER_SHELL_WAIT_MS = 15_000
DEFAULT_FILTER_INPUT_MOUNT_MS = 3_500
DEFAULT_FILTER_POLL_MS = 100
DEFAULT_MANUAL_WAIT_HEARTBEAT_SEC = 30

KR_ACTIVITY_LOG_TABLE = "\ud65c\ub3d9 \ub85c\uadf8 \ud14c\uc774\ube14"
TABLE_SELECTOR = (
    "table[role='grid'][aria-label='Activity log table'], "
    f"table[role='grid'][aria-label='{KR_ACTIVITY_LOG_TABLE}']"
)

# Locale labels (kept as constants to avoid ad-hoc literals and encoding drift)
KR_EDIT_FILTER = "\ud544\ud130 \uc218\uc815"
KR_REMOVE_FILTER = "\ud544\ud130 \uc81c\uac70"
KR_CLEAR = "\uc9c0\uc6b0\uae30"
KR_CAMPAIGN_NAME = "\ucea0\ud398\uc778 \uc774\ub984"
KR_CONTAINS_ALL = "\ub2e4\uc74c \ubaa8\ub450 \ud3ec\ud568"
KR_ENTER_NAME_OR_KEYWORD = "\uc774\ub984 \ub610\ub294 \ud0a4\uc6cc\ub4dc\ub97c \uc785\ub825\ud558\uc138\uc694"
KR_APPLY = "\uc801\uc6a9"
KR_CANCEL = "\ucde8\uc18c"
KR_HISTORY = "\uae30\ub85d"
KR_LAST = "\ucd5c\uadfc"
KR_DAY = "\uc77c"
KR_UPDATE = "\uc5c5\ub370\uc774\ud2b8"
KR_ACTIVITY_HISTORY = "\ud65c\ub3d9 \uae30\ub85d"
KR_AD_SETS = "\uad11\uace0 \uc138\ud2b8"
KR_ACTIVITY = "\ud65c\ub3d9"
KR_FILTERING_SEARCH = "\ud544\ud130\ub9c1 \uac80\uc0c9"
KR_SELECT_ALL_ADS = "\ubaa8\ub4e0 \uad11\uace0\ub97c \uc120\ud0dd\ud558\uae30 \uc704\ud55c \uccb4\ud06c \ubc15\uc2a4"
KR_NAME = "\uc774\ub984"

RE_CAMPAIGN_NAME = re.compile(rf"(Campaign name|{re.escape(KR_CAMPAIGN_NAME)})", re.IGNORECASE)
RE_CONTAINS_ALL = re.compile(rf"(contains all of|{re.escape(KR_CONTAINS_ALL)})", re.IGNORECASE)
RE_APPLY = re.compile(rf"^(Apply|{re.escape(KR_APPLY)})$")
RE_CANCEL = re.compile(rf"^(Cancel|{re.escape(KR_CANCEL)})$")


class ConfigError(RuntimeError):
    """Raised when shared config cannot be interpreted safely."""


class AutomationError(RuntimeError):
    """Raised when UI automation stage fails."""


class ManualInterventionRequired(AutomationError):
    """Raised when automatic filter steps fail and manual UI action is required."""


@dataclass(frozen=True)
class AccountTarget:
    act: str
    business_id: str


@dataclass(frozen=True)
class RunnerOptions:
    browser: str
    headless: bool
    user_data_dir: Path
    output_dir: Path
    log_dir: Path
    screenshot_dir: Path
    login_timeout_sec: int
    action_timeout_ms: int
    table_load_timeout_sec: int
    lazy_scroll_pause_sec: float
    lazy_scroll_max_rounds: int
    lazy_scroll_no_new_rounds: int
    step_retry_count: int


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = _safe_text(value).lower()
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return default


def _as_int(value: Any, default: int) -> int:
    text = _safe_text(value)
    if not text:
        return default
    try:
        return int(float(text))
    except Exception:
        return default


def _as_float(value: Any, default: float) -> float:
    text = _safe_text(value)
    if not text:
        return default
    try:
        return float(text)
    except Exception:
        return default


def _expand_path(raw_path: str, *, base_dir: Path) -> Path:
    text = _safe_text(raw_path)
    if not text:
        return base_dir
    expanded = os.path.expandvars(text)
    path = Path(expanded).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path.resolve()


def _slug(value: str) -> str:
    compact = re.sub(r"\s+", "_", _safe_text(value))
    compact = re.sub(r"[^\w\-]+", "_", compact, flags=re.UNICODE)
    compact = re.sub(r"_+", "_", compact).strip("_")
    return compact or "activity"


def _setup_logger(log_dir: Path, *, verbose: bool) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"meta_history_log_{ts}.log"

    logger = logging.getLogger("meta_history_log")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(stream_handler)

    logger.info("log_file=%s", log_path)
    return logger


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"JSON root must be an object: {path}")
    return raw


def _read_yaml_or_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    if path.suffix.lower() == ".json":
        return _read_json(path)

    try:
        import yaml  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise ConfigError("pyyaml is required to load .yaml config files.") from exc

    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise ConfigError(f"Config root must be an object: {path}")
    return parsed


def _resolve_paths(
    *,
    script_dir: Path,
    user_config: dict[str, Any],
) -> tuple[Path, Path]:
    candidate_roots: list[Path]
    if script_dir.name.lower() == "meta_history_log":
        candidate_roots = [script_dir.parent, script_dir]
    else:
        candidate_roots = [script_dir, script_dir.parent]

    default_catalog: Path | None = None
    default_runtime: Path | None = None
    for root in candidate_roots:
        catalog_candidate = root / "config" / "meta" / "activity_catalog.json"
        runtime_candidate = root / "config" / "meta" / "runtime_settings.json"
        if catalog_candidate.exists() and runtime_candidate.exists():
            default_catalog = catalog_candidate.resolve()
            default_runtime = runtime_candidate.resolve()
            break
    if default_catalog is None or default_runtime is None:
        fallback_root = candidate_roots[0]
        default_catalog = (fallback_root / "config" / "meta" / "activity_catalog.json").resolve()
        default_runtime = (fallback_root / "config" / "meta" / "runtime_settings.json").resolve()

    raw_paths = user_config.get("paths")
    if not isinstance(raw_paths, dict):
        raw_paths = {}

    catalog_path = _expand_path(
        _safe_text(raw_paths.get("activity_catalog_path")) or str(default_catalog),
        base_dir=script_dir,
    )
    runtime_path = _expand_path(
        _safe_text(raw_paths.get("runtime_settings_path")) or str(default_runtime),
        base_dir=script_dir,
    )
    return catalog_path, runtime_path


def _build_runner_options(
    *,
    script_dir: Path,
    runtime_settings: dict[str, Any],
    user_config: dict[str, Any],
) -> RunnerOptions:
    runner_cfg = user_config.get("runner")
    if not isinstance(runner_cfg, dict):
        runner_cfg = {}

    browser = (
        _safe_text(runtime_settings.get("history_browser"))
        or _safe_text(runtime_settings.get("browser"))
        or "msedge"
    ).lower()
    if browser not in {"msedge", "chrome", "chromium"}:
        browser = "msedge"

    headless = _as_bool(runtime_settings.get("history_headless"), False)
    headless = _as_bool(runner_cfg.get("headless"), headless)

    default_output_base = _expand_path(
        _safe_text(runtime_settings.get("output_dir")) or r"%USERPROFILE%\MetaAdsExport\output",
        base_dir=script_dir,
    )
    output_dir = (default_output_base / "history_logs").resolve()

    default_user_data = _expand_path(
        _safe_text(runtime_settings.get("history_user_data_dir"))
        or r"%USERPROFILE%\MetaAdsExport\user_data\meta_history_log",
        base_dir=script_dir,
    )
    user_data_dir = _expand_path(
        _safe_text(runner_cfg.get("user_data_dir")) or str(default_user_data),
        base_dir=script_dir,
    )

    log_dir = _expand_path(
        _safe_text(runner_cfg.get("log_dir")) or str(output_dir / "logs"),
        base_dir=script_dir,
    )
    screenshot_dir = _expand_path(
        _safe_text(runner_cfg.get("screenshot_dir")) or str(output_dir / "screenshots"),
        base_dir=script_dir,
    )

    return RunnerOptions(
        browser=browser,
        headless=headless,
        user_data_dir=user_data_dir,
        output_dir=output_dir,
        log_dir=log_dir,
        screenshot_dir=screenshot_dir,
        login_timeout_sec=_as_int(
            runner_cfg.get("login_timeout_sec"),
            _as_int(runtime_settings.get("history_login_timeout_sec"), DEFAULT_LOGIN_TIMEOUT_SEC),
        ),
        action_timeout_ms=_as_int(
            runner_cfg.get("action_timeout_ms"),
            _as_int(runtime_settings.get("history_action_timeout_ms"), DEFAULT_ACTION_TIMEOUT_MS),
        ),
        table_load_timeout_sec=_as_int(
            runner_cfg.get("table_load_timeout_sec"),
            _as_int(
                runtime_settings.get("history_table_load_timeout_sec"),
                DEFAULT_TABLE_LOAD_TIMEOUT_SEC,
            ),
        ),
        lazy_scroll_pause_sec=_as_float(
            runner_cfg.get("lazy_scroll_pause_sec"),
            _as_float(
                runtime_settings.get("history_lazy_scroll_pause_sec"),
                DEFAULT_LAZY_SCROLL_PAUSE_SEC,
            ),
        ),
        lazy_scroll_max_rounds=_as_int(
            runner_cfg.get("lazy_scroll_max_rounds"),
            _as_int(
                runtime_settings.get("history_lazy_scroll_max_rounds"),
                DEFAULT_LAZY_SCROLL_MAX_ROUNDS,
            ),
        ),
        lazy_scroll_no_new_rounds=_as_int(
            runner_cfg.get("lazy_scroll_no_new_rounds"),
            _as_int(
                runtime_settings.get("history_lazy_scroll_no_new_rounds"),
                DEFAULT_LAZY_SCROLL_NO_NEW_ROUNDS,
            ),
        ),
        step_retry_count=max(
            1,
            _as_int(
                runner_cfg.get("step_retry_count"),
                _as_int(runtime_settings.get("history_step_retry_count"), DEFAULT_STEP_RETRY_COUNT),
            ),
        ),
    )


def _extract_accounts_by_activity(
    catalog: dict[str, Any],
) -> tuple[OrderedDict[str, list[AccountTarget]], list[str]]:
    brands = catalog.get("brands")
    if not isinstance(brands, list):
        raise ConfigError("activity_catalog.json: `brands` must be a list.")

    grouped: OrderedDict[str, set[tuple[str, str]]] = OrderedDict()
    warnings: list[str] = []

    for brand in brands:
        if not isinstance(brand, dict):
            continue
        if not _as_bool(brand.get("enabled"), True):
            continue
        brand_name = _safe_text(brand.get("name")) or "<unknown_brand>"
        activities = brand.get("activities")
        if not isinstance(activities, list):
            continue

        for activity in activities:
            if not isinstance(activity, dict):
                continue
            if not _as_bool(activity.get("enabled"), True):
                continue

            activity_prefix = _safe_text(activity.get("name"))
            if not activity_prefix:
                warnings.append(f"{brand_name}: skipped activity with empty name")
                continue

            grouped.setdefault(activity_prefix, set())
            reports = activity.get("reports")
            if not isinstance(reports, dict):
                warnings.append(
                    f"{brand_name}/{activity_prefix}: `reports` missing or invalid; "
                    "activity will produce an empty output unless account URLs exist."
                )
                continue

            url_count = 0
            for entries in reports.values():
                if isinstance(entries, list):
                    iter_entries = entries
                else:
                    iter_entries = [entries]

                for entry in iter_entries:
                    if isinstance(entry, dict):
                        raw_url = _safe_text(entry.get("url"))
                    else:
                        raw_url = _safe_text(entry)
                    if not raw_url:
                        continue

                    url_count += 1
                    query = parse_qs(urlparse(raw_url).query or "")
                    act = _safe_text((query.get("act") or [""])[0])
                    business_id = _safe_text((query.get("business_id") or [""])[0])
                    if not act or not business_id:
                        raise ConfigError(
                            "Missing required URL params in activity_catalog report URL: "
                            f"brand={brand_name} activity={activity_prefix} "
                            f"act={act or '<missing>'} business_id={business_id or '<missing>'} "
                            f"url={raw_url}"
                        )
                    grouped[activity_prefix].add((act, business_id))

            if url_count == 0:
                warnings.append(
                    f"{brand_name}/{activity_prefix}: no report URL found; this activity will output an empty xlsx."
                )

    out: OrderedDict[str, list[AccountTarget]] = OrderedDict()
    for prefix, pair_set in grouped.items():
        sorted_pairs = sorted(pair_set, key=lambda item: (item[0], item[1]))
        out[prefix] = [AccountTarget(act=item[0], business_id=item[1]) for item in sorted_pairs]
    return out, warnings


def _table_locator(page: Any) -> Any:
    return page.locator(TABLE_SELECTOR).first


def _normalize_cell(text: str) -> str:
    return re.sub(r"\s+", " ", _safe_text(text))


def _capture_screenshot(
    *,
    page: Any,
    options: RunnerOptions,
    activity_prefix: str,
    account: AccountTarget | None,
    step_name: str,
) -> str:
    options.screenshot_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    account_token = "no_account"
    if account:
        account_token = f"act{account.act}"
    file_name = f"{_slug(activity_prefix)}_{account_token}_{_slug(step_name)}_{ts}.png"
    path = options.screenshot_dir / file_name
    try:
        page.screenshot(path=str(path), full_page=True)
        return str(path)
    except Exception:  # noqa: BLE001
        return ""


def _run_step(
    *,
    logger: logging.Logger,
    page: Any,
    options: RunnerOptions,
    activity_prefix: str,
    account: AccountTarget | None,
    step_name: str,
    fn: Any,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, options.step_retry_count + 1):
        try:
            logger.info(
                "step_start activity=%s account=%s step=%s attempt=%s/%s",
                activity_prefix,
                f"{account.act}/{account.business_id}" if account else "-",
                step_name,
                attempt,
                options.step_retry_count,
            )
            result = fn()
            logger.info(
                "step_success activity=%s account=%s step=%s",
                activity_prefix,
                f"{account.act}/{account.business_id}" if account else "-",
                step_name,
            )
            return result
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            shot = _capture_screenshot(
                page=page,
                options=options,
                activity_prefix=activity_prefix,
                account=account,
                step_name=f"{step_name}_attempt_{attempt}",
            )
            logger.exception(
                "step_fail activity=%s account=%s step=%s attempt=%s/%s screenshot=%s",
                activity_prefix,
                f"{account.act}/{account.business_id}" if account else "-",
                step_name,
                attempt,
                options.step_retry_count,
                shot or "<none>",
            )
            if attempt < options.step_retry_count:
                page.wait_for_timeout(900 * attempt)
    raise AutomationError(
        f"Stage failed after retries: {step_name} "
        f"(activity={activity_prefix}, account={account.act if account else '-'})"
    ) from last_exc


def _wait_for_login_context(page: Any, *, timeout_sec: int) -> None:
    deadline = time.time() + max(5, timeout_sec)
    while time.time() < deadline:
        url = _safe_text(page.url).lower()
        if "adsmanager.facebook.com/adsmanager" in url:
            return
        page.wait_for_timeout(2000)
    raise AutomationError(
        "Login/session not ready for Ads Manager. "
        "Please complete Meta login in the opened browser profile."
    )


def _activity_filter_token(activity_prefix: str) -> str:
    return f"{activity_prefix}_"


def _build_campaign_filter_set(*, activity_prefix: str) -> str:
    # Meta filter_set grammar (control-separator included):
    # SEARCH_BY_CAMPAIGN_GROUP_NAME-STRING<0x1E>CONTAINS_ALL<0x1E>"[\"RCRA_\"]"
    token = _activity_filter_token(activity_prefix)
    value_list = f'[\\\"{token}\\\"]'
    return (
        "SEARCH_BY_CAMPAIGN_GROUP_NAME-STRING"
        "\x1e"
        "CONTAINS_ALL"
        "\x1e"
        f"\"{value_list}\""
    )


def _partition_paths_by_run_date(options: RunnerOptions, *, run_date_token: str) -> RunnerOptions:
    day_dir = (options.output_dir / run_date_token).resolve()
    return replace(
        options,
        output_dir=day_dir,
        log_dir=(day_dir / "logs").resolve(),
        screenshot_dir=(day_dir / "screenshots").resolve(),
    )


def _goto_campaigns_with_bootstrap_filter(
    page: Any,
    account: AccountTarget,
    *,
    activity_prefix: str,
    options: RunnerOptions,
) -> None:
    params = {
        "act": account.act,
        "business_id": account.business_id,
        "columns": "name,campaign_group_name,campaign_id",
        "attribution_windows": "default",
        "filter_set": _build_campaign_filter_set(activity_prefix=activity_prefix),
    }
    url = (
        "https://adsmanager.facebook.com/adsmanager/manage/campaigns?"
        f"{urlencode(params)}"
    )
    page.goto(url, wait_until="domcontentloaded")
    _wait_for_login_context(page, timeout_sec=options.login_timeout_sec)
    page.wait_for_timeout(1200)


def _normalize_ui_text(text: str) -> str:
    return re.sub(r"\s+", " ", _safe_text(text).replace("\u200b", "")).strip()


def _search_combobox_selectors() -> tuple[str, ...]:
    return (
        "[role='search'] input[role='combobox'][aria-autocomplete='list'][aria-haspopup='listbox'][type='text']",
        "[role='search'] input[role='combobox']",
        "[role='search'] input[type='text'][role='combobox']",
    )


def _search_combobox_selectors_with_global_fallback() -> tuple[str, ...]:
    # Strict [role='search'] scope first, then a single safe global fallback.
    return _search_combobox_selectors() + (
        "input[role='combobox'][aria-autocomplete='list'][type='text']",
    )


def _candidate_is_search_input(candidate: Any) -> bool:
    try:
        meta = candidate.evaluate(
            """(el) => ({
                tag: (el.tagName || '').toUpperCase(),
                type: ((el.getAttribute('type') || '').toLowerCase()),
                role: ((el.getAttribute('role') || '').toLowerCase()),
                ariaAutocomplete: ((el.getAttribute('aria-autocomplete') || '').toLowerCase()),
                inSearch: !!(el.closest && el.closest("[role='search']")),
                inPopupLayer: !!(el.closest && el.closest("._5v-0, .uiContextualLayer, .uiContextualLayerPositioner")),
                placeholder: ((el.getAttribute('placeholder') || '').toLowerCase()),
                readonly: !!el.readOnly
            })"""
        )
    except Exception:  # noqa: BLE001
        return False
    tag_ok = meta.get("tag") == "INPUT"
    type_ok = meta.get("type") != "checkbox"
    readonly_ok = not bool(meta.get("readonly"))
    comboboxish = (meta.get("role") == "combobox") or (
        meta.get("ariaAutocomplete") == "list"
    )
    if not (tag_ok and type_ok and readonly_ok and comboboxish):
        return False

    if bool(meta.get("inSearch")):
        return True

    if bool(meta.get("inPopupLayer")):
        return False

    placeholder = _safe_text(meta.get("placeholder")).lower()
    return ("search to filter by" in placeholder) or (KR_FILTERING_SEARCH in placeholder)


def _resolve_search_combobox(page: Any, *, timeout_ms: int = 10_000) -> Any:
    deadline = time.time() + max(1.0, timeout_ms / 1000.0)
    last_seen: list[str] = []
    while time.time() < deadline:
        for selector in _search_combobox_selectors_with_global_fallback():
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 8)
            except Exception:  # noqa: BLE001
                continue
            if count <= 0:
                continue
            last_seen.append(f"{selector}({count})")
            for idx in range(count):
                candidate = locator.nth(idx)
                try:
                    if (
                        candidate.is_visible(timeout=300)
                        and candidate.is_enabled()
                        and _candidate_is_search_input(candidate)
                    ):
                        return candidate
                except Exception:  # noqa: BLE001
                    continue
        page.wait_for_timeout(200)
    raise AutomationError(
        "Search filter input not visible. "
        f"candidate_selectors={list(_search_combobox_selectors_with_global_fallback())} "
        f"selectors_seen={last_seen[:6]}"
    )


def _count_visible(locator: Any, *, hard_cap: int = 8) -> int:
    try:
        count = min(locator.count(), hard_cap)
    except Exception:  # noqa: BLE001
        return 0

    visible = 0
    for idx in range(count):
        try:
            if locator.nth(idx).is_visible(timeout=150):
                visible += 1
        except Exception:  # noqa: BLE001
            continue
    return visible


def _filter_surface_state(page: Any) -> dict[str, Any]:
    try:
        payload = page.evaluate(
            """() => {
                const isVisible = (el) => {
                  if (!el) return false;
                  const st = window.getComputedStyle(el);
                  const r = el.getBoundingClientRect();
                  return st.visibility !== 'hidden' && st.display !== 'none' && r.width > 0 && r.height > 0;
                };
                const txt = (el) => (el?.textContent || '').replace(/\\s+/g, ' ').trim();
                const shellNode = Array.from(document.querySelectorAll('span,div')).find((el) => {
                  if (!isVisible(el)) return false;
                  const t = txt(el);
                  return t.includes('Search to filter by') || t.includes('필터링 검색');
                });
                const ae = document.activeElement;
                return {
                  shell_text_found: !!shellNode,
                  search_role_count: document.querySelectorAll("[role='search']").length,
                  input_combobox_count: document.querySelectorAll("[role='search'] input[role='combobox'][type='text']").length,
                  global_input_combobox_count: document.querySelectorAll("input[role='combobox'][type='text']").length,
                  progress_visible: Array.from(document.querySelectorAll("[role='progressbar']")).filter(isVisible).length,
                  active_element_meta: ae ? {
                    tag: ae.tagName || '',
                    type: (ae.getAttribute && ae.getAttribute('type')) || '',
                    role: (ae.getAttribute && ae.getAttribute('role')) || '',
                    inSearch: !!(ae.closest && ae.closest("[role='search']")),
                    ariaLabel: (ae.getAttribute && ae.getAttribute('aria-label')) || ''
                  } : null
                };
            }"""
        )
    except Exception:  # noqa: BLE001
        payload = {
            "shell_text_found": False,
            "search_role_count": -1,
            "input_combobox_count": -1,
            "global_input_combobox_count": -1,
            "progress_visible": -1,
            "active_element_meta": {"tag": "<unavailable>"},
        }
    if not isinstance(payload, dict):
        return {
            "shell_text_found": False,
            "search_role_count": -1,
            "input_combobox_count": -1,
            "global_input_combobox_count": -1,
            "progress_visible": -1,
            "active_element_meta": {"tag": "<invalid>"},
        }
    return payload


def _search_ready_diagnostics(page: Any) -> str:
    """Collect lightweight page diagnostics when search input cannot be resolved."""
    try:
        payload = page.evaluate(
            """() => ({
              url: location.href,
              readyState: document.readyState,
              iframeCount: document.querySelectorAll('iframe').length,
              bodyTextHead: (document.body?.innerText || '').replace(/\\s+/g,' ').slice(0, 180),
            })"""
        )
        if not isinstance(payload, dict):
            payload = {}
        payload.update(_filter_surface_state(page))
        payload["candidate_selectors"] = list(_search_combobox_selectors_with_global_fallback())
        return json.dumps(payload, ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001
        return f"<diag_unavailable:{exc}>"


def _wait_for_search_surface_ready(page: Any, *, timeout_ms: int = 18_000) -> None:
    """Wait until Ads Manager mounts the filter search surface."""
    deadline = time.time() + max(2.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        state = _filter_surface_state(page)
        if (
            int(state.get("search_role_count", 0)) > 0
            or int(state.get("input_combobox_count", 0)) > 0
            or bool(state.get("shell_text_found"))
            or int(state.get("global_input_combobox_count", 0)) > 0
        ):
            return
        page.wait_for_timeout(250)


def _click_filter_shell_by_text(page: Any) -> bool:
    try:
        clicked = page.evaluate(
            """() => {
                const isVisible = (el) => {
                  if (!el) return false;
                  const st = window.getComputedStyle(el);
                  const r = el.getBoundingClientRect();
                  return st.visibility !== 'hidden' && st.display !== 'none' && r.width > 0 && r.height > 0;
                };
                const textOk = (el) => {
                  const t = (el?.textContent || '').replace(/\\s+/g, ' ').trim();
                  return t.includes('Search to filter by') || t.includes('필터링 검색');
                };
                const shells = Array.from(document.querySelectorAll('span,div')).filter((el) => isVisible(el) && textOk(el));
                if (!shells.length) return false;
                let target = shells[0];
                const chain = [];
                let cur = target;
                for (let i = 0; i < 6 && cur; i++) {
                  chain.push(cur);
                  cur = cur.parentElement;
                }
                const clickable = chain.find((el) => {
                  const st = window.getComputedStyle(el);
                  return st.cursor === 'text' || typeof el.onclick === 'function';
                }) || chain.find((el) => el.tagName === 'DIV') || target;
                clickable.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true }));
                clickable.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true }));
                clickable.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                return true;
            }"""
        )
        return bool(clicked)
    except Exception:  # noqa: BLE001
        return False


def _wait_for_input_mount_after_shell_click(page: Any, *, timeout_ms: int = DEFAULT_FILTER_INPUT_MOUNT_MS) -> Any | None:
    deadline = time.time() + max(0.5, timeout_ms / 1000.0)
    while time.time() < deadline:
        try:
            candidate = _resolve_search_combobox(page, timeout_ms=250)
            if candidate.is_visible(timeout=250):
                meta = candidate.evaluate(
                    """(el) => ({
                        connected: !!el.isConnected,
                        width: el.getBoundingClientRect().width,
                        height: el.getBoundingClientRect().height
                    })"""
                )
                if bool(meta.get("connected")) and float(meta.get("width", 0)) > 0 and float(meta.get("height", 0)) > 0:
                    return candidate
        except Exception:  # noqa: BLE001
            pass
        page.wait_for_timeout(DEFAULT_FILTER_POLL_MS)
    return None


def _wait_for_search_input_ready(page: Any, *, timeout_ms: int = 20_000) -> Any:
    """Wait for search combobox to be mounted and stable after React remount/loading."""
    try:
        page.wait_for_load_state("domcontentloaded", timeout=7000)
    except Exception:  # noqa: BLE001
        pass
    _wait_for_search_surface_ready(page, timeout_ms=min(timeout_ms, DEFAULT_FILTER_SHELL_WAIT_MS))
    deadline = time.time() + max(2.0, timeout_ms / 1000.0)
    last_error = ""
    candidate: Any | None = None

    while time.time() < deadline:
        try:
            candidate = _resolve_search_combobox(page, timeout_ms=1200)
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            candidate = None

        state = _filter_surface_state(page)
        loading_count = int(state.get("progress_visible", 0))
        active_meta = state.get("active_element_meta")
        active_role = _safe_text((active_meta or {}).get("role")).lower()
        active_tag = _safe_text((active_meta or {}).get("tag")).lower()

        if candidate is not None:
            if loading_count == 0:
                return candidate
            # Keep waiting briefly for remount to settle.
            page.wait_for_timeout(280)
            continue

        # During lazy mount, focus can briefly move to progressbar span.
        # This is not an immediate failure condition; keep polling.
        if active_role == "progressbar" or active_tag == "span":
            page.wait_for_timeout(DEFAULT_FILTER_POLL_MS)
            continue

        page.wait_for_timeout(DEFAULT_FILTER_POLL_MS)

    if candidate is not None:
        return candidate
    raise AutomationError(
        "Search input not ready within timeout. "
        f"last_error={last_error or '<none>'} "
        f"diag={_search_ready_diagnostics(page)}"
    )


def _focus_search_placeholder_area(page: Any) -> bool:
    # IMPORTANT: only click vetted search-input selectors (no table/checkbox area clicks).
    for selector in _search_combobox_selectors():
        loc = page.locator(selector).first
        try:
            if loc.is_visible(timeout=1400):
                loc.click(timeout=3000)
                page.wait_for_timeout(120)
                if _is_active_filter_entry(page):
                    return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _focus_search_shell_by_placeholder(page: Any) -> bool:
    # Legacy function name kept for compatibility; uses shell-text detection now.
    if not _click_filter_shell_by_text(page):
        return False
    page.wait_for_timeout(120)
    if _is_active_filter_entry(page):
        return True
    mounted = _wait_for_input_mount_after_shell_click(
        page, timeout_ms=DEFAULT_FILTER_INPUT_MOUNT_MS
    )
    return mounted is not None


def _active_element_meta(page: Any) -> dict[str, Any]:
    try:
        return page.evaluate(
            """() => {
                const el = document.activeElement;
                if (!el) return { ok: false, reason: 'no_active' };
                const role = (el.getAttribute && el.getAttribute('role')) || '';
                const tag = (el.tagName || '').toUpperCase();
                const type = ((el.getAttribute && el.getAttribute('type')) || '').toLowerCase();
                const ariaLabel = (el.getAttribute && el.getAttribute('aria-label')) || '';
                const inSearch = !!(el.closest && el.closest("[role='search']"));
                const ok = tag === 'INPUT' && type !== 'checkbox' && role === 'combobox' && inSearch;
                return {
                    ok,
                    tag,
                    type,
                    role,
                    ariaLabel,
                    inSearch
                };
            }"""
        )
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": str(exc)}


def _is_active_filter_entry(page: Any) -> bool:
    meta = _active_element_meta(page)
    return bool(meta.get("ok"))


def _ensure_search_input(page: Any, *, timeout_ms: int = 22_000) -> Any:
    """Resolve search input robustly without global keyboard shortcuts."""
    try:
        return _wait_for_search_input_ready(page, timeout_ms=timeout_ms)
    except Exception as first_exc:  # noqa: BLE001
        # Retry path A: click already-mounted input candidates
        if _focus_search_placeholder_area(page):
            try:
                return _wait_for_search_input_ready(
                    page,
                    timeout_ms=max(8_000, min(timeout_ms, 14_000)),
                )
            except Exception as second_exc:  # noqa: BLE001
                pass

        # Retry path B: click placeholder shell that may mount/focus the real input.
        if _focus_search_shell_by_placeholder(page):
            try:
                return _wait_for_search_input_ready(
                    page,
                    timeout_ms=max(8_000, min(timeout_ms, 14_000)),
                )
            except Exception as third_exc:  # noqa: BLE001
                raise AutomationError(
                    "Search input not found after shell click fallback. "
                    f"first={first_exc} third={third_exc} active={_active_element_meta(page)}"
                ) from third_exc
        raise


def _focus_search_input(page: Any, search_input: Any) -> Any:
    for _ in range(3):
        try:
            search_input.scroll_into_view_if_needed(timeout=2000)
        except Exception:  # noqa: BLE001
            pass
        search_input.click(timeout=3000)
        page.wait_for_timeout(120)
        if _is_active_filter_entry(page):
            return search_input
        search_input = _ensure_search_input(page, timeout_ms=6_000)
    raise AutomationError(
        "Search input focus not acquired. "
        f"active_element={_active_element_meta(page)}"
    )


def _checkbox_touch_metrics(page: Any) -> dict[str, int]:
    try:
        return page.evaluate(
            """() => {
                const boxes = Array.from(document.querySelectorAll("input[type='checkbox']"));
                let rowTotal = 0;
                let rowChecked = 0;
                for (const el of boxes) {
                    const label = (el.getAttribute('aria-label') || '');
                    const lower = label.toLowerCase();
                    const isSelectAll = lower.includes('select all') || lower.includes('checkbox to select all') || label.includes('모든 광고');
                    if (isSelectAll) continue;
                    if (!(el.closest('tbody') || el.closest("[role='row']"))) continue;
                    rowTotal += 1;
                    const checked = !!el.checked || (el.getAttribute('aria-checked') === 'true');
                    if (checked) rowChecked += 1;
                }
                return {
                    row_checkbox_total: rowTotal,
                    row_checkbox_checked: rowChecked
                };
            }"""
        )
    except Exception:  # noqa: BLE001
        return {"row_checkbox_total": -1, "row_checkbox_checked": -1}


def _log_checkbox_touch_if_changed(
    *,
    logger: logging.Logger,
    activity_prefix: str,
    attempt: int,
    route: str,
    before: dict[str, int],
    after: dict[str, int],
) -> None:
    if before == after:
        return
    logger.warning(
        "checkbox_touched_during_filter activity=%s attempt=%s route=%s before=%s after=%s",
        activity_prefix,
        attempt,
        route,
        before,
        after,
    )


def _is_filter_reload_eligible(error: Exception) -> bool:
    text = _safe_text(error).lower()
    return (
        "stage=resolve_search_input" in text
        or "search input not ready" in text
        or "search filter input not visible" in text
        or "typeahead did not appear" in text
        or "stage=type_activity_token" in text
        or "typeahead_missing=true" in text
    )


def _reload_adsets_once(
    *,
    page: Any,
    logger: logging.Logger,
    options: RunnerOptions,
    activity_prefix: str,
    account: AccountTarget,
) -> None:
    logger.warning(
        "filter_reload_once activity=%s account=%s/%s reason=search_or_typeahead_failure",
        activity_prefix,
        account.act,
        account.business_id,
    )
    page.reload(wait_until="domcontentloaded")
    _wait_for_login_context(page, timeout_sec=options.login_timeout_sec)
    page.wait_for_timeout(1200)


def _filter_popup_locator(page: Any) -> Any:
    return page.locator(
        "._5v-0._53ik, .uiContextualLayer ._5v-0, .uiContextualLayerPositioner ._5v-0._53ik"
    ).last


def has_active_filter(page: Any) -> bool:
    return (
        page.locator(
            f"[role='search'] button[aria-label='Edit filter'], "
            f"[role='search'] button[aria-label='{KR_EDIT_FILTER}']"
        ).count()
        > 0
    )


def get_combobox_text(combo: Any) -> str:
    return _normalize_ui_text(combo.text_content() or "")


def _click_option_by_text(
    *,
    options: Any,
    exact_labels: tuple[str, ...],
    regex: re.Pattern[str],
    prefer_last: bool = False,
) -> bool:
    count = options.count()
    if count <= 0:
        return False

    exact_hits: list[int] = []
    loose_hits: list[int] = []
    for idx in range(count):
        try:
            text = _normalize_ui_text(options.nth(idx).inner_text(timeout=1200))
        except Exception:  # noqa: BLE001
            continue
        if text in exact_labels:
            exact_hits.append(idx)
        elif regex.search(text):
            loose_hits.append(idx)

    for pool in (exact_hits, loose_hits):
        if not pool:
            continue
        target_idx = pool[-1] if prefer_last else pool[0]
        options.nth(target_idx).click(timeout=4000)
        return True
    return False


def _select_visible_listbox_option(
    *,
    page: Any,
    exact_labels: tuple[str, ...],
    regex: re.Pattern[str],
    prefer_last: bool = False,
) -> None:
    options = page.locator("[role='listbox'] [role='option']")
    options.first.wait_for(state="visible", timeout=4000)
    if not _click_option_by_text(
        options=options,
        exact_labels=exact_labels,
        regex=regex,
        prefer_last=prefer_last,
    ):
        raise AutomationError(f"Could not select listbox option. expected={exact_labels}")


def _close_any_popup(page: Any) -> None:
    popup = _filter_popup_locator(page)
    try:
        if not popup.is_visible(timeout=700):
            return
    except Exception:  # noqa: BLE001
        return

    cancel_btn = popup.locator("div[role='button']").filter(has_text=RE_CANCEL).first
    try:
        if cancel_btn.is_visible(timeout=600):
            cancel_btn.click(timeout=1500)
            page.wait_for_timeout(250)
            return
    except Exception:  # noqa: BLE001
        pass
    page.keyboard.press("Escape")
    page.wait_for_timeout(250)


def _force_clear_filter(page: Any) -> None:
    for _ in range(8):
        remove_btn = page.locator(
            f"button[aria-label='Remove filter'], button[aria-label='{KR_REMOVE_FILTER}']"
        ).first
        try:
            if remove_btn.is_visible(timeout=700):
                remove_btn.click(timeout=1500)
                page.wait_for_timeout(220)
                continue
        except Exception:  # noqa: BLE001
            pass
        break

    clear_link = page.locator("[role='search'] a").filter(
        has_text=re.compile(rf"^(Clear|{re.escape(KR_CLEAR)})$")
    ).first
    try:
        if clear_link.is_visible(timeout=900):
            clear_link.click(timeout=1500)
            page.wait_for_timeout(300)
    except Exception:  # noqa: BLE001
        pass


def verify_filter_chip(page: Any, *, activity_prefix: str) -> str:
    chip_btn = page.locator(
        f"[role='search'] button[aria-label='Edit filter'], "
        f"[role='search'] button[aria-label='{KR_EDIT_FILTER}']"
    ).first
    chip_btn.wait_for(state="visible", timeout=10_000)
    chip_text = _normalize_ui_text(chip_btn.text_content() or "")
    activity_token = _activity_filter_token(activity_prefix)

    expected = re.compile(
        rf"(Campaign name|{re.escape(KR_CAMPAIGN_NAME)}).*"
        rf"(contains all of|{re.escape(KR_CONTAINS_ALL)}).*"
        rf"{re.escape(activity_token)}",
        re.IGNORECASE,
    )
    if not expected.search(chip_text):
        raise AutomationError(
            "Filter chip mismatch. "
            f"expected_campaign+operator+value={activity_token} actual={chip_text}"
        )
    return chip_text


def _chip_text_matches_expected(*, activity_prefix: str, chip_text: str) -> bool:
    activity_token = _activity_filter_token(activity_prefix)
    expected = re.compile(
        rf"(Campaign name|{re.escape(KR_CAMPAIGN_NAME)}).*"
        rf"(contains all of|{re.escape(KR_CONTAINS_ALL)}).*"
        rf"{re.escape(activity_token)}",
        re.IGNORECASE,
    )
    return bool(expected.search(_normalize_ui_text(chip_text)))


def apply_filter_from_scratch(
    page: Any,
    *,
    activity_prefix: str,
    logger: logging.Logger | None = None,
    account: AccountTarget | None = None,
) -> str:
    stage = "resolve_search_input"
    try:
        # NOTE: some Ads Manager builds temporarily omit/hide the [role='search']
        # wrapper even though the input is visible; do not hard-block on wrapper.
        if _count_visible(page.locator("[role='search']"), hard_cap=4) == 0:
            page.wait_for_timeout(350)
        search_input = _ensure_search_input(page, timeout_ms=35_000)
        search_input = _focus_search_input(page, search_input)

        typeahead = page.locator("[data-testid='typeahead-filter-option']")
        activity_token = _activity_filter_token(activity_prefix)

        # Primary route: type activity token and click
        # "Campaign name contains all of {activity_token}" suggestion.
        stage = "type_activity_token"
        dropdown_ready = False
        for query in (activity_token, _activity_filter_token(activity_prefix.lower()), activity_prefix.lower()):
            try:
                search_input.fill("")
                search_input.fill(query)
            except Exception:  # noqa: BLE001
                # React remount can stale the previous reference; reacquire and retry.
                search_input = _ensure_search_input(page, timeout_ms=10_000)
                search_input = _focus_search_input(page, search_input)
                search_input.fill("")
                search_input.fill(query)
            try:
                typeahead.first.wait_for(state="visible", timeout=6500)
                dropdown_ready = True
                break
            except Exception:  # noqa: BLE001
                continue

        stage = "click_campaign_suggestion"
        typeahead_missing = not dropdown_ready
        if dropdown_ready:
            suggestion_regex = re.compile(
                rf"(Campaign name|{re.escape(KR_CAMPAIGN_NAME)}).*"
                rf"(contains all of|{re.escape(KR_CONTAINS_ALL)}).*"
                rf"{re.escape(activity_token)}",
                re.IGNORECASE,
            )
            clicked_suggestion = _click_option_by_text(
                options=typeahead,
                exact_labels=(),
                regex=suggestion_regex,
                prefer_last=False,
            )
            if clicked_suggestion:
                page.wait_for_timeout(650)
                return verify_filter_chip(page, activity_prefix=activity_prefix)
            if logger:
                logger.warning(
                    "ui_suggest_fail activity=%s account=%s/%s reason=suggestion_not_matched",
                    activity_prefix,
                    account.act if account else "-",
                    account.business_id if account else "-",
                )
        elif logger:
            logger.warning(
                "ui_suggest_fail activity=%s account=%s/%s reason=typeahead_missing",
                activity_prefix,
                account.act if account else "-",
                account.business_id if account else "-",
            )

        # Fallback route: Name option -> filter popup -> explicit field/operator/value.
        stage = "name_popup_fallback"
        popup = None
        try:
            search_input.fill("")
        except Exception:  # noqa: BLE001
            search_input = _ensure_search_input(page, timeout_ms=8_000)
            search_input = _focus_search_input(page, search_input)
        for name_query in ("Name", KR_NAME):
            try:
                search_input.fill(name_query)
            except Exception:  # noqa: BLE001
                search_input = _ensure_search_input(page, timeout_ms=8_000)
                search_input = _focus_search_input(page, search_input)
                search_input.fill(name_query)
            try:
                typeahead.first.wait_for(state="visible", timeout=3200)
            except Exception:  # noqa: BLE001
                continue
            clicked_name = _click_option_by_text(
                options=typeahead,
                exact_labels=("Name", KR_NAME),
                regex=re.compile(rf"^(Name|{re.escape(KR_NAME)})$", re.IGNORECASE),
                prefer_last=False,
            )
            if not clicked_name:
                continue
            popup = _filter_popup_locator(page)
            try:
                popup.wait_for(state="visible", timeout=5500)
                break
            except Exception:  # noqa: BLE001
                popup = None
                continue

        # Additional fallback for environments where Name option is absent:
        # choose "Campaign name" from filter-only options to open popup.
        if popup is None:
            try:
                search_input.fill("")
            except Exception:  # noqa: BLE001
                search_input = _ensure_search_input(page, timeout_ms=8_000)
                search_input = _focus_search_input(page, search_input)
            for query in ("Campaign name", KR_CAMPAIGN_NAME):
                try:
                    search_input.fill(query)
                except Exception:  # noqa: BLE001
                    search_input = _ensure_search_input(page, timeout_ms=8_000)
                    search_input = _focus_search_input(page, search_input)
                    search_input.fill(query)
                try:
                    typeahead.first.wait_for(state="visible", timeout=3000)
                except Exception:  # noqa: BLE001
                    continue
                clicked_field = _click_option_by_text(
                    options=typeahead,
                    exact_labels=("Campaign name", KR_CAMPAIGN_NAME),
                    regex=RE_CAMPAIGN_NAME,
                    prefer_last=True,
                )
                if not clicked_field:
                    continue
                popup = _filter_popup_locator(page)
                try:
                    popup.wait_for(state="visible", timeout=5000)
                    break
                except Exception:  # noqa: BLE001
                    popup = None
                    continue

        if popup is None:
            if logger:
                logger.warning(
                    "ui_popup_fail activity=%s account=%s/%s reason=popup_not_opened typeahead_missing=%s",
                    activity_prefix,
                    account.act if account else "-",
                    account.business_id if account else "-",
                    "true" if typeahead_missing else "false",
                )
            raise AutomationError(
                "Could not open filter popup via Name/Campaign name fallback. "
                f"typeahead_missing={'true' if typeahead_missing else 'false'}"
            )

        stage = "popup_field_operator_value_apply"
        field_combo = popup.locator("div[role='combobox'][aria-haspopup='listbox']").first
        field_combo.wait_for(state="visible", timeout=4000)
        field_text = get_combobox_text(field_combo)
        if not RE_CAMPAIGN_NAME.search(field_text):
            field_combo.click(timeout=3000)
            _select_visible_listbox_option(
                page=page,
                exact_labels=("Campaign name", KR_CAMPAIGN_NAME),
                regex=RE_CAMPAIGN_NAME,
            )

        operator_combo = popup.locator("div[role='combobox'][aria-haspopup='listbox']").nth(1)
        operator_combo.wait_for(state="visible", timeout=4000)
        operator_text = get_combobox_text(operator_combo)
        if not RE_CONTAINS_ALL.search(operator_text):
            operator_combo.click(timeout=3000)
            _select_visible_listbox_option(
                page=page,
                exact_labels=("contains all of", KR_CONTAINS_ALL),
                regex=RE_CONTAINS_ALL,
            )

        value_input = popup.locator(
            "input[role='combobox'][type='text'], "
            "input[placeholder='Enter a name or keyword'], "
            f"input[placeholder='{KR_ENTER_NAME_OR_KEYWORD}']"
        ).first
        value_input.wait_for(state="visible", timeout=4000)
        value_input.click(timeout=2000)
        value_input.fill("")
        value_input.fill(activity_token)
        page.keyboard.press("Enter")

        apply_btn = popup.locator("div[role='button']").filter(has_text=RE_APPLY).first
        apply_btn.wait_for(state="visible", timeout=3000)
        deadline = time.time() + 4.0
        while time.time() < deadline:
            disabled = _safe_text(apply_btn.get_attribute("aria-disabled")).lower()
            if disabled != "true":
                break
            page.wait_for_timeout(120)
        if _safe_text(apply_btn.get_attribute("aria-disabled")).lower() == "true":
            raise AutomationError("Apply button still disabled after value input.")
        apply_btn.click(timeout=3000)

        page.wait_for_timeout(700)
        return verify_filter_chip(page, activity_prefix=activity_prefix)
    except Exception as exc:  # noqa: BLE001
        raise AutomationError(
            f"apply_filter_from_scratch failed stage={stage} activity={activity_prefix}: {exc}"
        ) from exc


def apply_filter_from_existing(
    page: Any,
    *,
    activity_prefix: str,
    logger: logging.Logger | None = None,
    account: AccountTarget | None = None,
) -> str:
    _force_clear_filter(page)
    page.wait_for_timeout(350)
    return apply_filter_from_scratch(
        page,
        activity_prefix=activity_prefix,
        logger=logger,
        account=account,
    )


def wait_for_user_filter_click(
    *,
    page: Any,
    logger: logging.Logger,
    activity_prefix: str,
    account: AccountTarget,
) -> str:
    logger.warning(
        "manual_filter_intervention_wait_start activity=%s account=%s/%s message=Please click filter box and apply campaign filter manually.",
        activity_prefix,
        account.act,
        account.business_id,
    )
    last_heartbeat = 0.0
    while True:
        try:
            chip = verify_filter_chip(page, activity_prefix=activity_prefix)
            logger.info(
                "manual_filter_intervention_resumed activity=%s account=%s/%s chip_text=%s",
                activity_prefix,
                account.act,
                account.business_id,
                chip,
            )
            return chip
        except Exception:
            pass

        now = time.time()
        if now - last_heartbeat >= DEFAULT_MANUAL_WAIT_HEARTBEAT_SEC:
            state = _filter_surface_state(page)
            logger.warning(
                "manual_filter_intervention_waiting activity=%s account=%s/%s shell_text_found=%s search_role_count=%s input_combobox_count=%s progress_visible=%s active_element_meta=%s",
                activity_prefix,
                account.act,
                account.business_id,
                state.get("shell_text_found"),
                state.get("search_role_count"),
                state.get("input_combobox_count"),
                state.get("progress_visible"),
                state.get("active_element_meta"),
            )
            last_heartbeat = now
        page.wait_for_timeout(500)


def ensure_campaign_name_filter(
    *,
    page: Any,
    logger: logging.Logger,
    activity_prefix: str,
    account: AccountTarget,
    options: RunnerOptions,
    max_retries: int,
    force_ui_fallback: bool = False,
) -> None:
    attempts = max(1, int(max_retries))
    last_exc: Exception | None = None
    reload_used = False
    attempt = 1

    while attempt <= attempts:
        state = "B" if has_active_filter(page) else "A"
        route = "state_aware" if attempt % 2 == 1 else "force_clear_then_scratch"
        chip_text = ""
        before_touch = _checkbox_touch_metrics(page)

        # URL bootstrap guardrail: if the expected chip is already present,
        # skip UI filtering and continue.
        if attempt == 1 and not force_ui_fallback:
            try:
                chip_text = verify_filter_chip(page, activity_prefix=activity_prefix)
                logger.info(
                    "filter_apply_success activity=%s attempt=%s/%s state=%s route=%s chip_text=%s",
                    activity_prefix,
                    attempt,
                    attempts,
                    state,
                    "url_bootstrap_verified",
                    chip_text,
                )
                return
            except Exception as bootstrap_exc:  # noqa: BLE001
                logger.warning(
                    "bootstrap_verify_fail activity=%s account=%s/%s error=%s",
                    activity_prefix,
                    account.act,
                    account.business_id,
                    bootstrap_exc,
                )
        elif attempt == 1 and force_ui_fallback:
            logger.info(
                "force_ui_filter_fallback_enabled activity=%s account=%s/%s",
                activity_prefix,
                account.act,
                account.business_id,
            )
            try:
                _force_clear_filter(page)
            except Exception:  # noqa: BLE001
                pass

        try:
            if route == "force_clear_then_scratch":
                _force_clear_filter(page)
                chip_text = apply_filter_from_scratch(
                    page,
                    activity_prefix=activity_prefix,
                    logger=logger,
                    account=account,
                )
            else:
                if state == "B":
                    chip_text = apply_filter_from_existing(
                        page,
                        activity_prefix=activity_prefix,
                        logger=logger,
                        account=account,
                    )
                else:
                    chip_text = apply_filter_from_scratch(
                        page,
                        activity_prefix=activity_prefix,
                        logger=logger,
                        account=account,
                    )

            after_touch = _checkbox_touch_metrics(page)
            _log_checkbox_touch_if_changed(
                logger=logger,
                activity_prefix=activity_prefix,
                attempt=attempt,
                route=route,
                before=before_touch,
                after=after_touch,
            )
            logger.info(
                "filter_apply_success activity=%s attempt=%s/%s state=%s route=%s chip_text=%s",
                activity_prefix,
                attempt,
                attempts,
                state,
                route,
                chip_text,
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            try:
                current_chip = page.locator(
                    f"[role='search'] button[aria-label='Edit filter'], "
                    f"[role='search'] button[aria-label='{KR_EDIT_FILTER}']"
                ).first.text_content(timeout=1200)
                chip_text = _normalize_ui_text(current_chip or "")
            except Exception:  # noqa: BLE001
                chip_text = ""

            if chip_text and _chip_text_matches_expected(
                activity_prefix=activity_prefix,
                chip_text=chip_text,
            ):
                logger.warning(
                    "filter_apply_recovered_by_chip activity=%s attempt=%s/%s state=%s route=%s chip_text=%s",
                    activity_prefix,
                    attempt,
                    attempts,
                    state,
                    route,
                    chip_text,
                )
                return

            after_touch = _checkbox_touch_metrics(page)
            _log_checkbox_touch_if_changed(
                logger=logger,
                activity_prefix=activity_prefix,
                attempt=attempt,
                route=route,
                before=before_touch,
                after=after_touch,
            )
            logger.warning(
                "filter_apply_failed activity=%s attempt=%s/%s state=%s route=%s chip_text=%s error=%s",
                activity_prefix,
                attempt,
                attempts,
                state,
                route,
                chip_text or "<none>",
                exc,
            )
            if (not reload_used) and _is_filter_reload_eligible(exc):
                reload_used = True
                try:
                    _close_any_popup(page)
                    _force_clear_filter(page)
                except Exception:  # noqa: BLE001
                    pass
                _reload_adsets_once(
                    page=page,
                    logger=logger,
                    options=options,
                    activity_prefix=activity_prefix,
                    account=account,
                )
                continue

            _close_any_popup(page)
            _force_clear_filter(page)
            page.wait_for_timeout(750)
            attempt += 1

    if options.headless:
        raise ManualInterventionRequired(
            "manual_filter_intervention_required_in_headless "
            f"activity={activity_prefix} account={account.act}/{account.business_id}"
        ) from last_exc

    wait_for_user_filter_click(
        page=page,
        logger=logger,
        activity_prefix=activity_prefix,
        account=account,
    )


def _select_all_adsets(page: Any) -> None:
    checkbox = page.locator(
        "input[aria-label='Checkbox to select all the campaigns'], "
        "input[aria-label='모든 캠페인을 선택하기 위한 체크 박스'], "
        "input[aria-label='Checkbox to select all the ads'], "
        f"input[aria-label='{KR_SELECT_ALL_ADS}'], "
        "input[type='checkbox'][aria-label*='select all'], "
        "input[type='checkbox'][aria-label*='선택하기 위한 체크 박스']"
    ).first
    checkbox.wait_for(state="visible", timeout=12_000)

    busy_deadline = time.time() + 12
    while time.time() < busy_deadline:
        busy = _safe_text(checkbox.get_attribute("aria-busy")).lower()
        if busy in {"", "false"}:
            break
        page.wait_for_timeout(300)

    if not checkbox.is_checked():
        try:
            checkbox.check(timeout=5000, force=True)
        except Exception:
            checkbox.click(timeout=5000, force=True)
    page.wait_for_timeout(400)


def _open_history_panel(page: Any) -> None:
    table = page.locator(TABLE_SELECTOR).first
    history_button = page.locator(
        "xpath=(//div[@role='button' and @aria-label and "
        "(@aria-disabled='false' or not(@aria-disabled)) and "
        "(contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'history') "
        f"or contains(@aria-label,'{KR_HISTORY}'))])[1]"
    ).first
    clicked = False
    try:
        if history_button.is_visible(timeout=6000):
            history_button.click(timeout=5000)
            clicked = True
    except Exception:  # noqa: BLE001
        clicked = False

    if not clicked:
        try:
            page.keyboard.press("Escape")
        except Exception:  # noqa: BLE001
            pass
        page.keyboard.press("Control+i")
        page.wait_for_timeout(450)
        try:
            if not table.is_visible():
                page.keyboard.press("Control+i")
        except Exception:  # noqa: BLE001
            page.keyboard.press("Control+i")

    page.wait_for_timeout(1200)
    table.wait_for(state="visible", timeout=20_000)


def _ensure_last_7_days(page: Any) -> None:
    # NOTE: Date-range button text contains dynamic date strings.
    # Selector intentionally matches partial EN/KR text instead of exact value.
    date_btn = page.locator(
        "xpath=(//div[@role='button' and "
        "((contains(normalize-space(.),'Last') and contains(normalize-space(.),'days')) "
        f"or (contains(normalize-space(.),'{KR_LAST}') and contains(normalize-space(.),'{KR_DAY}')))"
        "])[1]"
    ).first
    date_btn.wait_for(state="visible", timeout=10_000)
    date_btn.click(timeout=5000)

    radio = page.locator("input[type='radio'][value='last_7d']").first
    radio.wait_for(state="visible", timeout=10_000)
    if not radio.is_checked():
        radio.check(timeout=5000, force=True)

    last_exc: Exception | None = None
    for _ in range(4):
        update_btn = page.get_by_role(
            "button", name=re.compile(rf"^(Update|{re.escape(KR_UPDATE)})$")
        ).first
        try:
            update_btn.wait_for(state="visible", timeout=3000)
            update_btn.click(timeout=5000)
            break
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            page.wait_for_timeout(250)
    else:
        raise AutomationError(f"Could not click date Update button: {last_exc}")
    page.wait_for_timeout(1200)


def _ensure_scope_adsets(page: Any) -> None:
    # NOTE: Scope dropdown wording can change by locale and Meta UI updates.
    scope_btn = page.locator(
        "xpath=(//div[@role='button' and @aria-haspopup='menu' and "
        f"(contains(normalize-space(.),'Activity history') or contains(normalize-space(.),'{KR_ACTIVITY_HISTORY}'))])[1]"
    ).first
    scope_btn.wait_for(state="visible", timeout=10_000)
    scope_btn.click(timeout=5000)

    try:
        page.get_by_role(
            "menuitem",
            name=re.compile(rf"^(Ad Sets|{re.escape(KR_AD_SETS)})$"),
        ).first.click(timeout=5000)
    except Exception:
        page.get_by_role(
            "menuitemradio",
            name=re.compile(rf"^(Ad Sets|{re.escape(KR_AD_SETS)})$"),
        ).first.click(timeout=5000)
    page.wait_for_timeout(900)


def _extract_visible_rows(table: Any) -> list[list[str]]:
    rows: list[list[str]] = []
    row_locator = table.locator("tbody tr[role='row']")
    row_count = row_locator.count()
    for row_idx in range(row_count):
        cell_locator = row_locator.nth(row_idx).locator("td[role='gridcell']")
        cell_count = cell_locator.count()
        if cell_count < 5:
            continue
        values: list[str] = []
        for col_idx in range(5):
            text = cell_locator.nth(col_idx).inner_text(timeout=2000)
            values.append(_normalize_cell(text))
        rows.append(values)
    return rows


def _extract_rows_dom(
    *,
    page: Any,
    options: RunnerOptions,
    logger: logging.Logger,
) -> list[list[str]]:
    table = _table_locator(page)
    table.wait_for(state="visible", timeout=max(1, options.table_load_timeout_sec) * 1000)

    seen: OrderedDict[str, list[str]] = OrderedDict()
    stable_rounds = 0

    for round_idx in range(1, options.lazy_scroll_max_rounds + 1):
        before = len(seen)
        for row in _extract_visible_rows(table):
            key = "\x1f".join(row)
            if key not in seen:
                seen[key] = row

        try:
            table.evaluate(
                "(el) => { el.scrollTop = el.scrollHeight; return [el.scrollTop, el.scrollHeight, el.clientHeight]; }"
            )
        except Exception:  # noqa: BLE001
            pass

        page.wait_for_timeout(max(100, int(options.lazy_scroll_pause_sec * 1000)))
        after = len(seen)
        if after == before:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= options.lazy_scroll_no_new_rounds:
            logger.info(
                "lazy_scroll_done rounds=%s rows=%s stable_rounds=%s",
                round_idx,
                len(seen),
                stable_rounds,
            )
            break

    return list(seen.values())


def _parse_clipboard_tsv(raw_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [_normalize_cell(part) for part in line.split("\t")]
        if len(parts) < 5:
            continue
        row = parts[:5]
        head = row[0].lower()
        if head in {"activity"} or row[0] in {KR_ACTIVITY}:
            continue
        key = tuple(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def _extract_rows_clipboard_fallback(page: Any, *, logger: logging.Logger) -> list[list[str]]:
    try:
        import pyperclip  # type: ignore
    except Exception as exc:  # noqa: BLE001
        logger.warning("clipboard_fallback_skip reason=pyperclip_import_failed error=%s", exc)
        return []

    table = _table_locator(page)
    try:
        table.click(timeout=3000)
        page.keyboard.press("Control+a")
        page.wait_for_timeout(200)
        page.keyboard.press("Control+c")
        page.wait_for_timeout(350)
    except Exception as exc:  # noqa: BLE001
        logger.warning("clipboard_fallback_copy_failed error=%s", exc)
        return []

    copied = _safe_text(pyperclip.paste())
    if not copied:
        logger.warning("clipboard_fallback_empty")
        return []

    rows = _parse_clipboard_tsv(copied)
    logger.info("clipboard_fallback_rows=%s", len(rows))
    return rows


def _collect_for_account(
    *,
    page: Any,
    logger: logging.Logger,
    options: RunnerOptions,
    activity_prefix: str,
    account: AccountTarget,
    force_ui_fallback: bool = False,
) -> list[list[str]]:
    _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="goto_campaigns_bootstrap",
        fn=lambda: _goto_campaigns_with_bootstrap_filter(
            page,
            account,
            activity_prefix=activity_prefix,
            options=options,
        ),
    )
    logger.info(
        "step_start activity=%s account=%s step=%s attempt=%s/%s",
        activity_prefix,
        f"{account.act}/{account.business_id}",
        "ensure_campaign_filter",
        1,
        options.step_retry_count,
    )
    try:
        ensure_campaign_name_filter(
            page=page,
            logger=logger,
            activity_prefix=activity_prefix,
            account=account,
            options=options,
            max_retries=options.step_retry_count,
            force_ui_fallback=force_ui_fallback,
        )
        logger.info(
            "step_success activity=%s account=%s step=%s",
            activity_prefix,
            f"{account.act}/{account.business_id}",
            "ensure_campaign_filter",
        )
    except Exception:
        shot = _capture_screenshot(
            page=page,
            options=options,
            activity_prefix=activity_prefix,
            account=account,
            step_name="ensure_campaign_filter_failed",
        )
        logger.exception(
            "step_fail activity=%s account=%s step=%s screenshot=%s",
            activity_prefix,
            f"{account.act}/{account.business_id}",
            "ensure_campaign_filter",
            shot or "<none>",
        )
        raise
    _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="select_all_filtered_rows",
        fn=lambda: _select_all_adsets(page),
    )
    _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="open_history_panel",
        fn=lambda: _open_history_panel(page),
    )
    _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="set_last_7_days",
        fn=lambda: _ensure_last_7_days(page),
    )
    _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="set_scope_adsets",
        fn=lambda: _ensure_scope_adsets(page),
    )

    rows = _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="extract_rows_dom",
        fn=lambda: _extract_rows_dom(page=page, options=options, logger=logger),
    )
    if rows:
        return rows

    logger.warning(
        "dom_extract_empty activity=%s account=%s/%s trying_clipboard_fallback=true",
        activity_prefix,
        account.act,
        account.business_id,
    )
    return _run_step(
        logger=logger,
        page=page,
        options=options,
        activity_prefix=activity_prefix,
        account=account,
        step_name="extract_rows_clipboard_fallback",
        fn=lambda: _extract_rows_clipboard_fallback(page, logger=logger),
    )


def _dedupe_rows(rows: list[list[str]]) -> list[list[str]]:
    deduped: OrderedDict[str, list[str]] = OrderedDict()
    for row in rows:
        if len(row) < 5:
            continue
        normalized = [_normalize_cell(item) for item in row[:5]]
        key = "\x1f".join(normalized)
        if key not in deduped:
            deduped[key] = normalized
    return list(deduped.values())


def _save_activity_xlsx(
    *,
    rows: list[list[str]],
    output_path: Path,
) -> None:
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("pandas/openpyxl are required to write xlsx output.") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=HISTORY_COLUMNS)
    frame.to_excel(output_path, index=False)


def _build_output_file_path(*, options: RunnerOptions, activity_prefix: str) -> Path:
    date_token = dt.datetime.now().strftime("%Y%m%d")
    file_name = f"{_slug(activity_prefix)}_history_{date_token}.xlsx"
    return (options.output_dir / file_name).resolve()


def _launch_context(playwright: Any, options: RunnerOptions) -> Any:
    channel: str | None = None
    if options.browser in {"msedge", "chrome"}:
        channel = options.browser

    context = playwright.chromium.launch_persistent_context(
        user_data_dir=str(options.user_data_dir),
        channel=channel,
        headless=options.headless,
        viewport={"width": 1600, "height": 920},
        args=["--disable-blink-features=AutomationControlled"],
    )
    context.set_default_timeout(options.action_timeout_ms)
    return context


def _open_output_in_explorer(*, path: Path, logger: logging.Logger) -> None:
    try:
        if os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
            logger.info("opened_output_in_explorer path=%s", path)
            return
    except Exception as exc:  # noqa: BLE001
        logger.warning("open_output_in_explorer_failed path=%s error=%s", path, exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Meta Ad Sets Activity History collector")
    parser.add_argument(
        "--config",
        help="Optional YAML/JSON config for script paths/runner options (no target overrides).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configs and print extracted activity/account targets without launching browser.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose console logging.",
    )
    parser.add_argument(
        "--force-ui-filter-fallback",
        action="store_true",
        help="Skip URL bootstrap verification and force UI filter flow (manual fallback test mode).",
    )
    return parser.parse_args()


def _resolve_script_dir() -> Path:
    # In PyInstaller onefile mode, __file__ points to a temp extraction path.
    # Use executable directory so config sharing via ../config/meta keeps working.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def main() -> int:
    args = parse_args()
    script_dir = _resolve_script_dir()
    run_date_token = dt.datetime.now().strftime("%Y%m%d")

    user_cfg: dict[str, Any] = {}
    if args.config:
        user_cfg = _read_yaml_or_json(_expand_path(args.config, base_dir=script_dir))
    else:
        auto_cfg = script_dir / "config.yaml"
        if auto_cfg.exists():
            user_cfg = _read_yaml_or_json(auto_cfg)

    catalog_path, runtime_path = _resolve_paths(script_dir=script_dir, user_config=user_cfg)
    runtime_settings = _read_json(runtime_path)
    options = _build_runner_options(
        script_dir=script_dir,
        runtime_settings=runtime_settings,
        user_config=user_cfg,
    )
    options = _partition_paths_by_run_date(options, run_date_token=run_date_token)
    try:
        options.output_dir.mkdir(parents=True, exist_ok=True)
        options.screenshot_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        fallback_root = (script_dir / "_local_output" / "history_logs" / run_date_token).resolve()
        options = replace(
            options,
            output_dir=fallback_root,
            log_dir=(fallback_root / "logs").resolve(),
            screenshot_dir=(fallback_root / "screenshots").resolve(),
        )
        options.output_dir.mkdir(parents=True, exist_ok=True)
        options.screenshot_dir.mkdir(parents=True, exist_ok=True)
    try:
        logger = _setup_logger(options.log_dir, verbose=args.verbose)
    except Exception:
        fallback_root = (script_dir / "_local_output" / "history_logs" / run_date_token).resolve()
        options = replace(
            options,
            output_dir=fallback_root,
            log_dir=(fallback_root / "logs").resolve(),
            screenshot_dir=(fallback_root / "screenshots").resolve(),
        )
        options.output_dir.mkdir(parents=True, exist_ok=True)
        options.screenshot_dir.mkdir(parents=True, exist_ok=True)
        logger = _setup_logger(options.log_dir, verbose=args.verbose)

    logger.info("catalog_path=%s", catalog_path)
    logger.info("runtime_settings_path=%s", runtime_path)
    logger.info(
        "runner_options browser=%s headless=%s user_data_dir=%s output_dir=%s",
        options.browser,
        options.headless,
        options.user_data_dir,
        options.output_dir,
    )

    catalog = _read_json(catalog_path)
    activity_targets, warnings = _extract_accounts_by_activity(catalog)
    for warning in warnings:
        logger.warning("config_warning %s", warning)

    if not activity_targets:
        logger.error("No enabled activities found in activity_catalog.json.")
        return 2

    logger.info("activity_count=%s", len(activity_targets))
    for activity_prefix, accounts in activity_targets.items():
        logger.info(
            "activity_target prefix=%s account_count=%s accounts=%s",
            activity_prefix,
            len(accounts),
            [f"{item.act}/{item.business_id}" for item in accounts],
        )

    if args.dry_run:
        logger.info("dry_run=true completed without browser launch.")
        return 0

    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        logger.exception("playwright_import_failed")
        raise RuntimeError("playwright is required. Run: pip install -r requirements.txt") from exc

    activity_results: dict[str, dict[str, Any]] = {}
    abort_run = False
    abort_reason = ""
    with sync_playwright() as playwright:
        active_options = options
        context = _launch_context(playwright, active_options)
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://business.facebook.com/", wait_until="domcontentloaded")
            logger.info("Opened Meta Business home. Reuse existing logged-in profile or log in once.")

            for activity_prefix, accounts in activity_targets.items():
                logger.info(
                    "activity_start prefix=%s account_count=%s",
                    activity_prefix,
                    len(accounts),
                )
                collected_rows: list[list[str]] = []
                failed_accounts: list[str] = []

                if not accounts:
                    logger.warning(
                        "activity_no_accounts prefix=%s output will be empty",
                        activity_prefix,
                    )

                manual_retry_done: set[str] = set()
                account_index = 0
                while account_index < len(accounts):
                    account = accounts[account_index]
                    retry_key = f"{activity_prefix}:{account.act}:{account.business_id}"
                    try:
                        rows = _collect_for_account(
                            page=page,
                            logger=logger,
                            options=active_options,
                            activity_prefix=activity_prefix,
                            account=account,
                            force_ui_fallback=args.force_ui_filter_fallback,
                        )
                        logger.info(
                            "account_collect_done activity=%s account=%s/%s rows=%s",
                            activity_prefix,
                            account.act,
                            account.business_id,
                            len(rows),
                        )
                        collected_rows.extend(rows)
                        account_index += 1
                    except ManualInterventionRequired as exc:
                        if active_options.headless and retry_key not in manual_retry_done:
                            manual_retry_done.add(retry_key)
                            logger.warning(
                                "manual_filter_headless_restart activity=%s account=%s/%s reason=%s",
                                activity_prefix,
                                account.act,
                                account.business_id,
                                exc,
                            )
                            try:
                                context.close()
                            except Exception:  # noqa: BLE001
                                pass
                            active_options = replace(active_options, headless=False)
                            context = _launch_context(playwright, active_options)
                            page = context.pages[0] if context.pages else context.new_page()
                            page.goto("https://business.facebook.com/", wait_until="domcontentloaded")
                            _wait_for_login_context(page, timeout_sec=active_options.login_timeout_sec)
                            logger.info(
                                "manual_filter_headful_restart_done activity=%s account=%s/%s",
                                activity_prefix,
                                account.act,
                                account.business_id,
                            )
                            continue
                        failed_accounts.append(f"{account.act}/{account.business_id}")
                        shot = _capture_screenshot(
                            page=page,
                            options=active_options,
                            activity_prefix=activity_prefix,
                            account=account,
                            step_name="account_failed_final",
                        )
                        logger.exception(
                            "account_collect_failed activity=%s account=%s/%s screenshot=%s error=%s",
                            activity_prefix,
                            account.act,
                            account.business_id,
                            shot or "<none>",
                            exc,
                        )
                        abort_run = True
                        abort_reason = (
                            f"first_failed_account={account.act}/{account.business_id} "
                            f"activity={activity_prefix} error={exc}"
                        )
                        break
                    except Exception as exc:  # noqa: BLE001
                        failed_accounts.append(f"{account.act}/{account.business_id}")
                        shot = _capture_screenshot(
                            page=page,
                            options=active_options,
                            activity_prefix=activity_prefix,
                            account=account,
                            step_name="account_failed_final",
                        )
                        logger.exception(
                            "account_collect_failed activity=%s account=%s/%s screenshot=%s error=%s",
                            activity_prefix,
                            account.act,
                            account.business_id,
                            shot or "<none>",
                            exc,
                        )
                        abort_run = True
                        abort_reason = (
                            f"first_failed_account={account.act}/{account.business_id} "
                            f"activity={activity_prefix} error={exc}"
                        )
                        break

                deduped = _dedupe_rows(collected_rows)
                output_path = _build_output_file_path(
                    options=active_options,
                    activity_prefix=activity_prefix,
                )
                _save_activity_xlsx(rows=deduped, output_path=output_path)
                logger.info(
                    "activity_output_saved prefix=%s rows=%s path=%s failed_accounts=%s",
                    activity_prefix,
                    len(deduped),
                    output_path,
                    failed_accounts,
                )

                activity_results[activity_prefix] = {
                    "rows": len(deduped),
                    "path": str(output_path),
                    "failed_accounts": failed_accounts,
                }
                if abort_run:
                    logger.error(
                        "run_abort_on_first_failed_account reason=%s",
                        abort_reason,
                    )
                    break
        finally:
            context.close()

    logger.info("run_complete activities=%s", len(activity_results))
    for prefix, result in activity_results.items():
        logger.info(
            "summary activity=%s rows=%s failed_accounts=%s file=%s",
            prefix,
            result["rows"],
            result["failed_accounts"],
            result["path"],
        )
    if not args.dry_run:
        _open_output_in_explorer(path=options.output_dir, logger=logger)
    return 1 if abort_run else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ConfigError as exc:
        print(f"[CONFIG ERROR] {exc}", file=sys.stderr)
        raise

