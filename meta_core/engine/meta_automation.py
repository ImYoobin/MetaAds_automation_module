from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from selenium.webdriver.common.by import By

from .common import get_logger
from .file_naming import format_name, newest_xlsx_in_dir


@dataclass
class ExportResult:
    success: bool
    method: str
    export_name: str
    request_ts: float
    report_name: str
    accepted_export_name: str = ""


@dataclass
class DownloadResult:
    success: bool
    method: str
    file_path: str


@dataclass
class ViewDownloadWaitResult:
    success: bool
    file_path: str = ""
    fallback_trigger_reason: str = ""
    view_toast_state: str = "none"
    view_toast_progress_pct: Optional[int] = None
    view_toast_text: str = ""
    toast_transition_seen: bool = False
    toast_wait_elapsed_ms: int = 0
    file_signal_detected_at: str = ""


@dataclass
class ExportTicket:
    sheet_name: str
    report_name: str
    report_id: str
    export_name: str
    request_ts: float
    target_path: str
    brand: str = ""
    exports_url: str = ""
    sheet_key: str = ""
    expected_account_id: str = ""
    expected_business_id: str = ""
    fallback_trigger_reason: str = ""
    view_toast_state: str = "none"
    view_toast_progress_pct: Optional[int] = None
    view_toast_text: str = ""
    toast_transition_seen: bool = False
    toast_wait_elapsed_ms: int = 0
    file_signal_detected_at: str = ""
    accepted_export_name: str = ""
    search_queries: Optional[List[str]] = None


@dataclass
class SheetDownloadResult:
    sheet_name: str
    success: bool
    file_path: str
    reason: str


@dataclass
class MetaBrandExportResult:
    # Public contract consumed by pipeline/public_api; keep fields backward-compatible.
    raw_files: Dict[str, str]
    sheet_status: Dict[str, Dict[str, Any]]
    warnings: List[str]


@dataclass
class ExportHistoryRow:
    name_raw: str
    name_key: str
    status_norm: str
    export_dt: Optional[float]
    row_index: int
    checkbox_el: Any
    row_el: Any
    row_top_px: Optional[float] = None
    download_button_el: Any = None
    export_date_raw: str = ""
    status_raw: str = ""
    source_mode: str = "legacy"


@dataclass
class ExportRowMatchStats:
    ui_visible_rows_count: int = 0
    parsed_rows_count: int = 0
    name_matched_rows_count: int = 0
    ready_rows_count: int = 0
    actionable_rows_count: int = 0


class AccountContextMismatchError(RuntimeError):
    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = dict(payload or {})
        super().__init__(json.dumps(self.payload, ensure_ascii=False))


class HistoryDownloadError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "", details: Optional[Dict[str, Any]] = None) -> None:
        self.reason = str(reason or "").strip()
        self.details = dict(details or {})
        super().__init__(message)


class MetaAutomation:
    def __init__(
        self,
        meta_config: Dict,
        naming_config: Dict,
        download_dir: str,
        headless: bool = False,
    ) -> None:
        self.meta_config = meta_config
        self.naming_config = naming_config
        self.download_dir = download_dir
        self.headless = headless
        self.logger = get_logger()
        self._browser_download_dir = ""
        self._download_behavior_configured = False

    def _progress(self, cb: Optional[Callable[[str], None]], message: str) -> None:
        if cb:
            cb(message)

    def _emit_export_progress_event(
        self,
        progress_event_cb: Optional[Callable[[Dict[str, Any]], None]],
        *,
        phase: str,
        attempt: int = 0,
        snapshot: Optional[Dict[str, Any]] = None,
        reason: str = "",
    ) -> None:
        if not progress_event_cb:
            return
        event = {
            "phase": str(phase or "").strip(),
            "attempt": max(0, int(attempt or 0)),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "snapshot": dict(snapshot or {}),
        }
        if reason:
            event["reason"] = str(reason)
        try:
            progress_event_cb(event)
        except Exception:
            self.logger.warning("Meta progress callback failed", exc_info=True)

    def _is_meta_logged_in(self, sb) -> bool:
        url = ""
        with suppress(Exception):
            url = (sb.get_current_url() or "").lower()

        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()

        has_fb_domain = ("facebook.com" in host) or ("fb.com" in host) or ("adsmanager.com" in host)
        is_auth_gate = any(
            token in path
            for token in [
                "/login",
                "/loginpage",
                "checkpoint",
                "recover",
                "two_factor",
                "device-based",
            ]
        )

        has_c_user_cookie = False
        with suppress(Exception):
            c_user = sb.driver.get_cookie("c_user")
            has_c_user_cookie = bool(c_user and c_user.get("value"))

        # URL-based pass OR cookie-based pass.
        if has_fb_domain and not is_auth_gate:
            return True
        if has_c_user_cookie:
            return True
        return False

    def _wait_for_meta_login(self, sb, progress_cb: Optional[Callable[[str], None]] = None) -> None:
        timeout = int(self.meta_config.get("login_timeout_sec", 300))
        start = time.time()
        self.logger.info("Waiting for META login (timeout=%ss)", timeout)
        tick = 0
        while time.time() - start <= timeout:
            tick += 1
            if self._is_meta_logged_in(sb):
                return
            if tick % 5 == 0:
                current_url = ""
                with suppress(Exception):
                    current_url = sb.get_current_url() or ""
                has_c_user = False
                with suppress(Exception):
                    c_user = sb.driver.get_cookie("c_user")
                    has_c_user = bool(c_user and c_user.get("value"))
                self.logger.info("META login wait... url=%s c_user=%s", current_url, has_c_user)
                self._progress(progress_cb, f"A-2 Waiting login... url={current_url[:120]}")
            sb.sleep(2)
        raise TimeoutError("META login timeout")

    def _safe_click_any_text(self, sb, text: str, timeout: float = 2.0) -> bool:
        xpaths = [
            f"//*[normalize-space()='{text}']",
            f"//*[contains(normalize-space(),'{text}')]",
            f"//span[normalize-space()='{text}']",
            f"//button[normalize-space()='{text}']",
            f"//div[normalize-space()='{text}']",
            f"//a[normalize-space()='{text}']",
        ]
        for xp in xpaths:
            try:
                sb.click(xp, timeout=timeout)
                return True
            except Exception:
                continue
        return False

    def _xpath_literal(self, value: str) -> str:
        if "'" not in value:
            return f"'{value}'"
        if '"' not in value:
            return f'"{value}"'
        parts = value.split("'")
        return "concat(" + ", \"'\", ".join(f"'{p}'" for p in parts) + ")"

    def _report_name_variants(self, report_name: str) -> List[str]:
        variants = [
            report_name,
            report_name.replace("-", " "),
            report_name.replace("_", " "),
            report_name.replace(" ", "_"),
            report_name.replace(" ", "-"),
        ]
        out: List[str] = []
        for item in variants:
            val = item.strip()
            if val and val not in out:
                out.append(val)
        return out

    def _export_history_name_variants(self, export_name: str) -> List[str]:
        variants = self._report_name_variants(export_name)
        out: List[str] = []
        for name in variants:
            if name not in out:
                out.append(name)
            if name.lower().endswith(".xlsx"):
                stem = name[:-5].strip()
                if stem and stem not in out:
                    out.append(stem)
            else:
                name_with_ext = f"{name}.xlsx"
                if name_with_ext not in out:
                    out.append(name_with_ext)
        return out

    def _extract_export_name_from_toast_text(self, toast_text: str) -> str:
        text = str(toast_text or "").replace("\u200b", " ")
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return ""

        # Prefer explicit [Report]... tokens that Meta shows in toast.
        bracket_match = re.search(
            r"(\[[^\]]+\][^\\n\\r]*?)(?:\s+\(\d{1,3}%\))?(?:\s+(?:view all|close)\b|$)",
            text,
            flags=re.IGNORECASE,
        )
        if bracket_match:
            extracted = re.sub(r"\s+", " ", bracket_match.group(1)).strip(" -:")
            extracted = re.sub(r"\s+\(\d{1,3}%\)$", "", extracted).strip()
            return extracted

        # Fallback: capture phrase between readiness token and trailing CTA text.
        generic_match = re.search(
            r"(?:your export is ready|export is ready|creating export|내보내기 준비|내보내기 완료)\s+(.+?)(?:\s+(?:view all|close)\b|$)",
            text,
            flags=re.IGNORECASE,
        )
        if generic_match:
            extracted = re.sub(r"\s+", " ", generic_match.group(1)).strip(" -:")
            extracted = re.sub(r"\s+\(\d{1,3}%\)$", "", extracted).strip()
            return extracted
        return ""

    def _build_history_search_queries(
        self,
        *,
        export_name: str,
        report_name: str,
        accepted_export_name: str = "",
    ) -> List[str]:
        out: List[str] = []

        def _add(value: str) -> None:
            val = str(value or "").replace("\u200b", " ")
            val = re.sub(r"\s+", " ", val).strip()
            if val and val not in out:
                out.append(val)

        requested = str(export_name or "").strip()
        accepted = str(accepted_export_name or "").strip()
        report = str(report_name or "").strip()

        _add(requested)
        _add(accepted)

        for candidate in (accepted, requested):
            if not candidate:
                continue
            match = re.search(r"(\[[^\]]+\][^\\n\\r]*)", candidate)
            if match:
                _add(match.group(1))
            stem = re.sub(r"\.xlsx$", "", candidate, flags=re.IGNORECASE).strip()
            _add(stem)

        if report:
            report_variants = self._report_name_variants(report)
            for rep in report_variants:
                _add(rep)
                _add(f"[{rep}]")

        # Add compact tokens as a last-resort fallback search key.
        for source_name in (accepted, requested):
            if not source_name:
                continue
            parts = [part for part in re.split(r"[_\s]+", source_name) if part]
            if len(parts) >= 2:
                _add(parts[1])
            if len(parts) >= 3:
                _add(parts[-1])

        return out[:10]

    def _merge_error_reason(self, existing: str, new_reason: str) -> str:
        old_val = str(existing or "").strip()
        new_val = str(new_reason or "").strip()
        if not old_val:
            return new_val
        if not new_val or new_val in old_val:
            return old_val
        return f"{old_val} | {new_val}"

    def _sorted_report_items(self, report_to_sheet: Dict[str, str]) -> List[tuple[str, str]]:
        sheet_order = {"Overall": 0, "Demo": 1, "Time": 2}
        items = list(report_to_sheet.items())
        items.sort(key=lambda item: (sheet_order.get(item[1], 99), item[0]))
        return items

    def _download_ticket_sort_key(self, ticket: ExportTicket) -> tuple[int, str]:
        sheet_order = {"Demo": 0, "Overall": 1, "Time": 2}
        return (sheet_order.get(ticket.sheet_name, 99), ticket.sheet_name)

    def _init_sheet_status(self, report_to_sheet: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for report_name, sheet_name in self._sorted_report_items(report_to_sheet):
            out[sheet_name] = {
                "report_name": report_name,
                "export_ok": False,
                "download_ok": False,
                "error_reason": "",
            }
        return out

    def _click_first_xpath(self, sb, xpaths: List[str], timeout: float = 1.5) -> bool:
        for xp in xpaths:
            with suppress(Exception):
                elements = sb.driver.find_elements(By.XPATH, xp)
                for element in elements:
                    if self._click_element_with_fallback(sb=sb, element=element):
                        return True
            try:
                sb.click(xp, timeout=timeout)
                return True
            except Exception:
                continue
        return False

    def _click_first_css(self, sb, selectors: List[str], timeout: float = 1.5) -> bool:
        for sel in selectors:
            with suppress(Exception):
                elements = sb.driver.find_elements(By.CSS_SELECTOR, sel)
                for element in elements:
                    if self._click_element_with_fallback(sb=sb, element=element):
                        return True
            try:
                sb.click(sel, by=By.CSS_SELECTOR, timeout=timeout)
                return True
            except Exception:
                continue
        return False

    def _is_element_clickable(self, element: Any) -> bool:
        visible = True
        enabled = True
        with suppress(Exception):
            visible = bool(element.is_displayed())
        with suppress(Exception):
            enabled = bool(element.is_enabled())
        with suppress(Exception):
            if str(element.get_attribute("aria-disabled") or "").strip().lower() == "true":
                enabled = False
        with suppress(Exception):
            raw_disabled_attr = element.get_attribute("disabled")
            if raw_disabled_attr is not None:
                disabled_attr = str(raw_disabled_attr).strip().lower()
                if disabled_attr in {"", "true", "disabled", "1"}:
                    enabled = False
        with suppress(Exception):
            if str(element.get_attribute("tabindex") or "").strip() == "-1":
                enabled = False
        with suppress(Exception):
            cls = str(element.get_attribute("class") or "").strip().lower()
            if "disabled" in cls:
                enabled = False
        return bool(visible and enabled)

    def _click_element_with_fallback(self, sb, element: Any) -> bool:
        if element is None:
            return False
        if not self._is_element_clickable(element):
            return False
        with suppress(Exception):
            element.click()
            return True
        with suppress(Exception):
            clicked = bool(
                sb.driver.execute_script(
                """
                const el = arguments[0];
                if (!el) return false;
                const isVisible = (node) => {
                  if (!node) return false;
                  const style = window.getComputedStyle(node);
                  if (style.display === 'none' || style.visibility === 'hidden' || style.pointerEvents === 'none') {
                    return false;
                  }
                  const rect = node.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const candidates = [];
                candidates.push(el);
                const closestInteractive = el.closest(
                  "[role='button'],button,a,[data-surface*='export_history_table_button']"
                );
                if (closestInteractive) candidates.push(closestInteractive);
                for (const candidate of candidates) {
                  if (!candidate) continue;
                  const ariaDisabled = String(candidate.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                  const disabledAttr = candidate.hasAttribute('disabled');
                  const cls = String(candidate.getAttribute('class') || '').toLowerCase();
                  if (ariaDisabled || disabledAttr || cls.includes('disabled')) continue;
                  if (!isVisible(candidate)) continue;
                  try {
                    if (candidate.scrollIntoView) {
                      candidate.scrollIntoView({ block: 'center', inline: 'nearest' });
                    }
                    candidate.click();
                    return true;
                  } catch (e) {}
                }
                return false;
                """,
                element,
                )
            )
            if clicked:
                return True
            clicked = sb.driver.execute_script(
                """
                const el = arguments[0];
                if (!el) return false;
                const target =
                  el.closest("[role='button'],button,a,[data-surface*='export_history_table_button']") || el;
                try {
                  target.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true }));
                  target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                  target.dispatchEvent(new MouseEvent('pointerup', { bubbles: true }));
                  target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                  target.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                  return true;
                } catch (e) {
                  return false;
                }
                """,
                element,
            )
            return bool(clicked)
        return False

    def _element_debug_snapshot(self, element: Any) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if element is None:
            return out
        with suppress(Exception):
            out["tag"] = str(getattr(element, "tag_name", "") or "")
        with suppress(Exception):
            out["role"] = str(element.get_attribute("role") or "")
        with suppress(Exception):
            out["aria_label"] = str(element.get_attribute("aria-label") or "")
        with suppress(Exception):
            out["title"] = str(element.get_attribute("title") or "")
        with suppress(Exception):
            out["data_surface"] = str(element.get_attribute("data-surface") or "")
        with suppress(Exception):
            out["data_testid"] = str(element.get_attribute("data-testid") or "")
        with suppress(Exception):
            out["tabindex"] = str(element.get_attribute("tabindex") or "")
        with suppress(Exception):
            out["aria_disabled"] = str(element.get_attribute("aria-disabled") or "")
        with suppress(Exception):
            out["disabled"] = str(element.get_attribute("disabled") or "")
        with suppress(Exception):
            out["class"] = str(element.get_attribute("class") or "")[:200]
        with suppress(Exception):
            out["text"] = re.sub(r"\s+", " ", str(getattr(element, "text", "") or "")).strip()[:200]
        return out

    def _open_reports_tab(self, sb) -> None:
        if self._safe_click_any_text(sb, "Reports", timeout=2):
            sb.sleep(1)
            return
        if self._safe_click_any_text(sb, "\ubcf4\uace0\uc11c", timeout=2):
            sb.sleep(1)
            return
        self.logger.info("Reports tab click skipped (already focused or selector changed)")

    def _open_exports_tab(self, sb) -> None:
        if self._safe_click_any_text(sb, "Export history", timeout=2):
            sb.sleep(2)
            return
        if self._safe_click_any_text(sb, "Exports", timeout=2):
            sb.sleep(2)
            return
        if self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b8 \ubaa9\ub85d", timeout=2):
            sb.sleep(2)
            return
        raise RuntimeError("Could not open Exports page/tab")

    def _select_report_checkbox(self, sb, report_name: str) -> bool:
        variants = self._report_name_variants(report_name)
        row_predicate = " or ".join(
            f"contains(normalize-space(.), {self._xpath_literal(v)})" for v in variants
        )
        row_xp = f"//*[self::tr or @role='row' or contains(@class,'row')][{row_predicate}]"
        checkbox_xpaths = [
            f"({row_xp}//input[@type='checkbox']/ancestor::label[1])[1]",
            f"({row_xp}//*[@role='checkbox'])[1]",
            f"({row_xp}//input[@type='checkbox'])[1]",
            f"({row_xp}//*[contains(@aria-label,'Select')])[1]",
        ]
        return self._click_first_xpath(sb, checkbox_xpaths, timeout=1.8)

    def _select_reports_checkboxes(self, sb, report_names: List[str]) -> None:
        self._open_reports_tab(sb)
        failed: List[str] = []
        for report_name in report_names:
            if self._select_report_checkbox(sb, report_name):
                self.logger.info("Report checkbox selected: %s", report_name)
            else:
                failed.append(report_name)
        if failed:
            raise RuntimeError(f"Could not select report checkbox for: {failed}")

    def _click_export_from_reports(self, sb) -> None:
        # Prefer top action bar Export button (not "Export history").
        export_btn_xpaths = [
            "(//button[.//span[normalize-space()='Export']])[1]",
            "(//button[normalize-space()='Export'])[1]",
            "(//*[contains(@role,'button')][normalize-space()='Export'])[1]",
        ]
        if self._click_first_xpath(sb, export_btn_xpaths, timeout=2):
            return
        if self._safe_click_any_text(sb, "Export", timeout=2):
            return
        if self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=2):
            return
        raise RuntimeError("Could not click Export action on Reports page")

    def _configure_export_modal(self, sb) -> None:
        self._select_raw_xlsx_export_type(sb)

        # Keep "Include summary row" default state unless explicit config is needed.
        dialog_export_xpaths = [
            "(//*[contains(@role,'dialog')]//button[.//span[normalize-space()='Export']])[1]",
            "(//*[contains(@role,'dialog')]//button[normalize-space()='Export'])[1]",
            "(//*[contains(@role,'dialog')]//*[contains(@role,'button')][normalize-space()='Export'])[1]",
        ]
        if not self._click_first_xpath(sb, dialog_export_xpaths, timeout=2):
            if not (self._safe_click_any_text(sb, "Export", timeout=2) or self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=2)):
                raise RuntimeError("Could not confirm Export in modal")

    def _is_raw_xlsx_selected(self, sb) -> bool:
        modal_el = self._find_export_modal_element(sb)
        with suppress(Exception):
            if modal_el:
                selected = sb.execute_script(
                    """
                    const dlg = arguments[0];
                    const norm = (t) => (t || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    const isVisible = (el) => {
                      if (!el) return false;
                      const style = window.getComputedStyle(el);
                      if (style.display === 'none' || style.visibility === 'hidden') return false;
                      const rect = el.getBoundingClientRect();
                      return rect.width > 0 && rect.height > 0;
                    };
                    const directRadio = dlg.querySelector('input[type="radio"][value="xlsx"]');
                    if (directRadio && directRadio.checked) return true;
                    const directRole = dlg.querySelector('[role="radio"][data-value="xlsx"], [role="radio"][value="xlsx"]');
                    if (directRole && (directRole.getAttribute('aria-checked') || '').toLowerCase() === 'true') return true;
                    const rows = [...dlg.querySelectorAll('label, li, div, [role="radio"]')].filter(isVisible);
                    const isRawXlsx = (t) => (
                      (t.includes('raw data table') || t.includes('원시')) &&
                      t.includes('.xlsx')
                    );
                    for (const row of rows) {
                      const text = norm(row.innerText);
                      if (!isRawXlsx(text)) continue;
                      const inputRadio = row.querySelector?.('input[type="radio"]');
                      if (inputRadio && inputRadio.checked) return true;
                      const radio =
                        (row.getAttribute && row.getAttribute('role') === 'radio' ? row : null) ||
                        row.querySelector?.('[role="radio"]') ||
                        row.closest?.('[role="radio"]');
                      if (radio && (radio.getAttribute('aria-checked') || '').toLowerCase() === 'true') {
                        return true;
                      }
                    }
                    return false;
                    """,
                    modal_el,
                )
                return bool(selected)
        return False

    def _select_raw_xlsx_export_type(self, sb) -> None:
        option_xpaths = [
            "(//*[contains(@role,'dialog')]//input[@type='radio' and @value='xlsx'])[1]",
            "(//input[@type='radio' and @value='xlsx'])[1]",
            "(//*[contains(@role,'dialog')]//input[@type='radio' and @value='xlsx']/ancestor::label[1])[1]",
            "(//input[@type='radio' and @value='xlsx']/ancestor::label[1])[1]",
            # Click radio control nearest to exact label first.
            (
                "(//*[contains(@role,'dialog')]//*[normalize-space()='Raw data table (.xlsx)']"
                "/preceding::*[@role='radio'][1])[1]"
            ),
            "(//*[normalize-space()='Raw data table (.xlsx)']/preceding::*[@role='radio'][1])[1]",
            (
                "(//*[contains(@role,'dialog')]//*[contains(normalize-space(.),'Raw data table')"
                " and contains(normalize-space(.),'.xlsx')]/preceding::*[@role='radio'][1])[1]"
            ),
            "(//*[contains(normalize-space(.),'Raw data table') and contains(normalize-space(.),'.xlsx')]/preceding::*[@role='radio'][1])[1]",
            (
                "(//*[contains(@role,'dialog')]//*[contains(normalize-space(.),'\uc6d0\uc2dc')"
                " and contains(normalize-space(.),'.xlsx')]/preceding::*[@role='radio'][1])[1]"
            ),
            "(//*[contains(normalize-space(.),'\uc6d0\uc2dc') and contains(normalize-space(.),'.xlsx')]/preceding::*[@role='radio'][1])[1]",
            (
                "(//*[contains(@role,'dialog')]//*[normalize-space()='Raw data table (.xlsx)']"
                "/ancestor::*[self::label or self::li or self::div][1])[1]"
            ),
            "(//*[normalize-space()='Raw data table (.xlsx)']/ancestor::*[self::label or self::li or self::div][1])[1]",
        ]

        for _ in range(3):
            self._dismiss_notification_overlay(sb, retries=1)
            clicked = False

            modal_el = self._find_export_modal_element(sb)
            with suppress(Exception):
                if modal_el:
                    clicked = bool(
                        sb.execute_script(
                            """
                            const dlg = arguments[0];
                            const norm = (t) => (t || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                            const isVisible = (el) => {
                              if (!el) return false;
                              const style = window.getComputedStyle(el);
                              if (style.display === 'none' || style.visibility === 'hidden') return false;
                              const rect = el.getBoundingClientRect();
                              return rect.width > 0 && rect.height > 0;
                            };
                            const rows = [...dlg.querySelectorAll('label,li,div,[role="radio"],span')].filter(isVisible);
                            const isRawXlsx = (t) => ((t.includes('raw data table') || t.includes('원시')) && t.includes('.xlsx'));
                            const directRadio = dlg.querySelector('input[type="radio"][value="xlsx"]');
                            if (directRadio) {
                              directRadio.click();
                              directRadio.dispatchEvent(new Event('input', { bubbles: true }));
                              directRadio.dispatchEvent(new Event('change', { bubbles: true }));
                              return true;
                            }
                            for (const row of rows) {
                              const text = norm(row.innerText);
                              if (!isRawXlsx(text)) continue;
                              const radio =
                                (row.getAttribute && row.getAttribute('role') === 'radio' ? row : null) ||
                                row.querySelector?.('[role="radio"]') ||
                                row.closest?.('[role="radio"]') ||
                                row;
                              if (radio && radio.click) {
                                radio.click();
                                return true;
                              }
                            }
                            return false;
                            """,
                            modal_el,
                        )
                    )

            if not clicked:
                clicked = self._click_first_xpath(sb, option_xpaths, timeout=1.8)
            if not clicked:
                clicked = (
                    self._safe_click_any_text(sb, "Raw data table (.xlsx)", timeout=1.5)
                    or self._safe_click_any_text(sb, "Raw data table", timeout=1.5)
                    or self._safe_click_any_text(sb, "\uc6d0\uc2dc \ub370\uc774\ud130 \ud14c\uc774\ube14 (.xlsx)", timeout=1.5)
                )

            sb.sleep(0.3)
            if clicked and self._is_raw_xlsx_selected(sb):
                self.logger.info("Export type selected: Raw data table (.xlsx)")
                return

        raise RuntimeError("Could not enforce export type: Raw data table (.xlsx)")

    def _is_loading_indicator_present(self, sb) -> bool:
        with suppress(Exception):
            found = sb.execute_script(
                """
                const selectors = [
                  '[aria-busy="true"]',
                  '[role="progressbar"]',
                  '.spinner',
                  '.loading',
                  '.skeleton',
                  '[class*="spinner"]',
                  '[class*="loading"]',
                  '[class*="skeleton"]'
                ];
                const isVisible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                for (const sel of selectors) {
                  const els = document.querySelectorAll(sel);
                  for (const el of els) {
                    if (isVisible(el)) return true;
                  }
                }
                const bodyText = (document.body && document.body.innerText ? document.body.innerText : '').toLowerCase();
                const textSignals = ['processing', 'loading', '처리중', '로딩', '생성 중'];
                return textSignals.some((token) => bodyText.includes(token));
                """
            )
            return bool(found)
        return False

    def _download_button_state(self, sb, row: Optional[ExportHistoryRow]) -> Dict[str, bool]:
        state = {"present": False, "visible": False, "enabled": False, "clickable": False}
        if not row:
            return state

        if row.source_mode == "rv" and row.row_top_px is not None:
            dom_cfg = self._export_history_dom_config()
            row_height = float(dom_cfg.get("row_height_px") or 52)
            tolerance = max(2.0, row_height * 0.12)
            with suppress(Exception):
                rv_state = sb.execute_script(
                    """
                    const cfg = arguments[0];
                    const rowTop = Number(arguments[1]);
                    const tolerance = Number(arguments[2]);
                    const parsePx = (style, key) => {
                        if (!style) return null;
                        const m = String(style).match(new RegExp(key + '\\\\s*:\\\\s*(-?\\\\d+(?:\\\\.\\\\d+)?)px', 'i'));
                        return m ? Number(m[1]) : null;
                    };
                    const isVisible = (el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') return false;
                        const rect = el.getBoundingClientRect();
                        return rect.width > 0 && rect.height > 0;
                    };
                    const grid = document.querySelector(cfg.data_grid);
                    if (!grid) return {present: false, visible: false, enabled: false, clickable: false};
                    const container = grid.querySelector(cfg.grid_container) || grid;
                    const cells = Array.from(container.querySelectorAll("div[style*='top:']")).filter((el) => {
                        const top = parsePx(el.getAttribute('style') || '', 'top');
                        return top !== null && Math.abs(top - rowTop) <= tolerance;
                    });
                    if (!cells.length) return {present: false, visible: false, enabled: false, clickable: false};
                    const downloadCell = cells[5] || cells[cells.length - 1];
                    const controls = Array.from(
                        downloadCell.querySelectorAll(cfg.download_button_selector + ", [role='button'], button, a")
                    );
                    if (!controls.length) return {present: false, visible: false, enabled: false, clickable: false};
                    const el = controls[0];
                    const ariaDisabled = String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                    const hasDisabledAttr = el.hasAttribute('disabled');
                    const classDisabled = String(el.getAttribute('class') || '').toLowerCase().includes('disabled');
                    const enabled = !(ariaDisabled || hasDisabledAttr || classDisabled);
                    const visible = isVisible(el);
                    return {
                      present: true,
                      visible,
                      enabled,
                      clickable: !!(visible && enabled),
                    };
                    """,
                    dom_cfg,
                    row.row_top_px,
                    tolerance,
                )
                if isinstance(rv_state, dict):
                    state = {
                        "present": bool(rv_state.get("present")),
                        "visible": bool(rv_state.get("visible")),
                        "enabled": bool(rv_state.get("enabled")),
                        "clickable": bool(rv_state.get("clickable")),
                    }
                    return state

        if row.row_el is None:
            return state

        row_download_xpaths = [
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')]",
            ".//button[contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@data-testid,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
        ]
        for xp in row_download_xpaths:
            with suppress(Exception):
                buttons = row.row_el.find_elements(By.XPATH, xp)
                if not buttons:
                    continue
                btn = buttons[0]
                state["present"] = True
                visible = False
                enabled = False
                with suppress(Exception):
                    visible = bool(btn.is_displayed())
                with suppress(Exception):
                    enabled = bool(btn.is_enabled())
                with suppress(Exception):
                    if str(btn.get_attribute("aria-disabled") or "").strip().lower() == "true":
                        enabled = False
                with suppress(Exception):
                    if str(btn.get_attribute("disabled") or "").strip().lower() in {"", "true", "disabled"}:
                        enabled = False
                with suppress(Exception):
                    if "disabled" in str(btn.get_attribute("class") or "").strip().lower():
                        enabled = False
                state["visible"] = bool(visible)
                state["enabled"] = bool(enabled)
                state["clickable"] = bool(state["present"] and state["visible"] and state["enabled"])
                return state
        return state

    def _extract_current_account_context(self, sb) -> Dict[str, str]:
        current_url = self._safe_current_url(sb)
        parsed = urlparse(current_url)
        query = parse_qs(parsed.query or "")

        def _first(keys: List[str]) -> str:
            for key in keys:
                values = query.get(key) or []
                if values:
                    return str(values[0] or "").strip()
            return ""

        return {
            "url": current_url,
            "account_id": _first(["act", "account_id", "ad_account_id"]),
            "business_id": _first(["business_id"]),
        }

    def _build_account_context_mismatch_payload(
        self,
        *,
        expected_account_id: str,
        current_account_id: str,
        url: str,
        sheet_name: str,
        expected_business_id: str = "",
        current_business_id: str = "",
    ) -> Dict[str, str]:
        return {
            "error": "account_context_mismatch",
            "expected_account_id": str(expected_account_id or ""),
            "current_account_id": str(current_account_id or ""),
            "expected_business_id": str(expected_business_id or ""),
            "current_business_id": str(current_business_id or ""),
            "url": str(url or ""),
            "sheet_name": str(sheet_name or ""),
        }

    def _ensure_account_context_with_ticket(self, sb, ticket: ExportTicket) -> None:
        expected_account_id = str(getattr(ticket, "expected_account_id", "") or "").strip()
        expected_business_id = str(getattr(ticket, "expected_business_id", "") or "").strip()

        if (not expected_account_id or not expected_business_id) and ticket.exports_url:
            parsed_expected = urlparse(str(ticket.exports_url or ""))
            expected_query = parse_qs(parsed_expected.query or "")
            if not expected_account_id:
                expected_account_id = str((expected_query.get("act") or [""])[0] or "").strip()
            if not expected_business_id:
                expected_business_id = str((expected_query.get("business_id") or [""])[0] or "").strip()

        current = self._extract_current_account_context(sb)
        current_account_id = str(current.get("account_id") or "").strip()
        current_business_id = str(current.get("business_id") or "").strip()
        mismatch = False
        if expected_account_id and current_account_id and expected_account_id != current_account_id:
            mismatch = True
        if expected_business_id and current_business_id and expected_business_id != current_business_id:
            mismatch = True

        if mismatch:
            raise AccountContextMismatchError(
                self._build_account_context_mismatch_payload(
                    expected_account_id=expected_account_id,
                    current_account_id=current_account_id,
                    expected_business_id=expected_business_id,
                    current_business_id=current_business_id,
                    url=str(current.get("url") or ""),
                    sheet_name=str(ticket.sheet_name or ticket.report_name or ""),
                )
            )

    def _build_row_poll_snapshot(
        self,
        *,
        sb,
        row: Optional[ExportHistoryRow],
        ticket: Optional[ExportTicket],
    ) -> Dict[str, Any]:
        ready_text_detected = bool(row and row.status_norm == "ready")
        button_state = self._download_button_state(sb=sb, row=row)
        loading_indicator_present = self._is_loading_indicator_present(sb=sb)
        account_context = self._extract_current_account_context(sb)
        expected_account_id = str(getattr(ticket, "expected_account_id", "") or "").strip()
        expected_business_id = str(getattr(ticket, "expected_business_id", "") or "").strip()

        snapshot = {
            "status_norm": str(getattr(row, "status_norm", "") or ""),
            "status_raw": str(getattr(row, "status_raw", "") or ""),
            "ready_text_detected": ready_text_detected,
            "download_button_state": button_state,
            "download_button_clickable": bool(button_state.get("clickable")),
            "loading_indicator_present": loading_indicator_present,
            "expected_account_id": expected_account_id,
            "expected_business_id": expected_business_id,
            "current_account_id": str(account_context.get("account_id") or ""),
            "current_business_id": str(account_context.get("business_id") or ""),
            "url": str(account_context.get("url") or ""),
        }
        if row is not None:
            snapshot["row_name"] = str(row.name_raw or "")
            snapshot["row_source"] = str(row.source_mode or "")
            snapshot["row_top_px"] = row.row_top_px
            snapshot["export_date_raw"] = str(row.export_date_raw or "")
        if ticket is not None:
            snapshot["sheet_name"] = str(ticket.sheet_name or "")
            snapshot["sheet_key"] = str(ticket.sheet_key or "")
        return snapshot

    def _is_ready_snapshot(self, snapshot: Dict[str, Any]) -> tuple[bool, str]:
        ready_text_detected = bool(snapshot.get("ready_text_detected"))
        download_button_clickable = bool(snapshot.get("download_button_clickable"))
        loading_indicator_present = bool(snapshot.get("loading_indicator_present"))
        if ready_text_detected:
            return True, "text"
        if download_button_clickable and not loading_indicator_present:
            return True, "button"
        return False, ""

    def _wait_export_ready(self, sb, report_name: str) -> None:
        timeout = int(self.meta_config.get("export_ready_timeout_sec", 300))
        poll_interval_sec = max(3.0, float(self.meta_config.get("history_poll_initial_sec", 3.0)))
        poll_backoff_factor = float(self.meta_config.get("history_poll_backoff_factor", 1.7))
        poll_max_sec = max(5.0, float(self.meta_config.get("history_poll_max_sec", 20.0)))
        dom_settle_sec = max(0.0, float(self.meta_config.get("history_dom_settle_sec", 2.0)))
        readiness_recheck_sec = float(self.meta_config.get("history_ready_recheck_sec", 0.7))
        stagnation_refresh_sec = max(
            1.0, float(self.meta_config.get("history_status_stagnation_refresh_sec", 30.0))
        )
        stage_tag = "[WAIT:READY]"
        start_ts = time.time()
        attempt = 0
        last_status_norm = ""
        last_status_change_ts = start_ts
        self.logger.info("%s waiting export ready report=%s timeout=%ss", stage_tag, report_name, timeout)

        while time.time() - start_ts <= timeout:
            attempt += 1
            rows = self._collect_export_rows(sb=sb, export_name=report_name)
            latest_row = self._select_latest_export_row(rows=rows, request_ts=0.0)
            status_norm = str(getattr(latest_row, "status_norm", "missing") or "missing")
            now_ts = time.time()
            if status_norm != last_status_norm:
                last_status_norm = status_norm
                last_status_change_ts = now_ts
                poll_interval_sec = max(3.0, float(self.meta_config.get("history_poll_initial_sec", 3.0)))

            snapshot = self._build_row_poll_snapshot(sb=sb, row=latest_row, ticket=None)
            snapshot["attempt"] = attempt
            self.logger.info("%s poll_snapshot attempt=%s snapshot=%s", stage_tag, attempt, snapshot)

            ready, _ = self._is_ready_snapshot(snapshot)
            if ready:
                sb.sleep(readiness_recheck_sec)
                rows_recheck = self._collect_export_rows(sb=sb, export_name=report_name)
                latest_recheck = self._select_latest_export_row(rows=rows_recheck, request_ts=0.0)
                recheck_snapshot = self._build_row_poll_snapshot(
                    sb=sb,
                    row=latest_recheck,
                    ticket=None,
                )
                ready_confirmed, ready_signal = self._is_ready_snapshot(recheck_snapshot)
                if ready_confirmed:
                    recheck_snapshot["ready_signal"] = ready_signal
                    self.logger.info(
                        "%s ready_confirmed attempt=%s ready_signal=%s snapshot=%s",
                        stage_tag,
                        attempt,
                        ready_signal,
                        recheck_snapshot,
                    )
                    return

            should_refresh = (attempt % 3 == 0) or (
                now_ts - last_status_change_ts > stagnation_refresh_sec
            )
            if should_refresh:
                with suppress(Exception):
                    sb.refresh()
                    if dom_settle_sec > 0:
                        sb.sleep(dom_settle_sec)

            sb.sleep(poll_interval_sec)
            poll_interval_sec = min(poll_max_sec, poll_interval_sec * max(1.1, poll_backoff_factor))

        raise TimeoutError(f"Export readiness timeout for report: {report_name}")

    def _download_ready_export(self, sb, report_name: str) -> str:
        pre_ts = time.time()
        variants = self._export_history_name_variants(report_name)
        row_match = " or ".join(
            f"contains(normalize-space(.), {self._xpath_literal(v)})" for v in variants
        )
        row_xp = f"//*[self::tr or @role='row' or contains(@class,'row')][{row_match}]"

        row_download_xpaths = [
            f"({row_xp}//button[contains(normalize-space(.),'Download') or contains(normalize-space(.),'Downl')])[1]",
            f"({row_xp}//button[contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')])[1]",
        ]
        if not self._click_first_xpath(sb, row_download_xpaths, timeout=1.8):
            # fallback: select row then click top Download
            checkbox_xpaths = [
                f"({row_xp}//input[@type='checkbox']/ancestor::label[1])[1]",
                f"({row_xp}//*[@role='checkbox'])[1]",
                f"({row_xp}//input[@type='checkbox'])[1]",
            ]
            if not self._click_first_xpath(sb, checkbox_xpaths, timeout=1.8):
                raise RuntimeError(f"Could not select export row for download: {report_name}")
            if not (self._safe_click_any_text(sb, "Download", timeout=2) or self._safe_click_any_text(sb, "\ub2e4\uc6b4\ub85c\ub4dc", timeout=2)):
                raise RuntimeError(f"Could not click Download for export row: {report_name}")

        timeout = int(self.meta_config.get("download_wait_timeout_sec", 180))
        start = time.time()
        while time.time() - start <= timeout:
            path = newest_xlsx_in_dir(self.download_dir, since_ts=pre_ts)
            if path and not path.lower().endswith(".crdownload"):
                return path
            sb.sleep(2)
        raise TimeoutError(f"META export download timeout for report: {report_name}")

    def _build_report_view_url(self, brand_cfg: Dict, report_id: str) -> str:
        ad_account_id, business_id = self._brand_ids(brand_cfg)
        if not ad_account_id or not business_id:
            raise ValueError(
                "report_id direct view requires act/business id "
                "(meta_act_id or meta_ad_account_id(alias), and meta_business_id)"
            )
        report_id = str(report_id or "").strip()
        if not report_id:
            raise ValueError("report_id is required for direct report view URL")
        global_scope_id = self._resolve_global_scope_id(brand_cfg, business_id)
        event_source = (brand_cfg.get("meta_view_event_source") or "").strip()
        params = [
            f"act={ad_account_id}",
            "ads_manager_write_regions=true",
            f"business_id={business_id}",
            f"global_scope_id={global_scope_id}",
        ]
        if event_source:
            params.append(f"event_source={event_source}")
        params.append(f"selected_report_id={report_id}")
        return "https://adsmanager.facebook.com/adsmanager/reporting/view?" + "&".join(params)

    def _build_exports_url(self, brand_cfg: Dict) -> str:
        ad_account_id, business_id = self._brand_ids(brand_cfg)
        if not ad_account_id or not business_id:
            return self.meta_config["reporting_url"]
        global_scope_id = self._resolve_global_scope_id(brand_cfg, business_id)
        event_source = (brand_cfg.get("meta_export_event_source") or "").strip()
        params = [
            f"act={ad_account_id}",
            "ads_manager_write_regions=true",
            f"business_id={business_id}",
            f"global_scope_id={global_scope_id}",
        ]
        if event_source:
            params.append(f"event_source={event_source}")
        return "https://adsmanager.facebook.com/adsmanager/reporting/export?" + "&".join(params)

    def _find_export_modal_element(self, sb):
        with suppress(Exception):
            dlg = sb.execute_script(
                """
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const isVisible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const notificationTokens = [
                  'notifications',
                  'notification',
                  'notificationspreferences',
                  'notificationsunreadpreferences',
                  'mark all as read',
                  'preferences',
                  '알림',
                  '읽음'
                ];
                const exportTokens = [
                  'export',
                  'raw data table',
                  'export name',
                  'xlsx',
                  '내보내기',
                  '원시'
                ];
                const isNotificationFlyout = (dlg) => {
                  if (!dlg) return false;
                  const id = String(dlg.id || '').toLowerCase();
                  if (id === 'fbnotificationsflyout') return true;
                  const cls = String(dlg.className || '').toLowerCase();
                  return cls.includes('uitoggleflyout');
                };
                const labelledText = (dlg) => {
                  if (!dlg) return '';
                  const labelledBy = String(dlg.getAttribute('aria-labelledby') || '').trim();
                  if (!labelledBy) return '';
                  const titleEl = document.getElementById(labelledBy);
                  return normalize(titleEl ? titleEl.innerText : '');
                };
                const hasExportControls = (dlg) => {
                  if (!dlg) return false;
                  const hasInput = !!dlg.querySelector('input[type="text"], input:not([type])');
                  const hasRadio = !!dlg.querySelector('[role="radio"], input[type="radio"], input[type="radio"][value="xlsx"]');
                  const hasXlsx = !!dlg.querySelector('input[type="radio"][value="xlsx"]');
                  const buttons = [...dlg.querySelectorAll('button, [role="button"]')];
                  const hasExportButton = buttons.some((btn) => {
                    const label = normalize(
                      btn.innerText ||
                      btn.getAttribute('aria-label') ||
                      btn.getAttribute('title') ||
                      ''
                    );
                    const surf = normalize(btn.getAttribute('data-surface') || '');
                    return label === 'export' || label === '내보내기' || surf.includes('export-confirm-button');
                  });
                  return {
                    hasInput,
                    hasRadio,
                    hasXlsx,
                    hasExportButton,
                    any: hasInput || hasRadio || hasExportButton,
                  };
                };
                const dialogs = [
                  ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                ].filter(isVisible);
                for (let i = dialogs.length - 1; i >= 0; i -= 1) {
                  const dlg = dialogs[i];
                  if (isNotificationFlyout(dlg)) continue;
                  const text = normalize(dlg.innerText || '');
                  const label = labelledText(dlg);
                  const controls = hasExportControls(dlg);
                  const isNotification = notificationTokens.some((token) => text.includes(token));
                  const looksByLabel = label === 'export report';
                  const looksByText = exportTokens.some((token) => text.includes(token));
                  const isExportLike = looksByLabel || (looksByText && controls.any) || (controls.hasInput && controls.hasXlsx);
                  if (!isExportLike) continue;
                  if (isNotification && !looksByLabel && !controls.hasXlsx) continue;
                  return dlg;
                }
                return null;
                """
            )
            if dlg:
                return dlg
        return None

    def _notification_overlay_present(self, sb) -> bool:
        with suppress(Exception):
            present = sb.execute_script(
                """
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const isVisible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const notificationTokens = [
                  'notifications',
                  'notification',
                  'notificationspreferences',
                  'notificationsunreadpreferences',
                  'mark all as read',
                  'preferences',
                  '알림',
                  '읽음'
                ];
                const looksExportDialog = (text, dlg) => {
                  if (!dlg) return false;
                  if (
                    text.includes('export') ||
                    text.includes('raw data table') ||
                    text.includes('export name') ||
                    text.includes('xlsx') ||
                    text.includes('내보내기') ||
                    text.includes('원시')
                  ) {
                    return true;
                  }
                  return !!dlg.querySelector('input[type="text"], [role="radio"], input[type="radio"]');
                };

                const dialogs = [
                  ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                ].filter(isVisible);
                for (const dlg of dialogs) {
                  const text = normalize(dlg.innerText || '');
                  if (!text) continue;
                  const isNotification = notificationTokens.some((token) => text.includes(token));
                  if (!isNotification) continue;
                  if (looksExportDialog(text, dlg)) continue;
                  return true;
                }
                return false;
                """
            )
            return bool(present)
        return False

    def _open_export_modal_from_view(self, sb) -> None:
        if self._find_export_modal_element(sb):
            return
        timeout_sec = max(2.0, float(self.meta_config.get("view_export_trigger_timeout_sec", 8.0)))
        deadline = time.time() + timeout_sec
        css_selectors = [
            "[data-surface='/am/lib:export_button']",
            "[role='button'][data-surface*='export_button']",
            "div[role='button'][data-surface*='export_button']",
        ]
        xpaths = [
            "(//div[@data-surface='/am/lib:export_button'])[1]",
            "(//*[@role='button' and contains(@data-surface,'export_button')])[1]",
            "(//button[.//span[normalize-space()='Export']])[1]",
            "(//button[normalize-space()='Export'])[1]",
            "(//*[contains(@role,'button')][normalize-space()='Export'])[1]",
        ]
        while time.time() <= deadline:
            self._dismiss_notification_overlay(sb, retries=2)
            if self._find_export_modal_element(sb):
                return
            clicked = False
            if self._click_first_css(sb, css_selectors, timeout=1.2):
                clicked = True
                self.logger.info("view_export_trigger_clicked strategy=data_surface")
            elif self._click_first_xpath(sb, xpaths, timeout=1.2):
                clicked = True
                self.logger.info("view_export_trigger_clicked strategy=xpath")
            elif self._safe_click_any_text(sb, "Export", timeout=1.0):
                clicked = True
                self.logger.info("view_export_trigger_clicked strategy=text_en")
            elif self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=1.0):
                clicked = True
                self.logger.info("view_export_trigger_clicked strategy=text_ko")
            else:
                with suppress(Exception):
                    clicked = bool(
                        sb.execute_script(
                            """
                            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                            const isVisible = (el) => {
                              if (!el) return false;
                              const style = window.getComputedStyle(el);
                              if (style.display === 'none' || style.visibility === 'hidden' || style.pointerEvents === 'none') return false;
                              const rect = el.getBoundingClientRect();
                              return rect.width > 0 && rect.height > 0;
                            };
                            const isEnabled = (el) => {
                              if (!el) return false;
                              const ariaDisabled = normalize(el.getAttribute('aria-disabled') || '');
                              const ariaBusy = normalize(el.getAttribute('aria-busy') || '');
                              const hasDisabled = el.hasAttribute && el.hasAttribute('disabled');
                              const cls = normalize(el.getAttribute('class') || '');
                              return ariaDisabled !== 'true' && ariaBusy !== 'true' && !hasDisabled && !cls.includes('disabled');
                            };
                            const shouldSkip = (el) => {
                              if (!el) return true;
                              if (el.closest('#fbNotificationsFlyout')) return true;
                              if (el.closest('[role="dialog"]')) return true;
                              if (el.closest('[role="menuitem"]')) return true;
                              const surface = normalize(el.getAttribute('data-surface') || '');
                              if (surface.includes('export-confirm-button')) return true;
                              if (normalize(el.getAttribute('aria-haspopup') || '') === 'menu') return true;
                              return false;
                            };
                            const selectors = [
                              "[data-surface='/am/lib:export_button']",
                              "[role='button'][data-surface*='export_button']",
                              "[data-surface*='export_button']"
                            ];
                            const candidates = [];
                            const seen = new Set();
                            for (const sel of selectors) {
                              for (const node of document.querySelectorAll(sel)) {
                                const key = normalize(node.getAttribute('data-surface') || '') + '|' + normalize(node.innerText || '');
                                if (seen.has(key)) continue;
                                seen.add(key);
                                if (shouldSkip(node)) continue;
                                if (!isVisible(node) || !isEnabled(node)) continue;
                                candidates.push(node);
                              }
                            }
                            if (!candidates.length) return false;
                            candidates.sort((a, b) => {
                              const aSurf = normalize(a.getAttribute('data-surface') || '');
                              const bSurf = normalize(b.getAttribute('data-surface') || '');
                              const aScore = aSurf === '/am/lib:export_button' ? 0 : 1;
                              const bScore = bSurf === '/am/lib:export_button' ? 0 : 1;
                              if (aScore !== bScore) return aScore - bScore;
                              const ar = a.getBoundingClientRect();
                              const br = b.getBoundingClientRect();
                              if (Math.abs(ar.top - br.top) > 2) return ar.top - br.top;
                              return ar.left - br.left;
                            });
                            const target = candidates[0];
                            try { target.click(); return true; } catch (_err) {}
                            const rect = target.getBoundingClientRect();
                            const x = Math.floor(rect.left + Math.max(4, rect.width / 2));
                            const y = Math.floor(rect.top + Math.max(4, rect.height / 2));
                            ['mousedown', 'mouseup', 'click'].forEach((evtName) => {
                              target.dispatchEvent(new MouseEvent(evtName, { bubbles: true, clientX: x, clientY: y }));
                            });
                            return true;
                            """
                        )
                    )
                    if clicked:
                        self.logger.info("view_export_trigger_clicked strategy=js_data_surface")
            if clicked:
                sb.sleep(0.35)
                if self._find_export_modal_element(sb):
                    return
            sb.sleep(0.2)

        with suppress(Exception):
            probe = self._probe_view_export_trigger_candidates(sb=sb)
            if probe:
                self.logger.warning("view_export_trigger_probe=%s", probe)
        raise RuntimeError("Could not click Export button on report view page")

    def _probe_view_export_trigger_candidates(self, sb) -> str:
        with suppress(Exception):
            payload = sb.execute_script(
                """
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const lower = (value) => normalize(value).toLowerCase();
                const isVisible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const selectors = [
                  "[data-surface='/am/lib:export_button']",
                  "[role='button'][data-surface*='export_button']",
                  "[data-surface*='export']"
                ];
                const out = [];
                const seen = new Set();
                for (const sel of selectors) {
                  for (const el of document.querySelectorAll(sel)) {
                    const key = lower(el.getAttribute('data-surface') || '') + '|' + lower(el.innerText || '');
                    if (seen.has(key)) continue;
                    seen.add(key);
                    const rect = el.getBoundingClientRect();
                    out.push({
                      selector: sel,
                      role: String(el.getAttribute('role') || ''),
                      text: normalize(el.innerText || '').slice(0, 80),
                      aria_label: String(el.getAttribute('aria-label') || ''),
                      title: String(el.getAttribute('title') || ''),
                      data_surface: String(el.getAttribute('data-surface') || ''),
                      in_dialog: !!el.closest('[role="dialog"]'),
                      in_notifications: !!el.closest('#fbNotificationsFlyout'),
                      visible: isVisible(el),
                      x: Math.round(rect.x),
                      y: Math.round(rect.y),
                      w: Math.round(rect.width),
                      h: Math.round(rect.height),
                    });
                  }
                }
                return JSON.stringify(out.slice(0, 10));
                """
            )
            if payload:
                return str(payload)
        return ""

    def _wait_view_export_entry_ready(self, sb, *, report_id: str = "", stage_tag: str = "[EXPORT:URL]") -> None:
        timeout_sec = max(1.0, float(self.meta_config.get("view_report_ready_timeout_sec", 12.0)))
        deadline = time.time() + timeout_sec
        expected_report_id = str(report_id or "").strip()
        while time.time() <= deadline:
            current_url = self._safe_current_url(sb)
            has_report_context = "/adsmanager/reporting/view" in str(current_url or "")
            if has_report_context and expected_report_id:
                has_report_context = f"selected_report_id={expected_report_id}" in current_url
            if has_report_context:
                with suppress(Exception):
                    ready = bool(
                        sb.execute_script(
                            """
                            const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                            const isVisible = (el) => {
                              if (!el) return false;
                              const style = window.getComputedStyle(el);
                              if (style.display === 'none' || style.visibility === 'hidden' || style.pointerEvents === 'none') {
                                return false;
                              }
                              const rect = el.getBoundingClientRect();
                              return rect.width > 0 && rect.height > 0;
                            };
                            const selectors = [
                              "[data-surface='/am/lib:export_button']",
                              "[role='button'][data-surface*='export_button']"
                            ];
                            for (const sel of selectors) {
                              for (const node of document.querySelectorAll(sel)) {
                                if (!isVisible(node)) continue;
                                if (node.closest('#fbNotificationsFlyout')) continue;
                                if (node.closest('[role="dialog"]')) continue;
                                if (normalize(node.getAttribute('aria-disabled') || '') === 'true') continue;
                                return true;
                              }
                            }
                            return false;
                            """
                        )
                    )
                    if ready:
                        return
            sb.sleep(0.25)
        self.logger.warning("%s report_view_ready_wait_timeout report_id=%s", stage_tag, expected_report_id)

    def _dismiss_notification_overlay(self, sb, retries: int = 2) -> bool:
        dismissed = False
        max_retries = max(1, int(retries))
        for _ in range(max_retries):
            if not self._notification_overlay_present(sb):
                break

            closed_by_button = False
            with suppress(Exception):
                closed_by_button = bool(
                    sb.execute_script(
                        """
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        const isVisible = (el) => {
                          if (!el) return false;
                          const style = window.getComputedStyle(el);
                          if (style.display === 'none' || style.visibility === 'hidden') return false;
                          const rect = el.getBoundingClientRect();
                          return rect.width > 0 && rect.height > 0;
                        };
                        const notificationTokens = [
                          'notifications',
                          'notification',
                          'notificationspreferences',
                          'notificationsunreadpreferences',
                          'mark all as read',
                          'preferences',
                          '알림',
                          '읽음'
                        ];
                        const dialogs = [
                          ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                        ].filter(isVisible);
                        for (let i = dialogs.length - 1; i >= 0; i -= 1) {
                          const dlg = dialogs[i];
                          const text = normalize(dlg.innerText || '');
                          const isNotification = notificationTokens.some((token) => text.includes(token));
                          if (!isNotification) continue;
                          const controls = [
                            ...dlg.querySelectorAll('button, [role="button"], [aria-label], [data-testid*="close"]')
                          ];
                          for (const control of controls) {
                            if (!isVisible(control)) continue;
                            const label = normalize(
                              control.innerText ||
                              control.value ||
                              control.getAttribute('aria-label') ||
                              control.getAttribute('title') ||
                              ''
                            );
                            const closable = (
                              label.includes('close') ||
                              label.includes('dismiss') ||
                              label.includes('cancel') ||
                              label.includes('닫기') ||
                              label === 'x'
                            );
                            if (!closable) continue;
                            control.click();
                            return true;
                          }
                        }
                        return false;
                        """
                    )
                )
            if closed_by_button:
                dismissed = True
                sb.sleep(0.25)
            if not self._notification_overlay_present(sb):
                break

            with suppress(Exception):
                sb.execute_script(
                    """
                    const targets = [document.activeElement, document.body, document.documentElement, window].filter(Boolean);
                    for (const t of targets) {
                      const event = new KeyboardEvent('keydown', {
                        key: 'Escape',
                        code: 'Escape',
                        keyCode: 27,
                        which: 27,
                        bubbles: true,
                      });
                      t.dispatchEvent(event);
                    }
                    """
                )
            sb.sleep(0.2)
            if not self._notification_overlay_present(sb):
                dismissed = True
                break

            toggled_bell = False
            with suppress(Exception):
                toggled_bell = bool(
                    sb.execute_script(
                        """
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        const isVisible = (el) => {
                          if (!el) return false;
                          const style = window.getComputedStyle(el);
                          if (style.display === 'none' || style.visibility === 'hidden') return false;
                          const rect = el.getBoundingClientRect();
                          return rect.width > 0 && rect.height > 0;
                        };
                        const nodes = [
                          ...document.querySelectorAll('button, [role="button"], [aria-label], [title], [data-testid]')
                        ];
                        for (const node of nodes) {
                          if (!isVisible(node)) continue;
                          const label = normalize(
                            node.innerText ||
                            node.getAttribute('aria-label') ||
                            node.getAttribute('title') ||
                            node.getAttribute('data-tooltip-content') ||
                            node.getAttribute('data-testid') ||
                            ''
                          );
                          if (!label) continue;
                          const isBellLike = (
                            label.includes('notification') ||
                            label.includes('notifications') ||
                            label.includes('unread') ||
                            label.includes('inbox') ||
                            label.includes('알림')
                          );
                          if (!isBellLike || label.includes('mark all as read')) continue;
                          node.click();
                          return true;
                        }
                        return false;
                        """
                    )
                )
            if toggled_bell:
                sb.sleep(0.25)
            if not self._notification_overlay_present(sb):
                dismissed = True
                break

            clicked_backdrop = False
            with suppress(Exception):
                clicked_backdrop = bool(
                    sb.execute_script(
                        """
                        const isVisible = (el) => {
                          if (!el) return false;
                          const style = window.getComputedStyle(el);
                          if (style.display === 'none' || style.visibility === 'hidden') return false;
                          const rect = el.getBoundingClientRect();
                          return rect.width > 0 && rect.height > 0;
                        };
                        const dialogs = [
                          ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                        ].filter(isVisible);
                        const targetDialog = dialogs[dialogs.length - 1];
                        let clickX = 8;
                        let clickY = 8;
                        if (targetDialog) {
                          const rect = targetDialog.getBoundingClientRect();
                          clickX = Math.max(2, Math.min(window.innerWidth - 2, rect.left - 12));
                          clickY = Math.max(2, Math.min(window.innerHeight - 2, rect.top + 12));
                        }
                        const target = document.elementFromPoint(clickX, clickY) || document.body;
                        if (!target) return false;
                        const down = new MouseEvent('mousedown', { bubbles: true, clientX: clickX, clientY: clickY });
                        const up = new MouseEvent('mouseup', { bubbles: true, clientX: clickX, clientY: clickY });
                        const click = new MouseEvent('click', { bubbles: true, clientX: clickX, clientY: clickY });
                        target.dispatchEvent(down);
                        target.dispatchEvent(up);
                        target.dispatchEvent(click);
                        return true;
                        """
                    )
                )
            if clicked_backdrop:
                sb.sleep(0.25)
            if not self._notification_overlay_present(sb):
                dismissed = True
                break

        if dismissed:
            self.logger.info("Dismissed notification overlay before export modal interaction.")
        elif self._notification_overlay_present(sb):
            self.logger.warning("Notification overlay still visible after dismissal attempts.")
        return dismissed

    def _set_export_name_in_modal(self, sb, export_name: str) -> None:
        target = self._normalize_export_name_value(export_name)
        if not target:
            return
        settle_sec = max(0.2, float(self.meta_config.get("export_name_settle_sec", 2.0)))

        modal_el = self._find_export_modal_element(sb)
        with suppress(Exception):
            if modal_el:
                ok = sb.execute_script(
                    """
                    const dlg = arguments[0];
                    const target = (arguments[1] || '').trim();
                    if (!dlg || !target) return false;
                    const isVisible = (el) => {
                      if (!el) return false;
                      const style = window.getComputedStyle(el);
                      if (style.display === 'none' || style.visibility === 'hidden') return false;
                      const rect = el.getBoundingClientRect();
                      return rect.width > 0 && rect.height > 0;
                    };
                    const inputs = [...dlg.querySelectorAll('input[type="text"], input:not([type])')].filter(isVisible);
                    if (!inputs.length) return false;
                    const preferred = inputs.find((input) => {
                      const aria = (input.getAttribute('aria-label') || '').toLowerCase();
                      const name = (input.getAttribute('name') || '').toLowerCase();
                      const placeholder = (input.getAttribute('placeholder') || '').toLowerCase();
                      return aria.includes('export') || name.includes('export') || placeholder.includes('export');
                    }) || inputs[0];
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
                    preferred.focus();
                    preferred.select?.();
                    if (setter) setter.call(preferred, '');
                    else preferred.value = '';
                    preferred.dispatchEvent(new Event('input', { bubbles: true }));
                    if (setter) setter.call(preferred, target);
                    else preferred.value = target;
                    preferred.dispatchEvent(new Event('input', { bubbles: true }));
                    preferred.dispatchEvent(new Event('change', { bubbles: true }));
                    preferred.dispatchEvent(new Event('blur', { bubbles: true }));
                    preferred.blur?.();
                    return String(preferred.value || '').replace(/\\u200b/g, ' ').replace(/\\s+/g, ' ').trim() === target;
                    """,
                    modal_el,
                    target,
                )
                if bool(ok):
                    sb.sleep(settle_sec)
                    return

        input_xpaths = [
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export') or contains(normalize-space(.),'\ub0b4\ubcf4\ub0b4\uae30')]//input[@type='text'])[1]",
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export name') or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'raw data table')]//input[@type='text'])[1]",
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'xlsx')]//input[@type='text'])[1]",
        ]
        for xp in input_xpaths:
            try:
                sb.clear(xp, timeout=1.5)
                sb.type(xp, target, timeout=1.5)
                with suppress(Exception):
                    sb.execute_script(
                        """
                        const el = arguments[0];
                        if (!el) return;
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        el.dispatchEvent(new Event('blur', { bubbles: true }));
                        if (typeof el.blur === 'function') el.blur();
                        """,
                        sb.driver.find_element(By.XPATH, xp),
                    )
                sb.sleep(settle_sec)
                return
            except Exception:
                continue
        self.logger.warning("Could not set export name in modal. Continue with existing name.")

    def _wait_xlsx_download(self, sb, pre_ts: float, timeout: Optional[int] = None) -> str:
        wait_timeout = int(timeout or self.meta_config.get("download_wait_timeout_sec", 180))
        start = time.time()
        while time.time() - start <= wait_timeout:
            path = newest_xlsx_in_dir(self.download_dir, since_ts=pre_ts)
            if path and path.lower().endswith(".xlsx") and not path.lower().endswith(".crdownload"):
                return path
            sb.sleep(2)
        raise TimeoutError("Timed out waiting for .xlsx download")

    def _wait_view_report_download_or_gate_fallback(
        self,
        sb,
        *,
        since_ts: float,
        report_name: str,
    ) -> ViewDownloadWaitResult:
        stage_tag = "[DOWNLOAD:VIEW]"
        poll_sec = max(0.2, float(self.meta_config.get("view_toast_poll_sec", 1.0)))
        creating_soft_limit_sec = max(
            1.0, float(self.meta_config.get("view_toast_creating_soft_limit_sec", 45.0))
        )
        creating_hard_limit_sec = max(
            creating_soft_limit_sec, float(self.meta_config.get("view_toast_creating_hard_limit_sec", 120.0))
        )
        ready_probe_sec = max(1.0, float(self.meta_config.get("view_toast_ready_probe_sec", 20.0)))
        ready_probe_max_sec = max(
            ready_probe_sec,
            float(self.meta_config.get("view_toast_ready_probe_max_sec", 40.0)),
        )
        download_cooldown_sec = max(1.0, float(self.meta_config.get("download_cooldown_sec", 45.0)))
        global_timeout_sec = max(1.0, float(self.meta_config.get("global_timeout_sec", 240.0)))

        start_ts = time.time()
        state = "WAIT_FOR_SIGNALS"
        creating_started_ts: Optional[float] = None
        ready_seen = False
        ready_seen_ts: Optional[float] = None
        ready_probe_deadline_ts: Optional[float] = None
        cooldown_until_ts = 0.0
        previous_download_started = False
        toast_transition_seen = False
        previous_toast_state = "none"
        file_signal_detected_at = ""
        fallback_trigger_reason = ""
        last_toast_state = "none"
        last_toast_text = ""
        last_toast_progress_pct: Optional[int] = None

        while True:
            now_ts = time.time()
            elapsed_sec = now_ts - start_ts
            elapsed_ms = int(max(0.0, elapsed_sec) * 1000)
            if elapsed_sec >= global_timeout_sec:
                fallback_trigger_reason = "global_timeout"
                decision = "ALLOW_FALLBACK"
                self.logger.info(
                    "%s state=%s toast_state=%s toast_text=%s toast_progress_pct=%s elapsed_ms=%s "
                    "download_started=%s download_completed=%s cooldown_active=%s decision=%s",
                    stage_tag,
                    state,
                    last_toast_state,
                    last_toast_text,
                    last_toast_progress_pct,
                    elapsed_ms,
                    False,
                    False,
                    False,
                    decision,
                )
                break

            signal_probe = self._probe_download_start_signals(
                sb=sb,
                since_ts=since_ts,
                timeout_sec=0.2,
                poll_interval_sec=0.2,
                break_on_started=False,
            )
            download_started = bool(signal_probe.get("download_started"))
            download_completed = bool(signal_probe.get("download_completed"))
            detected_file_path = str(signal_probe.get("completed_file_path") or signal_probe.get("file_path") or "").strip()
            if not file_signal_detected_at:
                file_signal_detected_at = str(signal_probe.get("file_signal_detected_at") or "").strip()

            if download_completed and detected_file_path:
                decision = "SUCCESS"
                self.logger.info(
                    "%s state=%s toast_state=%s toast_text=%s toast_progress_pct=%s elapsed_ms=%s "
                    "download_started=%s download_completed=%s cooldown_active=%s decision=%s",
                    stage_tag,
                    state,
                    last_toast_state,
                    last_toast_text,
                    last_toast_progress_pct,
                    elapsed_ms,
                    download_started,
                    True,
                    False,
                    decision,
                )
                return ViewDownloadWaitResult(
                    success=True,
                    file_path=detected_file_path,
                    fallback_trigger_reason="",
                    view_toast_state=last_toast_state,
                    view_toast_progress_pct=last_toast_progress_pct,
                    view_toast_text=last_toast_text,
                    toast_transition_seen=toast_transition_seen,
                    toast_wait_elapsed_ms=elapsed_ms,
                    file_signal_detected_at=file_signal_detected_at,
                )

            if download_started and not previous_download_started:
                cooldown_until_ts = max(cooldown_until_ts, now_ts + download_cooldown_sec)
                if state != "FALLBACK_COOLDOWN":
                    state = "FALLBACK_COOLDOWN"

            toast_probe = self._probe_view_export_toast(sb)
            toast_state = str(toast_probe.get("view_toast_state") or "none").strip().lower()
            if toast_state not in {"none", "creating", "ready"}:
                toast_state = "none"
            toast_text = str(toast_probe.get("view_toast_text") or "").strip()
            toast_progress_pct = toast_probe.get("view_toast_progress_pct")
            if not isinstance(toast_progress_pct, int):
                toast_progress_pct = None

            if previous_toast_state == "creating" and toast_state == "ready":
                toast_transition_seen = True
            previous_toast_state = toast_state
            last_toast_state = toast_state
            last_toast_text = toast_text
            last_toast_progress_pct = toast_progress_pct

            if toast_state == "ready":
                ready_seen = True
                if ready_seen_ts is None:
                    ready_seen_ts = now_ts
                if state != "POST_READY_PROBE":
                    state = "POST_READY_PROBE"
                    adaptive_probe_sec = ready_probe_sec
                    if creating_started_ts is not None and (now_ts - creating_started_ts) >= creating_soft_limit_sec:
                        adaptive_probe_sec = ready_probe_max_sec
                    ready_probe_deadline_ts = now_ts + adaptive_probe_sec
            elif toast_state == "creating":
                if creating_started_ts is None:
                    creating_started_ts = now_ts
                if state not in {"POST_READY_PROBE", "FALLBACK_COOLDOWN"}:
                    state = "WAIT_CREATING"
            elif state not in {"POST_READY_PROBE", "FALLBACK_COOLDOWN"}:
                state = "WAIT_FOR_SIGNALS"

            cooldown_active = now_ts < cooldown_until_ts
            decision = "WAIT"
            if cooldown_active:
                state = "FALLBACK_COOLDOWN"
            else:
                if state == "POST_READY_PROBE":
                    if ready_probe_deadline_ts is not None and now_ts >= ready_probe_deadline_ts:
                        fallback_trigger_reason = "ready_no_download"
                        decision = "ALLOW_FALLBACK"
                elif state == "WAIT_CREATING" and creating_started_ts is not None:
                    creating_elapsed = now_ts - creating_started_ts
                    if creating_elapsed >= creating_hard_limit_sec:
                        fallback_trigger_reason = "creating_timeout"
                        decision = "ALLOW_FALLBACK"
                    elif creating_elapsed >= creating_soft_limit_sec:
                        fallback_trigger_reason = "creating_timeout"
                        decision = "ALLOW_FALLBACK"
                elif ready_seen and ready_seen_ts is not None:
                    fallback_trigger_reason = "ready_no_download"
                    decision = "ALLOW_FALLBACK"

            self.logger.info(
                "%s state=%s toast_state=%s toast_text=%s toast_progress_pct=%s elapsed_ms=%s "
                "download_started=%s download_completed=%s cooldown_active=%s decision=%s",
                stage_tag,
                state,
                toast_state,
                toast_text,
                toast_progress_pct,
                elapsed_ms,
                download_started,
                download_completed,
                cooldown_active,
                decision,
            )

            if decision == "ALLOW_FALLBACK":
                break

            previous_download_started = bool(download_started)
            sb.sleep(poll_sec)

        if not fallback_trigger_reason:
            fallback_trigger_reason = "no_file_signal_timeout"

        return ViewDownloadWaitResult(
            success=False,
            file_path="",
            fallback_trigger_reason=fallback_trigger_reason,
            view_toast_state=last_toast_state,
            view_toast_progress_pct=last_toast_progress_pct,
            view_toast_text=last_toast_text,
            toast_transition_seen=toast_transition_seen,
            toast_wait_elapsed_ms=int(max(0.0, time.time() - start_ts) * 1000),
            file_signal_detected_at=file_signal_detected_at,
        )

    def _confirm_export_in_modal(self, sb) -> None:
        css_confirm_selectors = [
            "div[role='dialog']:not(#fbNotificationsFlyout) [data-surface*='export-confirm-button']",
            "[data-surface*='ads_report_builder_export_dialog_modal'][data-surface*='export-confirm-button']",
            "[data-surface*='export-confirm-button']",
        ]
        if self._click_first_css(sb, css_confirm_selectors, timeout=1.4):
            return

        modal_el = self._find_export_modal_element(sb)
        with suppress(Exception):
            if modal_el:
                clicked = sb.execute_script(
                    """
                    const dlg = arguments[0];
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    const isVisible = (el) => {
                      if (!el) return false;
                      const style = window.getComputedStyle(el);
                      if (style.display === 'none' || style.visibility === 'hidden') return false;
                      const rect = el.getBoundingClientRect();
                      return rect.width > 0 && rect.height > 0;
                    };
                    const controls = [...dlg.querySelectorAll('button, [role="button"]')].filter(isVisible);
                    for (const control of controls) {
                      const label = normalize(
                        control.innerText ||
                        control.getAttribute('aria-label') ||
                        control.getAttribute('title') ||
                        ''
                      );
                      const disabled = (
                        control.hasAttribute('disabled') ||
                        String(control.getAttribute('aria-disabled') || '').toLowerCase() === 'true' ||
                        String(control.getAttribute('aria-busy') || '').toLowerCase() === 'true'
                      );
                      if (disabled) continue;
                      const surface = normalize(control.getAttribute('data-surface') || '');
                      if (surface.includes('export-confirm-button') || label === 'export' || label === '내보내기') {
                        control.click();
                        return true;
                      }
                    }
                    return false;
                    """,
                    modal_el,
                )
                if bool(clicked):
                    return

        dialog_export_xpaths = [
            "(//*[contains(@role,'dialog') and not(@id='fbNotificationsFlyout')]//*[@data-surface and contains(@data-surface,'export-confirm-button')])[1]",
            "(//*[contains(@role,'dialog')]//button[.//span[normalize-space()='Export']])[1]",
            "(//*[contains(@role,'dialog')]//button[normalize-space()='Export'])[1]",
            "(//*[contains(@role,'dialog')]//*[contains(@role,'button')][normalize-space()='Export'])[1]",
        ]
        if self._click_first_xpath(sb, dialog_export_xpaths, timeout=2):
            return
        if self._safe_click_any_text(sb, "Export", timeout=2) or self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=2):
            return
        raise RuntimeError("Could not confirm Export in modal")

    def _safe_current_url(self, sb) -> str:
        with suppress(Exception):
            return sb.get_current_url() or ""
        return ""

    def _is_auth_gate_url(self, url: str) -> bool:
        normalized = str(url or "").strip().lower()
        if not normalized:
            return False
        tokens = (
            "/login",
            "/loginpage",
            "checkpoint",
            "recover",
            "two_factor",
            "two-factor",
            "device-based",
            "save-device",
            "save_browser",
            "remember",
        )
        return any(token in normalized for token in tokens)

    def _is_trust_prompt_visible(self, sb) -> bool:
        try:
            text = sb.execute_script(
                "return (document && document.body && document.body.innerText) ? document.body.innerText : '';"
            )
        except Exception:
            return False
        body = str(text or "").lower()
        if not body:
            return False
        trust_markers = (
            "trust this device",
            "save browser",
            "remember browser",
            "remember this browser",
            "trust device",
            "trusted device",
        )
        return any(marker in body for marker in trust_markers)

    def _probe_view_export_toast(self, sb) -> Dict[str, Any]:
        default = {
            "view_toast_state": "none",
            "view_toast_progress_pct": None,
            "view_toast_text": "",
        }
        with suppress(Exception):
            payload = sb.execute_script(
                """
                const normalize = (text) =>
                  (text || '').replace(/\\s+/g, ' ').trim();
                const toLower = (text) => normalize(text).toLowerCase();
                const visible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') === 0) {
                    return false;
                  }
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const selectors = [
                  '[role="alert"]',
                  '[aria-live="assertive"]',
                  '[aria-live="polite"]',
                  '[data-testid*="toast"]',
                  '[class*="toast"]',
                  '[class*="snackbar"]',
                  '[class*="Toaster"]',
                ];
                const candidates = [];
                for (const sel of selectors) {
                  document.querySelectorAll(sel).forEach((el) => {
                    if (visible(el)) candidates.push(el);
                  });
                }
                if (!candidates.length) {
                  return { view_toast_state: 'none', view_toast_progress_pct: null, view_toast_text: '' };
                }
                const creatingTokens = [
                  'creating export',
                  'creating',
                  'exporting',
                  '내보내기 생성',
                  '내보내기 중',
                  '생성 중',
                ];
                const readyTokens = [
                  'your export is ready',
                  'export is ready',
                  'export ready',
                  '내보내기 준비',
                  '내보내기 완료',
                  '다운로드 준비',
                ];
                let best = { score: -1, text: '' };
                for (const el of candidates) {
                  const text = normalize(el.innerText || el.textContent || '');
                  if (!text) continue;
                  const lower = text.toLowerCase();
                  let score = 0;
                  if (lower.includes('export') || text.includes('내보내기')) score += 2;
                  if (creatingTokens.some((t) => lower.includes(t.toLowerCase()))) score += 2;
                  if (readyTokens.some((t) => lower.includes(t.toLowerCase()))) score += 3;
                  if (score > best.score) {
                    best = { score, text };
                  }
                }
                if (best.score < 0 || !best.text) {
                  return { view_toast_state: 'none', view_toast_progress_pct: null, view_toast_text: '' };
                }
                const text = best.text;
                const lower = toLower(text);
                const pctMatch = text.match(/(\\d{1,3})\\s*%/);
                const pct = pctMatch ? Number(pctMatch[1]) : null;
                const ready = readyTokens.some((t) => lower.includes(t.toLowerCase()));
                const creating = creatingTokens.some((t) => lower.includes(t.toLowerCase()));
                let state = 'none';
                if (ready) state = 'ready';
                else if (creating || (pct !== null && pct < 100)) state = 'creating';
                return {
                  view_toast_state: state,
                  view_toast_progress_pct: Number.isFinite(pct) ? Math.max(0, Math.min(100, pct)) : null,
                  view_toast_text: text.slice(0, 500),
                };
                """
            )
            if isinstance(payload, dict):
                state = str(payload.get("view_toast_state") or "none").strip().lower()
                if state not in {"none", "creating", "ready"}:
                    state = "none"
                pct_raw = payload.get("view_toast_progress_pct")
                pct = None
                if isinstance(pct_raw, (int, float)):
                    pct = max(0, min(100, int(pct_raw)))
                return {
                    "view_toast_state": state,
                    "view_toast_progress_pct": pct,
                    "view_toast_text": str(payload.get("view_toast_text") or "").strip(),
                }
        return default

    def _sanitize_token(self, value: str) -> str:
        out: List[str] = []
        for ch in str(value or ""):
            low = ch.lower()
            if ("a" <= low <= "z") or ch.isdigit() or ch in {"-", "_"}:
                out.append(ch)
            else:
                out.append("_")
        token = "".join(out).strip("_")
        return token[:80] or "na"

    def _extract_modal_text(self, sb, max_chars: int = 1000) -> str:
        with suppress(Exception):
            payload = sb.execute_script(
                """
                const maxChars = Number(arguments[0] || 1000);
                const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const lower = (value) => normalize(value).toLowerCase();
                const isVisible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const notificationTokens = [
                  'notifications',
                  'notification',
                  'notificationspreferences',
                  'notificationsunreadpreferences',
                  'mark all as read',
                  'preferences',
                  '알림',
                  '읽음'
                ];
                const classify = (dlg) => {
                  const text = lower(dlg.innerText || '');
                  const isNotification = notificationTokens.some((token) => text.includes(token));
                  const isExport = (
                    text.includes('export') ||
                    text.includes('raw data table') ||
                    text.includes('export name') ||
                    text.includes('xlsx') ||
                    text.includes('내보내기') ||
                    text.includes('원시') ||
                    !!dlg.querySelector('input[type="text"], [role="radio"], input[type="radio"]')
                  );
                  if (isExport && !isNotification) return 'export';
                  if (isExport && isNotification) return 'mixed';
                  if (isNotification) return 'notification';
                  return 'unknown';
                };

                const dialogs = [
                  ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                ].filter(isVisible);
                if (!dialogs.length) return { kind: 'none', text: '' };

                let exportDialog = null;
                for (let i = dialogs.length - 1; i >= 0; i -= 1) {
                  const dlg = dialogs[i];
                  const kind = classify(dlg);
                  if (kind === 'export' || kind === 'mixed') {
                    exportDialog = dlg;
                    break;
                  }
                }
                if (exportDialog) {
                  return {
                    kind: classify(exportDialog),
                    text: normalize(exportDialog.innerText || '').slice(0, maxChars),
                  };
                }
                const top = dialogs[dialogs.length - 1];
                return {
                  kind: classify(top),
                  text: normalize(top.innerText || '').slice(0, maxChars),
                };
                """,
                int(max_chars),
            )
            if isinstance(payload, dict):
                kind = str(payload.get("kind") or "unknown").strip().lower() or "unknown"
                text = str(payload.get("text") or "").strip()
                if text:
                    return f"type={kind} text={text}"
                return f"type={kind}"
        return ""

    def _capture_stage_failure_evidence(
        self,
        sb,
        stage_tag: str,
        brand: str,
        report_name: str,
        reason: str,
    ) -> Dict[str, str]:
        current_url = self._safe_current_url(sb)
        modal_text = self._extract_modal_text(sb)

        evidence_dir = os.path.join(self.download_dir, "_meta_evidence")
        with suppress(Exception):
            os.makedirs(evidence_dir, exist_ok=True)

        stamp = time.strftime("%Y%m%d_%H%M%S")
        screenshot_name = (
            f"{self._sanitize_token(stage_tag)}_"
            f"{self._sanitize_token(brand)}_"
            f"{self._sanitize_token(report_name)}_"
            f"{stamp}.png"
        )
        screenshot_path = os.path.join(evidence_dir, screenshot_name)
        with suppress(Exception):
            sb.driver.save_screenshot(screenshot_path)

        self.logger.error(
            "%s failed | brand=%s report=%s reason=%s url=%s screenshot=%s modal=%s",
            stage_tag,
            brand,
            report_name,
            reason,
            current_url,
            screenshot_path,
            modal_text,
        )
        return {
            "current_url": current_url,
            "screenshot_path": screenshot_path,
            "modal_text": modal_text,
        }

    def _history_failure_payload(
        self,
        ticket: ExportTicket,
        state: str,
        reason: str,
        search_query: str,
        reopen_count: int,
        stats: ExportRowMatchStats,
        top_candidate: Optional[ExportHistoryRow],
        top_three_summary: str,
        current_url: str,
        dom_probe_path: str,
        screenshot_path: str,
        modal_text: str,
        stage_tag: str,
        download_dir: str = "",
        files_observed: Optional[List[str]] = None,
        crdownload_seen: bool = False,
        click_verified: bool = False,
        download_started: bool = False,
        file_detected: bool = False,
        browser_download_event: bool = False,
        download_dir_mismatch: bool = False,
        last_ready_signal: str = "",
        permission_prompt_detected: bool = False,
        legacy_reason: str = "",
        download_completed: bool = False,
        view_toast_state: str = "none",
        view_toast_progress_pct: Optional[int] = None,
        view_toast_text: str = "",
        toast_transition_seen: bool = False,
        toast_wait_elapsed_ms: int = 0,
        file_signal_detected_at: str = "",
        fallback_trigger_reason: str = "",
    ) -> Dict[str, Any]:
        brand = str(getattr(ticket, "brand", "") or "")
        report_name = str(getattr(ticket, "report_name", "") or "")
        export_name = str(getattr(ticket, "export_name", "") or "")
        exports_url = str(getattr(ticket, "exports_url", "") or "")
        grid_mode = "unknown"
        if top_candidate is not None:
            grid_mode = "rv" if top_candidate.source_mode == "rv" else "legacy"

        selectors = self._export_history_dom_config()
        if grid_mode == "legacy":
            selectors = {
                "row_xpath": "//*[self::tr or @role='row' or contains(@class,'row')]",
                "download_xpath": ".//button[contains(.,'Download')] | .//*[@aria-label*='download']",
            }

        dom_hierarchy = [
            "Exports page root",
            "Top candidate row",
            "Cell[1] Export Name",
            "Cell[3] Export Date",
            "Cell[4] Status",
            "Cell[5] Download control",
        ]
        if grid_mode == "rv":
            dom_hierarchy = [
                selectors.get("data_grid", ".ReactVirtualized__Grid._1zmk"),
                selectors.get("grid_container", ".ReactVirtualized__Grid__innerScrollContainer"),
                f"row(top={top_candidate.row_top_px if top_candidate else 'n/a'})",
                "cell[1] -> div.ellipsis (Export Name)",
                "cell[3] -> Export Date",
                "cell[4] -> Status",
                "cell[5] -> div[role='button'][data-surface*='export_history_table_button']",
            ]

        action_map = {
            "processing_timeout": "최신 export의 processing 장기 지속. 보고서 생성 백엔드 지연 여부 점검 후 재시도.",
            "row_disappeared": "행이 사라진 뒤 재오픈에도 복구 실패. 계정 컨텍스트/URL 고정 여부 점검.",
            "row_not_found_initial": "초기 검색에서 행 미발견. 검색어/계정/권한 범위 확인.",
            "download_control_missing": "다운로드 컨트롤 탐지 실패. DOM 선택자 재수집 필요.",
            "download_click_no_effect": "다운로드 클릭은 수행됐지만 시작 신호가 감지되지 않음.",
            "download_file_not_observed": "다운로드 시작 신호는 있었지만 watcher 경로에서 완료 파일을 찾지 못함.",
            "download_completion_timeout": ".crdownload 감지 이후 완료 파일 전환이 제한 시간 내 발생하지 않음.",
            "download_dir_mismatch": "브라우저 다운로드 경로와 watcher 경로가 불일치함.",
            "permission_prompt_detected": "다운로드 권한 팝업이 감지됨. 브라우저에서 허용 후 재시도 필요.",
            "permission_block_suspected": "브라우저 다운로드 권한 차단 의심. 다중 다운로드 허용 상태 확인.",
            "latest_failed": "Meta export 상태가 Failed. 리포트 자체 실패 원인 확인.",
            "browser_window_closed": "브라우저 창/세션이 닫혀 작업 중단. 브라우저 유지 상태 확인 후 재시도.",
            "no_file_signal_timeout": "파일 신호를 확인하지 못함. 로컬 다운로드 경로/권한/이벤트 확인 후 재시도.",
            "creating_timeout": "Creating export 상태가 임계 시간을 초과함. fallback 또는 재시도 필요.",
            "ready_no_download": "Ready 토스트 이후에도 다운로드 신호가 없어 fallback으로 전환됨.",
            "global_timeout": "전역 대기 시간을 초과함. fallback 또는 재시도 필요.",
        }

        return {
            "OBJECTIVE": {
                "brand": brand,
                "report_name": report_name,
                "export_name": export_name,
                "exports_url": exports_url,
                "state": state,
                "reason": reason,
                "stage_tag": stage_tag,
            },
            "PAGE_STRUCTURE": {
                "grid_mode": grid_mode,
                "ui_visible_rows_count": stats.ui_visible_rows_count,
                "parsed_rows_count": stats.parsed_rows_count,
                "name_matched_rows_count": stats.name_matched_rows_count,
                "ready_rows_count": stats.ready_rows_count,
                "actionable_rows_count": stats.actionable_rows_count,
            },
            "CSS_SELECTORS_AND_LOCATORS": selectors,
            "DOM_HIERARCHY": dom_hierarchy,
            "IMPLEMENTATION_STRATEGY": {
                "next_action": action_map.get(reason, "현재 상태/DOM 정보 재검증"),
                "reopen_count": reopen_count,
                "search_query": search_query,
                "top_candidate": self._format_export_row_debug(top_candidate),
                "top3_summary": top_three_summary,
            },
            "attachments": {
                "current_url": current_url,
                "search_query": search_query,
                "last_reason": reason,
                "legacy_reason": legacy_reason,
                "top3_row_summary": top_three_summary,
                "dom_probe_path": dom_probe_path,
                "screenshot_path": screenshot_path,
                "modal_text": modal_text,
                "download_dir": download_dir,
                "files_observed": list(files_observed or []),
                "crdownload_seen": bool(crdownload_seen),
                "click_verified": bool(click_verified),
                "download_started": bool(download_started),
                "file_detected": bool(file_detected),
                "browser_download_event": bool(browser_download_event),
                "download_dir_mismatch": bool(download_dir_mismatch),
                "last_ready_signal": str(last_ready_signal or ""),
                "permission_prompt_detected": bool(permission_prompt_detected),
                "download_completed": bool(download_completed),
                "view_toast_state": str(view_toast_state or "none"),
                "view_toast_progress_pct": view_toast_progress_pct,
                "view_toast_text": str(view_toast_text or ""),
                "toast_transition_seen": bool(toast_transition_seen),
                "toast_wait_elapsed_ms": int(max(0, int(toast_wait_elapsed_ms or 0))),
                "file_signal_detected_at": str(file_signal_detected_at or ""),
                "fallback_trigger_reason": str(fallback_trigger_reason or ""),
                "accepted_export_name": str(getattr(ticket, "accepted_export_name", "") or ""),
                "search_queries": list(getattr(ticket, "search_queries", None) or []),
            },
        }

    def _emit_history_failure_payload(
        self,
        payload: Dict[str, Any],
    ) -> Optional[str]:
        evidence_dir = os.path.join(self.download_dir, "_meta_evidence")
        with suppress(Exception):
            os.makedirs(evidence_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        payload_path = os.path.join(evidence_dir, f"CLAUDE_EXTENSION_{stamp}.json")
        with suppress(Exception):
            with open(payload_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self.logger.error("[CLAUDE_EXTENSION] payload_saved=%s", payload_path)
            return payload_path
        return None

    def _normalize_export_name_value(self, value: str) -> str:
        normalized = str(value or "").replace("\u200b", " ")
        return re.sub(r"\s+", " ", normalized).strip()

    def _is_export_name_set(self, sb, expected_name: str) -> bool:
        expected = self._normalize_export_name_value(expected_name)
        if not expected:
            return False

        modal_el = self._find_export_modal_element(sb)
        with suppress(Exception):
            if modal_el:
                is_set = sb.execute_script(
                    """
                    const dlg = arguments[0];
                    const expected = (arguments[1] || '').trim();
                    if (!dlg || !expected) return false;
                    const isVisible = (el) => {
                      if (!el) return false;
                      const style = window.getComputedStyle(el);
                      if (style.display === 'none' || style.visibility === 'hidden') return false;
                      const rect = el.getBoundingClientRect();
                      return rect.width > 0 && rect.height > 0;
                    };
                    const inputs = [...dlg.querySelectorAll('input[type="text"], input:not([type])')].filter(isVisible);
                    if (!inputs.length) return false;
                    const preferred = inputs.find((input) => {
                      const aria = (input.getAttribute('aria-label') || '').toLowerCase();
                      const name = (input.getAttribute('name') || '').toLowerCase();
                      const placeholder = (input.getAttribute('placeholder') || '').toLowerCase();
                      return aria.includes('export') || name.includes('export') || placeholder.includes('export');
                    }) || inputs[0];
                    const val = String(preferred.value || '').replace(/\\u200b/g, ' ').replace(/\\s+/g, ' ').trim();
                    return val === expected;
                    """,
                    modal_el,
                    expected,
                )
                if bool(is_set):
                    return True

        input_xpaths = [
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export') or contains(normalize-space(.),'\ub0b4\ubcf4\ub0b4\uae30')]//input[@type='text'])[1]",
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export name') or contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'raw data table')]//input[@type='text'])[1]",
            "(//*[contains(@role,'dialog')][contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'xlsx')]//input[@type='text'])[1]",
        ]
        for xp in input_xpaths:
            with suppress(Exception):
                el = sb.driver.find_element(By.XPATH, xp)
                val = self._normalize_export_name_value(str(el.get_attribute("value") or ""))
                if val == expected:
                    return True

        return False

    def _verify_export_name_stable(self, sb, expected_name: str) -> bool:
        expected = self._normalize_export_name_value(expected_name)
        if not expected:
            return False

        required = max(1, int(self.meta_config.get("export_name_verify_consecutive", 2)))
        settle_sec = max(0.2, float(self.meta_config.get("export_name_settle_sec", 2.0)))
        poll_sec = min(0.35, max(0.12, settle_sec / max(2.0, float(required + 1))))
        deadline = time.time() + max(settle_sec, poll_sec * float(required * 3))
        consecutive = 0

        while time.time() <= deadline:
            if self._is_export_name_set(sb, expected):
                consecutive += 1
                if consecutive >= required:
                    return True
            else:
                consecutive = 0
            sb.sleep(poll_sec)
        return False

    def _wait_export_acceptance_signal(self, sb, timeout: Optional[int] = None) -> Optional[str]:
        wait_timeout = int(timeout or self.meta_config.get("export_accept_timeout_sec", 20))
        start = time.time()

        toast_xpaths = [
            "//*[(@role='alert' or @aria-live='polite' or @aria-live='assertive') and (contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export') or contains(normalize-space(.),'\ub0b4\ubcf4\ub0b4\uae30'))]",
            "//*[contains(normalize-space(.),'\ub0b4\ubcf4\ub0b4\uae30') and (contains(normalize-space(.),'\uc644\ub8cc') or contains(normalize-space(.),'\uc694\uccad') or contains(normalize-space(.),'\uc2dc\uc791'))]",
        ]
        while time.time() - start <= wait_timeout:
            for xp in toast_xpaths:
                with suppress(Exception):
                    if sb.driver.find_elements(By.XPATH, xp):
                        return "toast"

            if not self._find_export_modal_element(sb):
                return "modal_closed"

            sb.sleep(1)

        return None

    def _build_raw_target_name(
        self,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        sheet_name: str,
    ) -> str:
        return format_name(
            pattern=self.naming_config["raw_file_name_pattern"],
            brand=brand_cfg["brand_ko"],
            activity=activity_for_filename,
            yymmdd=yymmdd,
            sheet=sheet_name,
        )

    def _move_download_to_target(self, src_path: str, target_path: str) -> str:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        if os.path.abspath(src_path) != os.path.abspath(target_path):
            os.replace(src_path, target_path)
        return target_path

    def _prepare_name_export_context(self, sb, brand_cfg: Dict) -> None:
        stage_tag = "[EXPORT:FALLBACK_NAME]"
        reporting_url = self._resolve_reporting_url(brand_cfg)
        self.logger.info("%s Prepare reporting context: brand=%s url=%s", stage_tag, brand_cfg["brand_ko"], reporting_url)
        sb.open(reporting_url)
        sb.sleep(3)

        id_direct_enabled = self._is_id_direct_url_enabled_for_brand(brand_cfg)
        if id_direct_enabled:
            current_url = self._safe_current_url(sb)
            if self._is_expected_id_url_context(current_url, brand_cfg):
                self.logger.info("%s ID direct URL context active. Skip manual picker.", stage_tag)
                return
            self.logger.warning(
                "%s ID URL context mismatch. Try manual picker (brand=%s, url=%s)",
                stage_tag,
                brand_cfg["brand_ko"],
                current_url,
            )

        self._try_select_portfolio_and_account(
            sb=sb,
            portfolio=brand_cfg["meta_business_portfolio"],
            ad_account=brand_cfg["meta_ad_account"],
        )

    def _trigger_export_from_current_context(
        self,
        sb,
        stage_tag: str,
        method: str,
        brand_cfg: Dict,
        report_name: str,
        export_name: str,
    ) -> ExportResult:
        self.logger.info("%s Trigger export: brand=%s report=%s export_name=%s", stage_tag, brand_cfg["brand_ko"], report_name, export_name)
        max_rounds = 3
        required_name_verify = max(1, int(self.meta_config.get("export_name_verify_consecutive", 2)))
        for round_idx in range(1, max_rounds + 1):
            self._dismiss_notification_overlay(sb, retries=2)
            if not self._find_export_modal_element(sb):
                self._open_export_modal_from_view(sb)
            self._dismiss_notification_overlay(sb, retries=2)

            if not self._find_export_modal_element(sb):
                self.logger.warning(
                    "%s Export modal not ready. round=%s/%s report=%s",
                    stage_tag,
                    round_idx,
                    max_rounds,
                    report_name,
                )
                if round_idx < max_rounds:
                    sb.sleep(0.5)
                    continue
                modal_text = self._extract_modal_text(sb, max_chars=200)
                if modal_text:
                    raise RuntimeError(
                        f"Could not locate export modal for export name set: {export_name} | modal={modal_text}"
                    )
                raise RuntimeError(f"Could not locate export modal for export name set: {export_name}")

            self._set_export_name_in_modal(sb, export_name)
            if self._verify_export_name_stable(sb, export_name):
                self.logger.info(
                    "%s Export name verified report=%s required_consecutive=%s",
                    stage_tag,
                    report_name,
                    required_name_verify,
                )
                break

            self.logger.warning(
                "%s Export name verify retry. round=%s/%s report=%s",
                stage_tag,
                round_idx,
                max_rounds,
                report_name,
            )
            if round_idx < max_rounds:
                sb.sleep(0.4)
                continue

            modal_text = self._extract_modal_text(sb, max_chars=200)
            if modal_text:
                raise RuntimeError(
                    f"Could not verify export name in modal: {export_name} | modal={modal_text}"
                )
            raise RuntimeError(f"Could not verify export name in modal: {export_name}")

        self._dismiss_notification_overlay(sb, retries=1)
        self._select_raw_xlsx_export_type(sb)
        name_ok = self._verify_export_name_stable(sb, export_name)
        xlsx_ok = self._is_raw_xlsx_selected(sb)
        if not (name_ok and xlsx_ok):
            raise RuntimeError(
                "Export preflight gate failed before confirm: "
                f"name_verified={name_ok} raw_xlsx_selected={xlsx_ok}"
            )

        request_ts = time.time()
        confirm_error: Optional[Exception] = None
        for confirm_round in range(1, 3):
            self._dismiss_notification_overlay(sb, retries=1)
            try:
                self._confirm_export_in_modal(sb)
                confirm_error = None
                break
            except Exception as exc:  # noqa: BLE001
                confirm_error = exc
                self.logger.warning(
                    "%s Export confirm retry. round=%s/2 report=%s",
                    stage_tag,
                    confirm_round,
                    report_name,
                )
                if confirm_round < 2:
                    sb.sleep(0.4)
        if confirm_error is not None:
            raise confirm_error

        signal = self._wait_export_acceptance_signal(sb)
        if not signal:
            raise TimeoutError("No export acceptance signal detected after Export click")

        toast_probe = self._probe_view_export_toast(sb)
        accepted_export_name = self._extract_export_name_from_toast_text(
            str(toast_probe.get("view_toast_text") or "")
        )
        if accepted_export_name:
            self.logger.info(
                "%s Export accepted: report=%s signal=%s accepted_export_name=%s",
                stage_tag,
                report_name,
                signal,
                accepted_export_name,
            )
        else:
            self.logger.info("%s Export accepted: report=%s signal=%s", stage_tag, report_name, signal)
        return ExportResult(
            success=True,
            method=method,
            export_name=export_name,
            request_ts=request_ts,
            report_name=report_name,
            accepted_export_name=accepted_export_name,
        )

    def _export_stage_via_report_id_url(
        self,
        sb,
        brand_cfg: Dict,
        report_name: str,
        report_id: str,
        export_name: str,
    ) -> ExportResult:
        stage_tag = "[EXPORT:URL]"
        view_url = self._build_report_view_url(brand_cfg, report_id)
        self.logger.info("%s Open report view by ID: brand=%s report=%s report_id=%s", stage_tag, brand_cfg["brand_ko"], report_name, report_id)
        sb.open(view_url)
        sb.sleep(3)
        expected_account_id, expected_business_id = self._brand_ids(brand_cfg)
        self._ensure_account_context_with_ticket(
            sb=sb,
            ticket=ExportTicket(
                sheet_name=report_name,
                report_name=report_name,
                report_id=report_id,
                export_name=export_name,
                request_ts=time.time(),
                target_path="",
                expected_account_id=expected_account_id,
                expected_business_id=expected_business_id,
            ),
        )
        self._wait_view_export_entry_ready(sb=sb, report_id=report_id, stage_tag=stage_tag)
        try:
            return self._trigger_export_from_current_context(
                sb=sb,
                stage_tag=stage_tag,
                method="URL",
                brand_cfg=brand_cfg,
                report_name=report_name,
                export_name=export_name,
            )
        except Exception as e:
            self._capture_stage_failure_evidence(
                sb=sb,
                stage_tag=stage_tag,
                brand=brand_cfg["brand_ko"],
                report_name=report_name,
                reason=str(e),
            )
            raise RuntimeError(f"{stage_tag} failed for report={report_name}: {e}") from e

    def _export_stage_via_report_name(
        self,
        sb,
        brand_cfg: Dict,
        report_name: str,
        export_name: str,
    ) -> ExportResult:
        stage_tag = "[EXPORT:FALLBACK_NAME]"
        self.logger.info("%s Open report by name: brand=%s report=%s", stage_tag, brand_cfg["brand_ko"], report_name)
        try:
            self._open_report_by_name(sb, report_name)
            return self._trigger_export_from_current_context(
                sb=sb,
                stage_tag=stage_tag,
                method="FALLBACK_NAME",
                brand_cfg=brand_cfg,
                report_name=report_name,
                export_name=export_name,
            )
        except Exception as e:
            self._capture_stage_failure_evidence(
                sb=sb,
                stage_tag=stage_tag,
                brand=brand_cfg["brand_ko"],
                report_name=report_name,
                reason=str(e),
            )
            raise RuntimeError(f"{stage_tag} failed for report={report_name}: {e}") from e

    def _download_stage_for_export(
        self,
        sb,
        brand_cfg: Dict,
        report_name: str,
        export_result: ExportResult,
        target_path: str,
        sheet_name: str = "",
        sheet_key: str = "",
        progress_event_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> DownloadResult:
        if not export_result.success:
            raise RuntimeError("Download stage requires export success")

        stage_url = "[DOWNLOAD:URL]"
        stage_history = "[DOWNLOAD:HISTORY]"
        self._emit_export_progress_event(
            progress_event_cb,
            phase="downloading",
            snapshot={
                "sheet_name": sheet_name,
                "sheet_key": sheet_key,
                "method": "URL",
            },
        )
        self.logger.info("%s Wait view_report download signals: report=%s", stage_url, report_name)
        view_wait = self._wait_view_report_download_or_gate_fallback(
            sb=sb,
            since_ts=export_result.request_ts,
            report_name=report_name,
        )
        if view_wait.success and view_wait.file_path:
            final_path = self._move_download_to_target(view_wait.file_path, target_path)
            self.logger.info("%s Download success: %s", stage_url, final_path)
            self._emit_export_progress_event(
                progress_event_cb,
                phase="downloaded",
                snapshot={
                    "sheet_name": sheet_name,
                    "sheet_key": sheet_key,
                    "method": "URL",
                    "file_path": final_path,
                    "view_toast_state": view_wait.view_toast_state,
                    "view_toast_progress_pct": view_wait.view_toast_progress_pct,
                    "view_toast_text": view_wait.view_toast_text,
                    "toast_transition_seen": view_wait.toast_transition_seen,
                    "toast_wait_elapsed_ms": view_wait.toast_wait_elapsed_ms,
                    "file_signal_detected_at": view_wait.file_signal_detected_at,
                },
            )
            return DownloadResult(success=True, method="URL", file_path=final_path)

        fallback_trigger_reason = str(view_wait.fallback_trigger_reason or "no_file_signal_timeout")
        toast_export_name = self._extract_export_name_from_toast_text(view_wait.view_toast_text)
        accepted_export_name = str(export_result.accepted_export_name or "").strip() or toast_export_name
        history_search_queries = self._build_history_search_queries(
            export_name=export_result.export_name,
            report_name=report_name,
            accepted_export_name=accepted_export_name,
        )
        self.logger.info(
            "%s history_search_candidates report=%s queries=%s",
            stage_history,
            report_name,
            history_search_queries,
        )
        self.logger.warning(
            "%s fallback gate opened report=%s reason=%s toast_state=%s toast_transition_seen=%s elapsed_ms=%s",
            stage_url,
            report_name,
            fallback_trigger_reason,
            view_wait.view_toast_state,
            bool(view_wait.toast_transition_seen),
            int(view_wait.toast_wait_elapsed_ms or 0),
        )
        self._emit_export_progress_event(
            progress_event_cb,
            phase="waiting_ready",
            snapshot={
                "sheet_name": sheet_name,
                "sheet_key": sheet_key,
                "method": "URL",
                "fallback_trigger_reason": fallback_trigger_reason,
                "view_toast_state": view_wait.view_toast_state,
                "view_toast_progress_pct": view_wait.view_toast_progress_pct,
                "view_toast_text": view_wait.view_toast_text,
                "toast_transition_seen": bool(view_wait.toast_transition_seen),
                "toast_wait_elapsed_ms": int(view_wait.toast_wait_elapsed_ms or 0),
                "file_signal_detected_at": view_wait.file_signal_detected_at,
            },
        )

        fallback_max_attempts = max(1, int(self.meta_config.get("view_fallback_max_attempts", 1)))
        fallback_retry_delay_sec = max(0.5, float(self.meta_config.get("view_fallback_retry_delay_sec", 3.0)))
        last_fallback_error: Optional[Exception] = None

        for fallback_attempt in range(1, fallback_max_attempts + 1):
            current_url = self._safe_current_url(sb)
            if self._is_auth_gate_url(current_url) or self._is_trust_prompt_visible(sb):
                raise RuntimeError("login_checkpoint_blocked")
            try:
                exports_url = self._build_exports_url(brand_cfg)
                self.logger.info(
                    "%s Open export history url=%s attempt=%s/%s trigger_reason=%s",
                    stage_history,
                    exports_url,
                    fallback_attempt,
                    fallback_max_attempts,
                    fallback_trigger_reason,
                )
                sb.open(exports_url)
                sb.sleep(2)
                expected_account_id, expected_business_id = self._brand_ids(brand_cfg)
                ticket = ExportTicket(
                    sheet_name=sheet_name or report_name,
                    report_name=report_name,
                    report_id="",
                    export_name=export_result.export_name,
                    request_ts=export_result.request_ts,
                    target_path=target_path,
                    brand=str(brand_cfg.get("brand_ko") or ""),
                    exports_url=exports_url,
                    sheet_key=sheet_key,
                    expected_account_id=expected_account_id,
                    expected_business_id=expected_business_id,
                    fallback_trigger_reason=fallback_trigger_reason,
                    view_toast_state=view_wait.view_toast_state,
                    view_toast_progress_pct=view_wait.view_toast_progress_pct,
                    view_toast_text=view_wait.view_toast_text,
                    toast_transition_seen=view_wait.toast_transition_seen,
                    toast_wait_elapsed_ms=view_wait.toast_wait_elapsed_ms,
                    file_signal_detected_at=view_wait.file_signal_detected_at,
                    accepted_export_name=accepted_export_name,
                    search_queries=history_search_queries,
                )
                final_path = self._download_export_from_history_with_bounded_polling(
                    sb=sb,
                    ticket=ticket,
                    progress_event_cb=progress_event_cb,
                )
                self.logger.info("%s Download success: %s", stage_history, final_path)
                return DownloadResult(success=True, method="HISTORY", file_path=final_path)
            except Exception as e:
                last_fallback_error = e
                reason_text = self._normalized_stage_error_reason(e)
                self._capture_stage_failure_evidence(
                    sb=sb,
                    stage_tag=stage_history,
                    brand=brand_cfg["brand_ko"],
                    report_name=report_name,
                    reason=reason_text,
                )
                if fallback_attempt >= fallback_max_attempts:
                    raise RuntimeError(f"{stage_history} failed for report={report_name}: {reason_text}") from e
                sb.sleep(fallback_retry_delay_sec)

        if last_fallback_error is not None:
            raise RuntimeError(
                f"{stage_history} failed for report={report_name}: "
                f"{self._normalized_stage_error_reason(last_fallback_error)}"
            ) from last_fallback_error
        raise RuntimeError(f"{stage_history} failed for report={report_name}: no_file_signal_timeout")

    def _export_history_dom_config(self) -> Dict[str, Any]:
        cfg = self.meta_config.get("export_history_dom") or {}
        return {
            "data_grid": str(cfg.get("data_grid") or ".ReactVirtualized__Grid._1zmk"),
            "grid_container": str(cfg.get("grid_container") or ".ReactVirtualized__Grid__innerScrollContainer"),
            "export_name_selector": str(cfg.get("export_name_selector") or "div.ellipsis"),
            "download_button_selector": str(
                cfg.get("download_button_selector")
                or 'div[role="button"][data-surface*="export_history_table_button"]'
            ),
            "row_height_px": max(1.0, float(cfg.get("row_height_px") or 52)),
        }

    def _set_exports_search_query(self, sb, query: str) -> bool:
        text = str(query or "").strip()
        if not text:
            return False

        search_xpaths = [
            "//input[contains(translate(@placeholder,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'search exports')]",
            "//input[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'search exports')]",
            "//input[contains(@placeholder,'\uac80\uc0c9')]",
            "//input[contains(@aria-label,'\uac80\uc0c9')]",
            "//input[@type='search']",
        ]
        for xp in search_xpaths:
            try:
                sb.clear(xp, timeout=1.5)
                sb.type(xp, text, timeout=1.5)
                sb.sleep(0.5)
                return True
            except Exception:
                continue

        with suppress(Exception):
            ok = sb.execute_script(
                """
                const text = arguments[0];
                const candidates = [...document.querySelectorAll('input')].filter((el) => {
                    const p = (el.getAttribute('placeholder') || '').toLowerCase();
                    const a = (el.getAttribute('aria-label') || '').toLowerCase();
                    return p.includes('search exports') || a.includes('search exports') || p.includes('\uac80\uc0c9') || a.includes('\uac80\uc0c9');
                });
                if (!candidates.length) return false;
                const input = candidates[0];
                input.focus();
                input.value = '';
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.value = text;
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                return true;
                """,
                text,
            )
            if ok:
                sb.sleep(0.5)
                return True
        return False

    def _normalize_export_name_key(self, value: str) -> str:
        key = unicodedata.normalize("NFKC", str(value or ""))
        key = key.replace("\u200b", "").replace("\ufeff", "").strip().lower()
        if key.endswith(".xlsx"):
            key = key[:-5].strip()
        key = re.sub(r"\s+", " ", key)
        return key

    def _normalize_export_status(self, value: str) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip().lower()
        if not text:
            return "unknown"
        if "ready" in text or "\uc900\ube44" in text:
            return "ready"
        if "processing" in text or "\ucc98\ub9ac\uc911" in text:
            return "processing"
        if "failed" in text or "fail" in text or "\uc2e4\ud328" in text:
            return "failed"
        return "unknown"

    def _is_browser_window_closed_error(self, exc: Exception) -> bool:
        text = re.sub(r"\s+", " ", str(exc or "")).strip().lower()
        if not text:
            return False
        tokens = (
            "active window was already closed",
            "no such window",
            "target window already closed",
            "window not found",
            "browsing context has been discarded",
            "invalid session id",
        )
        return any(token in text for token in tokens)

    def _normalized_stage_error_reason(self, exc: Exception) -> str:
        if isinstance(exc, HistoryDownloadError):
            detailed_reason = str(getattr(exc, "reason", "") or "").strip()
            if detailed_reason:
                return detailed_reason
        if self._is_browser_window_closed_error(exc):
            return "browser_window_closed"
        text = re.sub(r"\s+", " ", str(exc or "")).strip()
        return text or exc.__class__.__name__

    def _parse_export_date_text(self, value: str) -> Optional[float]:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            return None

        english_patterns = [
            "%b %d, %Y at %I:%M %p",
            "%B %d, %Y at %I:%M %p",
            "%b %d, %Y %I:%M %p",
            "%B %d, %Y %I:%M %p",
        ]
        for pattern in english_patterns:
            with suppress(Exception):
                return datetime.strptime(text, pattern).timestamp()

        with suppress(Exception):
            # Handles truncated hour suffix like "Mar 10, 2026 at 11:12 P..."
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\s+at\s+\d{1,2}:\d{2})", text)
            if m:
                for p in ("%b %d, %Y at %I:%M", "%B %d, %Y at %I:%M", "%b %d, %Y at %H:%M", "%B %d, %Y at %H:%M"):
                    with suppress(Exception):
                        return datetime.strptime(m.group(1), p).timestamp()

        with suppress(Exception):
            m = re.search(
                r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}\s+at\s+\d{1,2}:\d{2}\s+[APMapm]{2})",
                text,
            )
            if m:
                return datetime.strptime(m.group(1), "%b %d, %Y at %I:%M %p").timestamp()

        with suppress(Exception):
            # Date-only fallback; row order resolves same-day duplicates.
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", text)
            if m:
                for p in ("%b %d, %Y", "%B %d, %Y"):
                    with suppress(Exception):
                        return datetime.strptime(m.group(1), p).timestamp()

        m = re.search(
            r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})\D+(?:(AM|PM|\uc624\uc804|\uc624\ud6c4)\s*)?(\d{1,2}):(\d{2})",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            ampm = (m.group(4) or "").strip().lower()
            hour = int(m.group(5))
            minute = int(m.group(6))
            if ampm in ("pm", "\uc624\ud6c4") and hour < 12:
                hour += 12
            if ampm in ("am", "\uc624\uc804") and hour == 12:
                hour = 0
            with suppress(Exception):
                return datetime(year, month, day, hour, minute).timestamp()

        return None

    def _find_export_row_for_download_button(self, button_el: Any) -> Optional[Any]:
        row_xpaths = [
            "./ancestor::*[(self::tr or @role='row' or contains(@class,'row'))][1]",
            "./ancestor::*[contains(@class,'table') or @role='grid' or @role='table'][1]",
        ]
        for xp in row_xpaths:
            with suppress(Exception):
                rows = button_el.find_elements(By.XPATH, xp)
                if rows:
                    return rows[0]
        return None

    def _collect_export_rows_react_virtualized(
        self,
        sb,
    ) -> List[ExportHistoryRow]:
        dom_cfg = self._export_history_dom_config()
        rows_data: List[Dict[str, Any]] = []
        with suppress(Exception):
            rows_data = sb.execute_script(
                """
                const cfg = arguments[0];
                const norm = (txt) => (txt || '').replace(/\\s+/g, ' ').trim();
                const parsePx = (style, key) => {
                    if (!style) return null;
                    const m = String(style).match(new RegExp(key + '\\\\s*:\\\\s*(-?\\\\d+(?:\\\\.\\\\d+)?)px', 'i'));
                    return m ? Number(m[1]) : null;
                };
                const grid = document.querySelector(cfg.data_grid);
                if (!grid) return [];
                const container = grid.querySelector(cfg.grid_container) || grid;
                const cellNodes = Array.from(container.querySelectorAll("div[style*='top:']"));
                if (!cellNodes.length) return [];

                const rows = new Map();
                for (const cell of cellNodes) {
                    const style = cell.getAttribute('style') || '';
                    const top = parsePx(style, 'top');
                    const left = parsePx(style, 'left');
                    if (top === null || left === null) continue;

                    const key = String(Math.round(top * 100) / 100);
                    if (!rows.has(key)) {
                        rows.set(key, { top, cells: [] });
                    }

                    const nameEl = cell.querySelector(cfg.export_name_selector);
                    const nameText = norm(nameEl ? nameEl.textContent : '');
                    const text = norm(cell.textContent || '');
                    const controls = Array.from(
                        cell.querySelectorAll(
                            cfg.download_button_selector + ", [role='button'], button, a"
                        )
                    );
                    const hasDownload = controls.some((el) => {
                        const txt = norm(el.textContent || '').toLowerCase();
                        const aria = String(el.getAttribute('aria-label') || '').toLowerCase();
                        const title = String(el.getAttribute('title') || '').toLowerCase();
                        const surf = String(el.getAttribute('data-surface') || '').toLowerCase();
                        const testId = String(el.getAttribute('data-testid') || '').toLowerCase();
                        return (
                            surf.includes('export_history_table_button') ||
                            txt.includes('download') ||
                            txt.includes('다운로드') ||
                            aria.includes('download') ||
                            title.includes('download') ||
                            testId.includes('download')
                        );
                    });

                    rows.get(key).cells.push({
                        left,
                        text,
                        nameText,
                        hasDownload,
                    });
                }

                const out = [];
                for (const row of rows.values()) {
                    const cells = row.cells
                        .slice()
                        .sort((a, b) => (a.left || 0) - (b.left || 0));
                    const c0 = cells[0] || { text: '' };
                    const c1 = cells[1] || { text: '' };
                    const c3 = cells[3] || { text: '' };
                    const c4 = cells[4] || { text: '' };
                    const c5 = cells[5] || { text: '' };
                    let nameRaw = c1.nameText || c1.text || '';
                    if (!nameRaw) {
                        const fallbackNameCell = cells.find((c) => !!c.nameText);
                        if (fallbackNameCell) {
                            nameRaw = fallbackNameCell.nameText || fallbackNameCell.text || '';
                        }
                    }
                    if (!nameRaw && cells.length) {
                        nameRaw = cells[0].nameText || cells[0].text || '';
                    }
                    const statusRaw = c4.text || '';
                    const exportDateRaw = c3.text || '';
                    const hasDownload =
                        !!c5.hasDownload || cells.some((c) => !!c.hasDownload);
                    out.push({
                        row_top_px: Number(row.top),
                        name_raw: nameRaw,
                        status_raw: statusRaw,
                        export_date_raw: exportDateRaw,
                        has_download: hasDownload,
                        cell_count: cells.length,
                        row_text: norm(cells.map((c) => c.text).join(' ')),
                        checkbox_hint_text: c0.text || '',
                    });
                }
                out.sort((a, b) => (a.row_top_px || 0) - (b.row_top_px || 0));
                return out;
                """,
                dom_cfg,
            ) or []

        out: List[ExportHistoryRow] = []
        for idx, row_data in enumerate(rows_data):
            name_raw = str(row_data.get("name_raw") or "").strip()
            if not name_raw:
                continue
            status_raw = str(row_data.get("status_raw") or "").strip()
            export_date_raw = str(row_data.get("export_date_raw") or "").strip()
            row_top_raw = row_data.get("row_top_px")
            row_top_px = None
            with suppress(Exception):
                row_top_px = float(row_top_raw)
            status_norm = self._normalize_export_status(status_raw)
            export_dt = self._parse_export_date_text(export_date_raw)
            out.append(
                ExportHistoryRow(
                    name_raw=name_raw,
                    name_key=self._normalize_export_name_key(name_raw),
                    status_norm=status_norm,
                    export_dt=export_dt,
                    row_index=idx,
                    checkbox_el=None,
                    row_el=None,
                    row_top_px=row_top_px,
                    download_button_el=(True if bool(row_data.get("has_download")) else None),
                    export_date_raw=export_date_raw,
                    status_raw=status_raw,
                    source_mode="rv",
                )
            )
        return out

    def _click_react_virtualized_row_control_by_top(self, sb, row_top_px: float, mode: str) -> bool:
        dom_cfg = self._export_history_dom_config()
        row_height = float(dom_cfg.get("row_height_px") or 52)
        tolerance = max(2.0, row_height * 0.12)
        with suppress(Exception):
            return bool(
                sb.execute_script(
                    """
                    const cfg = arguments[0];
                    const rowTop = Number(arguments[1]);
                    const mode = String(arguments[2] || '');
                    const tolerance = Number(arguments[3]);
                    const norm = (txt) => (txt || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    const parsePx = (style, key) => {
                        if (!style) return null;
                        const m = String(style).match(new RegExp(key + '\\\\s*:\\\\s*(-?\\\\d+(?:\\\\.\\\\d+)?)px', 'i'));
                        return m ? Number(m[1]) : null;
                    };
                    const grid = document.querySelector(cfg.data_grid);
                    if (!grid) return false;
                    const container = grid.querySelector(cfg.grid_container) || grid;
                    const allCells = Array.from(container.querySelectorAll("div[style*='top:']"));
                    const rowCells = allCells
                        .filter((el) => {
                            const top = parsePx(el.getAttribute('style') || '', 'top');
                            return top !== null && Math.abs(top - rowTop) <= tolerance;
                        })
                        .map((el) => ({
                            el,
                            left: parsePx(el.getAttribute('style') || '', 'left') ?? 0,
                        }))
                        .sort((a, b) => a.left - b.left)
                        .map((item) => item.el);
                    if (!rowCells.length) return false;

                    if (mode === 'checkbox') {
                        const firstCell = rowCells[0];
                        const candidates = [
                            ...firstCell.querySelectorAll("input[type='checkbox'], [role='checkbox'], label"),
                        ];
                        for (const item of candidates) {
                            try {
                                if (item.scrollIntoView) item.scrollIntoView({ block: 'center', inline: 'nearest' });
                                item.click();
                                return true;
                            } catch (e) {}
                        }
                        try {
                            firstCell.click();
                            return true;
                        } catch (e) {}
                        return false;
                    }

                    if (mode === 'download') {
                        const downloadCell = rowCells[5] || rowCells[rowCells.length - 1];
                        const candidates = [
                            ...downloadCell.querySelectorAll(
                                cfg.download_button_selector + ", [role='button'], button, a"
                            ),
                        ];
                        const visible = (el) => {
                            if (!el) return false;
                            const style = window.getComputedStyle(el);
                            if (style.display === 'none' || style.visibility === 'hidden' || style.pointerEvents === 'none') {
                                return false;
                            }
                            const rect = el.getBoundingClientRect();
                            return rect.width > 0 && rect.height > 0;
                        };
                        const isEnabled = (el) => {
                            if (!el) return false;
                            const ariaDisabled = String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                            const disabledAttr = el.hasAttribute('disabled');
                            const cls = String(el.getAttribute('class') || '').toLowerCase();
                            const tabIndex = String(el.getAttribute('tabindex') || '');
                            if (ariaDisabled || disabledAttr || cls.includes('disabled')) return false;
                            if (tabIndex === '-1' && el.getAttribute('role') === 'button') return false;
                            return true;
                        };
                        for (const item of candidates) {
                            const target =
                                item.closest("[role='button'],button,a,[data-surface*='export_history_table_button']") || item;
                            const txt = norm(target.textContent || item.textContent);
                            const aria = String(target.getAttribute('aria-label') || item.getAttribute('aria-label') || '').toLowerCase();
                            const title = String(target.getAttribute('title') || item.getAttribute('title') || '').toLowerCase();
                            const surf = String(target.getAttribute('data-surface') || item.getAttribute('data-surface') || '').toLowerCase();
                            const testId = String(target.getAttribute('data-testid') || item.getAttribute('data-testid') || '').toLowerCase();
                            const ok =
                                surf.includes('export_history_table_button') ||
                                txt.includes('download') ||
                                txt.includes('다운로드') ||
                                aria.includes('download') ||
                                title.includes('download') ||
                                testId.includes('download');
                            if (!ok) continue;
                            if (!visible(target) || !isEnabled(target)) continue;
                            try {
                                if (target.scrollIntoView) target.scrollIntoView({ block: 'center', inline: 'nearest' });
                                target.click();
                                return true;
                            } catch (e) {}
                            try {
                                target.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true }));
                                target.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
                                target.dispatchEvent(new MouseEvent('pointerup', { bubbles: true }));
                                target.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
                                target.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                                return true;
                            } catch (e) {}
                        }
                        return false;
                    }
                    return false;
                    """,
                    dom_cfg,
                    row_top_px,
                    mode,
                    tolerance,
                )
            )
        return False

    def _export_row_identity_key(self, row_el: Any) -> str:
        row_id = ""
        with suppress(Exception):
            row_id = str(getattr(row_el, "id", "") or "")
        if not row_id:
            with suppress(Exception):
                row_id = str(row_el.get_attribute("id") or "")
        if row_id:
            return f"id:{row_id}"

        row_index = ""
        with suppress(Exception):
            row_index = str(row_el.get_attribute("aria-rowindex") or "")

        row_text = self._normalize_export_name_key(str(getattr(row_el, "text", "") or ""))
        if row_text:
            row_text = row_text[:200]
        return f"idx:{row_index}|txt:{row_text}"

    def _header_text_to_key(self, value: str) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip().lower()
        if not text:
            return ""
        if "export name" in text or "name" == text:
            return "name"
        if "export type" in text or "type" == text:
            return "type"
        if "export date" in text or "date" == text:
            return "date"
        if "status" in text or "\uc0c1\ud0dc" in text:
            return "status"
        return ""

    def _collect_exports_table_roots(self, sb) -> List[Any]:
        root_xpaths = [
            "//*[(@role='grid' or @role='table') and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export name')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export date')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'status')]]",
            "//table[.//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export name')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export date')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'status')]]",
            "//*[contains(@class,'table') or contains(@class,'grid')][.//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export name')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export date')] and .//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'status')]]",
        ]

        out: List[Any] = []
        seen: set[str] = set()
        for xp in root_xpaths:
            with suppress(Exception):
                for root in sb.driver.find_elements(By.XPATH, xp):
                    key = self._export_row_identity_key(root)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(root)
        return out

    def _collect_rows_from_root(self, root_el: Any) -> List[Any]:
        row_xpaths = [
            ".//*[self::tr or @role='row']",
            ".//*[contains(@class,'row') and not(contains(@class,'arrow'))]",
        ]
        out: List[Any] = []
        seen: set[str] = set()
        for xp in row_xpaths:
            with suppress(Exception):
                for row_el in root_el.find_elements(By.XPATH, xp):
                    key = self._export_row_identity_key(row_el)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(row_el)
        return out

    def _collect_export_row_candidates(self, sb) -> List[Any]:
        roots = self._collect_exports_table_roots(sb)
        out: List[Any] = []
        seen: set[str] = set()

        for root in roots:
            for row_el in self._collect_rows_from_root(root):
                key = self._export_row_identity_key(row_el)
                if key in seen:
                    continue
                seen.add(key)
                out.append(row_el)

        if out:
            return out

        # Fallback when table root selectors are not stable in a specific account UI.
        row_xpaths = [
            "//*[self::tr or @role='row' or contains(@class,'row')]",
        ]
        for xp in row_xpaths:
            with suppress(Exception):
                for row_el in sb.driver.find_elements(By.XPATH, xp):
                    key = self._export_row_identity_key(row_el)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(row_el)
        return out

    def _row_has_download_button(self, row_el: Any) -> bool:
        row_download_xpaths = [
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')]",
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//button[contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@data-testid,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
        ]
        for xp in row_download_xpaths:
            with suppress(Exception):
                if row_el.find_elements(By.XPATH, xp):
                    return True
        return False

    def _format_export_row_debug(self, row: Optional[ExportHistoryRow]) -> str:
        if not row:
            return "none"
        return (
            f"name={row.name_raw} status={row.status_norm} "
            f"date={row.export_date_raw or 'n/a'} index={row.row_index} "
            f"source={row.source_mode} top={row.row_top_px if row.row_top_px is not None else 'n/a'}"
        )

    def _wait_for_download_file_since(self, sb, since_ts: float, timeout_sec: int) -> Optional[str]:
        start = time.time()
        while time.time() - start <= max(0, int(timeout_sec)):
            path = newest_xlsx_in_dir(self.download_dir, since_ts=since_ts)
            if path and path.lower().endswith(".xlsx") and not path.lower().endswith(".crdownload"):
                return path
            sb.sleep(1)
        return None

    def _normalize_dir_path(self, path: str) -> str:
        value = str(path or "").strip()
        if not value:
            return ""
        with suppress(Exception):
            return os.path.normcase(os.path.abspath(value))
        return value.lower()

    def _download_dir_check_snapshot(self) -> Dict[str, Any]:
        browser_download_dir = str(self._browser_download_dir or self.download_dir or "").strip()
        watcher_dir = str(self.download_dir or "").strip()
        browser_norm = self._normalize_dir_path(browser_download_dir)
        watcher_norm = self._normalize_dir_path(watcher_dir)
        return {
            "browser_download_dir": browser_download_dir,
            "watcher_dir": watcher_dir,
            "match": bool(browser_norm and watcher_norm and browser_norm == watcher_norm),
        }

    def _scan_download_dir_since(self, since_ts: float, max_items: int = 40) -> Dict[str, Any]:
        files: List[tuple[float, str, str]] = []
        crdownload_seen = False
        detected_file_path = ""
        detected_file_mtime = 0.0
        detected_file_size = 0

        with suppress(Exception):
            for entry in os.scandir(self.download_dir):
                if not entry.is_file():
                    continue
                stat = entry.stat()
                mtime = float(getattr(stat, "st_mtime", 0.0) or 0.0)
                if mtime + 0.05 < float(since_ts or 0.0):
                    continue
                file_name = str(entry.name or "").strip()
                if not file_name:
                    continue

                files.append((mtime, file_name, str(entry.path or "")))
                lower_name = file_name.lower()
                if lower_name.endswith(".crdownload"):
                    crdownload_seen = True
                if lower_name.endswith(".xlsx") and not lower_name.endswith(".crdownload"):
                    if mtime >= detected_file_mtime:
                        detected_file_mtime = mtime
                        detected_file_path = str(entry.path or "")
                        detected_file_size = int(getattr(stat, "st_size", 0) or 0)

        files.sort(key=lambda item: item[0], reverse=True)
        observed_names = [name for _, name, _ in files[: max(1, int(max_items))]]
        return {
            "files_observed": observed_names,
            "crdownload_seen": crdownload_seen,
            "file_detected": bool(detected_file_path),
            "file_path": detected_file_path,
            "file_size": int(detected_file_size),
            "file_mtime": float(detected_file_mtime),
        }

    def _detect_browser_download_event_best_effort(self, sb, since_ts: float) -> bool:
        with suppress(Exception):
            perf_logs = sb.driver.get_log("performance")
            for item in reversed(perf_logs[-250:]):
                raw_ts = item.get("timestamp")
                with suppress(Exception):
                    ts_sec = float(raw_ts) / 1000.0
                    if ts_sec + 5 < float(since_ts or 0.0):
                        continue

                raw_message = str(item.get("message") or "")
                if not raw_message:
                    continue
                parsed = json.loads(raw_message)
                message = parsed.get("message") or {}
                method = str(message.get("method") or "")
                params = message.get("params") or {}

                if method in {"Browser.downloadWillBegin", "Page.downloadWillBegin"}:
                    return True

                request = params.get("request") or {}
                response = params.get("response") or {}
                candidate_url = str(
                    params.get("url")
                    or request.get("url")
                    or response.get("url")
                    or ""
                ).lower()
                mime_type = str(
                    response.get("mimeType")
                    or params.get("mimeType")
                    or ""
                ).lower()
                if ".xlsx" in candidate_url or ".crdownload" in candidate_url:
                    return True
                if "spreadsheetml" in mime_type or "application/vnd.ms-excel" in mime_type:
                    return True
        return False

    def _probe_download_start_signals(
        self,
        sb,
        *,
        since_ts: float,
        timeout_sec: float,
        poll_interval_sec: float = 0.5,
        break_on_started: bool = True,
    ) -> Dict[str, Any]:
        started_at = time.time()
        observed_names: List[str] = []
        observed_set: set[str] = set()
        crdownload_seen = False
        browser_download_event = False
        detected_file_path = ""
        completed_file_path = ""
        completed_file_size = 0
        file_signal_detected_at = ""
        stable_wait_sec = max(0.5, float(self.meta_config.get("download_file_stable_sec", 2.0)))
        stable_path = ""
        stable_last_size = -1
        stable_last_change_ts = started_at

        while time.time() - started_at <= max(0.0, float(timeout_sec or 0.0)):
            now_ts = time.time()
            scan = self._scan_download_dir_since(since_ts=since_ts, max_items=80)
            for name in list(scan.get("files_observed") or []):
                normalized = str(name or "").strip()
                if not normalized or normalized in observed_set:
                    continue
                observed_set.add(normalized)
                observed_names.append(normalized)
            if bool(scan.get("crdownload_seen")):
                crdownload_seen = True
                if not file_signal_detected_at:
                    file_signal_detected_at = datetime.utcnow().isoformat() + "Z"
            if bool(scan.get("file_detected")):
                candidate_path = str(scan.get("file_path") or "").strip()
                candidate_size = int(scan.get("file_size") or 0)
                candidate_mtime = float(scan.get("file_mtime") or 0.0)
                if candidate_path and not detected_file_path:
                    detected_file_path = candidate_path
                if candidate_path and not file_signal_detected_at:
                    file_signal_detected_at = datetime.utcnow().isoformat() + "Z"
                if candidate_path and candidate_size > 0:
                    # Short probe loops (e.g. 0.2s in view polling) reinitialize local stable state
                    # each call, so rely on file mtime as an additional completion heuristic.
                    if candidate_mtime > 0 and (now_ts - candidate_mtime) >= stable_wait_sec:
                        completed_file_path = candidate_path
                        completed_file_size = candidate_size
                        break
                    if stable_path != candidate_path:
                        stable_path = candidate_path
                        stable_last_size = candidate_size
                        stable_last_change_ts = now_ts
                    elif candidate_size != stable_last_size:
                        stable_last_size = candidate_size
                        stable_last_change_ts = now_ts
                    elif now_ts - stable_last_change_ts >= stable_wait_sec:
                        completed_file_path = candidate_path
                        completed_file_size = candidate_size
                        break
                else:
                    stable_path = candidate_path
                    stable_last_size = candidate_size
                    stable_last_change_ts = now_ts

            browser_download_event = (
                browser_download_event
                or self._detect_browser_download_event_best_effort(sb=sb, since_ts=since_ts)
            )
            if browser_download_event and not file_signal_detected_at:
                file_signal_detected_at = datetime.utcnow().isoformat() + "Z"

            download_started = bool(detected_file_path or crdownload_seen or browser_download_event)
            download_completed = bool(completed_file_path)
            if download_completed:
                break
            if download_started and break_on_started:
                break
            sb.sleep(max(0.2, float(poll_interval_sec or 0.2)))

        return {
            "download_started": bool(detected_file_path or crdownload_seen or browser_download_event),
            "download_completed": bool(completed_file_path),
            "file_detected": bool(detected_file_path),
            "file_path": completed_file_path or detected_file_path,
            "completed_file_path": completed_file_path,
            "completed_file_size": int(completed_file_size),
            "crdownload_seen": bool(crdownload_seen),
            "browser_download_event": bool(browser_download_event),
            "files_observed": observed_names[-80:],
            "file_signal_detected_at": file_signal_detected_at,
        }

    def _detect_permission_prompt_signal(self, sb) -> Dict[str, Any]:
        with suppress(Exception):
            result = sb.execute_script(
                """
                const visible = (el) => {
                  if (!el) return false;
                  const style = window.getComputedStyle(el);
                  if (style.display === 'none' || style.visibility === 'hidden') return false;
                  const rect = el.getBoundingClientRect();
                  return rect.width > 0 && rect.height > 0;
                };
                const compact = (text) => (text || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                const signalTokens = [
                  'download multiple files',
                  'multiple files',
                  'always allow',
                  'allow downloads',
                  'site settings',
                  'permission',
                  '\\ub2e4\\uc911 \\ud30c\\uc77c',
                  '\\ub2e4\\uc911 \\ub2e4\\uc6b4\\ub85c\\ub4dc',
                  '\\ud5c8\\uc6a9',
                  '\\uad8c\\ud55c'
                ];
                const buttonTokens = [
                  'allow',
                  'block',
                  'always allow',
                  'cancel',
                  'done',
                  '\\ud5c8\\uc6a9',
                  '\\ucc28\\ub2e8',
                  '\\uc644\\ub8cc',
                  '\\ucde8\\uc18c'
                ];
                const modalRoots = [
                  ...document.querySelectorAll('[role="dialog"], [aria-modal="true"], [data-testid*="dialog"]')
                ].filter((el) => visible(el));
                for (const root of modalRoots) {
                  const text = compact(root.innerText || '');
                  if (!text) continue;
                  const matchedSignal = signalTokens.find((token) => text.includes(token));
                  if (!matchedSignal) continue;
                  const buttons = [
                    ...root.querySelectorAll('button, [role="button"], input[type="button"], input[type="submit"]')
                  ];
                  const hasPermissionButton = buttons.some((btn) => {
                    const label = compact((btn.innerText || btn.value || btn.getAttribute('aria-label') || ''));
                    return buttonTokens.some((token) => label.includes(token));
                  });
                  if (!hasPermissionButton) continue;
                  return {
                    detected: true,
                    modal_text: (root.innerText || '').replace(/\\s+/g, ' ').trim().slice(0, 1000),
                    matched_token: matchedSignal
                  };
                }
                return { detected: false, modal_text: '', matched_token: '' };
                """
            )
            if isinstance(result, dict):
                return {
                    "detected": bool(result.get("detected")),
                    "modal_text": str(result.get("modal_text") or "").strip(),
                    "matched_token": str(result.get("matched_token") or "").strip(),
                }
        return {"detected": False, "modal_text": "", "matched_token": ""}

    def _classify_download_verify_failure(
        self,
        *,
        click_verified: bool,
        download_started: bool,
        file_detected: bool,
        crdownload_seen: bool,
        permission_prompt_detected: bool,
        download_dir_mismatch: bool,
    ) -> str:
        if permission_prompt_detected:
            return "permission_prompt_detected"
        if download_dir_mismatch and not download_started:
            return "download_dir_mismatch"
        if click_verified and not download_started:
            return "download_click_no_effect"
        if crdownload_seen and not file_detected:
            return "download_completion_timeout"
        if download_started and not file_detected:
            return "download_file_not_observed"
        return "download_file_not_observed"

    def _wait_for_user_manual_download_allow(self, sb, stage_tag: str, wait_sec: int) -> None:
        timeout = max(0, int(wait_sec))
        if timeout == 0:
            return
        self.logger.warning(
            "%s waiting_user_permission: In browser popup, choose "
            "\"Always allow https://adsmanager.facebook.com to download multiple files\" then click Done.",
            stage_tag,
        )
        start = time.time()
        tick = 0
        while time.time() - start <= timeout:
            tick += 1
            if tick % 5 == 0:
                self.logger.info("%s waiting_user_permission elapsed=%ss", stage_tag, int(time.time() - start))
            sb.sleep(1)

    def _extract_header_index_map(self, row_candidates: List[Any]) -> Dict[str, int]:
        for row_el in row_candidates[:4]:
            with suppress(Exception):
                cells = row_el.find_elements(
                    By.XPATH,
                    ".//*[self::th or @role='columnheader' or contains(@class,'header')]",
                )
                if not cells:
                    cells = row_el.find_elements(
                        By.XPATH,
                        "./*[self::th or @role='columnheader' or contains(@class,'header')]",
                    )
                if not cells:
                    continue

                out: Dict[str, int] = {}
                for idx, cell in enumerate(cells):
                    txt = re.sub(r"\s+", " ", str(getattr(cell, "text", "") or "")).strip()
                    key = self._header_text_to_key(txt)
                    if key and key not in out:
                        out[key] = idx
                if "name" in out and "date" in out and "status" in out:
                    return out
        return {}

    def _extract_row_cell_texts(self, row_el: Any) -> List[str]:
        out: List[str] = []
        cell_xpaths = [
            "./*[self::td or @role='gridcell' or @role='cell' or contains(@class,'cell')]",
            ".//*[self::td or @role='gridcell' or @role='cell' or contains(@class,'cell')]",
        ]
        seen_text: set[str] = set()
        for xp in cell_xpaths:
            with suppress(Exception):
                for cell in row_el.find_elements(By.XPATH, xp):
                    txt = re.sub(r"\s+", " ", str(getattr(cell, "text", "") or "")).strip()
                    if not txt:
                        continue
                    key = self._normalize_export_name_key(txt)
                    if key and key in seen_text:
                        continue
                    if key:
                        seen_text.add(key)
                    out.append(txt)
            if out:
                break
        return out

    def _extract_export_history_row(
        self,
        row_el: Any,
        row_index: int,
        header_idx: Optional[Dict[str, int]] = None,
    ) -> Optional[ExportHistoryRow]:
        row_text = re.sub(r"\s+", " ", str(getattr(row_el, "text", "") or "")).strip()
        if not row_text:
            return None
        if "export name" in row_text.lower() and "export date" in row_text.lower():
            return None

        checkbox_el: Any = None
        with suppress(Exception):
            boxes = row_el.find_elements(By.XPATH, ".//input[@type='checkbox'] | .//*[@role='checkbox']")
            if boxes:
                checkbox_el = boxes[0]

        cell_texts = self._extract_row_cell_texts(row_el)
        if not cell_texts:
            cell_texts = [row_text]

        idx_map = header_idx or {}
        name_raw = ""
        export_date_raw = ""
        status_raw = ""

        name_idx = idx_map.get("name")
        if name_idx is not None and name_idx < len(cell_texts):
            name_raw = cell_texts[name_idx]
        date_idx = idx_map.get("date")
        if date_idx is not None and date_idx < len(cell_texts):
            export_date_raw = cell_texts[date_idx]
        status_idx = idx_map.get("status")
        if status_idx is not None and status_idx < len(cell_texts):
            status_raw = cell_texts[status_idx]

        if not export_date_raw or self._parse_export_date_text(export_date_raw) is None:
            for txt in cell_texts:
                if not export_date_raw and self._parse_export_date_text(txt) is not None:
                    export_date_raw = txt

        if not status_raw:
            for txt in cell_texts:
                if self._normalize_export_status(txt) != "unknown":
                    status_raw = txt
                    break

        if not name_raw:
            for txt in cell_texts:
                if txt == export_date_raw or txt == status_raw:
                    continue
                if self._parse_export_date_text(txt) is not None:
                    continue
                if self._normalize_export_status(txt) != "unknown":
                    continue
                lower_txt = txt.lower()
                if lower_txt in {"report", "download", "delete"}:
                    continue
                if "expires" in lower_txt:
                    continue
                name_raw = txt
                break
        if not name_raw:
            name_raw = cell_texts[0]

        status_norm = self._normalize_export_status(status_raw or row_text)
        export_dt = self._parse_export_date_text(export_date_raw or row_text)
        return ExportHistoryRow(
            name_raw=name_raw,
            name_key=self._normalize_export_name_key(name_raw),
            status_norm=status_norm,
            export_dt=export_dt,
            row_index=row_index,
            checkbox_el=checkbox_el,
            row_el=row_el,
            export_date_raw=export_date_raw,
            status_raw=status_raw,
        )

    def _row_matches_variant_keys(self, row: ExportHistoryRow, variant_keys: set[str]) -> bool:
        if row.name_key and row.name_key in variant_keys:
            return True
        row_text_key = self._normalize_export_name_key(str(getattr(row.row_el, "text", "") or ""))
        return any(key and (key in row_text_key or row_text_key in key) for key in variant_keys)

    def _row_has_download_control(self, row: ExportHistoryRow) -> bool:
        if row.download_button_el is not None:
            return True
        if row.source_mode == "rv" and row.row_top_px is not None:
            return True
        if row.row_el is None:
            return False
        return self._row_has_download_button(row.row_el)

    def _collect_export_rows_with_stats_legacy(
        self,
        sb,
        variant_keys: set[str],
    ) -> tuple[List[ExportHistoryRow], ExportRowMatchStats]:
        stats = ExportRowMatchStats()
        out: List[ExportHistoryRow] = []
        row_candidates = self._collect_export_row_candidates(sb)
        stats.ui_visible_rows_count = len(row_candidates)
        header_idx = self._extract_header_index_map(row_candidates)
        row_order = 0
        for row_el in row_candidates:
            row = self._extract_export_history_row(
                row_el=row_el,
                row_index=row_order,
                header_idx=header_idx,
            )
            row_order += 1
            if not row:
                continue
            stats.parsed_rows_count += 1

            if self._row_matches_variant_keys(row=row, variant_keys=variant_keys):
                out.append(row)
                stats.name_matched_rows_count += 1
                if row.status_norm == "ready":
                    stats.ready_rows_count += 1
                if self._row_has_download_control(row):
                    stats.actionable_rows_count += 1

        return out, stats

    def _collect_export_rows_with_stats_react_virtualized(
        self,
        sb,
        variant_keys: set[str],
    ) -> tuple[List[ExportHistoryRow], ExportRowMatchStats]:
        rows = self._collect_export_rows_react_virtualized(sb=sb)
        stats = ExportRowMatchStats(
            ui_visible_rows_count=len(rows),
            parsed_rows_count=len(rows),
        )
        out: List[ExportHistoryRow] = []
        for row in rows:
            if self._row_matches_variant_keys(row=row, variant_keys=variant_keys):
                out.append(row)
                stats.name_matched_rows_count += 1
                if row.status_norm == "ready":
                    stats.ready_rows_count += 1
                if self._row_has_download_control(row):
                    stats.actionable_rows_count += 1
        return out, stats

    def _collect_export_rows_with_stats(
        self,
        sb,
        export_name: str,
    ) -> tuple[List[ExportHistoryRow], ExportRowMatchStats]:
        variants = self._export_history_name_variants(export_name)
        variant_keys = {self._normalize_export_name_key(v) for v in variants if self._normalize_export_name_key(v)}

        rv_rows, rv_stats = self._collect_export_rows_with_stats_react_virtualized(
            sb=sb,
            variant_keys=variant_keys,
        )
        if rv_stats.name_matched_rows_count > 0:
            return rv_rows, rv_stats

        legacy_rows, legacy_stats = self._collect_export_rows_with_stats_legacy(
            sb=sb,
            variant_keys=variant_keys,
        )
        if legacy_stats.name_matched_rows_count > 0:
            return legacy_rows, legacy_stats
        if rv_stats.parsed_rows_count > 0:
            return rv_rows, rv_stats
        return legacy_rows, legacy_stats

    def _build_row_match_stats_from_rows(self, rows: List[ExportHistoryRow]) -> ExportRowMatchStats:
        stats = ExportRowMatchStats(
            ui_visible_rows_count=len(rows),
            parsed_rows_count=len(rows),
            name_matched_rows_count=len(rows),
        )
        for row in rows:
            if row.status_norm == "ready":
                stats.ready_rows_count += 1
            if self._row_has_download_control(row):
                stats.actionable_rows_count += 1
        return stats

    def _format_row_stats(self, stats: ExportRowMatchStats) -> str:
        return (
            "ui_visible_rows_count={ui} parsed_rows_count={parsed} "
            "name_matched_rows_count={matched} ready_rows_count={ready} "
            "actionable_rows_count={actionable}"
        ).format(
            ui=stats.ui_visible_rows_count,
            parsed=stats.parsed_rows_count,
            matched=stats.name_matched_rows_count,
            ready=stats.ready_rows_count,
            actionable=stats.actionable_rows_count,
        )

    def _log_history_state(
        self,
        stage_tag: str,
        state: str,
        event: str,
        state_enter_ts: float,
        poll_count: int,
        search_query: str,
        reopen_count: int,
        stats: ExportRowMatchStats,
        top_candidate: Optional[ExportHistoryRow],
    ) -> None:
        elapsed_sec = int(max(0.0, time.time() - state_enter_ts))
        self.logger.info(
            "%s state=%s event=%s elapsed_sec=%s poll=%s search_query=%s reopen_count=%s matched_rows_count=%s %s top_candidate=%s",
            stage_tag,
            state,
            event,
            elapsed_sec,
            poll_count,
            search_query,
            reopen_count,
            stats.name_matched_rows_count,
            self._format_row_stats(stats),
            self._format_export_row_debug(top_candidate),
        )

    def _effective_history_total_timeout(
        self,
        row_appear_timeout_sec: int,
        processing_timeout_sec: int,
        row_disappear_grace_sec: int,
        reopen_cooldown_sec: int,
        max_reopen_attempts: int,
    ) -> int:
        configured = int(self.meta_config.get("history_download_wait_timeout_sec", 300))
        floor = (
            int(row_appear_timeout_sec)
            + int(processing_timeout_sec)
            + int(row_disappear_grace_sec)
            + int(reopen_cooldown_sec) * max(0, int(max_reopen_attempts))
            + 5
        )
        return max(configured, floor)

    def _collect_export_rows(self, sb, export_name: str) -> List[ExportHistoryRow]:
        rows, stats = self._collect_export_rows_with_stats(sb=sb, export_name=export_name)
        setattr(self, "_last_export_row_match_stats", stats)
        return rows

    def _dom_probe_row_controls(self, row_el: Any) -> List[Dict[str, str]]:
        control_xpaths = [
            ".//*[self::button or @role='button' or self::a]",
        ]
        out: List[Dict[str, str]] = []
        seen: set[str] = set()
        for xp in control_xpaths:
            with suppress(Exception):
                for el in row_el.find_elements(By.XPATH, xp):
                    tag = str(getattr(el, "tag_name", "") or "")
                    text = re.sub(r"\s+", " ", str(getattr(el, "text", "") or "")).strip()
                    role = str(el.get_attribute("role") or "")
                    aria = str(el.get_attribute("aria-label") or "")
                    title = str(el.get_attribute("title") or "")
                    data_testid = str(el.get_attribute("data-testid") or "")
                    cls = str(el.get_attribute("class") or "")
                    fp = f"{tag}|{role}|{aria}|{title}|{data_testid}|{cls}|{text}"
                    if fp in seen:
                        continue
                    seen.add(fp)
                    out.append(
                        {
                            "tag": tag,
                            "role": role,
                            "aria_label": aria,
                            "title": title,
                            "data_testid": data_testid,
                            "class": cls,
                            "text": text,
                        }
                    )
                    if len(out) >= 20:
                        return out
        return out

    def _capture_exports_dom_probe(
        self,
        sb,
        stage_tag: str,
        export_name: str,
        reason: str,
    ) -> Optional[str]:
        evidence_dir = os.path.join(self.download_dir, "_meta_evidence")
        with suppress(Exception):
            os.makedirs(evidence_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        probe_path = os.path.join(evidence_dir, f"DOM_PROBE_{stamp}.json")

        payload: Dict[str, Any] = {
            "stage_tag": stage_tag,
            "export_name": export_name,
            "reason": reason,
            "captured_at": stamp,
            "current_url": self._safe_current_url(sb),
            "frames": [],
            "react_virtualized_rows_count": 0,
            "react_virtualized_rows_sample": [],
            "table_roots": [],
            "fallback_rows_count": 0,
            "fallback_rows_sample": [],
        }

        with suppress(Exception):
            frames = sb.driver.find_elements(By.XPATH, "//iframe | //frame")
            for idx, frame in enumerate(frames):
                payload["frames"].append(
                    {
                        "index": idx,
                        "id": str(frame.get_attribute("id") or ""),
                        "name": str(frame.get_attribute("name") or ""),
                        "src": str(frame.get_attribute("src") or ""),
                        "title": str(frame.get_attribute("title") or ""),
                        "class": str(frame.get_attribute("class") or ""),
                    }
                )

        rv_rows = self._collect_export_rows_react_virtualized(sb=sb)
        payload["react_virtualized_rows_count"] = len(rv_rows)
        for row in rv_rows[:5]:
            payload["react_virtualized_rows_sample"].append(
                {
                    "name_raw": row.name_raw,
                    "status_norm": row.status_norm,
                    "export_date_raw": row.export_date_raw,
                    "row_top_px": row.row_top_px,
                    "row_index": row.row_index,
                    "source_mode": row.source_mode,
                    "has_download_control": self._row_has_download_control(row),
                }
            )

        roots = self._collect_exports_table_roots(sb)
        for root_idx, root in enumerate(roots[:3]):
            headers: List[str] = []
            with suppress(Exception):
                header_els = root.find_elements(
                    By.XPATH,
                    ".//*[self::th or @role='columnheader' or contains(@class,'header')]",
                )
                for h in header_els:
                    txt = re.sub(r"\s+", " ", str(getattr(h, "text", "") or "")).strip()
                    if txt:
                        headers.append(txt)

            rows = self._collect_rows_from_root(root)
            row_samples: List[Dict[str, Any]] = []
            for row in rows[:5]:
                row_txt = re.sub(r"\s+", " ", str(getattr(row, "text", "") or "")).strip()
                row_samples.append(
                    {
                        "row_text": row_txt,
                        "cells": self._extract_row_cell_texts(row)[:8],
                        "controls": self._dom_probe_row_controls(row)[:8],
                    }
                )
            payload["table_roots"].append(
                {
                    "root_index": root_idx,
                    "headers": headers[:10],
                    "row_count": len(rows),
                    "row_samples": row_samples,
                }
            )

        if not payload["table_roots"]:
            fallback_rows = self._collect_export_row_candidates(sb)
            payload["fallback_rows_count"] = len(fallback_rows)
            for row in fallback_rows[:5]:
                row_txt = re.sub(r"\s+", " ", str(getattr(row, "text", "") or "")).strip()
                payload["fallback_rows_sample"].append(
                    {
                        "row_text": row_txt,
                        "cells": self._extract_row_cell_texts(row)[:8],
                        "controls": self._dom_probe_row_controls(row)[:8],
                    }
                )

        with suppress(Exception):
            with open(probe_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            return probe_path
        return None

    def _select_latest_export_row(
        self,
        rows: List[ExportHistoryRow],
        request_ts: float,
    ) -> Optional[ExportHistoryRow]:
        if not rows:
            return None

        window_sec = int(self.meta_config.get("history_recent_window_sec", 21600))
        recent_rows: List[ExportHistoryRow] = []
        for row in rows:
            if row.export_dt is None:
                recent_rows.append(row)
                continue
            if row.export_dt + window_sec >= request_ts:
                recent_rows.append(row)

        candidates = recent_rows or rows
        candidates.sort(
            key=lambda r: (
                r.export_dt is None,
                -(r.export_dt or 0.0),
                r.row_index,
            )
        )
        return candidates[0]

    def _is_export_row_selected(self, row: ExportHistoryRow) -> bool:
        if row.checkbox_el is not None:
            with suppress(Exception):
                if (row.checkbox_el.get_attribute("aria-checked") or "").strip().lower() == "true":
                    return True
            with suppress(Exception):
                if (row.checkbox_el.tag_name or "").strip().lower() == "input" and row.checkbox_el.is_selected():
                    return True
            with suppress(Exception):
                if (row.checkbox_el.get_attribute("checked") or "").strip().lower() in ("true", "checked"):
                    return True

        with suppress(Exception):
            if (row.row_el.get_attribute("aria-selected") or "").strip().lower() == "true":
                return True
        with suppress(Exception):
            if "selected" in (row.row_el.get_attribute("class") or "").strip().lower():
                return True
        return False

    def _click_export_row_checkbox(self, sb, row: ExportHistoryRow) -> bool:
        if row.source_mode == "rv" and row.row_top_px is not None:
            if self._click_react_virtualized_row_control_by_top(sb=sb, row_top_px=row.row_top_px, mode="checkbox"):
                self.logger.info(
                    "[DOWNLOAD:HISTORY] row_checkbox_clicked source=rv row_index=%s row_top_px=%s row_name=%s",
                    int(row.row_index),
                    row.row_top_px,
                    str(row.name_raw or ""),
                )
                return True
        if row.row_el is None:
            return False

        checkbox_xpaths = [
            ".//input[@type='checkbox']/ancestor::label[1]",
            ".//*[@role='checkbox']",
            ".//input[@type='checkbox']",
        ]
        for _ in range(2):
            for xp in checkbox_xpaths:
                with suppress(Exception):
                    items = row.row_el.find_elements(By.XPATH, xp)
                    for item in items:
                        with suppress(Exception):
                            item.click()
                        if not self._is_export_row_selected(row):
                            with suppress(Exception):
                                sb.driver.execute_script("arguments[0].click();", item)
                        sb.sleep(0.2)
                        row.checkbox_el = item
                        if self._is_export_row_selected(row):
                            return True
            sb.sleep(0.2)
        # Some UIs do not expose a checkbox element; row click can still activate top download.
        with suppress(Exception):
            row.row_el.click()
            sb.sleep(0.2)
            return True
        with suppress(Exception):
            sb.driver.execute_script("arguments[0].click();", row.row_el)
            sb.sleep(0.2)
            return True
        return False

    def _click_top_exports_download_button(self, sb) -> bool:
        top_download_xpaths = [
            "(//button[not(ancestor::tr) and not(ancestor::*[@role='row']) and .//span[normalize-space()='Download']])[1]",
            "(//button[not(ancestor::tr) and not(ancestor::*[@role='row']) and normalize-space()='Download'])[1]",
            "(//button[not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')])[1]",
            "(//*[(@role='button' or self::button or self::a or self::div) and not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button_download')])[1]",
            "(//*[(@role='button' or self::button or self::a) and not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button') and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')])[1]",
            "(//*[ (self::button or @role='button' or self::a) and not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')])[1]",
            "(//*[ (self::button or @role='button' or self::a) and not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')])[1]",
            "(//*[(@role='button' or self::button or self::a) and not(ancestor::tr) and not(ancestor::*[@role='row']) and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')])[1]",
        ]
        if self._click_first_xpath(sb, top_download_xpaths, timeout=1.5):
            self.logger.info("[DOWNLOAD:HISTORY] top_download_button_clicked strategy=xpath")
            return True
        for xp in top_download_xpaths:
            with suppress(Exception):
                btn = sb.driver.find_element(By.XPATH, xp)
                if self._click_element_with_fallback(sb=sb, element=btn):
                    self.logger.info(
                        "[DOWNLOAD:HISTORY] top_download_button_clicked strategy=element snapshot=%s",
                        self._element_debug_snapshot(btn),
                    )
                    return True
        return bool(
            self._safe_click_any_text(sb, "Download", timeout=1.2)
            or self._safe_click_any_text(sb, "\ub2e4\uc6b4\ub85c\ub4dc", timeout=1.2)
        )

    def _click_row_download_button(self, sb, row: ExportHistoryRow) -> bool:
        btn = row.download_button_el
        if btn is not None and hasattr(btn, "click"):
            with suppress(Exception):
                btn.click()
                return True
            with suppress(Exception):
                sb.driver.execute_script("arguments[0].click();", btn)
                return True

        if row.source_mode == "rv" and row.row_top_px is not None:
            if self._click_react_virtualized_row_control_by_top(sb=sb, row_top_px=row.row_top_px, mode="download"):
                self.logger.info(
                    "[DOWNLOAD:HISTORY] row_download_button_clicked source=rv row_index=%s row_top_px=%s row_name=%s status=%s export_date=%s",
                    int(row.row_index),
                    row.row_top_px,
                    str(row.name_raw or ""),
                    str(row.status_norm or ""),
                    str(row.export_date_raw or ""),
                )
                return True

        if row.row_el is None:
            return False

        row_download_xpaths = [
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')]",
            ".//button[contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')]",
            ".//*[(@role='button' or self::button or self::a or self::div) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button_download')]",
            ".//*[(@role='button' or self::button or self::a) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button') and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')]",
            ".//*[(@role='button' or self::button or self::a) and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
            ".//*[self::button or @role='button' or self::a][contains(translate(@data-testid,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')]",
        ]
        for xp in row_download_xpaths:
            with suppress(Exception):
                buttons = row.row_el.find_elements(By.XPATH, xp)
                for btn in buttons:
                    if self._click_element_with_fallback(sb=sb, element=btn):
                        self.logger.info(
                            "[DOWNLOAD:HISTORY] row_download_button_clicked source=%s row_index=%s snapshot=%s",
                            str(row.source_mode or ""),
                            int(row.row_index),
                            self._element_debug_snapshot(btn),
                        )
                        return True
        return False

    def _click_download_in_exports_row(self, sb, export_name: str) -> bool:
        variants = self._export_history_name_variants(export_name)
        row_match = " or ".join(
            f"contains(normalize-space(.), {self._xpath_literal(v)})" for v in variants
        )
        row_xp = f"//*[self::tr or @role='row' or contains(@class,'row')][{row_match}]"
        legacy_row_download_xpaths = [
            f"({row_xp}//button[contains(normalize-space(.),'Download') or contains(normalize-space(.),'Downl')])[1]",
            f"({row_xp}//button[contains(normalize-space(.),'\ub2e4\uc6b4\ub85c\ub4dc')])[1]",
            f"({row_xp}//*[(@role='button' or self::button or self::a or self::div) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button_download')])[1]",
            f"({row_xp}//*[(@role='button' or self::button or self::a) and contains(translate(@data-surface,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'export_history_table_button') and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')])[1]",
            f"({row_xp}//*[(@role='button' or self::button or self::a) and contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'downl')])[1]",
            f"({row_xp}//*[self::button or @role='button' or self::a][contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')])[1]",
            f"({row_xp}//*[self::button or @role='button' or self::a][contains(translate(@title,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')])[1]",
            f"({row_xp}//*[self::button or @role='button' or self::a][contains(translate(@data-testid,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download')])[1]",
        ]
        if self._click_first_xpath(sb, legacy_row_download_xpaths, timeout=1.2):
            self.logger.info(
                "[DOWNLOAD:HISTORY] legacy_row_download_clicked strategy=xpath export=%s",
                export_name,
            )
            return True

        rows = self._collect_export_rows(sb=sb, export_name=export_name)
        latest = self._select_latest_export_row(rows=rows, request_ts=0.0)
        if not latest:
            return False
        return self._click_row_download_button(sb=sb, row=latest)

    def _download_export_from_history_with_bounded_polling(
        self,
        sb,
        ticket: ExportTicket,
        progress_event_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> str:
        stage_history = "[DOWNLOAD:HISTORY]"
        row_appear_timeout_sec = max(1, int(self.meta_config.get("history_row_appear_timeout_sec", 90)))
        processing_timeout_sec = max(1, int(self.meta_config.get("history_processing_timeout_sec", 480)))
        poll_initial_sec = max(3.0, float(self.meta_config.get("history_poll_initial_sec", 3.0)))
        poll_backoff_factor = float(self.meta_config.get("history_poll_backoff_factor", 1.7))
        poll_max_sec = max(5.0, float(self.meta_config.get("history_poll_max_sec", 20.0)))
        heartbeat_sec = max(1, int(self.meta_config.get("history_log_heartbeat_sec", 30)))
        row_disappear_grace_sec = max(0, int(self.meta_config.get("history_row_disappear_grace_sec", 60)))
        reopen_cooldown_sec = max(0, int(self.meta_config.get("history_reopen_cooldown_sec", 120)))
        max_reopen_attempts = max(0, int(self.meta_config.get("history_max_reopen_attempts", 2)))
        set_search_each_poll = bool(self.meta_config.get("history_set_search_each_poll", False))
        dom_settle_sec = max(0.0, float(self.meta_config.get("history_dom_settle_sec", 2.0)))
        readiness_recheck_sec = float(self.meta_config.get("history_ready_recheck_sec", 0.7))
        status_stagnation_refresh_sec = max(
            1.0, float(self.meta_config.get("history_status_stagnation_refresh_sec", 30.0))
        )
        timeout = self._effective_history_total_timeout(
            row_appear_timeout_sec=row_appear_timeout_sec,
            processing_timeout_sec=processing_timeout_sec,
            row_disappear_grace_sec=row_disappear_grace_sec,
            reopen_cooldown_sec=reopen_cooldown_sec,
            max_reopen_attempts=max_reopen_attempts,
        )
        manual_allow_window_sec = int(self.meta_config.get("manual_allow_window_sec", 45))
        manual_allow_retry_count = max(0, int(self.meta_config.get("manual_allow_retry_count", 2)))
        quick_probe_sec = max(1.0, float(self.meta_config.get("history_verify_quick_probe_sec", 3.0)))
        extended_wait_sec = max(3.0, float(self.meta_config.get("history_verify_extended_wait_sec", 15.0)))
        completion_wait_sec = max(3.0, float(self.meta_config.get("history_verify_completion_wait_sec", 20.0)))
        pre_click_probe_sec = max(
            2.0,
            float(self.meta_config.get("history_pre_click_probe_sec", 8.0)),
        )
        manual_allow_post_wait_sec = max(
            2.0,
            float(self.meta_config.get("manual_allow_post_wait_sec", 3.0)),
        )
        click_no_effect_retry_count = max(
            0,
            int(self.meta_config.get("history_click_no_effect_retry_count", 1)),
        )
        download_cooldown_sec = max(1.0, float(self.meta_config.get("download_cooldown_sec", 45.0)))
        stage_start_ts = time.time()
        state = "WAIT_ROW_APPEAR"
        state_enter_ts = stage_start_ts
        poll_count = 0
        reopen_count = 0
        row_disappear_reopen_count = 0
        initial_reopen_count = 0
        last_reopen_ts = 0.0
        last_heartbeat_log_ts = 0.0
        export_name = str(getattr(ticket, "export_name", "") or "")
        request_ts = float(getattr(ticket, "request_ts", 0.0) or 0.0)
        target_path = str(getattr(ticket, "target_path", "") or "")
        search_queries = [
            str(query or "").strip()
            for query in (getattr(ticket, "search_queries", None) or [])
            if str(query or "").strip()
        ]
        if not search_queries:
            search_queries = [export_name]
        search_query_idx = 0
        search_query = str(search_queries[search_query_idx] or "").strip() or export_name
        search_applied = False
        search_applied_query = ""
        disappear_since_ts: Optional[float] = None
        processing_start_ts: Optional[float] = None
        verify_click_ts: Optional[float] = None
        permission_retry_used = 0
        fail_exception: Optional[Exception] = None
        last_reason = "row_not_found_initial"
        last_rows: List[ExportHistoryRow] = []
        last_stats = ExportRowMatchStats()
        top_candidate: Optional[ExportHistoryRow] = None
        last_status_norm = "missing"
        last_status_change_ts = stage_start_ts
        last_ready_signal = ""
        current_poll_sec = poll_initial_sec
        click_verified = False
        download_started = False
        download_completed = False
        file_detected = False
        crdownload_seen = False
        browser_download_event_seen = False
        files_observed: List[str] = []
        permission_prompt_detected = False
        permission_modal_text = ""
        legacy_reason = ""
        fallback_trigger_reason = str(getattr(ticket, "fallback_trigger_reason", "") or "")
        file_signal_detected_at = str(getattr(ticket, "file_signal_detected_at", "") or "")
        fallback_cooldown_until_ts = 0.0
        previous_download_started = False
        click_no_effect_retry_used = 0
        download_dir_snapshot = self._download_dir_check_snapshot()
        download_dir_mismatch = not bool(download_dir_snapshot.get("match"))
        self.logger.info(
            "%s download_dir_check browser_download_dir=%s watcher_dir=%s match=%s",
            stage_history,
            str(download_dir_snapshot.get("browser_download_dir") or ""),
            str(download_dir_snapshot.get("watcher_dir") or ""),
            bool(download_dir_snapshot.get("match")),
        )
        if download_dir_mismatch:
            self.logger.warning(
                "%s download_dir_mismatch browser_download_dir=%s watcher_dir=%s",
                stage_history,
                str(download_dir_snapshot.get("browser_download_dir") or ""),
                str(download_dir_snapshot.get("watcher_dir") or ""),
            )

        def _refresh_rows() -> Optional[ExportHistoryRow]:
            nonlocal poll_count, last_rows, last_stats, top_candidate
            rows = self._collect_export_rows(sb=sb, export_name=search_query or export_name)
            last_rows = rows
            last_stats_obj = getattr(self, "_last_export_row_match_stats", None)
            if isinstance(last_stats_obj, ExportRowMatchStats):
                last_stats = last_stats_obj
            else:
                last_stats = self._build_row_match_stats_from_rows(rows)
            latest = self._select_latest_export_row(rows=rows, request_ts=request_ts)
            top_candidate = latest
            poll_count += 1
            return latest

        def _reset_poll_interval() -> None:
            nonlocal current_poll_sec
            current_poll_sec = poll_initial_sec

        def _sleep_with_backoff() -> None:
            nonlocal current_poll_sec
            sb.sleep(current_poll_sec)
            current_poll_sec = min(poll_max_sec, current_poll_sec * max(1.1, poll_backoff_factor))

        def _maybe_set_search(force: bool = False) -> None:
            nonlocal search_applied, search_applied_query
            if force or set_search_each_poll or (not search_applied) or (search_applied_query != search_query):
                self._set_exports_search_query(sb, search_query)
                search_applied = True
                search_applied_query = search_query

        def _should_heartbeat(now_ts: float) -> bool:
            return now_ts - last_heartbeat_log_ts >= heartbeat_sec

        def _open_exports_again(reason: str) -> bool:
            nonlocal reopen_count, row_disappear_reopen_count, initial_reopen_count
            nonlocal last_reopen_ts, state, state_enter_ts, search_applied, search_applied_query, disappear_since_ts
            url = str(getattr(ticket, "exports_url", "") or self._safe_current_url(sb) or "").strip()
            if not url:
                return False
            reopen_count += 1
            if reason == "row_disappeared":
                row_disappear_reopen_count += 1
            elif reason == "row_not_found_initial":
                initial_reopen_count += 1
            last_reopen_ts = time.time()
            self.logger.warning(
                "%s state=%s reopen_exports attempt_total=%s reason=%s url=%s "
                "initial_reopen_count=%s row_disappear_reopen_count=%s/%s",
                stage_history,
                state,
                reopen_count,
                reason,
                url,
                initial_reopen_count,
                row_disappear_reopen_count,
                max_reopen_attempts,
            )
            sb.open(url)
            sb.sleep(2)
            state = "WAIT_ROW_APPEAR"
            state_enter_ts = time.time()
            search_applied = False
            search_applied_query = ""
            disappear_since_ts = None
            _maybe_set_search(force=True)
            return True

        def _emit_waiting_snapshot(latest_row: Optional[ExportHistoryRow]) -> Dict[str, Any]:
            snapshot = self._build_row_poll_snapshot(sb=sb, row=latest_row, ticket=ticket)
            snapshot["state"] = state
            snapshot["attempt"] = poll_count
            snapshot["timestamp"] = datetime.utcnow().isoformat() + "Z"
            snapshot["elapsed_ms"] = int(max(0.0, time.time() - stage_start_ts) * 1000)
            snapshot["last_status_change_ms"] = int(
                max(0.0, time.time() - last_status_change_ts) * 1000
            )
            snapshot["download_started"] = bool(download_started)
            snapshot["download_completed"] = bool(download_completed)
            snapshot["cooldown_active"] = bool(time.time() < fallback_cooldown_until_ts)
            snapshot["decision"] = "WAIT"
            snapshot["view_toast_state"] = str(getattr(ticket, "view_toast_state", "none") or "none")
            snapshot["view_toast_progress_pct"] = getattr(ticket, "view_toast_progress_pct", None)
            snapshot["view_toast_text"] = str(getattr(ticket, "view_toast_text", "") or "")
            if last_ready_signal:
                snapshot["last_ready_signal"] = last_ready_signal
            self.logger.info(
                "%s poll_snapshot attempt=%s state=%s snapshot=%s",
                stage_history,
                poll_count,
                state,
                snapshot,
            )
            self._emit_export_progress_event(
                progress_event_cb,
                phase="waiting_ready",
                attempt=poll_count,
                snapshot=snapshot,
            )
            return snapshot

        while time.time() - stage_start_ts <= timeout:
            now_ts = time.time()
            interrupt_probe = self._probe_download_start_signals(
                sb=sb,
                since_ts=max(0.0, request_ts or stage_start_ts),
                timeout_sec=0.2,
                poll_interval_sec=0.2,
                break_on_started=False,
            )
            for name in list(interrupt_probe.get("files_observed") or []):
                normalized_name = str(name or "").strip()
                if normalized_name and normalized_name not in files_observed:
                    files_observed.append(normalized_name)
            files_observed = files_observed[-80:]

            if not file_signal_detected_at:
                file_signal_detected_at = str(interrupt_probe.get("file_signal_detected_at") or "").strip()
            download_started = download_started or bool(interrupt_probe.get("download_started"))
            download_completed = download_completed or bool(interrupt_probe.get("download_completed"))
            file_detected = file_detected or bool(interrupt_probe.get("file_detected"))
            crdownload_seen = crdownload_seen or bool(interrupt_probe.get("crdownload_seen"))
            browser_download_event_seen = (
                browser_download_event_seen or bool(interrupt_probe.get("browser_download_event"))
            )

            interrupt_file_path = str(
                interrupt_probe.get("completed_file_path") or interrupt_probe.get("file_path") or ""
            ).strip()
            if download_completed and interrupt_file_path:
                final_path = self._move_download_to_target(interrupt_file_path, target_path)
                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="downloaded",
                    attempt=poll_count,
                    snapshot={
                        "sheet_name": str(ticket.sheet_name or ""),
                        "sheet_key": str(ticket.sheet_key or ""),
                        "file_path": final_path,
                        "method": "history_interrupt",
                        "last_ready_signal": last_ready_signal,
                        "download_started": bool(download_started),
                        "download_completed": True,
                        "crdownload_seen": bool(crdownload_seen),
                        "click_verified": bool(click_verified),
                        "browser_download_event": bool(browser_download_event_seen),
                        "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
                        "file_signal_detected_at": file_signal_detected_at,
                        "fallback_trigger_reason": fallback_trigger_reason,
                    },
                )
                return final_path

            if download_started and not previous_download_started and not download_completed:
                fallback_cooldown_until_ts = max(
                    fallback_cooldown_until_ts,
                    now_ts + download_cooldown_sec,
                )
            cooldown_active = now_ts < fallback_cooldown_until_ts
            if cooldown_active:
                self.logger.info(
                    "%s poll_snapshot attempt=%s state=%s toast_state=%s toast_text=%s toast_progress_pct=%s "
                    "elapsed_ms=%s download_started=%s download_completed=%s cooldown_active=%s decision=%s",
                    stage_history,
                    poll_count,
                    "FALLBACK_COOLDOWN",
                    str(getattr(ticket, "view_toast_state", "none") or "none"),
                    str(getattr(ticket, "view_toast_text", "") or ""),
                    getattr(ticket, "view_toast_progress_pct", None),
                    int(max(0.0, now_ts - stage_start_ts) * 1000),
                    bool(download_started),
                    bool(download_completed),
                    True,
                    "WAIT",
                )
                previous_download_started = bool(download_started)
                sb.sleep(min(current_poll_sec, max(0.3, poll_initial_sec)))
                continue
            previous_download_started = bool(download_started)

            if state in {"WAIT_ROW_APPEAR", "WAIT_LATEST_READY"}:
                _maybe_set_search()
                latest_row = _refresh_rows()
                current_status_norm = str(getattr(latest_row, "status_norm", "missing") or "missing")
                if current_status_norm != last_status_norm:
                    last_status_norm = current_status_norm
                    last_status_change_ts = now_ts
                    _reset_poll_interval()

                try:
                    self._ensure_account_context_with_ticket(sb=sb, ticket=ticket)
                except AccountContextMismatchError as mismatch_exc:
                    fail_exception = mismatch_exc
                    last_reason = "account_context_mismatch"
                    self._emit_export_progress_event(
                        progress_event_cb,
                        phase="failed",
                        attempt=poll_count,
                        snapshot=dict(mismatch_exc.payload),
                        reason="account_context_mismatch",
                    )
                    break

                latest_snapshot = _emit_waiting_snapshot(latest_row)
                if _should_heartbeat(now_ts):
                    self._log_history_state(
                        stage_tag=stage_history,
                        state=state,
                        event="heartbeat",
                        state_enter_ts=state_enter_ts,
                        poll_count=poll_count,
                        search_query=search_query,
                        reopen_count=reopen_count,
                        stats=last_stats,
                        top_candidate=latest_row,
                    )
                    last_heartbeat_log_ts = now_ts

                if state == "WAIT_ROW_APPEAR":
                    if latest_row:
                        state = "WAIT_LATEST_READY"
                        state_enter_ts = now_ts
                        search_applied = set_search_each_poll
                        disappear_since_ts = None
                        self._log_history_state(
                            stage_tag=stage_history,
                            state=state,
                            event="row_found",
                            state_enter_ts=state_enter_ts,
                            poll_count=poll_count,
                            search_query=search_query,
                            reopen_count=reopen_count,
                            stats=last_stats,
                            top_candidate=latest_row,
                        )
                        continue

                    rotate_every_polls = max(1, int(self.meta_config.get("history_search_rotate_every_polls", 2)))
                    if len(search_queries) > 1 and (poll_count % rotate_every_polls == 0):
                        prev_query = search_query
                        search_query_idx = (search_query_idx + 1) % len(search_queries)
                        search_query = str(search_queries[search_query_idx] or "").strip() or prev_query
                        if search_query != prev_query:
                            self.logger.info(
                                "%s search_query_rotated prev=%s next=%s idx=%s/%s",
                                stage_history,
                                prev_query,
                                search_query,
                                search_query_idx + 1,
                                len(search_queries),
                            )
                            _maybe_set_search(force=True)

                    if now_ts - state_enter_ts >= row_appear_timeout_sec:
                        if initial_reopen_count < 1 and _open_exports_again("row_not_found_initial"):
                            continue
                        last_reason = "row_not_found_initial"
                        break

                    should_refresh = (poll_count % 3 == 0) or (
                        now_ts - last_status_change_ts > status_stagnation_refresh_sec
                    )
                    if should_refresh:
                        self.logger.info(
                            "%s state=%s event=refresh attempt=%s stagnation_sec=%s",
                            stage_history,
                            state,
                            poll_count,
                            int(max(0.0, now_ts - last_status_change_ts)),
                        )
                        sb.refresh()
                        if dom_settle_sec > 0:
                            sb.sleep(dom_settle_sec)
                        _maybe_set_search(force=True)
                        _reset_poll_interval()
                        continue

                    _sleep_with_backoff()
                    continue

                # WAIT_LATEST_READY
                if not latest_row:
                    if disappear_since_ts is None:
                        disappear_since_ts = now_ts
                        self.logger.warning(
                            "%s state=%s event=row_disappeared grace_sec=%s",
                            stage_history,
                            state,
                            row_disappear_grace_sec,
                        )
                    elif now_ts - disappear_since_ts >= row_disappear_grace_sec:
                        cooldown_left = reopen_cooldown_sec - max(0.0, now_ts - last_reopen_ts)
                        can_reopen = row_disappear_reopen_count < max_reopen_attempts and cooldown_left <= 0
                        if can_reopen and _open_exports_again("row_disappeared"):
                            continue
                        if row_disappear_reopen_count < max_reopen_attempts and cooldown_left > 0:
                            sb.sleep(min(current_poll_sec, max(1.0, cooldown_left)))
                            continue
                        last_reason = "row_disappeared"
                        break

                    should_refresh = (poll_count % 3 == 0) or (
                        now_ts - last_status_change_ts > status_stagnation_refresh_sec
                    )
                    if should_refresh:
                        self.logger.info(
                            "%s state=%s event=refresh attempt=%s stagnation_sec=%s",
                            stage_history,
                            state,
                            poll_count,
                            int(max(0.0, now_ts - last_status_change_ts)),
                        )
                        sb.refresh()
                        if dom_settle_sec > 0:
                            sb.sleep(dom_settle_sec)
                        _maybe_set_search(force=True)
                        _reset_poll_interval()
                        continue
                    _sleep_with_backoff()
                    continue

                disappear_since_ts = None

                if latest_row.status_norm == "failed":
                    last_reason = "latest_failed"
                    fail_exception = RuntimeError(
                        "latest_failed(status=failed,"
                        f"export={export_name},"
                        f"export_date={latest_row.export_date_raw or 'n/a'})"
                    )
                    break

                ready_candidate, _ = self._is_ready_snapshot(latest_snapshot)
                if ready_candidate:
                    sb.sleep(readiness_recheck_sec)
                    _maybe_set_search(force=True)
                    latest_row = _refresh_rows()
                    current_status_norm = str(getattr(latest_row, "status_norm", "missing") or "missing")
                    if current_status_norm != last_status_norm:
                        last_status_norm = current_status_norm
                        last_status_change_ts = time.time()
                        _reset_poll_interval()
                    recheck_snapshot = _emit_waiting_snapshot(latest_row)
                    ready_confirmed, ready_signal = self._is_ready_snapshot(recheck_snapshot)
                    if ready_confirmed:
                        recheck_snapshot["ready_signal"] = ready_signal
                        last_ready_signal = ready_signal
                        self._emit_export_progress_event(
                            progress_event_cb,
                            phase="ready",
                            attempt=poll_count,
                            snapshot=recheck_snapshot,
                        )
                        state = "DOWNLOAD_ATTEMPT"
                        state_enter_ts = now_ts
                        self._log_history_state(
                            stage_tag=stage_history,
                            state=state,
                            event="ready_detected",
                            state_enter_ts=state_enter_ts,
                            poll_count=poll_count,
                            search_query=search_query,
                            reopen_count=reopen_count,
                            stats=last_stats,
                            top_candidate=latest_row,
                        )
                        continue

                if processing_start_ts is None:
                    processing_start_ts = now_ts
                if now_ts - processing_start_ts >= processing_timeout_sec:
                    last_reason = "processing_timeout"
                    break

                should_refresh = (poll_count % 3 == 0) or (
                    now_ts - last_status_change_ts > status_stagnation_refresh_sec
                )
                if should_refresh:
                    self.logger.info(
                        "%s state=%s event=refresh attempt=%s stagnation_sec=%s",
                        stage_history,
                        state,
                        poll_count,
                        int(max(0.0, now_ts - last_status_change_ts)),
                    )
                    sb.refresh()
                    if dom_settle_sec > 0:
                        sb.sleep(dom_settle_sec)
                    _maybe_set_search(force=True)
                    _reset_poll_interval()
                    continue

                _sleep_with_backoff()
                continue

            if state == "DOWNLOAD_ATTEMPT":
                latest_row = top_candidate
                if not latest_row:
                    last_reason = "row_disappeared"
                    break

                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="downloading",
                    attempt=poll_count,
                    snapshot=self._build_row_poll_snapshot(sb=sb, row=latest_row, ticket=ticket),
                )
                # Some Meta report views auto-start download after ready toast.
                # Probe briefly before forcing a click to avoid missing pre-click downloads.
                pre_click_since_ts = float(
                    max(
                        0.0,
                        now_ts - max(2.0, pre_click_probe_sec + 1.0),
                    )
                )
                pre_click_probe = self._probe_download_start_signals(
                    sb=sb,
                    since_ts=pre_click_since_ts,
                    timeout_sec=pre_click_probe_sec,
                    poll_interval_sec=0.6,
                    break_on_started=False,
                )
                for name in list(pre_click_probe.get("files_observed") or []):
                    normalized_name = str(name or "").strip()
                    if normalized_name and normalized_name not in files_observed:
                        files_observed.append(normalized_name)
                files_observed = files_observed[-80:]
                if not file_signal_detected_at:
                    file_signal_detected_at = str(pre_click_probe.get("file_signal_detected_at") or "").strip()
                pre_click_file_path = str(pre_click_probe.get("completed_file_path") or "").strip()
                pre_click_started = bool(pre_click_probe.get("download_started"))
                if pre_click_file_path:
                    final_path = self._move_download_to_target(pre_click_file_path, target_path)
                    self._emit_export_progress_event(
                        progress_event_cb,
                        phase="downloaded",
                        attempt=poll_count,
                        snapshot={
                            "sheet_name": str(ticket.sheet_name or ""),
                            "sheet_key": str(ticket.sheet_key or ""),
                            "file_path": final_path,
                            "method": "history_auto_preclick",
                            "last_ready_signal": last_ready_signal,
                            "download_started": True,
                            "crdownload_seen": bool(pre_click_probe.get("crdownload_seen")),
                            "click_verified": False,
                            "browser_download_event": bool(pre_click_probe.get("browser_download_event")),
                            "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
                            "pre_click_auto_download": True,
                            "download_completed": True,
                            "file_signal_detected_at": file_signal_detected_at,
                            "fallback_trigger_reason": fallback_trigger_reason,
                        },
                    )
                    return final_path
                if pre_click_started:
                    self.logger.info(
                        "%s pre_click_download_started export=%s crdownload_seen=%s browser_event=%s",
                        stage_history,
                        export_name,
                        bool(pre_click_probe.get("crdownload_seen")),
                        bool(pre_click_probe.get("browser_download_event")),
                    )
                    verify_click_ts = pre_click_since_ts
                    click_verified = False
                    download_started = bool(pre_click_probe.get("download_started"))
                    download_completed = bool(pre_click_probe.get("download_completed"))
                    file_detected = bool(pre_click_probe.get("file_detected"))
                    crdownload_seen = bool(pre_click_probe.get("crdownload_seen"))
                    browser_download_event_seen = bool(pre_click_probe.get("browser_download_event"))
                    permission_prompt_detected = False
                    permission_modal_text = ""
                    state = "VERIFY_FILE"
                    state_enter_ts = time.time()
                    self._log_history_state(
                        stage_tag=stage_history,
                        state=state,
                        event="download_started_preclick",
                        state_enter_ts=state_enter_ts,
                        poll_count=poll_count,
                        search_query=search_query,
                        reopen_count=reopen_count,
                        stats=last_stats,
                        top_candidate=latest_row,
                    )
                    continue

                verify_click_ts = time.time()
                click_verified = False
                clicked = False
                clicked_strategy = ""
                if latest_row.source_mode == "rv":
                    self._click_export_row_checkbox(sb=sb, row=latest_row)
                    clicked = self._click_top_exports_download_button(sb=sb)
                    if clicked:
                        clicked_strategy = "top_download_button(rv_first)"
                if not clicked:
                    clicked = self._click_row_download_button(sb=sb, row=latest_row)
                    if clicked:
                        clicked_strategy = "row_download_button"
                if not clicked:
                    self._click_export_row_checkbox(sb=sb, row=latest_row)
                    clicked = self._click_top_exports_download_button(sb=sb)
                    if clicked:
                        clicked_strategy = "top_download_button(after_row)"
                if not clicked:
                    clicked = self._click_download_in_exports_row(sb=sb, export_name=export_name)
                    if clicked:
                        clicked_strategy = "legacy_row_download"
                if not clicked:
                    last_reason = "download_control_missing"
                    break
                self.logger.info(
                    "%s download_control_clicked strategy=%s row_source=%s row_name=%s row_top_px=%s",
                    stage_history,
                    clicked_strategy or "unknown",
                    str(latest_row.source_mode or ""),
                    str(latest_row.name_raw or ""),
                    latest_row.row_top_px,
                )
                click_verified = True
                download_started = False
                download_completed = False
                file_detected = False
                crdownload_seen = False
                browser_download_event_seen = False
                files_observed = []
                permission_prompt_detected = False
                permission_modal_text = ""

                state = "VERIFY_FILE"
                state_enter_ts = time.time()
                self._log_history_state(
                    stage_tag=stage_history,
                    state=state,
                    event="download_clicked",
                    state_enter_ts=state_enter_ts,
                    poll_count=poll_count,
                    search_query=search_query,
                    reopen_count=reopen_count,
                    stats=last_stats,
                    top_candidate=latest_row,
                )
                continue

            if state == "VERIFY_FILE":
                click_ts = float(verify_click_ts or time.time())
                observed_file_path = ""

                phase_1 = self._probe_download_start_signals(
                    sb=sb,
                    since_ts=click_ts,
                    timeout_sec=quick_probe_sec,
                    poll_interval_sec=0.5,
                )
                for name in list(phase_1.get("files_observed") or []):
                    normalized_name = str(name or "").strip()
                    if normalized_name and normalized_name not in files_observed:
                        files_observed.append(normalized_name)
                files_observed = files_observed[-80:]
                download_started = download_started or bool(phase_1.get("download_started"))
                download_completed = download_completed or bool(phase_1.get("download_completed"))
                file_detected = file_detected or bool(phase_1.get("file_detected"))
                crdownload_seen = crdownload_seen or bool(phase_1.get("crdownload_seen"))
                browser_download_event_seen = (
                    browser_download_event_seen or bool(phase_1.get("browser_download_event"))
                )
                if not file_signal_detected_at:
                    file_signal_detected_at = str(phase_1.get("file_signal_detected_at") or "").strip()
                observed_file_path = str(phase_1.get("completed_file_path") or "").strip()

                if not observed_file_path:
                    phase_2 = self._probe_download_start_signals(
                        sb=sb,
                        since_ts=click_ts,
                        timeout_sec=extended_wait_sec,
                        poll_interval_sec=1.0,
                        break_on_started=False,
                    )
                    for name in list(phase_2.get("files_observed") or []):
                        normalized_name = str(name or "").strip()
                        if normalized_name and normalized_name not in files_observed:
                            files_observed.append(normalized_name)
                    files_observed = files_observed[-80:]
                    download_started = download_started or bool(phase_2.get("download_started"))
                    download_completed = download_completed or bool(phase_2.get("download_completed"))
                    file_detected = file_detected or bool(phase_2.get("file_detected"))
                    crdownload_seen = crdownload_seen or bool(phase_2.get("crdownload_seen"))
                    browser_download_event_seen = (
                        browser_download_event_seen or bool(phase_2.get("browser_download_event"))
                    )
                    if not file_signal_detected_at:
                        file_signal_detected_at = str(phase_2.get("file_signal_detected_at") or "").strip()
                    observed_file_path = str(phase_2.get("completed_file_path") or "").strip()

                if observed_file_path:
                    final_path = self._move_download_to_target(observed_file_path, target_path)
                    self._emit_export_progress_event(
                        progress_event_cb,
                        phase="downloaded",
                        attempt=poll_count,
                        snapshot={
                            "sheet_name": str(ticket.sheet_name or ""),
                            "sheet_key": str(ticket.sheet_key or ""),
                            "file_path": final_path,
                            "method": "history",
                            "last_ready_signal": last_ready_signal,
                            "download_started": bool(download_started),
                            "crdownload_seen": bool(crdownload_seen),
                            "click_verified": bool(click_verified),
                            "browser_download_event": bool(browser_download_event_seen),
                            "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
                            "download_completed": True,
                            "file_signal_detected_at": file_signal_detected_at,
                            "fallback_trigger_reason": fallback_trigger_reason,
                        },
                    )
                    return final_path

                if crdownload_seen and not file_detected:
                    completion_path = self._wait_for_download_file_since(
                        sb=sb,
                        since_ts=click_ts,
                        timeout_sec=int(completion_wait_sec),
                    )
                    if completion_path:
                        final_path = self._move_download_to_target(completion_path, target_path)
                        self._emit_export_progress_event(
                            progress_event_cb,
                            phase="downloaded",
                            attempt=poll_count,
                            snapshot={
                                "sheet_name": str(ticket.sheet_name or ""),
                                "sheet_key": str(ticket.sheet_key or ""),
                                "file_path": final_path,
                                "method": "history",
                                "last_ready_signal": last_ready_signal,
                                "download_started": True,
                                "crdownload_seen": True,
                                "click_verified": bool(click_verified),
                                "browser_download_event": bool(browser_download_event_seen),
                                "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
                                "download_completed": True,
                                "file_signal_detected_at": file_signal_detected_at,
                                "fallback_trigger_reason": fallback_trigger_reason,
                            },
                        )
                        return final_path

                permission_probe = self._detect_permission_prompt_signal(sb=sb)
                permission_prompt_detected = bool(permission_probe.get("detected"))
                permission_modal_text = str(permission_probe.get("modal_text") or "").strip()

                if permission_prompt_detected and permission_retry_used < manual_allow_retry_count:
                    permission_retry_used += 1
                    self.logger.warning(
                        "%s permission_prompt_detected (retry=%s/%s) export=%s",
                        stage_history,
                        permission_retry_used,
                        manual_allow_retry_count,
                        export_name,
                    )
                    self._wait_for_user_manual_download_allow(
                        sb=sb,
                        stage_tag=stage_history,
                        wait_sec=manual_allow_window_sec,
                    )
                    retry_phase = self._probe_download_start_signals(
                        sb=sb,
                        since_ts=click_ts,
                        timeout_sec=manual_allow_post_wait_sec,
                        poll_interval_sec=0.6,
                        break_on_started=False,
                    )
                    for name in list(retry_phase.get("files_observed") or []):
                        normalized_name = str(name or "").strip()
                        if normalized_name and normalized_name not in files_observed:
                            files_observed.append(normalized_name)
                    files_observed = files_observed[-80:]
                    download_started = download_started or bool(retry_phase.get("download_started"))
                    download_completed = download_completed or bool(retry_phase.get("download_completed"))
                    file_detected = file_detected or bool(retry_phase.get("file_detected"))
                    crdownload_seen = crdownload_seen or bool(retry_phase.get("crdownload_seen"))
                    browser_download_event_seen = (
                        browser_download_event_seen or bool(retry_phase.get("browser_download_event"))
                    )
                    if not file_signal_detected_at:
                        file_signal_detected_at = str(retry_phase.get("file_signal_detected_at") or "").strip()
                    retry_file = str(retry_phase.get("completed_file_path") or "").strip()

                    if not retry_file and crdownload_seen and not file_detected:
                        retry_file = self._wait_for_download_file_since(
                            sb=sb,
                            since_ts=click_ts,
                            timeout_sec=int(max(2.0, manual_allow_post_wait_sec)),
                        ) or ""

                    if retry_file:
                        final_path = self._move_download_to_target(retry_file, target_path)
                        self._emit_export_progress_event(
                            progress_event_cb,
                            phase="downloaded",
                            attempt=poll_count,
                            snapshot={
                                "sheet_name": str(ticket.sheet_name or ""),
                                "sheet_key": str(ticket.sheet_key or ""),
                                "file_path": final_path,
                                "method": "history",
                                "permission_retry_used": permission_retry_used,
                                "last_ready_signal": last_ready_signal,
                                "download_started": bool(download_started),
                                "crdownload_seen": bool(crdownload_seen),
                                "click_verified": bool(click_verified),
                                "browser_download_event": bool(browser_download_event_seen),
                                "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
                                "download_completed": True,
                                "file_signal_detected_at": file_signal_detected_at,
                                "fallback_trigger_reason": fallback_trigger_reason,
                            },
                        )
                        return final_path

                    permission_probe = self._detect_permission_prompt_signal(sb=sb)
                    permission_prompt_detected = bool(permission_probe.get("detected"))
                    permission_modal_text = str(permission_probe.get("modal_text") or "").strip()

                last_reason = self._classify_download_verify_failure(
                    click_verified=click_verified,
                    download_started=download_started,
                    file_detected=file_detected,
                    crdownload_seen=crdownload_seen,
                    permission_prompt_detected=permission_prompt_detected,
                    download_dir_mismatch=download_dir_mismatch,
                )
                if last_reason == "permission_prompt_detected":
                    legacy_reason = "permission_block_suspected"
                if (
                    last_reason == "download_click_no_effect"
                    and click_no_effect_retry_used < click_no_effect_retry_count
                    and int(last_stats.actionable_rows_count) > 1
                ):
                    click_no_effect_retry_used += 1
                    self.logger.warning(
                        "%s click_no_effect_retry attempt=%s/%s export=%s actionable_rows=%s",
                        stage_history,
                        click_no_effect_retry_used,
                        click_no_effect_retry_count,
                        search_query or export_name,
                        int(last_stats.actionable_rows_count),
                    )
                    candidate_rows = self._collect_export_rows(sb=sb, export_name=search_query or export_name)
                    candidate_rows = sorted(
                        candidate_rows,
                        key=lambda r: (
                            r.export_dt is None,
                            -(r.export_dt or 0.0),
                            r.row_index,
                        ),
                    )

                    def _row_identity(row: ExportHistoryRow) -> tuple:
                        return (
                            str(row.name_key or ""),
                            str(row.status_norm or ""),
                            float(row.export_dt or 0.0),
                            int(row.row_index),
                            str(row.source_mode or ""),
                            float(row.row_top_px or -1.0),
                        )

                    current_identity = _row_identity(top_candidate) if top_candidate else None
                    retry_clicked = False
                    for candidate in candidate_rows:
                        if candidate.status_norm != "ready":
                            continue
                        if current_identity is not None and _row_identity(candidate) == current_identity:
                            continue
                        retry_clicked_strategy = ""
                        if candidate.source_mode == "rv":
                            self._click_export_row_checkbox(sb=sb, row=candidate)
                            retry_clicked = self._click_top_exports_download_button(sb=sb)
                            if retry_clicked:
                                retry_clicked_strategy = "top_download_button(rv_first)"
                        if not retry_clicked:
                            retry_clicked = self._click_row_download_button(sb=sb, row=candidate)
                            if retry_clicked:
                                retry_clicked_strategy = "row_download_button"
                        if not retry_clicked:
                            self._click_export_row_checkbox(sb=sb, row=candidate)
                            retry_clicked = self._click_top_exports_download_button(sb=sb)
                            if retry_clicked:
                                retry_clicked_strategy = "top_download_button(after_row)"
                        if not retry_clicked:
                            retry_clicked = self._click_download_in_exports_row(
                                sb=sb,
                                export_name=export_name,
                            )
                            if retry_clicked:
                                retry_clicked_strategy = "legacy_row_download"
                        if not retry_clicked:
                            continue
                        self.logger.info(
                            "%s download_control_clicked_retry strategy=%s row_source=%s row_name=%s row_top_px=%s",
                            stage_history,
                            retry_clicked_strategy or "unknown",
                            str(candidate.source_mode or ""),
                            str(candidate.name_raw or ""),
                            candidate.row_top_px,
                        )
                        top_candidate = candidate
                        verify_click_ts = time.time()
                        click_verified = True
                        download_started = False
                        download_completed = False
                        file_detected = False
                        crdownload_seen = False
                        browser_download_event_seen = False
                        files_observed = []
                        permission_prompt_detected = False
                        permission_modal_text = ""
                        state = "VERIFY_FILE"
                        state_enter_ts = time.time()
                        self._log_history_state(
                            stage_tag=stage_history,
                            state=state,
                            event="download_clicked_retry_alt_row",
                            state_enter_ts=state_enter_ts,
                            poll_count=poll_count,
                            search_query=search_query,
                            reopen_count=reopen_count,
                            stats=last_stats,
                            top_candidate=top_candidate,
                        )
                        break
                    if retry_clicked:
                        continue
                self.logger.warning(
                    "%s verify_file_failed reason=%s click_verified=%s download_started=%s "
                    "download_completed=%s file_detected=%s crdownload_seen=%s browser_download_event=%s "
                    "permission_prompt_detected=%s files_observed_count=%s",
                    stage_history,
                    last_reason,
                    click_verified,
                    download_started,
                    download_completed,
                    file_detected,
                    crdownload_seen,
                    browser_download_event_seen,
                    permission_prompt_detected,
                    len(files_observed),
                )
                break

        candidate_summary = self._format_export_row_debug(top_candidate)
        top_three_summary = " | ".join(
            self._format_export_row_debug(r)
            for r in sorted(
                last_rows,
                key=lambda r: (r.export_dt is None, -(r.export_dt or 0.0), r.row_index),
            )[:3]
        ) or "none"
        self.logger.error(
            "%s timeout export=%s last_reason=%s fallback_trigger_reason=%s matched_rows_count=%s %s "
            "top_candidate=%s top3=%s file_signal_detected_at=%s",
            stage_history,
            export_name,
            last_reason,
            fallback_trigger_reason,
            last_stats.name_matched_rows_count,
            self._format_row_stats(last_stats),
            candidate_summary,
            top_three_summary,
            file_signal_detected_at,
        )

        dom_probe_path = self._capture_exports_dom_probe(
            sb=sb,
            stage_tag=stage_history,
            export_name=export_name,
            reason=last_reason,
        )
        if dom_probe_path:
            self.logger.error("%s dom_probe_saved=%s", stage_history, dom_probe_path)

        evidence = self._capture_stage_failure_evidence(
            sb=sb,
            stage_tag=stage_history,
            brand=str(getattr(ticket, "brand", "") or "na"),
            report_name=str(getattr(ticket, "report_name", "") or export_name),
            reason=last_reason,
        )
        payload = self._history_failure_payload(
            ticket=ticket,
            state=state,
            reason=last_reason,
            search_query=search_query,
            reopen_count=reopen_count,
            stats=last_stats,
            top_candidate=top_candidate,
            top_three_summary=top_three_summary,
            current_url=evidence.get("current_url", ""),
            dom_probe_path=str(dom_probe_path or ""),
            screenshot_path=evidence.get("screenshot_path", ""),
            modal_text=permission_modal_text or evidence.get("modal_text", ""),
            stage_tag=stage_history,
            download_dir=str(download_dir_snapshot.get("watcher_dir") or ""),
            files_observed=files_observed,
            crdownload_seen=crdownload_seen,
            click_verified=click_verified,
            download_started=download_started,
            file_detected=file_detected,
            browser_download_event=browser_download_event_seen,
            download_dir_mismatch=download_dir_mismatch,
            last_ready_signal=last_ready_signal,
            permission_prompt_detected=permission_prompt_detected,
            legacy_reason=legacy_reason,
            download_completed=download_completed,
            view_toast_state=str(getattr(ticket, "view_toast_state", "none") or "none"),
            view_toast_progress_pct=getattr(ticket, "view_toast_progress_pct", None),
            view_toast_text=str(getattr(ticket, "view_toast_text", "") or ""),
            toast_transition_seen=bool(getattr(ticket, "toast_transition_seen", False)),
            toast_wait_elapsed_ms=int(getattr(ticket, "toast_wait_elapsed_ms", 0) or 0),
            file_signal_detected_at=file_signal_detected_at,
            fallback_trigger_reason=fallback_trigger_reason,
        )
        self._emit_history_failure_payload(payload)

        failure_snapshot = {
            "sheet_name": str(ticket.sheet_name or ""),
            "sheet_key": str(ticket.sheet_key or ""),
            "reason": last_reason,
            "state": state,
            "top_candidate": candidate_summary,
            "top3": top_three_summary,
            "last_ready_signal": last_ready_signal,
            "url": evidence.get("current_url", ""),
            "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
            "files_observed": files_observed,
            "crdownload_seen": bool(crdownload_seen),
            "click_verified": bool(click_verified),
            "download_started": bool(download_started),
            "download_completed": bool(download_completed),
            "file_detected": bool(file_detected),
            "browser_download_event": bool(browser_download_event_seen),
            "download_dir_mismatch": bool(download_dir_mismatch),
            "permission_prompt_detected": bool(permission_prompt_detected),
            "view_toast_state": str(getattr(ticket, "view_toast_state", "none") or "none"),
            "view_toast_progress_pct": getattr(ticket, "view_toast_progress_pct", None),
            "view_toast_text": str(getattr(ticket, "view_toast_text", "") or ""),
            "toast_transition_seen": bool(getattr(ticket, "toast_transition_seen", False)),
            "toast_wait_elapsed_ms": int(getattr(ticket, "toast_wait_elapsed_ms", 0) or 0),
            "file_signal_detected_at": file_signal_detected_at,
            "fallback_trigger_reason": fallback_trigger_reason,
            "accepted_export_name": str(getattr(ticket, "accepted_export_name", "") or ""),
            "search_queries": list(search_queries),
        }
        if legacy_reason:
            failure_snapshot["legacy_reason"] = legacy_reason
        if isinstance(fail_exception, AccountContextMismatchError):
            failure_snapshot.update(dict(fail_exception.payload or {}))
            self._emit_export_progress_event(
                progress_event_cb,
                phase="failed",
                attempt=poll_count,
                snapshot=failure_snapshot,
                reason="account_context_mismatch",
            )
        else:
            self._emit_export_progress_event(
                progress_event_cb,
                phase="failed",
                attempt=poll_count,
                snapshot=failure_snapshot,
                reason=last_reason,
            )

        if fail_exception is not None:
            raise fail_exception

        failure_details = {
            "reason": last_reason,
            "legacy_reason": legacy_reason,
            "state": state,
            "last_ready_signal": last_ready_signal,
            "download_dir": str(download_dir_snapshot.get("watcher_dir") or ""),
            "browser_download_dir": str(download_dir_snapshot.get("browser_download_dir") or ""),
            "download_dir_match": bool(download_dir_snapshot.get("match")),
            "files_observed": files_observed,
            "crdownload_seen": bool(crdownload_seen),
            "click_verified": bool(click_verified),
            "download_started": bool(download_started),
            "download_completed": bool(download_completed),
            "browser_download_event": bool(browser_download_event_seen),
            "permission_prompt_detected": bool(permission_prompt_detected),
            "view_toast_state": str(getattr(ticket, "view_toast_state", "none") or "none"),
            "view_toast_progress_pct": getattr(ticket, "view_toast_progress_pct", None),
            "view_toast_text": str(getattr(ticket, "view_toast_text", "") or ""),
            "toast_transition_seen": bool(getattr(ticket, "toast_transition_seen", False)),
            "toast_wait_elapsed_ms": int(getattr(ticket, "toast_wait_elapsed_ms", 0) or 0),
            "file_signal_detected_at": file_signal_detected_at,
            "fallback_trigger_reason": fallback_trigger_reason,
            "accepted_export_name": str(getattr(ticket, "accepted_export_name", "") or ""),
            "search_queries": list(search_queries),
            "dom_probe_path": str(dom_probe_path or ""),
            "screenshot_path": str(evidence.get("screenshot_path") or ""),
            "modal_text": permission_modal_text or str(evidence.get("modal_text") or ""),
            "url": str(evidence.get("current_url") or ""),
            "ui_visible_rows_count": int(last_stats.ui_visible_rows_count),
            "parsed_rows_count": int(last_stats.parsed_rows_count),
            "name_matched_rows_count": int(last_stats.name_matched_rows_count),
            "ready_rows_count": int(last_stats.ready_rows_count),
            "actionable_rows_count": int(last_stats.actionable_rows_count),
        }
        raise HistoryDownloadError(
            (
                f"History download timeout for export={export_name} "
                f"(last_reason={last_reason},"
                f"ui_visible_rows_count={last_stats.ui_visible_rows_count},"
                f"parsed_rows_count={last_stats.parsed_rows_count},"
                f"name_matched_rows_count={last_stats.name_matched_rows_count},"
                f"ready_rows_count={last_stats.ready_rows_count},"
                f"actionable_rows_count={last_stats.actionable_rows_count})"
            ),
            reason=last_reason,
            details=failure_details,
        )

    def _export_and_download_single_report(
        self,
        sb,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        report_name: str,
        sheet_name: str,
        report_id: str,
        ensure_name_export_context: Callable[[], None],
        allow_name_fallback: bool = True,
        sheet_key: str = "",
        progress_event_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> str:
        target_name = self._build_raw_target_name(
            brand_cfg=brand_cfg,
            activity_for_filename=activity_for_filename,
            yymmdd=yymmdd,
            sheet_name=sheet_name,
        )
        target_path = os.path.join(self.download_dir, target_name)
        export_name = os.path.splitext(target_name)[0]
        normalized_report_id = str(report_id or "").strip()
        export_result: Optional[ExportResult] = None

        self._emit_export_progress_event(
            progress_event_cb,
            phase="started",
            snapshot={
                "sheet_name": sheet_name,
                "sheet_key": sheet_key,
                "report_name": report_name,
                "report_id": normalized_report_id,
                "target_name": target_name,
            },
        )

        try:
            if bool(self.meta_config.get("use_report_id_direct_view", True)) and normalized_report_id:
                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="exporting",
                    snapshot={
                        "sheet_name": sheet_name,
                        "sheet_key": sheet_key,
                        "report_name": report_name,
                        "report_id": normalized_report_id,
                        "method": "URL",
                    },
                )
                try:
                    export_result = self._export_stage_via_report_id_url(
                        sb=sb,
                        brand_cfg=brand_cfg,
                        report_name=report_name,
                        report_id=normalized_report_id,
                        export_name=export_name,
                    )
                except Exception as e:
                    if not allow_name_fallback:
                        raise RuntimeError(
                            f"[EXPORT:URL] URL export failed with report_id-only mode: report={report_name} reason={e}"
                        ) from e
                    self.logger.warning("[EXPORT:URL] Fallback to name-direct export: report=%s reason=%s", report_name, e)
            else:
                if not bool(self.meta_config.get("use_report_id_direct_view", True)):
                    raise RuntimeError(
                        "[EXPORT:URL] use_report_id_direct_view=false is not allowed in report_id-only mode"
                    )
                if not normalized_report_id and not allow_name_fallback:
                    raise ValueError(f"[EXPORT:URL] Missing required report_id for report={report_name}")
                self.logger.info("[EXPORT:URL] Skip report_id export for report=%s (report_id missing or disabled)", report_name)

            if not export_result or not export_result.success:
                if not allow_name_fallback:
                    raise RuntimeError(
                        f"[EXPORT:URL] Export stage failed and name fallback disabled for report={report_name}"
                    )
                ensure_name_export_context()
                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="exporting",
                    snapshot={
                        "sheet_name": sheet_name,
                        "sheet_key": sheet_key,
                        "report_name": report_name,
                        "method": "FALLBACK_NAME",
                    },
                )
                export_result = self._export_stage_via_report_name(
                    sb=sb,
                    brand_cfg=brand_cfg,
                    report_name=report_name,
                    export_name=export_name,
                )

            self._emit_export_progress_event(
                progress_event_cb,
                phase="waiting_ready",
                snapshot={
                    "sheet_name": sheet_name,
                    "sheet_key": sheet_key,
                    "report_name": report_name,
                },
            )
            download_result = self._download_stage_for_export(
                sb=sb,
                brand_cfg=brand_cfg,
                report_name=report_name,
                export_result=export_result,
                target_path=target_path,
                sheet_name=sheet_name,
                sheet_key=sheet_key,
                progress_event_cb=progress_event_cb,
            )
            return download_result.file_path
        except Exception as exc:
            reason = str(exc or "").strip() or "export_failed"
            if isinstance(exc, AccountContextMismatchError):
                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="failed",
                    snapshot=dict(exc.payload or {}),
                    reason="account_context_mismatch",
                )
            else:
                self._emit_export_progress_event(
                    progress_event_cb,
                    phase="failed",
                    snapshot={
                        "sheet_name": sheet_name,
                        "sheet_key": sheet_key,
                        "report_name": report_name,
                        "error": reason[:500],
                    },
                    reason=reason[:200],
                )
            raise

    def _export_report_via_view_id(
        self,
        sb,
        brand_cfg: Dict,
        report_name: str,
        report_id: str,
        activity_for_filename: str,
        yymmdd: str,
        report_to_sheet: Dict[str, str],
        sheet_key: str = "",
        progress_event_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> str:
        if report_name not in report_to_sheet:
            raise ValueError(f"No report->sheet mapping configured for report '{report_name}'")
        mapped_sheet_name = report_to_sheet[report_name]
        resolved_sheet_key = (
            str(sheet_key or "").strip()
            or self._normalize_export_name_key(mapped_sheet_name)
            .replace(" ", "_")
            .replace("-", "_")
        )
        return self._export_and_download_single_report(
            sb=sb,
            brand_cfg=brand_cfg,
            activity_for_filename=activity_for_filename,
            yymmdd=yymmdd,
            report_name=report_name,
            sheet_name=mapped_sheet_name,
            report_id=report_id,
            ensure_name_export_context=lambda: None,
            allow_name_fallback=False,
            sheet_key=resolved_sheet_key,
            progress_event_cb=progress_event_cb,
        )

    def _export_reports_via_history_flow(
        self,
        sb,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        report_to_sheet: Dict[str, str],
        report_names: List[str],
    ) -> Dict[str, str]:
        out: Dict[str, str] = {}
        self._open_reports_tab(sb)
        self._select_reports_checkboxes(sb, report_names)
        self._click_export_from_reports(sb)
        self._configure_export_modal(sb)
        sb.sleep(2)

        self._open_exports_tab(sb)
        for report_name in report_names:
            self._wait_export_ready(sb, report_name)
            src_path = self._download_ready_export(sb, report_name)
            sheet_name = report_to_sheet[report_name]
            target_name = format_name(
                pattern=self.naming_config["raw_file_name_pattern"],
                brand=brand_cfg["brand_ko"],
                activity=activity_for_filename,
                yymmdd=yymmdd,
                sheet=sheet_name,
            )
            target_path = os.path.join(self.download_dir, target_name)
            if os.path.abspath(src_path) != os.path.abspath(target_path):
                os.replace(src_path, target_path)
            out[report_name] = target_path
            self.logger.info("Saved raw report (history flow): %s", target_path)

        return out

    def _resolve_brand_act_id(self, brand_cfg: Dict, require: bool = False) -> str:
        canonical = (brand_cfg.get("meta_act_id") or "").strip()
        alias = (brand_cfg.get("meta_ad_account_id") or "").strip()
        brand = str(brand_cfg.get("brand_ko") or "(unknown)").strip()
        if canonical and alias and canonical != alias:
            raise ValueError(
                f"[{brand}] Act ID mismatch: meta_act_id={canonical}, meta_ad_account_id(alias)={alias}"
            )
        resolved = canonical or alias
        if require and not resolved:
            raise ValueError(
                f"[{brand}] Missing act id. Provide meta_act_id (canonical) or meta_ad_account_id (alias)."
            )
        return resolved

    def _brand_ids(self, brand_cfg: Dict) -> tuple[str, str]:
        ad_account_id = self._resolve_brand_act_id(brand_cfg, require=False)
        business_id = (brand_cfg.get("meta_business_id") or "").strip()
        return ad_account_id, business_id

    def _resolve_global_scope_id(self, brand_cfg: Dict, business_id: str) -> str:
        configured = (brand_cfg.get("meta_global_scope_id") or "").strip()
        return configured or str(business_id or "").strip()

    def _is_id_direct_url_enabled_for_brand(self, brand_cfg: Dict) -> bool:
        if not bool(self.meta_config.get("use_id_direct_url", True)):
            return False
        ad_account_id, business_id = self._brand_ids(brand_cfg)
        return bool(ad_account_id and business_id)

    def _is_expected_id_url_context(self, current_url: str, brand_cfg: Dict) -> bool:
        ad_account_id, business_id = self._brand_ids(brand_cfg)
        if not ad_account_id or not business_id:
            return False
        return (f"act={ad_account_id}" in current_url) and (f"business_id={business_id}" in current_url)

    def _resolve_reporting_url(self, brand_cfg: Dict) -> str:
        # ID-based fast path is more stable than UI account picker.
        if not bool(self.meta_config.get("use_id_direct_url", True)):
            return self.meta_config["reporting_url"]

        ad_account_id, business_id = self._brand_ids(brand_cfg)
        if ad_account_id and business_id:
            global_scope_id = self._resolve_global_scope_id(brand_cfg, business_id)
            params = [
                f"act={ad_account_id}",
                "ads_manager_write_regions=true",
                f"business_id={business_id}",
                f"global_scope_id={global_scope_id}",
            ]
            return "https://adsmanager.facebook.com/adsmanager/reporting/manage?" + "&".join(params)

        if bool(self.meta_config.get("require_id_direct_url", False)):
            brand = brand_cfg.get("brand_ko", "(unknown)")
            raise ValueError(
                f"[{brand}] Missing act/business id while require_id_direct_url=true "
                "(act from meta_act_id or meta_ad_account_id(alias), business from meta_business_id)"
            )
        return self.meta_config["reporting_url"]

    def _enable_download_behavior(self, sb) -> None:
        # Best effort: reduce browser prompts (including repeated download permission prompts).
        configured = False
        with suppress(Exception):
            sb.driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": self.download_dir},
            )
            configured = True
        with suppress(Exception):
            sb.driver.execute_cdp_cmd(
                "Browser.setDownloadBehavior",
                {"behavior": "allow", "downloadPath": self.download_dir, "eventsEnabled": True},
            )
            configured = True
        self._download_behavior_configured = bool(configured)
        self._browser_download_dir = str(self.download_dir or "") if configured else ""
        if not configured:
            self.logger.warning("Download behavior could not be configured via CDP; browser dir is unknown.")

    def _build_sb_kwargs(self, browser: str) -> Dict:
        browser_name = (browser or "edge").strip().lower()
        if browser_name not in {"edge", "chrome"}:
            raise ValueError(f"Unsupported browser for META export: {browser_name}")

        sb_kwargs = {
            "browser": browser_name,
            "headless": self.headless,
            "locale_code": "ko",
        }
        if browser_name == "chrome":
            sb_kwargs["uc"] = True
            sb_kwargs["uc_subprocess"] = True
        return sb_kwargs

    def _export_brand_reports_with_session(
        self,
        sb,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        report_to_sheet: Dict[str, str],
        report_names: List[str],
        report_id_by_name: Optional[Dict[str, str]] = None,
        allow_name_fallback: bool = True,
    ) -> Dict[str, str]:
        out: Dict[str, str] = {}

        normalized_report_ids = {
            str(k): str(v or "").strip() for k, v in (report_id_by_name or {}).items()
        }

        name_export_context_ready = False

        def ensure_name_export_context() -> None:
            nonlocal name_export_context_ready
            if name_export_context_ready:
                return
            self._prepare_name_export_context(sb=sb, brand_cfg=brand_cfg)
            name_export_context_ready = True

        for report_name in report_names:
            if report_name not in report_to_sheet:
                raise ValueError(f"No report->sheet mapping configured for report '{report_name}'")
            sheet_name = report_to_sheet[report_name]
            report_id = normalized_report_ids.get(report_name, "")
            out[report_name] = self._export_and_download_single_report(
                sb=sb,
                brand_cfg=brand_cfg,
                activity_for_filename=activity_for_filename,
                yymmdd=yymmdd,
                report_name=report_name,
                sheet_name=sheet_name,
                report_id=report_id,
                ensure_name_export_context=ensure_name_export_context,
                allow_name_fallback=allow_name_fallback,
                sheet_key=self._normalize_export_name_key(sheet_name).replace(" ", "_").replace("-", "_"),
            )
            self.logger.info("Saved raw report: report=%s path=%s", report_name, out[report_name])

        return out

    def _build_failed_brand_export_result(
        self,
        report_to_sheet: Dict[str, str],
        reason: str,
    ) -> MetaBrandExportResult:
        sheet_status = self._init_sheet_status(report_to_sheet)
        for sheet_name, status in sheet_status.items():
            status["error_reason"] = self._merge_error_reason(str(status.get("error_reason") or ""), reason)
            self.logger.warning("[META][%s] %s", sheet_name, reason)
        return MetaBrandExportResult(raw_files={}, sheet_status=sheet_status, warnings=[reason])

    def _export_brand_reports_batch_with_session(
        self,
        sb,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        report_to_sheet: Dict[str, str],
        report_id_by_name: Optional[Dict[str, str]] = None,
    ) -> MetaBrandExportResult:
        stage_export = "[EXPORT:URL]"
        stage_history = "[DOWNLOAD:HISTORY]"

        raw_files: Dict[str, str] = {}
        warnings: List[str] = []
        sheet_status = self._init_sheet_status(report_to_sheet)
        normalized_report_ids = {
            str(k): str(v or "").strip() for k, v in (report_id_by_name or {}).items()
        }
        tickets: List[ExportTicket] = []

        for report_name, sheet_name in self._sorted_report_items(report_to_sheet):
            status = sheet_status.setdefault(
                sheet_name,
                {"report_name": report_name, "export_ok": False, "download_ok": False, "error_reason": ""},
            )
            report_id = normalized_report_ids.get(report_name, "")
            if not report_id:
                reason = f"{stage_export} Missing report_id for sheet={sheet_name}"
                status["error_reason"] = self._merge_error_reason(str(status.get("error_reason") or ""), reason)
                warnings.append(reason)
                continue

            target_name = self._build_raw_target_name(
                brand_cfg=brand_cfg,
                activity_for_filename=activity_for_filename,
                yymmdd=yymmdd,
                sheet_name=sheet_name,
            )
            target_path = os.path.join(self.download_dir, target_name)
            export_name = os.path.splitext(target_name)[0]

            try:
                export_result = self._export_stage_via_report_id_url(
                    sb=sb,
                    brand_cfg=brand_cfg,
                    report_name=report_name,
                    report_id=report_id,
                    export_name=export_name,
                )
                status["export_ok"] = True
                tickets.append(
                    ExportTicket(
                        sheet_name=sheet_name,
                        report_name=report_name,
                        report_id=report_id,
                        export_name=export_result.export_name,
                        request_ts=export_result.request_ts,
                        target_path=target_path,
                        brand=str(brand_cfg.get("brand_ko") or ""),
                        sheet_key=self._normalize_export_name_key(sheet_name)
                        .replace(" ", "_")
                        .replace("-", "_"),
                        expected_account_id=self._brand_ids(brand_cfg)[0],
                        expected_business_id=self._brand_ids(brand_cfg)[1],
                    )
                )
            except Exception as e:
                reason = f"{stage_export} {e}"
                status["error_reason"] = self._merge_error_reason(str(status.get("error_reason") or ""), reason)
                warnings.append(reason)

        if not tickets:
            return MetaBrandExportResult(raw_files=raw_files, sheet_status=sheet_status, warnings=warnings)

        exports_url = self._build_exports_url(brand_cfg)
        for ticket in tickets:
            ticket.exports_url = exports_url
        try:
            self.logger.info("%s Open export history url=%s", stage_history, exports_url)
            sb.open(exports_url)
            sb.sleep(2)
        except Exception as e:
            reason_text = self._normalized_stage_error_reason(e)
            reason = f"{stage_history} Could not open exports page: {reason_text}"
            warnings.append(reason)
            for ticket in tickets:
                status = sheet_status.get(ticket.sheet_name, {})
                status["error_reason"] = self._merge_error_reason(str(status.get("error_reason") or ""), reason)
                self._capture_stage_failure_evidence(
                    sb=sb,
                    stage_tag=stage_history,
                    brand=brand_cfg["brand_ko"],
                    report_name=ticket.report_name,
                    reason=reason_text,
                )
            return MetaBrandExportResult(raw_files=raw_files, sheet_status=sheet_status, warnings=warnings)

        for ticket in sorted(tickets, key=self._download_ticket_sort_key):
            status = sheet_status.get(ticket.sheet_name, {})
            try:
                final_path = self._download_export_from_history_with_bounded_polling(
                    sb=sb,
                    ticket=ticket,
                )
                raw_files[ticket.report_name] = final_path
                status["download_ok"] = True
                self.logger.info(
                    "%s Download success: brand=%s sheet=%s path=%s",
                    stage_history,
                    brand_cfg["brand_ko"],
                    ticket.sheet_name,
                    final_path,
                )
            except Exception as e:
                reason_text = self._normalized_stage_error_reason(e)
                reason = f"{stage_history} {reason_text}"
                status["error_reason"] = self._merge_error_reason(str(status.get("error_reason") or ""), reason)
                warnings.append(reason)
                self._capture_stage_failure_evidence(
                    sb=sb,
                    stage_tag=stage_history,
                    brand=brand_cfg["brand_ko"],
                    report_name=ticket.report_name,
                    reason=reason_text,
                )

        return MetaBrandExportResult(raw_files=raw_files, sheet_status=sheet_status, warnings=warnings)

    def export_multi_brand_reports_detailed(
        self,
        brand_cfgs: List[Dict],
        activity_by_brand: Dict[str, str],
        yymmdd: str,
        brand_report_to_sheet_by_brand: Dict[str, Dict[str, str]],
        brand_report_id_by_brand: Optional[Dict[str, Dict[str, str]]] = None,
        browser: str = "edge",
        enforce_report_id_only: bool = False,
    ) -> Dict[str, MetaBrandExportResult]:
        try:
            from seleniumbase import SB
        except Exception as e:
            raise RuntimeError("seleniumbase is required for META automation") from e

        os.makedirs(self.download_dir, exist_ok=True)
        sb_kwargs = self._build_sb_kwargs(browser)
        out_by_brand: Dict[str, MetaBrandExportResult] = {}

        with SB(**sb_kwargs) as sb:
            self._enable_download_behavior(sb)
            self.logger.info("Open META home for login (multi-brand detailed)")
            sb.open(self.meta_config["home_url"])
            self._wait_for_meta_login(sb)

            for brand_cfg in brand_cfgs:
                brand = brand_cfg["brand_ko"]
                report_to_sheet = brand_report_to_sheet_by_brand.get(brand, {})
                if brand not in activity_by_brand:
                    out_by_brand[brand] = self._build_failed_brand_export_result(
                        report_to_sheet=report_to_sheet,
                        reason=f"Missing activity mapping for brand={brand}",
                    )
                    continue
                if not report_to_sheet:
                    out_by_brand[brand] = self._build_failed_brand_export_result(
                        report_to_sheet={},
                        reason=f"Missing report mapping for brand={brand}",
                    )
                    continue

                try:
                    if enforce_report_id_only:
                        out_by_brand[brand] = self._export_brand_reports_batch_with_session(
                            sb=sb,
                            brand_cfg=brand_cfg,
                            activity_for_filename=activity_by_brand[brand],
                            yymmdd=yymmdd,
                            report_to_sheet=report_to_sheet,
                            report_id_by_name=(brand_report_id_by_brand or {}).get(brand, {}),
                        )
                    else:
                        raw_files = self._export_brand_reports_with_session(
                            sb=sb,
                            brand_cfg=brand_cfg,
                            activity_for_filename=activity_by_brand[brand],
                            yymmdd=yymmdd,
                            report_to_sheet=report_to_sheet,
                            report_names=list(report_to_sheet.keys()),
                            report_id_by_name=(brand_report_id_by_brand or {}).get(brand, {}),
                            allow_name_fallback=True,
                        )
                        sheet_status = self._init_sheet_status(report_to_sheet)
                        for report_name, sheet_name in report_to_sheet.items():
                            status = sheet_status.setdefault(
                                sheet_name,
                                {"report_name": report_name, "export_ok": False, "download_ok": False, "error_reason": ""},
                            )
                            if report_name in raw_files:
                                status["export_ok"] = True
                                status["download_ok"] = True
                        out_by_brand[brand] = MetaBrandExportResult(
                            raw_files=raw_files,
                            sheet_status=sheet_status,
                            warnings=[],
                        )
                except Exception as e:
                    out_by_brand[brand] = self._build_failed_brand_export_result(
                        report_to_sheet=report_to_sheet,
                        reason=str(e),
                    )

        return out_by_brand

    def export_multi_brand_reports(
        self,
        brand_cfgs: List[Dict],
        activity_by_brand: Dict[str, str],
        yymmdd: str,
        brand_report_to_sheet_by_brand: Dict[str, Dict[str, str]],
        brand_report_id_by_brand: Optional[Dict[str, Dict[str, str]]] = None,
        browser: str = "edge",
        enforce_report_id_only: bool = False,
    ) -> Dict[str, Dict[str, str]]:
        try:
            from seleniumbase import SB
        except Exception as e:
            raise RuntimeError("seleniumbase is required for META automation") from e

        os.makedirs(self.download_dir, exist_ok=True)

        sb_kwargs = self._build_sb_kwargs(browser)
        out_by_brand: Dict[str, Dict[str, str]] = {}
        with SB(**sb_kwargs) as sb:
            self._enable_download_behavior(sb)
            self.logger.info("Open META home for login (multi-brand)")
            sb.open(self.meta_config["home_url"])
            self._wait_for_meta_login(sb)
            for brand_cfg in brand_cfgs:
                brand = brand_cfg["brand_ko"]
                if brand not in activity_by_brand:
                    raise ValueError(f"Missing activity mapping for brand: {brand}")
                report_to_sheet = brand_report_to_sheet_by_brand.get(brand, {})
                if not report_to_sheet:
                    raise ValueError(f"Missing report mapping for brand: {brand}")
                report_names = list(report_to_sheet.keys())
                if not report_names:
                    raise ValueError(f"At least one report must be mapped for brand: {brand}")
                report_id_by_name = (brand_report_id_by_brand or {}).get(brand, {})
                out_by_brand[brand] = self._export_brand_reports_with_session(
                    sb=sb,
                    brand_cfg=brand_cfg,
                    activity_for_filename=activity_by_brand[brand],
                    yymmdd=yymmdd,
                    report_to_sheet=report_to_sheet,
                    report_names=report_names,
                    report_id_by_name=report_id_by_name,
                    allow_name_fallback=not enforce_report_id_only,
                )
        return out_by_brand

    def _extract_page_text(self, sb, max_chars: int = 1200) -> str:
        try:
            text = sb.execute_script(
                "return (document && document.body && document.body.innerText) ? document.body.innerText : '';"
            )
            text = str(text or "").replace("\r", "").strip()
            return text[:max_chars]
        except Exception:
            return ""

    def _try_select_portfolio_and_account(self, sb, portfolio: str, ad_account: str) -> None:
        # Stage-B requirement: portfolio/account selection must succeed.
        if not self._safe_click_any_text(sb, portfolio, timeout=2.0):
            raise RuntimeError(f"Could not select Meta Business Portfolio: {portfolio}")
        self.logger.info("Portfolio selected: %s", portfolio)
        sb.sleep(1)

        if not self._safe_click_any_text(sb, ad_account, timeout=2.0):
            raise RuntimeError(f"Could not select Meta Ad Account: {ad_account}")
        self.logger.info("Ad account selected: %s", ad_account)
        sb.sleep(1)

    def _open_report_by_name(self, sb, report_name: str) -> None:
        if not self._safe_click_any_text(sb, "All reports", timeout=1.5):
            self._safe_click_any_text(sb, "\ubaa8\ub4e0 \ubcf4\uace0\uc11c", timeout=1.5)
        if not self._safe_click_any_text(sb, report_name, timeout=3):
            raise RuntimeError(f"Report not found/clickable: {report_name}")

    def _export_current_report_and_wait_file(self, sb) -> str:
        pre_ts = time.time()
        clicked_export = self._safe_click_any_text(sb, "Export", timeout=2) or self._safe_click_any_text(
            sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=2
        )
        if not clicked_export:
            raise RuntimeError("Could not click Export button")

        # Force selection to Raw data table (.xlsx) to avoid accidental PNG export.
        self._select_raw_xlsx_export_type(sb)
        if not (self._safe_click_any_text(sb, "Export", timeout=2) or self._safe_click_any_text(sb, "\ub0b4\ubcf4\ub0b4\uae30", timeout=2)):
            raise RuntimeError("Could not confirm Export after selecting Raw data table (.xlsx)")

        timeout = int(self.meta_config.get("download_wait_timeout_sec", 180))
        start = time.time()
        while time.time() - start <= timeout:
            path = newest_xlsx_in_dir(self.download_dir, since_ts=pre_ts)
            if path and not path.lower().endswith(".crdownload"):
                return path
            sb.sleep(2)
        raise TimeoutError("META report download timeout")

    def export_brand_reports(
        self,
        brand_cfg: Dict,
        activity_for_filename: str,
        yymmdd: str,
        report_to_sheet: Dict[str, str],
        report_names: Optional[List[str]] = None,
        report_id_by_name: Optional[Dict[str, str]] = None,
        browser: str = "edge",
        enforce_report_id_only: bool = False,
    ) -> Dict[str, str]:
        try:
            from seleniumbase import SB
        except Exception as e:
            raise RuntimeError("seleniumbase is required for META automation") from e

        os.makedirs(self.download_dir, exist_ok=True)
        selected_report_names = report_names or list(report_to_sheet.keys())
        if not selected_report_names:
            raise ValueError("At least one report name is required")

        sb_kwargs = self._build_sb_kwargs(browser)

        with SB(**sb_kwargs) as sb:
            self._enable_download_behavior(sb)
            self.logger.info("Open META home for login")
            sb.open(self.meta_config["home_url"])
            self._wait_for_meta_login(sb)
            return self._export_brand_reports_with_session(
                sb=sb,
                brand_cfg=brand_cfg,
                activity_for_filename=activity_for_filename,
                yymmdd=yymmdd,
                report_to_sheet=report_to_sheet,
                report_names=selected_report_names,
                report_id_by_name=report_id_by_name,
                allow_name_fallback=not enforce_report_id_only,
            )

    def run_stage_a_login_flow(
        self,
        logs_dir: str,
        run_id: str,
        progress_cb: Optional[Callable[[str], None]] = None,
        browser: str = "edge",
        user_data_dir: Optional[str] = None,
        extension_dir: Optional[str] = None,
        enable_extension: bool = False,
        headless: bool = False,
        use_uc: bool = True,
    ) -> Dict[str, str]:
        """
        Stage A probe:
        - open business.facebook.com
        - wait for user login
        - verify login
        - run best-effort post-login UI actions
        """
        try:
            from seleniumbase import SB
        except Exception as e:
            raise RuntimeError("seleniumbase is required for META automation") from e

        os.makedirs(logs_dir, exist_ok=True)
        screenshot_path = os.path.join(logs_dir, f"stage_a_{run_id}.png")
        clicked_actions: List[str] = []
        browser_name = (browser or "edge").strip().lower()
        if browser_name not in {"chrome", "edge"}:
            raise ValueError(f"Unsupported browser: {browser_name} (allowed: chrome, edge)")

        resolved_extension_dir = os.path.abspath(extension_dir) if (extension_dir and enable_extension) else None
        resolved_user_data_dir = os.path.abspath(user_data_dir) if user_data_dir else None

        # UC mode may fail if SeleniumBase creates temp profiles in restricted paths.
        # Default to a writable profile dir inside logs when user_data_dir is not provided.
        if browser_name == "chrome":
            uc_attempts = [True, False] if use_uc else [False]
        else:
            # UC mode is Chromium/Chrome-specific.
            uc_attempts = [False]
        last_error: Optional[Exception] = None

        for uc_flag in uc_attempts:
            # Fresh profile per run/mode avoids lock/corruption from previous crashed sessions.
            mode_name = "uc" if uc_flag else "std"
            auto_profile_dir = os.path.join(logs_dir, f"stage_a_profile_{run_id}_{mode_name}")
            chosen_user_data_dir = resolved_user_data_dir or auto_profile_dir
            os.makedirs(chosen_user_data_dir, exist_ok=True)

            chromium_args = [
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-breakpad",
                "--disable-background-networking",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-dev-shm-usage",
            ]
            if headless:
                chromium_args.append("--disable-gpu")

            sb_kwargs = {
                "browser": browser_name,
                "headless": headless,
                "locale_code": "ko",
                "user_data_dir": chosen_user_data_dir,
            }
            if browser_name == "chrome":
                sb_kwargs["uc"] = uc_flag
                sb_kwargs["uc_subprocess"] = uc_flag
                sb_kwargs["chromium_arg"] = chromium_args
                sb_kwargs["disable_features"] = "RendererCodeIntegrity"

            if resolved_extension_dir and browser_name == "chrome":
                sb_kwargs["extension_dir"] = resolved_extension_dir

            try:
                self.logger.info(
                    "Stage A: launch browser=%s uc=%s headless=%s profile=%s extension=%s",
                    browser_name,
                    uc_flag,
                    headless,
                    chosen_user_data_dir,
                    bool(resolved_extension_dir),
                )
                with SB(**sb_kwargs) as sb:
                    self._progress(progress_cb, "A-1 Open META: business.facebook.com")
                    self.logger.info("Stage A: open META home (browser=%s, uc=%s)", browser_name, uc_flag)
                    sb.open(self.meta_config["home_url"])

                    self._progress(progress_cb, "A-2 Login required: please complete login in the browser")
                    self.logger.info("Stage A: waiting for user login")
                    self._wait_for_meta_login(sb, progress_cb=progress_cb)
                    login_confirmed_url = sb.get_current_url()
                    self.logger.info("Stage A: login confirmed at URL=%s", login_confirmed_url)
                    self._progress(progress_cb, "A-3 Login confirmed")

                    self._progress(progress_cb, "A-4 Post-login auto controls")
                    self.logger.info("Stage A: open reporting page for post-login probe")
                    sb.open(self.meta_config["reporting_url"])
                    sb.sleep(3)

                    probe_click_texts = [
                        "All reports",
                        "\ubaa8\ub4e0 \ubcf4\uace0\uc11c",
                        "Export",
                        "\ub0b4\ubcf4\ub0b4\uae30",
                        "Columns",
                        "\uc5f4",
                    ]
                    for txt in probe_click_texts:
                        if self._safe_click_any_text(sb, txt, timeout=1.5):
                            clicked_actions.append(txt)
                            self.logger.info("Stage A: clicked text '%s' (best effort)", txt)
                            sb.sleep(1)

                    final_url = sb.get_current_url()
                    page_text_sample = self._extract_page_text(sb)
                    sb.driver.save_screenshot(screenshot_path)
                    self.logger.info("Stage A: screenshot saved: %s", screenshot_path)
                    self._progress(progress_cb, "A-5 Auto controls done, evidence saved")

                    return {
                        "login_confirmed_url": login_confirmed_url,
                        "final_url": final_url,
                        "clicked_actions": ", ".join(clicked_actions),
                        "screenshot_path": screenshot_path,
                        "page_text_sample": page_text_sample,
                        "browser": browser_name,
                        "driver_mode": "uc" if uc_flag else "standard",
                    }
            except Exception as e:
                last_error = e
                self.logger.exception("Stage A failed with driver mode uc=%s", uc_flag)
                if uc_flag and False in uc_attempts:
                    self._progress(progress_cb, "UC mode failed, retrying with standard mode")
                    continue
                break

        raise RuntimeError("Stage A browser flow failed in all driver modes") from last_error
