"""Report URL parsing and cleaning helpers."""

from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse

from dashboard.models import ParsedReportUrl


ALLOWED_DOMAINS: tuple[str, ...] = ("facebook.com", "fb.com", "adsmanager.com")


class UrlValidationError(ValueError):
    """Raised when report URL cannot be parsed to required parts."""


def _first(query: dict[str, list[str]], keys: tuple[str, ...]) -> str:
    for key in keys:
        values = query.get(key) or []
        if values:
            value = str(values[0] or "").strip()
            if value:
                return value
    return ""


def _is_allowed_domain(host: str) -> bool:
    host_text = str(host or "").strip().lower()
    if not host_text:
        return False
    return any(domain in host_text for domain in ALLOWED_DOMAINS)


def parse_report_url(raw_url: str, *, default_event_source: str = "") -> ParsedReportUrl:
    text = str(raw_url or "").strip()
    if not text:
        raise UrlValidationError("URL이 비어 있습니다.")

    parsed = urlparse(text)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UrlValidationError("URL은 http 또는 https로 시작해야 합니다.")

    if not _is_allowed_domain(parsed.netloc):
        raise UrlValidationError("Meta 도메인(facebook.com/fb.com/adsmanager.com) URL만 허용됩니다.")

    query = parse_qs(parsed.query or "")
    act_id = _first(query, ("act", "account_id", "ad_account_id"))
    business_id = _first(query, ("business_id",))
    global_scope_id = _first(query, ("global_scope_id",)) or business_id
    report_id = _first(query, ("selected_report_id", "report_id", "meta_report_id"))
    event_source = _first(query, ("event_source",)) or str(default_event_source or "").strip()

    missing: list[str] = []
    if not act_id:
        missing.append("act")
    if not business_id:
        missing.append("business_id")
    if not report_id:
        missing.append("selected_report_id")

    if missing:
        raise UrlValidationError(
            "URL에서 필수 파라미터를 찾을 수 없습니다: " + ", ".join(missing)
        )

    cleaned_url = _build_report_view_url(
        act_id=act_id,
        business_id=business_id,
        global_scope_id=global_scope_id or business_id,
        report_id=report_id,
        event_source=event_source,
    )
    return ParsedReportUrl(
        raw_url=text,
        cleaned_url=cleaned_url,
        act_id=act_id,
        business_id=business_id,
        global_scope_id=global_scope_id or business_id,
        report_id=report_id,
        event_source=event_source,
    )


def parse_cleaned_url(url: str, *, default_event_source: str = "") -> ParsedReportUrl:
    return parse_report_url(url, default_event_source=default_event_source)


def clean_report_url(raw_url: str, *, default_event_source: str = "") -> str:
    return parse_report_url(raw_url, default_event_source=default_event_source).cleaned_url


def build_cleaned_url_from_parts(
    *,
    act_id: str,
    business_id: str,
    global_scope_id: str,
    report_id: str,
    event_source: str = "",
) -> str:
    act_value = str(act_id or "").strip()
    business_value = str(business_id or "").strip()
    global_scope_value = str(global_scope_id or "").strip() or business_value
    report_value = str(report_id or "").strip()
    if not act_value or not business_value or not report_value:
        raise UrlValidationError("cleaned URL 생성을 위한 필수 값이 누락되었습니다.")
    return _build_report_view_url(
        act_id=act_value,
        business_id=business_value,
        global_scope_id=global_scope_value,
        report_id=report_value,
        event_source=str(event_source or "").strip(),
    )


def is_cleaned_url_valid(url: str, *, default_event_source: str = "") -> tuple[bool, str]:
    try:
        parse_cleaned_url(url, default_event_source=default_event_source)
        return True, ""
    except UrlValidationError as exc:
        return False, str(exc)


def _build_report_view_url(
    *,
    act_id: str,
    business_id: str,
    global_scope_id: str,
    report_id: str,
    event_source: str = "",
) -> str:
    query: dict[str, str] = {
        "act": str(act_id),
        "ads_manager_write_regions": "true",
        "business_id": str(business_id),
        "global_scope_id": str(global_scope_id or business_id),
        "selected_report_id": str(report_id),
    }
    source = str(event_source or "").strip()
    if source:
        query["event_source"] = source
    return "https://adsmanager.facebook.com/adsmanager/reporting/view?" + urlencode(query)
