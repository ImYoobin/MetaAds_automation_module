"""URL helpers for Meta report view/export navigation."""

from __future__ import annotations

from urllib.parse import urlencode


def build_report_view_url(
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
    if str(event_source).strip():
        query["event_source"] = str(event_source).strip()
    return "https://adsmanager.facebook.com/adsmanager/reporting/view?" + urlencode(query)


def build_report_export_url(
    *,
    act_id: str,
    business_id: str,
    global_scope_id: str,
    event_source: str = "",
) -> str:
    query: dict[str, str] = {
        "act": str(act_id),
        "ads_manager_write_regions": "true",
        "business_id": str(business_id),
        "global_scope_id": str(global_scope_id or business_id),
    }
    if str(event_source).strip():
        query["event_source"] = str(event_source).strip()
    return "https://adsmanager.facebook.com/adsmanager/reporting/export?" + urlencode(query)

