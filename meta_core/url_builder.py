"""Deprecated wrapper around internal URL builder helpers."""

from __future__ import annotations

import warnings

from .internal.url_builder import build_report_export_url, build_report_view_url

warnings.warn(
    "meta_core.url_builder is deprecated. Use dashboard.services.url_service for public URL handling.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["build_report_view_url", "build_report_export_url"]
