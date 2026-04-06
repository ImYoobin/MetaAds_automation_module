"""Constants for standalone Meta export v1."""

from __future__ import annotations

from pathlib import Path

DEFAULT_BROWSER = "msedge"
DEFAULT_ACTIVITY_NAME = "Lumiwise"
DEFAULT_BRAND_CODE = "meta_default"
DEFAULT_BRAND_NAME = "Meta Brand"

DEFAULT_VIEW_EVENT_SOURCE = "CLICK_REPORTS_FROM_SIDE_NAV"
DEFAULT_EXPORT_EVENT_SOURCE = "CLICK_EXPORT_HISTORY_FROM_SIDE_NAV"

REQUIRED_SHEETS: tuple[str, ...] = ("overall", "demo", "overall_bof", "demo_bof")
OPTIONAL_SHEETS: tuple[str, ...] = ("time", "time_bof")
ALL_SHEETS: tuple[str, ...] = REQUIRED_SHEETS + OPTIONAL_SHEETS

DEFAULT_OUTPUT_DIR = Path("standalone_output")
DEFAULT_DOWNLOADS_DIR = Path("standalone_downloads")
DEFAULT_LOGS_DIR = Path("standalone_logs")
DEFAULT_USER_BASE_DIR = Path.home() / "MetaExportStandalone"
DEFAULT_USER_OUTPUT_DIR = DEFAULT_USER_BASE_DIR / "output"
DEFAULT_USER_DOWNLOADS_DIR = DEFAULT_USER_BASE_DIR / "downloads"
DEFAULT_USER_LOGS_DIR = DEFAULT_USER_BASE_DIR / "logs"
