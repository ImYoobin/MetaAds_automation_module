"""Core Meta export engine package."""

from .catalog import load_activity_catalog
from .config import StandaloneMetaExportConfig, load_config


def run_standalone_export(*args, **kwargs):  # type: ignore[no-untyped-def]
    from .orchestrator import run_standalone_export as _run

    return _run(*args, **kwargs)


def run_standalone_export_batch(*args, **kwargs):  # type: ignore[no-untyped-def]
    from .orchestrator import run_standalone_export_batch as _run

    return _run(*args, **kwargs)

__all__ = [
    "StandaloneMetaExportConfig",
    "load_config",
    "load_activity_catalog",
    "run_standalone_export",
    "run_standalone_export_batch",
]
