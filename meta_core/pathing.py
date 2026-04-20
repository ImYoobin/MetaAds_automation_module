"""Shared path helpers for Meta export storage and browser profiles."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EXPORT_ROOT_DIRNAME = "MetaAdsExport"
USER_DATA_ROOT_DIRNAME = "user_data"
META_PROFILE_ROOT_DIRNAME = "meta"
LEGACY_HISTORY_PROFILE_DIRNAME = "meta_history_log"


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_browser_profile_name(browser: str) -> str:
    normalized = _safe_text(browser).lower()
    if normalized in {"msedge", "chrome", "chromium"}:
        return normalized
    return "msedge"


def build_meta_export_root(base_parent_dir: str | Path) -> Path:
    return (Path(base_parent_dir).expanduser().resolve() / EXPORT_ROOT_DIRNAME).resolve()


def build_meta_shared_user_data_dir(base_parent_dir: str | Path, browser: str) -> Path:
    export_root = build_meta_export_root(base_parent_dir)
    return (
        export_root
        / USER_DATA_ROOT_DIRNAME
        / META_PROFILE_ROOT_DIRNAME
        / normalize_browser_profile_name(browser)
    ).resolve()


def build_legacy_meta_history_user_data_dir(base_parent_dir: str | Path) -> Path:
    export_root = build_meta_export_root(base_parent_dir)
    return (export_root / USER_DATA_ROOT_DIRNAME / LEGACY_HISTORY_PROFILE_DIRNAME).resolve()


def infer_base_parent_dir_from_user_data_dir(user_data_dir: str | Path) -> Path | None:
    resolved = Path(user_data_dir).expanduser().resolve()
    if resolved.parent.name.lower() != META_PROFILE_ROOT_DIRNAME:
        return None
    if resolved.parent.parent.name.lower() != USER_DATA_ROOT_DIRNAME:
        return None
    export_root = resolved.parent.parent.parent
    if export_root.name.lower() != EXPORT_ROOT_DIRNAME.lower():
        return None
    return export_root.parent.resolve()


def _directory_is_empty(path: Path) -> bool:
    if not path.exists():
        return True
    try:
        next(path.iterdir())
    except StopIteration:
        return True
    return False


@dataclass(frozen=True)
class PreparedMetaUserDataDir:
    requested_dir: Path
    effective_dir: Path
    legacy_dir: Path | None
    migration_mode: str
    warning: str = ""


def prepare_meta_user_data_dir(
    *,
    requested_dir: str | Path,
    legacy_dir: str | Path | None = None,
) -> PreparedMetaUserDataDir:
    requested_path = Path(requested_dir).expanduser().resolve()
    requested_path.parent.mkdir(parents=True, exist_ok=True)

    resolved_legacy_dir: Path | None = None
    if legacy_dir is not None:
        resolved_legacy_dir = Path(legacy_dir).expanduser().resolve()
    else:
        inferred_base_parent = infer_base_parent_dir_from_user_data_dir(requested_path)
        if inferred_base_parent is not None:
            resolved_legacy_dir = build_legacy_meta_history_user_data_dir(inferred_base_parent)

    if not _directory_is_empty(requested_path):
        requested_path.mkdir(parents=True, exist_ok=True)
        return PreparedMetaUserDataDir(
            requested_dir=requested_path,
            effective_dir=requested_path,
            legacy_dir=resolved_legacy_dir,
            migration_mode="none",
        )

    if resolved_legacy_dir is None or _directory_is_empty(resolved_legacy_dir):
        requested_path.mkdir(parents=True, exist_ok=True)
        return PreparedMetaUserDataDir(
            requested_dir=requested_path,
            effective_dir=requested_path,
            legacy_dir=resolved_legacy_dir,
            migration_mode="none",
        )

    try:
        if requested_path.exists():
            requested_path.rmdir()
    except OSError:
        pass

    try:
        requested_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(resolved_legacy_dir), str(requested_path))
        requested_path.mkdir(parents=True, exist_ok=True)
        return PreparedMetaUserDataDir(
            requested_dir=requested_path,
            effective_dir=requested_path,
            legacy_dir=resolved_legacy_dir,
            migration_mode="move",
        )
    except Exception as move_exc:  # noqa: BLE001
        requested_path.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copytree(resolved_legacy_dir, requested_path, dirs_exist_ok=True)
            return PreparedMetaUserDataDir(
                requested_dir=requested_path,
                effective_dir=requested_path,
                legacy_dir=resolved_legacy_dir,
                migration_mode="copy",
                warning=f"move_failed={move_exc}",
            )
        except Exception as copy_exc:  # noqa: BLE001
            return PreparedMetaUserDataDir(
                requested_dir=requested_path,
                effective_dir=resolved_legacy_dir,
                legacy_dir=resolved_legacy_dir,
                migration_mode="legacy_fallback",
                warning=f"legacy_profile_migration_failed move={move_exc} copy={copy_exc}",
            )
