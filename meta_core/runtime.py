"""Runtime helpers for standalone execution."""

from __future__ import annotations

import json
import logging
import os
import re
import ssl
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable


def truthy_env(name: str, default: str = "1") -> bool:
    value = str(os.getenv(name, default) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def configure_insecure_https_bootstrap() -> None:
    """Optional local workaround for enterprise SSL interception."""
    if not truthy_env("META_RUNNER_INSECURE_SSL", "1"):
        return

    os.environ.setdefault("WDM_SSL_VERIFY", "0")
    os.environ.setdefault("PYTHONHTTPSVERIFY", "0")

    with suppress(Exception):
        ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[attr-defined]

    with suppress(Exception):
        import urllib3  # type: ignore

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    with suppress(Exception):
        import requests  # type: ignore

        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]


def load_embedded_engine_config() -> dict[str, Any]:
    config_path = (Path(__file__).resolve().parent / "engine" / "config.json").resolve()
    if not config_path.exists():
        raise RuntimeError(f"embedded engine config.json not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def configure_logger(*, logs_dir: Path, run_id: str) -> logging.Logger:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = (logs_dir / f"run_{run_id}.log").resolve()

    logger = logging.getLogger("meta_export_runner")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.info("log_file=%s", log_file)
    return logger


def verify_download_context(meta: Any, *, watcher_dir: Path, logger: logging.Logger) -> dict[str, Any]:
    watcher_path = watcher_dir.expanduser().resolve()
    watcher_path.mkdir(parents=True, exist_ok=True)

    probe_path = watcher_path / ".standalone_meta_write_probe"
    writable = False
    write_error = ""
    try:
        probe_path.write_text("ok", encoding="utf-8")
        probe_path.unlink(missing_ok=True)
        writable = True
    except Exception as exc:
        write_error = str(exc)[:300]

    browser_download_dir = str(getattr(meta, "_browser_download_dir", "") or "")
    behavior_configured = bool(getattr(meta, "_download_behavior_configured", False))
    dir_match = False
    with suppress(Exception):
        if browser_download_dir:
            dir_match = watcher_path == Path(browser_download_dir).expanduser().resolve()

    snapshot = {
        "browser_download_dir": browser_download_dir,
        "watcher_dir": str(watcher_path),
        "download_behavior_configured": behavior_configured,
        "watcher_writable": writable,
        "match": dir_match,
    }
    if write_error:
        snapshot["watcher_write_error"] = write_error

    logger.info(
        "download_dir_check browser_download_dir=%s watcher_dir=%s match=%s configured=%s watcher_writable=%s",
        snapshot["browser_download_dir"],
        snapshot["watcher_dir"],
        snapshot["match"],
        snapshot["download_behavior_configured"],
        snapshot["watcher_writable"],
    )

    if not writable:
        raise RuntimeError("Download watcher directory is not writable.")
    if not behavior_configured:
        raise RuntimeError("Browser download behavior is not configured.")
    if not dir_match:
        raise RuntimeError("Browser download directory mismatch.")
    return snapshot


def build_sb_kwargs(meta: Any, requested_browser: str) -> dict[str, Any]:
    browser = (requested_browser or "chrome").strip().lower()
    sb_kwargs = dict(meta._build_sb_kwargs(browser))  # noqa: SLF001 - reuse existing runtime kwargs
    sb_kwargs.pop("user_data_dir", None)
    if truthy_env("META_RUNNER_DISABLE_UC", "1"):
        sb_kwargs.pop("uc", None)
        sb_kwargs.pop("uc_subprocess", None)
    sb_kwargs["guest_mode"] = False
    sb_kwargs["incognito"] = False
    sb_kwargs.pop("chromium_arg", None)
    return sb_kwargs


def _parse_major(version: str) -> int | None:
    text = str(version or "").strip()
    if not text:
        return None
    match = re.match(r"^(\d+)", text)
    if not match:
        return None
    with suppress(Exception):
        return int(match.group(1))
    return None


def _locate_packaged_chromedriver() -> Path | None:
    with suppress(Exception):
        import seleniumbase  # type: ignore

        base = Path(seleniumbase.__file__).resolve().parent
        for name in ("chromedriver.exe", "chromedriver"):
            candidate = (base / "drivers" / name).resolve()
            if candidate.exists():
                return candidate
    return None


def _patch_sb_install_for_insecure_ssl(*, sb_install: Any, logger: logging.Logger) -> Callable[[], None]:
    if not truthy_env("META_RUNNER_INSECURE_SSL", "1"):
        return lambda: None

    try:
        import requests  # type: ignore
    except Exception as exc:
        logger.warning("driver_preflight_ssl_patch_skip reason=requests_import_failed error=%s", exc)
        return lambda: None

    original_requests_get = getattr(sb_install, "requests_get", None)
    original_requests_get_with_retry = getattr(sb_install, "requests_get_with_retry", None)

    def _request_with_proxy(url: str, timeout: float) -> Any:
        use_proxy, protocol, proxy_string = sb_install.get_proxy_info()
        proxies = {protocol: proxy_string} if use_proxy else None
        return requests.get(url, proxies=proxies, timeout=timeout, verify=False)

    def _requests_get_insecure(url: str) -> Any:
        try:
            return _request_with_proxy(url, 1.25)
        except Exception:
            url = url.replace("https://", "http://")
            time.sleep(0.04)
            return _request_with_proxy(url, 2.75)

    def _requests_get_with_retry_insecure(url: str) -> Any:
        try:
            return _request_with_proxy(url, 1.35)
        except Exception:
            time.sleep(1)
            try:
                return _request_with_proxy(url, 2.45)
            except Exception:
                time.sleep(1)
                return _request_with_proxy(url, 3.55)

    sb_install.requests_get = _requests_get_insecure
    sb_install.requests_get_with_retry = _requests_get_with_retry_insecure
    logger.warning("driver_preflight_ssl_patch_enabled insecure_ssl=1")

    def _restore() -> None:
        if original_requests_get is not None:
            sb_install.requests_get = original_requests_get
        if original_requests_get_with_retry is not None:
            sb_install.requests_get_with_retry = original_requests_get_with_retry

    return _restore


def ensure_browser_driver_ready(*, browser: str, logger: logging.Logger) -> None:
    browser_name = (browser or "chrome").strip().lower()
    if browser_name != "chrome":
        return
    try:
        from seleniumbase.console_scripts import sb_install  # type: ignore
        from seleniumbase.core import detect_b_ver  # type: ignore
    except Exception as exc:
        logger.warning("driver_prepare_skip reason=seleniumbase_import_failed error=%s", exc)
        return

    chrome_version = str(
        detect_b_ver.get_browser_version_from_os(detect_b_ver.ChromeType.GOOGLE) or ""
    ).strip()
    chrome_major = _parse_major(chrome_version)
    chromedriver_path = _locate_packaged_chromedriver()
    driver_version = ""
    driver_major = None
    if chromedriver_path:
        with suppress(Exception):
            driver_version = str(
                detect_b_ver.get_browser_version_from_binary(str(chromedriver_path)) or ""
            ).strip()
            driver_major = _parse_major(driver_version)

    logger.info(
        "driver_preflight browser=chrome chrome_version=%s chromedriver_version=%s chromedriver_path=%s",
        chrome_version,
        driver_version,
        str(chromedriver_path or ""),
    )

    if chrome_major and driver_major and chrome_major == driver_major:
        logger.info("driver_preflight_ok browser=chrome major=%s", chrome_major)
        return

    if chromedriver_path and chrome_major and driver_major and chrome_major != driver_major:
        logger.warning(
            "driver_preflight_mismatch chrome_major=%s driver_major=%s deleting_stale=%s",
            chrome_major,
            driver_major,
            chromedriver_path,
        )
        with suppress(Exception):
            chromedriver_path.unlink()

    override = f"chromedriver {chrome_major}" if chrome_major else "chromedriver"
    logger.info("driver_preflight_install start override=%s", override)
    old_argv = list(sys.argv)
    restore_sb_install = lambda: None
    try:
        restore_sb_install = _patch_sb_install_for_insecure_ssl(sb_install=sb_install, logger=logger)
        sb_install.main(override=override)
    except Exception as exc:
        logger.exception("driver_preflight_install_failed override=%s error=%s", override, exc)
        raise RuntimeError(
            "Failed to auto-install compatible chromedriver. "
            "Check network/SSL policy and rerun."
        ) from exc
    finally:
        with suppress(Exception):
            restore_sb_install()
        sys.argv = old_argv

    refreshed_path = _locate_packaged_chromedriver()
    refreshed_version = ""
    refreshed_major = None
    if refreshed_path:
        with suppress(Exception):
            refreshed_version = str(
                detect_b_ver.get_browser_version_from_binary(str(refreshed_path)) or ""
            ).strip()
            refreshed_major = _parse_major(refreshed_version)

    logger.info(
        "driver_preflight_install_done chromedriver_version=%s chromedriver_path=%s",
        refreshed_version,
        str(refreshed_path or ""),
    )

    if chrome_major and refreshed_major and chrome_major != refreshed_major:
        raise RuntimeError(
            f"Auto-installed chromedriver major({refreshed_major}) does not match Chrome major({chrome_major})."
        )


def progress_log_cb(logger: logging.Logger, prefix: str = "progress") -> Callable[[str], None]:
    def _cb(message: str) -> None:
        logger.info("%s %s", prefix, str(message or ""))

    return _cb
