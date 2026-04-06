import json
import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class RunContext:
    run_id: str
    started_at: datetime
    log_file: str


CONFIG_TEMPLATE_FILE = "config.template.json"
CONFIG_LOCAL_FILE = "config.local.json"
CONFIG_FALLBACK_FILE = "config.json"
ENV_LOCAL_PATH = "META_AIM_CONFIG_LOCAL_PATH"
ENV_TEMPLATE_PATH = "META_AIM_CONFIG_TEMPLATE_PATH"
ENV_JSON_OVERRIDES = "META_AIM_CONFIG_OVERRIDES_JSON"
ENV_PATH_OVERRIDE_PREFIX = "META_AIM_CFG__"


def _read_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"Config file must be a JSON object: {path}")
    return payload


def _deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _parse_env_value(raw: str) -> Any:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        return json.loads(text)
    except Exception:
        return text


def _ensure_list_len(out: List[Any], index: int) -> None:
    while len(out) <= index:
        out.append({})


def _set_nested_value(container: Any, path_parts: List[str], value: Any) -> None:
    cur = container
    for idx, part in enumerate(path_parts):
        is_last = idx == len(path_parts) - 1
        next_part = path_parts[idx + 1] if not is_last else ""
        next_is_index = next_part.isdigit()

        if isinstance(cur, list):
            if not part.isdigit():
                raise ValueError(f"List path must use numeric index. path={path_parts}")
            list_index = int(part)
            _ensure_list_len(cur, list_index)
            if is_last:
                cur[list_index] = value
                return
            child = cur[list_index]
            if not isinstance(child, (dict, list)):
                child = [] if next_is_index else {}
                cur[list_index] = child
            cur = child
            continue

        if not isinstance(cur, dict):
            raise ValueError(f"Invalid config path container for {path_parts}")

        if is_last:
            cur[part] = value
            return

        child = cur.get(part)
        if not isinstance(child, (dict, list)):
            child = [] if next_is_index else {}
            cur[part] = child
        cur = child


def _apply_env_path_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(config)
    for key, raw_val in os.environ.items():
        if not key.startswith(ENV_PATH_OVERRIDE_PREFIX):
            continue
        raw_path = key[len(ENV_PATH_OVERRIDE_PREFIX):].strip()
        if not raw_path:
            continue
        path_parts = [part.lower() for part in raw_path.split("__") if part]
        if not path_parts:
            continue
        parsed_value = _parse_env_value(raw_val)
        _set_nested_value(out, path_parts, parsed_value)
    return out


def _resolve_config_base_dir(config_path: Optional[str]) -> str:
    if not config_path:
        return os.getcwd()

    abs_path = os.path.abspath(config_path)
    if os.path.isdir(abs_path):
        return abs_path

    file_name = os.path.basename(abs_path).lower()
    if os.path.isfile(abs_path):
        if file_name in {CONFIG_TEMPLATE_FILE, CONFIG_LOCAL_FILE, CONFIG_FALLBACK_FILE}:
            return os.path.dirname(abs_path)
        return os.path.dirname(abs_path)
    return os.path.dirname(abs_path)


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load config with precedence: template -> local -> env.

    - template: config.template.json (or META_AIM_CONFIG_TEMPLATE_PATH)
    - local: config.local.json (or META_AIM_CONFIG_LOCAL_PATH)
    - env object override: META_AIM_CONFIG_OVERRIDES_JSON
    - env path override: META_AIM_CFG__A__B__C=value -> config["a"]["b"]["c"]

    Backward compatibility:
    - If template file is missing, falls back to config.json.
    - If config_path points to a non-standard json file, that file is loaded as-is.
    """
    if config_path:
        abs_path = os.path.abspath(config_path)
        file_name = os.path.basename(abs_path).lower()
        if (
            os.path.isfile(abs_path)
            and file_name not in {CONFIG_TEMPLATE_FILE, CONFIG_LOCAL_FILE, CONFIG_FALLBACK_FILE}
        ):
            return _read_json_file(abs_path)

    base_dir = _resolve_config_base_dir(config_path)
    template_path = os.getenv(ENV_TEMPLATE_PATH, "").strip() or os.path.join(base_dir, CONFIG_TEMPLATE_FILE)
    fallback_path = os.path.join(base_dir, CONFIG_FALLBACK_FILE)
    local_path = os.getenv(ENV_LOCAL_PATH, "").strip() or os.path.join(base_dir, CONFIG_LOCAL_FILE)

    if os.path.isfile(template_path):
        cfg = _read_json_file(template_path)
    elif os.path.isfile(fallback_path):
        cfg = _read_json_file(fallback_path)
    else:
        raise FileNotFoundError(
            f"Config not found. Expected '{CONFIG_TEMPLATE_FILE}' (preferred) or '{CONFIG_FALLBACK_FILE}' in {base_dir}"
        )

    if os.path.isfile(local_path):
        cfg = _deep_merge_dict(cfg, _read_json_file(local_path))

    json_override_raw = os.getenv(ENV_JSON_OVERRIDES, "").strip()
    if json_override_raw:
        parsed = _parse_env_value(json_override_raw)
        if not isinstance(parsed, dict):
            raise ValueError(f"{ENV_JSON_OVERRIDES} must be a JSON object")
        cfg = _deep_merge_dict(cfg, parsed)

    cfg = _apply_env_path_overrides(cfg)
    return cfg


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def make_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def build_logger(log_dir: str, run_id: str) -> RunContext:
    ensure_dir(log_dir)
    log_file = os.path.join(log_dir, f"run_{run_id}.log")
    logger = logging.getLogger("meta_export_runner")
    logger.setLevel(logging.INFO)

    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return RunContext(run_id=run_id, started_at=datetime.now(), log_file=log_file)


def get_logger() -> logging.Logger:
    return logging.getLogger("meta_export_runner")


def yymmdd_today() -> str:
    return datetime.now().strftime("%y%m%d")
