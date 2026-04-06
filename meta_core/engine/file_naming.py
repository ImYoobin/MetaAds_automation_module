from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Optional


def format_name(pattern: str, brand: str, activity: str, yymmdd: Optional[str], sheet: str) -> str:
    date_key = yymmdd or datetime.now().strftime("%y%m%d")
    return pattern.format(
        brand=brand,
        activity=activity,
        yyMMdd=date_key,
        sheet=sheet,
        template_sheet=sheet,
    )


def parse_brand_from_filename(file_path: str) -> str:
    base = os.path.basename(file_path)
    stem = os.path.splitext(base)[0]
    parts = stem.split("_")
    if not parts:
        raise ValueError(f"Invalid file name: {base}")
    return parts[0]


def newest_xlsx_in_dir(directory: str, since_ts: float = 0.0) -> Optional[str]:
    candidates = []
    if not os.path.isdir(directory):
        return None
    for name in os.listdir(directory):
        if not name.lower().endswith(".xlsx"):
            continue
        path = os.path.join(directory, name)
        mtime = os.path.getmtime(path)
        if mtime >= since_ts:
            candidates.append((mtime, path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda x: x[0], reverse=True)[0][1]


def build_brand_lookup(config: Dict) -> Dict[str, Dict]:
    out = {}
    for b in config["brands"]:
        out[b["brand_ko"]] = b
    return out
