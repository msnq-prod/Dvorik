from __future__ import annotations

import re
from pathlib import Path

SAFE_NAME_RX = re.compile(r"[^A-Za-z0-9А-Яа-я_.\-]+")


def sanitize_filename(name: str) -> str:
    """Return a filesystem-safe version of ``name`` limited to base filename."""

    basename = Path(name).name
    cleaned = SAFE_NAME_RX.sub("_", basename)
    cleaned = cleaned.strip("._")
    return cleaned or "upload"


__all__ = ["sanitize_filename", "SAFE_NAME_RX"]
