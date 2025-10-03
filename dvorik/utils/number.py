"""Numeric helpers used across the service layer."""

from __future__ import annotations

import math
import re
from typing import Any, Optional

__all__ = ["display_qty", "to_float_qty"]


_NUMERIC_FRAGMENT_RX = re.compile(
    r"(?<![A-Za-zА-Яа-я0-9])(?<![A-Za-zА-Яа-я0-9]-)([+-]?(?:\d{1,3}(?:[ \u00A0]\d{3})+|\d+)(?:[\.,]\d+)?)"
)


def display_qty(value: Any) -> str:
    """Return a concise human-readable representation of ``value``.

    Integers are rendered without a decimal part while other finite numbers use
    the general format to strip trailing zeros. Non-numeric values fall back to
    ``str(value)``.
    """

    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        candidate: Any = text
    else:
        candidate = value
    try:
        num = float(candidate)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(num):
        return str(value)
    rounded = round(num)
    if math.isclose(num, rounded, rel_tol=1e-9, abs_tol=1e-9):
        return str(int(rounded))
    return format(num, "g")


def to_float_qty(value: Any) -> Optional[float]:
    """Parse ``value`` into a floating point quantity if possible.

    The parser is intentionally liberal: it extracts the first standalone
    numeric fragment, tolerates thin spaces as thousands separators and both
    comma/period decimal markers, but skips identifiers such as ``№ 12``.
    """

    if value is None:
        return None
    if isinstance(value, float):
        return float(value) if math.isfinite(value) else None
    text = str(value).strip()
    if not text:
        return None

    fragment_match: Optional[str] = None
    for candidate in _NUMERIC_FRAGMENT_RX.finditer(text):
        fragment = candidate.group(1)
        if not fragment:
            continue
        prefix = text[: candidate.start(1)]
        if prefix.rstrip().endswith("№"):
            continue
        fragment_match = fragment
        break

    if fragment_match is None:
        return None

    fragment = fragment_match.replace("\u00A0", " ").replace(" ", "").replace(",", ".")

    try:
        num = float(fragment)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(num):
        return None
    return num
