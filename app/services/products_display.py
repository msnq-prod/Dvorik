from __future__ import annotations

import re
from typing import Optional, Sequence

_DIAMOND_CHARS = "◇◆⬥⬦⬧⬨⬩⬪⬫⬬⬭⬮⬯⟐⋄♢♦◊⧫"
_DIAMOND_SPLIT_RE = re.compile(rf"\s*[{re.escape(_DIAMOND_CHARS)}]+\s*")


def primary_product_name(name: Optional[str]) -> str:
    if not name:
        return ""
    text = str(name).strip()
    if not text:
        return ""
    parts = [part.strip(" \u00b7·-–—") for part in _DIAMOND_SPLIT_RE.split(text) if part.strip()]
    return parts[0] if parts else text


def strip_display_exceptions(name: Optional[str], phrases: Sequence[str]) -> str:
    if not name:
        return ""
    cleaned = str(name)
    for raw_phrase in phrases:
        if raw_phrase is None:
            continue
        phrase = str(raw_phrase)
        if not phrase.strip():
            continue
        cleaned = re.sub(
            rf"\s*{re.escape(phrase)}\s*",
            " ",
            cleaned,
            flags=re.IGNORECASE,
        )
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip(" ,.;:-")


__all__ = ["primary_product_name", "strip_display_exceptions"]
