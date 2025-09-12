from __future__ import annotations

from typing import Optional

from aiogram.types import CallbackQuery


async def safe_cb_answer(cb: CallbackQuery) -> None:
    try:
        await cb.answer()
    except Exception:
        pass


def extract_pid_from_cbdata(data: str) -> Optional[int]:
    if not data:
        return None
    if data.startswith("open|"):
        try:
            return int(data.split("|", 1)[1])
        except Exception:
            return None
    parts = data.split("|")
    for token in parts[1:]:
        if token.isdigit():
            try:
                return int(token)
            except Exception:
                pass
    return None
