"""Helpers for handling supply document uploads in the new bot stack."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from aiogram.types import Message

_ALLOWED_SUFFIXES = {".csv", ".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}

__all__ = ["_resolve_file_name", "on_document"]


def _resolve_file_name(file: Any) -> str:
    """Resolve a human-friendly name for the uploaded ``file``."""

    name = getattr(file, "file_name", None)
    if name:
        return name

    mime_type = (getattr(file, "mime_type", None) or "").split(";", 1)[0].strip()
    ext = ""
    if mime_type:
        ext = mimetypes.guess_extension(mime_type) or ""
        if ext == ".jpe":  # normalise rare jpeg alias
            ext = ".jpg"
    if not ext and mime_type and "/" in mime_type:
        subtype = mime_type.rsplit("/", 1)[-1].strip()
        if subtype and subtype != "*":
            ext = f".{subtype}" if not subtype.startswith(".") else subtype

    if ext:
        return f"upload{ext}"

    file_id = getattr(file, "file_id", None) or ""
    if file_id:
        return f"upload_{file_id}"

    return "upload"


async def on_document(message: Message, state: Any) -> None:
    """Validate uploaded documents before dispatching to the supply pipeline."""

    data = await state.get_data()
    if not data.get("expect_excel"):
        return

    document = message.document
    resolved_name = _resolve_file_name(document)
    suffix = Path(resolved_name.lower()).suffix
    if suffix not in _ALLOWED_SUFFIXES:
        await message.answer(
            (
                "Принимаем только CSV или Excel (.xls/.xlsx).\n"
                "Пожалуйста, конвертируйте исходный файл в CSV: https://convertio.co/ru/xls-csv/"
            )
        )
        return

    await message.answer(
        "Импорт поставок в новой архитектуре ещё разворачивается. Попробуйте снова позже.",
    )
