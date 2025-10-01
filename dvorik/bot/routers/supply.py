"""Routers focused on supply import and tracking."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_supply")


@router.message(Command("supply"))
async def handle_supply_entry(message: Message) -> None:
    """Placeholder entrypoint announcing upcoming supply workflows."""

    await message.answer(
        "Импорт поставок пока в процессе миграции. Эта команда заработает позже.",
    )


def register() -> None:
    """Register the supply router in the registry."""

    BotRouterRegistry.ensure("builtin.supply", lambda: router)
