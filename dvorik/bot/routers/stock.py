"""Routers dedicated to stock management flows."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_stock")


@router.message(Command("stock"))
async def handle_stock_entry(message: Message) -> None:
    """Placeholder entrypoint for upcoming stock operations."""

    await message.answer(
        "Задачи по остаткам ещё переносятся в новую версию. Следите за обновлениями!",
    )


def register() -> None:
    """Register the stock router in the registry."""

    BotRouterRegistry.ensure("builtin.stock", lambda: router)
