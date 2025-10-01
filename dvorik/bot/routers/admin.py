"""Administrative tooling routers bundled with the bot."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_admin")


@router.message(Command("admin"))
async def handle_admin_entry(message: Message) -> None:
    """Placeholder handler advertising upcoming admin features."""

    await message.answer(
        "Админ-команды скоро переедут сюда. Пока что раздел находится в разработке.",
    )


def register() -> None:
    """Register the admin router in the registry."""

    BotRouterRegistry.ensure("builtin.admin", lambda: router)
