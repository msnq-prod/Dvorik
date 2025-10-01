"""Core command handlers available to every operator."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_core")


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    """Respond to the ``/start`` command with a generic greeting."""

    await message.answer(
        "Привет! Я бот новой архитектуры Dvorik. Функционал скоро появится.",
    )


def register() -> None:
    """Register the core router in the registry."""

    BotRouterRegistry.ensure("builtin.core", lambda: router)
