from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_core")


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    """Respond to the ``/start`` command with a generic greeting."""

    await message.answer(
        "Привет! Я бот новой архитектуры Dvorik. Функционал скоро появится.",
        reply_markup=keyboards.core_entry_keyboard(),
    )


@router.callback_query()
async def handle_core_callbacks(query: CallbackQuery) -> None:
    """Process callbacks from the core namespace."""

    if not query.data:
        return

    try:
        payload = callbacks.parse(query.data, expected_namespace="builtin.core")
    except ValueError:
        return

    if payload.action == "status":
        await query.answer("Мы активно переносим функциональность в новую версию.")
    elif payload.action == "feedback":
        await query.answer("Будем рады обратной связи!", show_alert=False)
        if query.message:
            await query.message.answer(
                "Напишите нам в служебный чат или через поддержку, чтобы поделиться предложениями."
            )
    else:
        await query.answer("Действие пока не поддерживается.")


def register() -> None:
    """Register the core router in the registry."""

    BotRouterRegistry.ensure("builtin.core", lambda: router)
