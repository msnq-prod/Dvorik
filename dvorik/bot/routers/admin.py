from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_admin")


@router.message(Command("admin"))
async def handle_admin_entry(message: Message) -> None:
    """Placeholder handler advertising upcoming admin features."""

    await message.answer(
        "Админ-команды скоро переедут сюда. Пока что раздел находится в разработке.",
        reply_markup=keyboards.admin_entry_keyboard(),
    )


@router.callback_query()
async def handle_admin_callbacks(query: CallbackQuery) -> None:
    """React to callbacks associated with the admin namespace."""

    if not query.data:
        return

    try:
        payload = callbacks.parse(query.data, expected_namespace="builtin.admin")
    except ValueError:
        return

    if payload.action == "status":
        await query.answer("Мы готовим обновлённую админку, скоро поделимся деталями.")
    elif payload.action == "feedback":
        await query.answer("Спасибо!", show_alert=False)
        if query.message:
            await query.message.answer(
                "Напишите свои пожелания в чат администраторов, чтобы мы учли их при релизе."
            )
    else:
        await query.answer("Эта кнопка пока неактивна.")


def register() -> None:
    """Register the admin router in the registry."""

    BotRouterRegistry.ensure("builtin.admin", lambda: router)
