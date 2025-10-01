from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_stock")


@router.message(Command("stock"))
async def handle_stock_entry(message: Message) -> None:
    """Placeholder entrypoint for upcoming stock operations."""

    await message.answer(
        "Задачи по остаткам ещё переносятся в новую версию. Следите за обновлениями!",
        reply_markup=keyboards.stock_entry_keyboard(),
    )


@router.callback_query()
async def handle_stock_callbacks(query: CallbackQuery) -> None:
    """Handle callbacks scoped to stock actions."""

    if not query.data:
        return

    try:
        payload = callbacks.parse(query.data, expected_namespace="builtin.stock")
    except ValueError:
        return

    if payload.action == "status":
        await query.answer("Модуль остатков в работе, планируем запуск после тестов.")
    elif payload.action == "feedback":
        await query.answer("Принято!", show_alert=False)
        if query.message:
            await query.message.answer(
                "Сообщите, какие сценарии по остаткам важнее всего, чтобы мы приоритизировали их."
            )
    else:
        await query.answer("Пока без действий.")


def register() -> None:
    """Register the stock router in the registry."""

    BotRouterRegistry.ensure("builtin.stock", lambda: router)
