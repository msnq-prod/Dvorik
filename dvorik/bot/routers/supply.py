from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_supply")


@router.message(Command("supply"))
async def handle_supply_entry(message: Message) -> None:
    """Placeholder entrypoint announcing upcoming supply workflows."""

    await message.answer(
        "Импорт поставок пока в процессе миграции. Эта команда заработает позже.",
        reply_markup=keyboards.supply_entry_keyboard(),
    )


@router.callback_query()
async def handle_supply_callbacks(query: CallbackQuery) -> None:
    """Handle callbacks linked to supply features."""

    if not query.data:
        return

    try:
        payload = callbacks.parse(query.data, expected_namespace="builtin.supply")
    except ValueError:
        return

    if payload.action == "status":
        await query.answer("Импорт поставок появится после переноса сервисов импорта.")
    elif payload.action == "feedback":
        await query.answer("Записали!")
        if query.message:
            await query.message.answer(
                "Сообщите, какие форматы поставок важны, чтобы мы учли это при запуске."
            )
    else:
        await query.answer("Эта функция скоро появится.")


def register() -> None:
    """Register the supply router in the registry."""

    BotRouterRegistry.ensure("builtin.supply", lambda: router)
