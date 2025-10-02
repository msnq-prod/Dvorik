from __future__ import annotations

from datetime import timedelta

from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core import scheduler
from dvorik.core.registry import BotRouterRegistry

__all__ = ["router", "register"]

router = Router(name="builtin_core")

_SCHEDULER_STALE_THRESHOLD = timedelta(seconds=90)


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


def _format_delta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    parts.append(f"{seconds} с")
    return " ".join(parts)


@router.message(Command("ping"))
async def handle_ping(message: Message) -> None:
    """Report scheduler heartbeat information for quick diagnostics."""

    heartbeat = scheduler.heartbeat()
    if heartbeat is None:
        await message.answer(
            "Планировщик ещё не запускался. Проверьте, что бот был инициализирован.",
        )
        return

    age = scheduler.heartbeat_age()
    if age is None:
        await message.answer("Не удалось определить состояние планировщика.")
        return

    heartbeat_text = heartbeat.astimezone().isoformat(timespec="seconds")
    age_text = _format_delta(age)

    if age > _SCHEDULER_STALE_THRESHOLD:
        await message.answer(
            "⚠️ Планировщик выглядит остановившимся: последний цикл был "
            f"{age_text} назад ({heartbeat_text}).",
        )
        return

    await message.answer(
        "✅ Планировщик активен. Последний цикл был "
        f"{age_text} назад ({heartbeat_text}).",
    )


def register() -> None:
    """Register the core router in the registry."""

    BotRouterRegistry.ensure("builtin.core", lambda: router)
