"""Inline keyboard factories for the Telegram bot."""

from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .callbacks import build

__all__ = [
    "core_entry_keyboard",
    "admin_entry_keyboard",
    "stock_entry_keyboard",
    "supply_entry_keyboard",
]


def core_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/start`` command."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Команды",
                    callback_data=build("builtin.core", "help"),
                )
            ],
        ]
    )


def admin_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/admin`` command."""

    namespace = "builtin.admin"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Активные задачи",
                    callback_data=build(namespace, "jobs"),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Заявки на доступ",
                    callback_data=build(namespace, "requests"),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Обновить",
                    callback_data=build(namespace, "refresh"),
                )
            ],
        ]
    )


def stock_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/stock`` command."""

    namespace = "builtin.stock"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Низкие остатки",
                    callback_data=build(namespace, "low"),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Справка",
                    callback_data=build(namespace, "help"),
                )
            ],
        ]
    )


def supply_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/supply`` command."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Уведомить о поставке",
                    callback_data=build("builtin.supply", "announce"),
                )
            ],
        ]
    )
