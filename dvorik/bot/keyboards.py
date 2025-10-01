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


def _coming_soon_keyboard(namespace: str) -> InlineKeyboardMarkup:
    """Return a shared keyboard notifying users about pending features."""

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Узнать статус",
                    callback_data=build(namespace, "status"),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Оставить отзыв",
                    callback_data=build(namespace, "feedback"),
                )
            ],
        ]
    )


def core_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/start`` command."""

    return _coming_soon_keyboard("builtin.core")


def admin_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/admin`` command."""

    return _coming_soon_keyboard("builtin.admin")


def stock_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/stock`` command."""

    return _coming_soon_keyboard("builtin.stock")


def supply_entry_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the ``/supply`` command."""

    return _coming_soon_keyboard("builtin.supply")
