from __future__ import annotations

import asyncio
import datetime as dt
import sqlite3
from contextlib import closing
from html import escape
from typing import Sequence

from aiogram import Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.core.registry import BotRouterRegistry, JobRegistry
from dvorik.db.conn import db
from dvorik.domain.models import ScheduleTransferRequest
from dvorik.repo.schedule_repo import SQLiteScheduleRepo

__all__ = ["router", "register"]

router = Router(name="builtin_admin")

_REGISTRATION_LIMIT = 5
_TRANSFER_LIMIT = 5


@router.message(Command("admin"))
async def handle_admin_entry(message: Message) -> None:
    """Send admin overview containing job and request summaries."""

    overview = await asyncio.to_thread(_build_admin_overview)
    await message.answer(overview, reply_markup=keyboards.admin_entry_keyboard())


@router.callback_query()
async def handle_admin_callbacks(query: CallbackQuery) -> None:
    """React to callbacks associated with the admin namespace."""

    if not query.data:
        return

    try:
        payload = callbacks.parse(query.data, expected_namespace="builtin.admin")
    except ValueError:
        return

    if payload.action == "jobs":
        await query.answer("Задачи обновлены", show_alert=False)
        summary = await asyncio.to_thread(_format_jobs_section)
        if query.message:
            await query.message.answer(summary)
    elif payload.action == "requests":
        await query.answer("Заявки загружены", show_alert=False)
        details = await asyncio.to_thread(_build_requests_overview)
        if query.message:
            await query.message.answer(details)
    elif payload.action == "refresh":
        await query.answer("Обновляю сводку", show_alert=False)
        overview = await asyncio.to_thread(_build_admin_overview)
        if query.message:
            try:
                await query.message.edit_text(
                    overview,
                    reply_markup=keyboards.admin_entry_keyboard(),
                )
            except TelegramBadRequest:
                await query.message.answer(
                    overview,
                    reply_markup=keyboards.admin_entry_keyboard(),
                )
    else:
        await query.answer("Команда пока не поддерживается.")


def register() -> None:
    """Register the admin router in the registry."""

    BotRouterRegistry.ensure("builtin.admin", lambda: router)


def _build_admin_overview() -> str:
    sections = (
        _format_jobs_section(),
        _format_registration_section(limit=_REGISTRATION_LIMIT),
        _format_transfer_section(limit=_TRANSFER_LIMIT),
    )
    return "\n\n".join(filter(None, sections))


def _build_requests_overview() -> str:
    sections = (
        _format_registration_section(limit=10),
        _format_transfer_section(limit=10),
    )
    return "\n\n".join(filter(None, sections))


def _format_jobs_section() -> str:
    entries = sorted(JobRegistry.items())
    if not entries:
        return "<b>Фоновые задачи</b>\nНет зарегистрированных задач."

    lines = ["<b>Фоновые задачи</b>"]
    for key, job in entries:
        next_run = _format_next_run(getattr(job, "next_run", None))
        lines.append(f"• {escape(key)} — следующее исполнение: {next_run}")
    return "\n".join(lines)


def _format_registration_section(*, limit: int) -> str:
    rows = _load_registration_requests(limit=limit)
    if not rows:
        return "<b>Заявки на доступ</b>\nАктивных заявок нет."

    lines = ["<b>Заявки на доступ</b>"]
    for row in rows:
        lines.append(f"• {_format_registration_row(row)}")
    return "\n".join(lines)


def _format_transfer_section(*, limit: int) -> str:
    requests = _load_transfer_requests(limit=limit)
    if not requests:
        return "<b>Запросы на обмен сменами</b>\nНет активных запросов."

    lines = ["<b>Запросы на обмен сменами</b>"]
    for request in requests:
        lines.append(f"• {_format_transfer_request(request)}")
    return "\n".join(lines)


def _load_registration_requests(*, limit: int) -> Sequence[sqlite3.Row]:
    with closing(db()) as conn:
        cursor = conn.execute(
            """
            SELECT id, tg_id, username, first_name, last_name,
                   requested_role, status, created_at
            FROM registration_request
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT :limit
            """,
            {"limit": limit},
        )
        return cursor.fetchall()


def _load_transfer_requests(*, limit: int) -> Sequence[ScheduleTransferRequest]:
    with closing(db()) as conn:
        repo = SQLiteScheduleRepo(conn)
        requests = repo.transfer_requests(status="pending")
        return tuple(requests[:limit])


def _format_registration_row(row: sqlite3.Row) -> str:
    username = f"@{row['username']}" if row["username"] else "—"
    full_name = " ".join(part for part in (row["first_name"], row["last_name"]) if part).strip() or "—"
    created = _format_timestamp(row["created_at"])
    role = escape(str(row["requested_role"]))
    tg_id = row["tg_id"]
    identity = escape(f"{tg_id}" if tg_id else f"id={row['id']}")
    return f"{identity} — {escape(username)} ({escape(full_name)}), роль {role}, подана {created}"


def _format_transfer_request(request: ScheduleTransferRequest) -> str:
    created = _format_timestamp(request.created_at)
    expires = _format_timestamp(request.expires_at)
    return (
        f"{escape(request.date)} — {escape(str(request.from_tg_id))} → {escape(str(request.to_tg_id))}, "
        f"создано {created}, действует до {expires}"
    )


def _format_timestamp(value: str | None) -> str:
    if not value:
        return "неизвестно"
    try:
        dt_value = dt.datetime.fromisoformat(value)
    except ValueError:
        return escape(value)
    local = dt_value.astimezone(dt.datetime.now().astimezone().tzinfo)
    return local.strftime("%d.%m %H:%M")


def _format_next_run(next_run: dt.datetime | None) -> str:
    if not next_run:
        return "не запланировано"
    local = next_run.astimezone(dt.datetime.now().astimezone().tzinfo)
    return local.strftime("%d.%m %H:%M")
