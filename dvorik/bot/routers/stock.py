from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from html import escape
from typing import Sequence

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from dvorik.bot import callbacks, keyboards
from dvorik.bot.cards import RenderedCard, product_card
from dvorik.core.registry import BotRouterRegistry
from dvorik.db.conn import db
from dvorik.domain.models import LowStockRecord, Product, StockSnapshot
from dvorik.repo.product_repo import SQLiteProductRepo
from dvorik.repo.stock_repo import SQLiteStockRepo

__all__ = ["router", "register"]

router = Router(name="builtin_stock")

_SEARCH_LIMIT = 5
_LOW_STOCK_LIMIT = 8
_LOW_STOCK_THRESHOLD = 2.0


@router.message(Command("stock"))
async def handle_stock_entry(message: Message) -> None:
    """Entry point for stock lookups and shortcuts."""

    query = _extract_query(message)
    if query:
        await _handle_stock_search(message, query)
        return

    await message.answer(
        "Введите запрос, например <code>/stock кофе</code> или артикул товара.",
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

    if payload.action == "low":
        await query.answer("Собираю данные…", show_alert=False)
        summary = await _load_low_stock_summary()
        if query.message:
            await query.message.answer(summary)
    elif payload.action == "help":
        await query.answer("Инструкция отправлена", show_alert=False)
        if query.message:
            await query.message.answer(
                "Используйте <code>/stock &lt;поиск&gt;</code> для выдачи карточек товаров.\n"
                "Поддерживаются артикулы, штрихкоды и поиск по названию.",
            )
    else:
        await query.answer("Команда пока не поддерживается.")


def register() -> None:
    """Register the stock router in the registry."""

    BotRouterRegistry.ensure("builtin.stock", lambda: router)


def _extract_query(message: Message) -> str | None:
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    query = parts[1].strip()
    return query or None


async def _handle_stock_search(message: Message, query: str) -> None:
    safe_query = escape(query)
    await message.answer(f"Поиск по запросу <b>{safe_query}</b>…")

    cards = await asyncio.to_thread(_search_stock_cards, query, _SEARCH_LIMIT)
    if not cards:
        await message.answer("Совпадений не найдено.")
        return

    for card in cards:
        await _send_card(message, card)


async def _send_card(message: Message, card: RenderedCard) -> None:
    payload = card.as_message_kwargs()
    text = str(payload.get("text", ""))
    parse_mode = str(payload.get("parse_mode", "HTML"))
    photo = payload.get("photo")

    if photo:
        await message.answer_photo(photo=photo, caption=text, parse_mode=parse_mode)
    else:
        await message.answer(text=text, parse_mode=parse_mode)


def _search_stock_cards(query: str, limit: int) -> Sequence[RenderedCard]:
    with closing(db()) as conn:
        product_repo = SQLiteProductRepo(conn)
        stock_repo = SQLiteStockRepo(conn)

        products = list(product_repo.search_fts(query, limit=limit))
        if not products:
            fallback = _fallback_product_lookup(product_repo, conn, query)
            if fallback is not None:
                products = [fallback]

        product_ids = [product.id for product in products if product.id is not None]
        stock_snapshots = stock_repo.stock_by_location(product_ids=product_ids)
        stock_map = _group_stock_by_product(stock_snapshots)

        cards: list[RenderedCard] = []
        for product in products:
            if product.id is None:
                continue
            snapshots = stock_map.get(product.id, ())
            cards.append(product_card(product, stock=snapshots))
        return cards


def _fallback_product_lookup(
    repo: SQLiteProductRepo,
    conn: sqlite3.Connection,
    query: str,
) -> Product | None:
    query = query.strip()
    if not query:
        return None

    if query.isdigit():
        product = repo.get(int(query))
        if product is not None:
            return product

    row = conn.execute(
        """
        SELECT id FROM product
        WHERE article = :query OR barcode = :query
        ORDER BY id ASC
        LIMIT 1
        """,
        {"query": query},
    ).fetchone()
    if row is None:
        return None
    return repo.get(int(row["id"]))


def _group_stock_by_product(snapshots: Sequence[StockSnapshot]) -> dict[int, list[StockSnapshot]]:
    grouped: dict[int, list[StockSnapshot]] = {}
    for snapshot in snapshots:
        product_id = snapshot.product.id
        if product_id is None:
            continue
        grouped.setdefault(product_id, []).append(snapshot)
    return grouped


async def _load_low_stock_summary() -> str:
    records = await asyncio.to_thread(
        _load_low_stock_records,
        _LOW_STOCK_THRESHOLD,
        _LOW_STOCK_LIMIT,
    )
    if not records:
        return "<b>Низкие остатки</b>\nПо текущему порогу всё в порядке."

    lines = ["<b>Низкие остатки</b>"]
    for record in records:
        lines.append(_format_low_stock_record(record))
    return "\n".join(lines)


def _load_low_stock_records(threshold: float, limit: int) -> Sequence[LowStockRecord]:
    with closing(db()) as conn:
        repo = SQLiteStockRepo(conn)
        return repo.low_stock(threshold=threshold, limit=limit)


def _format_low_stock_record(record: LowStockRecord) -> str:
    product_name = escape(record.product.name or "Без названия")
    location_name = escape(record.location.title or record.location.code)
    qty_text = _format_quantity(record.qty_pack, record.product.unit)
    return f"• {product_name} — {qty_text} ({location_name})"


def _format_quantity(value: float, unit: str | None) -> str:
    qty = f"{value:.2f}".rstrip("0").rstrip(".")
    if unit:
        return f"{qty} {escape(unit)}"
    return qty
