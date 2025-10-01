from __future__ import annotations

import asyncio
import logging
from contextlib import closing
from html import escape
from typing import Callable, Mapping, Sequence

from aiogram import Bot

from dvorik.core import events
from dvorik.core.config import Config
from dvorik.db.conn import db
from dvorik.domain.models import LowStockRecord, Product
from dvorik.repo.product_repo import SQLiteProductRepo

logger = logging.getLogger(__name__)

_NOTIFICATION_EVENT = "bot.notifications.generated"

__all__ = ["setup_notification_bridge"]


def setup_notification_bridge(bot: Bot, config: Config) -> Callable[[], None]:
    """Subscribe to notification events and relay them to Telegram."""

    async def _listener(*, payload: Mapping[str, object]) -> None:
        await _dispatch_notification(bot, config, payload)

    events.subscribe(_NOTIFICATION_EVENT, _listener)
    logger.debug("Notification bridge subscribed to '%s'", _NOTIFICATION_EVENT)

    def _unsubscribe() -> None:
        events.unsubscribe(_NOTIFICATION_EVENT, _listener)
        logger.debug("Notification bridge unsubscribed from '%s'", _NOTIFICATION_EVENT)

    return _unsubscribe


async def _dispatch_notification(bot: Bot, config: Config, payload: Mapping[str, object]) -> None:
    recipients = _resolve_recipients(config)
    if not recipients:
        logger.debug("No recipients configured for notification payload: %s", payload)
        return

    notification_type = payload.get("type")
    text: str | None = None

    if notification_type == "threshold":
        record = payload.get("record")
        if isinstance(record, LowStockRecord):
            text = _format_threshold_notification(record, float(payload.get("threshold", 0)))
    elif notification_type == "to_skl":
        text = await _format_transfer_notification(payload)
    elif notification_type == "daily_digest":
        records = payload.get("records")
        if isinstance(records, Sequence):
            text = _format_digest_notification(tuple(r for r in records if isinstance(r, LowStockRecord)))
    else:
        logger.debug("Unsupported notification payload: %s", payload)
        return

    if not text:
        logger.debug("Notification payload produced no message: %s", payload)
        return

    for chat_id in recipients:
        try:
            await bot.send_message(chat_id, text)
        except Exception:  # pragma: no cover - network failures logged
            logger.exception("Failed to send notification to %s", chat_id)


def _resolve_recipients(config: Config) -> Sequence[int]:
    if config.super_admin_id is None:
        return ()
    return (config.super_admin_id,)


def _format_threshold_notification(record: LowStockRecord, threshold: float) -> str:
    product_name = escape(record.product.name or "Без названия")
    location_name = escape(record.location.title or record.location.code)
    qty_text = _format_quantity(record.qty_pack, record.product.unit)
    return (
        "<b>Низкий остаток</b>\n"
        f"{product_name} — {qty_text}\n"
        f"Локация: {location_name}\n"
        f"Порог: {threshold:.0f}"
    )


async def _format_transfer_notification(payload: Mapping[str, object]) -> str | None:
    product_id = payload.get("product_id")
    if not isinstance(product_id, int):
        return None

    qty = float(payload.get("qty") or 0)
    from_code = payload.get("from_location")
    to_code = payload.get("to_location")

    product_display, unit, from_title, to_title = await asyncio.to_thread(
        _lookup_transfer_context,
        product_id,
        from_code,
        to_code,
    )

    qty_text = _format_quantity(qty, unit)
    return (
        "<b>Движение на склад</b>\n"
        f"{product_display} — +{qty_text}\n"
        f"Из {escape(from_title)} → {escape(to_title)}"
    )


def _format_digest_notification(records: Sequence[LowStockRecord]) -> str | None:
    if not records:
        return None

    lines = ["<b>Сводка по остаткам</b>"]
    for record in records[:20]:
        product_name = escape(record.product.name or "Без названия")
        location = escape(record.location.title or record.location.code)
        qty_text = _format_quantity(record.qty_pack, record.product.unit)
        lines.append(f"• {product_name} — {qty_text} ({location})")
    return "\n".join(lines)


def _lookup_transfer_context(
    product_id: int,
    from_code: object,
    to_code: object,
) -> tuple[str, str | None, str, str]:
    with closing(db()) as conn:
        repo = SQLiteProductRepo(conn)
        product = repo.get(product_id)
        product_display = _describe_product(product_id, product)
        unit = product.unit if product else None
        from_title = _lookup_location_title(conn, from_code)
        to_title = _lookup_location_title(conn, to_code)
    return product_display, unit, from_title, to_title


def _describe_product(product_id: int, product: Product | None) -> str:
    if product is None:
        return escape(f"Товар #{product_id}")
    name = product.name or product.local_name or f"Товар #{product_id}"
    article = product.article or product.barcode
    if article:
        return f"{escape(name)} ({escape(article)})"
    return escape(name)


def _lookup_location_title(conn, code: object) -> str:
    if not code:
        return "не указано"
    try:
        row = conn.execute(
            "SELECT title FROM location WHERE code = ?",
            (str(code),),
        ).fetchone()
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to look up location title for %s", code)
        return str(code)
    if row and row["title"]:
        return str(row["title"])
    return str(code)


def _format_quantity(value: float, unit: str | None) -> str:
    qty = f"{value:.2f}".rstrip("0").rstrip(".")
    if unit:
        return f"{qty} {escape(unit)}"
    return qty
