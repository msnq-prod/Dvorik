from __future__ import annotations

import html
import sqlite3
from typing import Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import constants as const
from app.db import db
from app.ui.texts import product_caption
from app.services.move_ctx import get_ctx, ctx_badge, move_ctx


def kb_card(pid: int, uid: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🛒 В зал (−1)", callback_data=f"mv_hall|{pid}|1")
    b.adjust(1)
    try:
        conn = db()
        rows = conn.execute(
            "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
            (pid,),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass
    try:
        has_skl0 = any((r["location_code"] == const.HUB_LOCATION_CODE) for r in rows)
        non_skl0_dests = [
            r["location_code"]
            for r in rows
            if r["location_code"] not in (const.HUB_LOCATION_CODE, "HALL")
        ]
        if has_skl0 and len(non_skl0_dests) == 1:
            dst = non_skl0_dests[0]
            b.button(
                text=f"⇥ Всё из {const.HUB_LOCATION_CODE} → {dst}",
                callback_data=f"skl0all|{pid}|{dst}",
            )
            b.adjust(1)
    except Exception:
        pass
    b.button(text="🚚 Маршрут: откуда → куда", callback_data=f"route|{pid}")
    b.adjust(1)
    ctx = get_ctx(uid, pid)
    qty = int(ctx.get("qty") or 1)
    b.button(text="⚙️ Количество", callback_data=f"pick_qty|{pid}")
    b.adjust(1)
    b.button(text=f"Переместить {qty}", callback_data=f"commit_move|{pid}")
    b.adjust(1)
    b.button(text="← В меню", callback_data="home")
    return b.as_markup()


def build_card_for_user(pid: int, uid: int, conn: Optional[sqlite3.Connection] = None, product_row: Optional[sqlite3.Row] = None):
    close_later = False
    if conn is None:
        conn = db()
        close_later = True
    try:
        r = product_row or conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
        if not r:
            return None, None
        caption = product_caption(conn, r)
        ctx = move_ctx.get((uid, pid))
        if ctx:
            caption += f"\n\n<i>Выбрано для перемещения: {ctx_badge(ctx)}</i>"
        kb = kb_card(pid, uid)
        return caption, kb
    finally:
        if close_later:
            conn.close()


def build_admin_item_card(pid: int) -> tuple[Optional[str], Optional[InlineKeyboardMarkup]]:
    conn = db()
    try:
        r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
        if not r:
            return None, None
        caption = product_caption(conn, r)
        b = InlineKeyboardBuilder()
        b.button(text="✏️ Редактировать", callback_data=f"admin_edit|{pid}")
        b.button(text="🗑️ Удалить товар", callback_data=f"admin_del|{pid}")
        b.button(text=f"➕ В {const.HUB_LOCATION_CODE} (+1)", callback_data=f"admin_skl0|{pid}|add")
        b.button(text=f"➖ Из {const.HUB_LOCATION_CODE} (−1)", callback_data=f"admin_skl0|{pid}|sub")
        b.button(text="➕ На локацию…", callback_data=f"admin_add_loc|{pid}")
        b.button(text="↔️ Переместить", callback_data=f"route|{pid}")
        b.adjust(1)
        b.button(text="← Назад", callback_data="admin")
        return caption, b.as_markup()
    finally:
        conn.close()
