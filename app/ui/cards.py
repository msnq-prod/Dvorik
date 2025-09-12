from __future__ import annotations

import html
import sqlite3
from typing import Optional, Dict

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.db import db
from app.ui.texts import product_caption
from app.services.move_ctx import get_ctx, ctx_badge, move_ctx


def kb_card(pid: int, uid: int, is_new: int = 0, need_local: bool = False, need_photo: bool = False) -> InlineKeyboardMarkup:
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
        has_skl0 = any((r["location_code"] == "SKL-0") for r in rows)
        non_skl0_dests = [r["location_code"] for r in rows if r["location_code"] not in ("SKL-0", "HALL")]
        if has_skl0 and len(non_skl0_dests) == 1:
            dst = non_skl0_dests[0]
            b.button(text=f"⇥ Всё из SKL-0 → {dst}", callback_data=f"skl0all|{pid}|{dst}")
            b.adjust(1)
    except Exception:
        pass
    b.button(text="🚚 Маршрут: откуда → куда", callback_data=f"route|{pid}")
    b.adjust(1)
    q = int(get_ctx(uid, pid).get("qty") or 1)
    b.button(text="−1", callback_data=f"qty_card|{pid}|-1")
    b.button(text="+1", callback_data=f"qty_card|{pid}|1")
    b.adjust(2)
    b.button(text=f"Переместить {q}", callback_data=f"commit_move|{pid}")
    b.adjust(1)
    if is_new:
        b.button(text="✅ Готово (снять NEW)", callback_data=f"unset_new|{pid}")
        b.adjust(1)
    if need_local:
        b.button(text="📝 Добавить название", callback_data=f"add_local_name|{pid}")
        b.adjust(1)
    if need_photo:
        b.button(text="🖼️ Добавить фото", callback_data=f"add_photo|{pid}")
        b.adjust(1)
    b.button(text="← Назад к списку", callback_data="complete_cards|1")
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
        local_val = (r["local_name"] or "") if "local_name" in r.keys() else ""
        photo_id = r["photo_file_id"] if "photo_file_id" in r.keys() else None
        photo_path = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else ""
        need_local = (local_val.strip() == "")
        need_photo = (not bool(photo_id) and not bool(photo_path))
        is_new = int(r["is_new"] or 0)
        if is_new and (not need_local) and (not need_photo):
            try:
                with conn:
                    conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
                is_new = 0
            except Exception:
                pass
        ctx = move_ctx.get((uid, pid))
        if ctx:
            caption += f"\n\n<i>Выбрано для перемещения: {ctx_badge(ctx)}</i>"
        kb = kb_card(pid, uid, is_new, need_local, need_photo)
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
        b.button(text="➕ В SKL-0 (+1)", callback_data=f"admin_skl0|{pid}|add")
        b.button(text="➖ Из SKL-0 (−1)", callback_data=f"admin_skl0|{pid}|sub")
        b.button(text="➕ На локацию…", callback_data=f"admin_add_loc|{pid}")
        b.button(text="↔️ Переместить", callback_data=f"route|{pid}")
        b.button(text="📄 Открыть карточку", callback_data=f"open|{pid}")
        b.adjust(1)
        b.button(text="← Назад", callback_data="admin")
        return caption, b.as_markup()
    finally:
        conn.close()
