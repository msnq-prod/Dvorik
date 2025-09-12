from __future__ import annotations

import os
import re
from typing import Optional

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

router = Router()


@router.message(F.text.regexp(r"^/open_(\d+)$"))
async def cmd_open(m: Message):
    import marm_bot as botmod
    if not botmod.is_allowed(m.from_user.id, m.from_user.username):
        return
    pid = int(re.search(r"^/open_(\d+)$", m.text).group(1))
    conn = botmod.db()
    r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
    if not r:
        conn.close()
        await m.answer("Товар не найден")
        return
    caption, kb = botmod.build_card_for_user(pid, m.from_user.id, conn=conn, product_row=r)
    photo_id = r["photo_file_id"]
    photo_path = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else None
    if (not photo_path or not os.path.isfile(photo_path)) and photo_id:
        try:
            photo_path = await botmod._ensure_local_photo(m.bot, pid, photo_id)
        except Exception:
            pass
    conn.close()
    # Send photo if available; gracefully fall back to text on any error
    try:
        if photo_path and os.path.isfile(photo_path):
            await m.answer_photo(
                FSInputFile(photo_path), caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML
            )
            return
        if photo_id:
            await m.answer_photo(photo_id, caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
            return
    except Exception as e:
        # If Telegram rejects the photo/caption for any reason, show text-only card
        try:
            print(f"WARN: sending photo for pid={pid} failed: {e}")
        except Exception:
            pass
    await m.answer(caption, reply_markup=kb, parse_mode=ParseMode.HTML)


@router.message(F.text.regexp(r"^/admin_(\d+)$"))
async def cmd_admin_item(m: Message):
    import marm_bot as botmod
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    pid = int(re.search(r"^/admin_(\d+)$", m.text).group(1))
    caption, kb = botmod.build_admin_item_card(pid)
    if caption is None:
        await m.answer("Товар не найден")
        return
    await m.answer(caption, reply_markup=kb, parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("open|"))
async def open_card(cb: CallbackQuery):
    import marm_bot as botmod
    pid = botmod._extract_pid_from_cbdata(cb.data)
    if pid is None:
        await cb.answer("Не удалось определить товар", show_alert=True)
        return
    conn = botmod.db()
    r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
    if not r:
        conn.close()
        await cb.answer("Товар не найден", show_alert=True)
        return
    caption, kb = botmod.build_card_for_user(pid, cb.from_user.id, conn=conn, product_row=r)
    photo_id = r["photo_file_id"]
    photo_path = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else None
    if (not photo_path or not os.path.isfile(photo_path)) and photo_id:
        try:
            photo_path = await botmod._ensure_local_photo(cb.bot, pid, photo_id)
        except Exception:
            pass
    conn.close()
    try:
        await cb.message.delete()
    except Exception:
        pass
    try:
        if photo_path and os.path.isfile(photo_path):
            await cb.message.answer_photo(
                FSInputFile(photo_path), caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML
            )
        elif photo_id:
            await cb.message.answer_photo(
                photo_id, caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML
            )
        else:
            raise RuntimeError("no_photo")
    except Exception as e:
        # Fallback to text-only card if photo sending fails
        try:
            print(f"WARN: sending photo (cb) for pid={pid} failed: {e}")
        except Exception:
            pass
        try:
            await cb.message.edit_text(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            await cb.message.answer(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cb.answer()


@router.callback_query(F.data.startswith("pick_src|"))
async def pick_src(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    kb = botmod.kb_pick_src(conn, pid)
    conn.close()
    try:
        await cb.message.edit_text("Выберите источник (откуда забираем):", reply_markup=kb)
    except Exception:
        await cb.message.answer("Выберите источник (откуда забираем):", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("src_chosen|"))
async def src_chosen(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    botmod._ctx(cb.from_user.id, pid)["src"] = code
    await cb.answer(f"Источник: {code}")
    await open_card(cb)


@router.callback_query(F.data.startswith("pick_dst|"))
async def pick_dst(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    try:
        await cb.message.edit_text("Выберите назначение (куда кладём):", reply_markup=botmod.kb_pick_dst(pid))
    except Exception:
        await cb.message.answer("Выберите назначение (куда кладём):", reply_markup=botmod.kb_pick_dst(pid))
    await cb.answer()


@router.callback_query(F.data.startswith("route|"))
async def route_start(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    kb = botmod.kb_route_src(conn, pid)
    conn.close()
    try:
        await cb.message.edit_text("Маршрут: выберите источник (откуда):", reply_markup=kb)
    except Exception:
        await cb.message.answer("Маршрут: выберите источник (откуда):", reply_markup=kb)


@router.callback_query(F.data.startswith("route_src_chosen|"))
async def route_src_chosen(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    botmod._ctx(cb.from_user.id, pid)["src"] = code
    await botmod._safe_cb_answer(cb)
    try:
        await cb.message.edit_text("Маршрут: выберите назначение (куда):", reply_markup=botmod.kb_route_dst(pid))
    except Exception:
        await cb.message.answer("Маршрут: выберите назначение (куда):", reply_markup=botmod.kb_route_dst(pid))


@router.callback_query(F.data.startswith("route_dst_chosen|"))
async def route_dst_chosen(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    botmod._ctx(cb.from_user.id, pid)["dst"] = code
    await botmod._safe_cb_answer(cb)
    await open_card(cb)


@router.callback_query(F.data.startswith("admin_item|"))
async def admin_item(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
    if not r:
        conn.close()
        await cb.answer("Товар не найден", show_alert=True)
        return
    caption = botmod.product_caption(conn, r)
    conn.close()
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
    kb = b.as_markup()
    try:
        await cb.message.edit_text(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception:
        await cb.message.answer(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_del|"))
async def admin_del(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"admin_del_yes|{pid}")],
            [InlineKeyboardButton(text="← Отмена", callback_data=f"admin_item|{pid}")],
        ]
    )
    await cb.message.edit_text("Удалить товар безвозвратно?", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_del_yes|"))
async def admin_del_yes(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    with conn:
        conn.execute("DELETE FROM product WHERE id=?", (pid,))
    conn.close()
    await cb.answer("Товар удалён")
    await cb.message.edit_text(
        "Товар удалён.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin")]]
        ),
    )


@router.callback_query(F.data.startswith("admin_skl0|"))
async def admin_skl0(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s, mode = cb.data.split("|", 2)
    pid = int(pid_s)
    delta = 1 if mode == "add" else -1
    conn = botmod.db()
    ok, msg = botmod.adjust_location_qty(conn, pid, "SKL-0", delta)
    conn.close()
    if not ok:
        await cb.answer(msg, show_alert=True)
        return
    await cb.answer("Остаток обновлён")
    await admin_item(cb)


@router.callback_query(F.data.startswith("dst_hall|"))
async def dst_hall(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    botmod._ctx(cb.from_user.id, pid)["dst"] = "HALL"
    await cb.answer("Назначение: ЗАЛ")
    await open_card(cb)


@router.callback_query(F.data.startswith("dst_chosen|"))
async def dst_chosen(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    botmod._ctx(cb.from_user.id, pid)["dst"] = code
    await cb.answer(f"Назначение: {code}")
    await open_card(cb)


@router.callback_query(F.data.startswith("pick_qty|"))
async def pick_qty(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    q = int(botmod._ctx(cb.from_user.id, pid).get("qty") or 1)
    try:
        await cb.message.edit_text(
            f"Выбор количества {botmod._ctx_badge(botmod._ctx(cb.from_user.id, pid))}",
            reply_markup=botmod.kb_qty(pid, "CTX", q),
        )
    except Exception:
        await cb.message.answer(
            f"Выбор количества {botmod._ctx_badge(botmod._ctx(cb.from_user.id, pid))}",
            reply_markup=botmod.kb_qty(pid, "CTX", q),
        )
    await cb.answer()


@router.callback_query(F.data.startswith("qty|"))
async def qty_change(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, _dest_ignored, delta_s = cb.data.split("|", 3)
    pid = int(pid_s)
    delta = int(delta_s)
    ctx = botmod._ctx(cb.from_user.id, pid)
    cur = max(1, int(ctx.get("qty") or 1) + delta)
    ctx["qty"] = cur
    await cb.message.edit_reply_markup(reply_markup=botmod.kb_qty(pid, "CTX", cur))
    await cb.answer()


@router.callback_query(F.data.startswith("qty_card|"))
async def qty_change_on_card(cb: CallbackQuery):
    import marm_bot as botmod
    try:
        _, pid_s, delta_s = cb.data.split("|", 2)
        pid = int(pid_s)
        delta = int(delta_s)
    except Exception:
        await cb.answer("Неверные данные", show_alert=True)
        return
    ctx = botmod._ctx(cb.from_user.id, pid)
    cur = max(1, int(ctx.get("qty") or 1) + delta)
    ctx["qty"] = cur
    await open_card(cb)


@router.callback_query(F.data.startswith("qty_ok|"))
async def qty_ok(cb: CallbackQuery):
    import marm_bot as botmod
    parts = cb.data.split("|", 3)
    pid = int(parts[1])
    val = int(parts[3])
    botmod._ctx(cb.from_user.id, pid)["qty"] = val
    await cb.answer("Количество сохранено")
    await open_card(cb)


@router.callback_query(F.data.startswith("unset_new|"))
async def unset_new(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    with conn:
        conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
    conn.close()
    await cb.answer("Снята метка NEW")
    await open_card(cb)

@router.callback_query(F.data.startswith("commit_move|"))
async def commit_move(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    ctx = botmod._ctx(cb.from_user.id, pid)
    src, dst = ctx.get("src"), ctx.get("dst")
    qty = int(ctx.get("qty") or 1)
    if not src:
        await cb.answer("Выберите источник.", show_alert=True)
        return
    if not dst:
        await cb.answer("Выберите назначение.", show_alert=True)
        return
    conn = botmod.db()
    before = botmod.total_stock(conn, pid)
    ok, msg = botmod.move_specific(conn, pid, src, dst, qty)
    conn.close()
    if not ok:
        await cb.answer(msg, show_alert=True)
        return
    after = botmod.total_stock(botmod.db(), pid)
    await botmod._notify_instant_thresholds(cb.bot, pid, before, after)
    await botmod._notify_instant_to_skl(cb.bot, pid, dst, qty)
    botmod._log_event_to_skl(botmod.db(), pid, dst, qty)
    botmod.move_ctx.pop((cb.from_user.id, pid), None)
    await cb.answer("Перемещено")
    await open_card(cb)


@router.callback_query(F.data.startswith("skl0all|"))
async def skl0_all_to_single(cb: CallbackQuery):
    import marm_bot as botmod
    try:
        _, pid_s, dst = cb.data.split("|", 2)
        pid = int(pid_s)
    except Exception:
        await cb.answer("Неверные данные", show_alert=True)
        return
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    try:
        row = conn.execute(
            "SELECT qty_pack FROM stock WHERE product_id=? AND location_code='SKL-0'",
            (pid,),
        ).fetchone()
        have = float(row["qty_pack"]) if row else 0.0
        if have <= 0:
            await cb.answer("В SKL-0 пусто", show_alert=True)
            return
        before = botmod.total_stock(conn, pid)
        ok, msg = botmod.move_specific(conn, pid, "SKL-0", dst, have)
        if not ok:
            await cb.answer(msg, show_alert=True)
            return
    finally:
        conn.close()
    after = botmod.total_stock(botmod.db(), pid)
    await botmod._notify_instant_thresholds(cb.bot, pid, before, after)
    await botmod._notify_instant_to_skl(cb.bot, pid, dst, have)
    botmod._log_event_to_skl(botmod.db(), pid, dst, have)
    await cb.answer(f"Перемещено всё из SKL-0 → {dst}")
    await open_card(cb)


@router.callback_query(F.data.startswith("mv_hall|"))
async def mv_hall(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, qty_s = cb.data.split("|", 2)
    pid = int(pid_s)
    qty = int(qty_s)
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    rows = conn.execute(
        """
        SELECT location_code, qty_pack FROM stock
        WHERE product_id=? AND qty_pack>0 ORDER BY location_code
        """,
        (pid,),
    ).fetchall()
    if not rows:
        conn.close()
        await cb.answer("Нет остатков для списания.", show_alert=True)
        return
    if len(rows) == 1 and float(rows[0]["qty_pack"]) >= qty:
        ok, msg = botmod.move_specific(conn, pid, rows[0]["location_code"], "HALL", qty)
        conn.close()
        if not ok:
            await cb.answer(msg, show_alert=True)
            return
        await cb.answer("Списано в зал")
        await open_card(cb)
        return
    label = {}
    codes = []
    for r in rows:
        codes.append(r["location_code"])
        q = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        disp = int(q) if float(q).is_integer() else q
        label[r["location_code"]] = f"{r['location_code']} ({disp})"
    kb = botmod.locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"mv_hall_from|{pid}|{code}|{qty}",
        label_for=label,
        back_cb=f"open|{pid}",
    )
    conn.close()
    try:
        await cb.message.edit_text("С какой локации списать в зал?", reply_markup=kb)
    except Exception:
        await cb.message.answer("С какой локации списать в зал?", reply_markup=kb)


@router.callback_query(F.data.startswith("mv_hall_from|"))
async def mv_hall_from(cb: CallbackQuery):
    import marm_bot as botmod
    _, pid_s, src, qty_s = cb.data.split("|", 3)
    pid = int(pid_s)
    qty = int(qty_s)
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    ok, msg = botmod.move_specific(conn, pid, src, "HALL", qty)
    conn.close()
    if not ok:
        await cb.answer(msg, show_alert=True)
        return
    await cb.answer("Списано в зал")
    await open_card(cb)
