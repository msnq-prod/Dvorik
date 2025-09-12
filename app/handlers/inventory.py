from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message

router = Router()


@router.callback_query(F.data == "inventory")
async def inv_root(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    locs = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    kb = botmod.locations_2col_keyboard(
        active_codes=locs,
        cb_for=lambda code: f"inv_loc|{code}",
        back_cb="home",
    )
    try:
        await cb.message.edit_text("Инвентаризация: выберите локацию", reply_markup=kb)
    except Exception:
        await cb.message.answer("Инвентаризация: выберите локацию", reply_markup=kb)
    await cb.answer()


def kb_inventory_location(conn, code: str, page: int = 1) -> InlineKeyboardMarkup:
    import app.bot as botmod
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM stock WHERE location_code=? AND qty_pack>0",
        (code,),
    ).fetchone()["c"]
    off = (page - 1) * botmod.STOCK_PAGE_SIZE
    rows = conn.execute(
        """
        SELECT p.id, p.name, p.local_name, s.qty_pack
        FROM stock s JOIN product p ON p.id=s.product_id
        WHERE s.location_code=? AND s.qty_pack>0
        ORDER BY p.name
        LIMIT ? OFFSET ?
        """,
        (code, botmod.STOCK_PAGE_SIZE, off),
    ).fetchall()

    # Строим кастомную клавиатуру: строка с названием (неактивная) и ниже ±1
    rows_kb: list[list[InlineKeyboardButton]] = []
    # Кнопка добавления товара через inline-поиск
    rows_kb.append([InlineKeyboardButton(text="➕ Добавить товар", switch_inline_query_current_chat="INV ")])

    for r in rows:
        disp_name = (r["local_name"] or r["name"]).strip()
        qty_val = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        disp_qty = int(qty_val) if qty_val.is_integer() else qty_val
        # Непереходная строка с названием и текущим остатком
        rows_kb.append([InlineKeyboardButton(text=f"{disp_name[:35]} | {disp_qty}", callback_data="noop")])
        # Кнопки -1 / +1 для немедленной корректировки
        pid = int(r["id"]) if "id" in r.keys() else int(r[0])
        rows_kb.append([
            InlineKeyboardButton(text="−1", callback_data=f"inv_adj|{pid}|{code}|-1|{page}"),
            InlineKeyboardButton(text="+1", callback_data=f"inv_adj|{pid}|{code}|1|{page}"),
        ])

    # Навигация по страницам
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv_loc|{code}|{page-1}"))
    if off + botmod.STOCK_PAGE_SIZE < count:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"inv_loc|{code}|{page+1}"))
    if nav:
        rows_kb.append(nav)
    # Кнопка назад
    rows_kb.append([InlineKeyboardButton(text="← Назад", callback_data="inventory")])

    return InlineKeyboardMarkup(inline_keyboard=rows_kb)


@router.callback_query(F.data.startswith("inv_loc|"))
async def inv_loc(cb: CallbackQuery):
    import app.bot as botmod
    parts = cb.data.split("|")
    code = parts[1]
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
    botmod._inv_loc_set(cb.from_user.id, code)
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    kb = kb_inventory_location(conn, code, page)
    conn.close()
    try:
        await cb.message.edit_text(f"Инвентаризация • {code} (стр. {page})", reply_markup=kb)
    except Exception:
        await cb.message.answer(f"Инвентаризация • {code} (стр. {page})", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("inv_open|"))
async def inv_open(cb: CallbackQuery):
    import app.bot as botmod
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    botmod._inv_loc_set(cb.from_user.id, code)
    await botmod._safe_cb_answer(cb)
    await inv_open_card(cb, pid, code)


async def inv_open_card(cb: CallbackQuery, pid: int, code: str):
    import os
    import app.bot as botmod
    conn = botmod.db()
    r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
    if not r:
        conn.close()
        await cb.answer("Товар не найден", show_alert=True)
        return
    row = conn.execute(
        "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
        (pid, code),
    ).fetchone()
    loc_qty = float(row["qty_pack"]) if row else 0.0
    disp_qty = int(loc_qty) if loc_qty.is_integer() else loc_qty
    caption = (
        botmod.product_caption(conn, r)
        + f"\n\n<b>Режим инвентаризации</b>: {code}\n"
        + f"Текущий остаток здесь: <b>{disp_qty}</b>"
    )
    ctx = botmod._inv_ctx(cb.from_user.id, pid)
    ctx["loc"] = code
    q = int(ctx.get("qty") or 1)
    from aiogram.utils.keyboard import InlineKeyboardBuilder

    b = InlineKeyboardBuilder()
    for d in (-5, -1, +1, +5):
        sign = "−" if d < 0 else "+"
        b.button(text=f"{sign}{abs(d)}", callback_data=f"inv_qty|{pid}|{d}")
    b.adjust(4)
    b.button(text=f"✅ {q}", callback_data=f"inv_qty_ok|{pid}|{q}")
    b.adjust(1)
    b.button(text=f"➕ Добавить в {code}", callback_data=f"inv_commit|{pid}|add")
    b.button(text=f"➖ Убавить из {code}", callback_data=f"inv_commit|{pid}|sub")
    b.adjust(1)
    b.button(text="← Назад", callback_data=f"inv_loc|{code}")
    kb = b.as_markup()
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
    from aiogram.enums import ParseMode
    try:
        if photo_path and os.path.isfile(photo_path):
            await cb.message.answer_photo(FSInputFile(photo_path), caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        elif photo_id:
            await cb.message.answer_photo(photo_id, caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        else:
            raise RuntimeError("no_photo")
    except Exception as e:
        await cb.message.answer(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        try:
            print(f"WARN: inventory photo send failed pid={pid}: {e}")
        except Exception:
            pass
    await cb.answer()


@router.message(F.text.regexp(r"^/inv_(\d+)$"))
async def cmd_inv(m: Message):
    import re
    import app.bot as botmod
    if not botmod.is_allowed(m.from_user.id, m.from_user.username):
        return
    pid = int(re.search(r"^/inv_(\\d+)$", m.text).group(1))
    code = botmod._inv_loc_get(m.from_user.id)
    if not code:
        await m.answer("Сначала выберите локацию в «Инвентаризация».")
        return
    await inv_open_card_message(m, pid, code)


@router.callback_query(F.data.startswith("inv_qty|"))
async def inv_qty_change(cb: CallbackQuery):
    import app.bot as botmod
    _, pid_s, delta_s = cb.data.split("|", 2)
    pid = int(pid_s)
    delta = int(delta_s)
    ctx = botmod._inv_ctx(cb.from_user.id, pid)
    cur = max(1, int(ctx.get("qty") or 1) + delta)
    ctx["qty"] = cur
    code = ctx.get("loc") or botmod._inv_loc_get(cb.from_user.id) or "UNK"
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    for d in (-5, -1, +1, +5):
        sign = "−" if d < 0 else "+"
        b.button(text=f"{sign}{abs(d)}", callback_data=f"inv_qty|{pid}|{d}")
    b.adjust(4)
    b.button(text=f"✅ {cur}", callback_data=f"inv_qty_ok|{pid}|{cur}")
    b.adjust(1)
    b.button(text=f"➕ Добавить в {code}", callback_data=f"inv_commit|{pid}|add")
    b.button(text=f"➖ Убавить из {code}", callback_data=f"inv_commit|{pid}|sub")
    b.adjust(1)
    b.button(text="← Назад", callback_data=f"inv_loc|{code}")
    await cb.message.edit_reply_markup(reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("inv_qty_ok|"))
async def inv_qty_ok(cb: CallbackQuery):
    import app.bot as botmod
    _, pid_s, val_s = cb.data.split("|", 2)
    pid = int(pid_s)
    val = int(val_s)
    botmod._inv_ctx(cb.from_user.id, pid)["qty"] = val
    await cb.answer("Количество сохранено")


@router.callback_query(F.data.startswith("inv_commit|"))
async def inv_commit(cb: CallbackQuery):
    import app.bot as botmod
    _, pid_s, mode = cb.data.split("|", 2)
    pid = int(pid_s)
    ctx = botmod._inv_ctx(cb.from_user.id, pid)
    code = ctx.get("loc") or botmod._inv_loc_get(cb.from_user.id)
    if not code:
        await cb.answer("Не выбрана локация.", show_alert=True)
        return
    qty = int(ctx.get("qty") or 1)
    delta = qty if mode == "add" else -qty
    conn = botmod.db()
    before = botmod.total_stock(conn, pid)
    ok, msg = botmod.adjust_location_qty(conn, pid, code, delta)
    conn.close()
    if not ok:
        await cb.answer(msg, show_alert=True)
        return
    after = botmod.total_stock(botmod.db(), pid)
    await botmod._notify_instant_thresholds(cb.bot, pid, before, after)
    if delta > 0:
        await botmod._notify_instant_to_skl(cb.bot, pid, code, delta)
        botmod._log_event_to_skl(botmod.db(), pid, code, delta)
    await cb.answer("Остаток обновлён")
    await inv_open_card(cb, pid, code)


@router.callback_query(F.data.startswith("inv_adj|"))
async def inv_adj(cb: CallbackQuery):
    """Мгновенная корректировка на ±1 из списка локации, без подменю."""
    import app.bot as botmod
    parts = cb.data.split("|")
    # inv_adj|pid|code|delta|page
    if len(parts) < 5:
        await cb.answer()
        return
    _, pid_s, code, delta_s, page_s = parts
    pid = int(pid_s)
    delta = int(delta_s)
    page = int(page_s) if page_s.isdigit() else 1
    # Применяем изменение
    conn = botmod.db()
    before = botmod.total_stock(conn, pid)
    ok, msg = botmod.adjust_location_qty(conn, pid, code, delta)
    conn.close()
    if not ok:
        await cb.answer(msg, show_alert=True)
        return
    after = botmod.total_stock(botmod.db(), pid)
    await botmod._notify_instant_thresholds(cb.bot, pid, before, after)
    if delta > 0:
        await botmod._notify_instant_to_skl(cb.bot, pid, code, delta)
        botmod._log_event_to_skl(botmod.db(), pid, code, delta)
    # Обновляем клавиатуру списка
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    kb = kb_inventory_location(conn, code, page)
    conn.close()
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer("Готово")
