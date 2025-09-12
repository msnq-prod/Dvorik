from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

router = Router()


@router.callback_query(F.data == "stock")
async def cb_stock(cb: CallbackQuery):
    import app.bot as botmod
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    locs = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    kb = botmod.locations_2col_keyboard(
        active_codes=locs,
        cb_for=lambda code: f"stock_loc|{code}|1",
        back_cb="home",
    )
    try:
        await cb.message.edit_text("Выберите локацию:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Выберите локацию:", reply_markup=kb)


@router.callback_query(F.data.startswith("stock_loc|"))
async def stock_loc(cb: CallbackQuery):
    import app.bot as botmod
    parts = cb.data.split("|")
    code = parts[1]
    page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
    conn = botmod.db()
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM stock WHERE location_code=? AND qty_pack>0",
        (code,),
    ).fetchone()["c"]
    if count == 0:
        conn.close()
        try:
            await cb.message.edit_text(
                f"{code}: пусто",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="stock")]]
                ),
            )
        except Exception:
            await cb.message.answer(
                f"{code}: пусто",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="stock")]]
                ),
            )
        await cb.answer()
        return
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
    conn.close()
    items = []
    for r in rows:
        disp_name = (r["local_name"] or r["name"]).strip()
        qty_val = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        disp_qty = int(qty_val) if qty_val.is_integer() else qty_val
        items.append((f"{disp_name[:35]} | {disp_qty}", f"open|{r['id']}"))
    kb = botmod.grid_buttons(items, per_row=1, back_cb="stock")
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"stock_loc|{code}|{page-1}"))
    if off + botmod.STOCK_PAGE_SIZE < count:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"stock_loc|{code}|{page+1}"))
    if nav:
        kb.inline_keyboard.append(nav)
    try:
        await cb.message.edit_text(f"Товары в {code} (стр. {page}):", reply_markup=kb)
    except Exception:
        await cb.message.answer(f"Товары в {code} (стр. {page}):", reply_markup=kb)
