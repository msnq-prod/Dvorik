from __future__ import annotations

import re
from typing import Optional

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
)
from app.ui.states import AdminCreate

router = Router()


def _gen_article() -> str:
    import datetime as _dt
    return "A-" + _dt.datetime.now().strftime("%Y%m%d%H%M%S")


@router.message(F.text.regexp(r"^/admin_new\b"))
async def admin_new_start(m: Message, state: FSMContext):
    import marm_bot as botmod
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    # Extract proposed name from the command tail
    mtext = m.text or ""
    name = mtext.split(" ", 1)[1].strip() if " " in mtext else ""
    if not name:
        name = "Новый товар"
    await state.set_state(botmod.AdminCreate.wait_article)
    await state.update_data(_new_name=name)
    await m.answer(
        "Создание товара.\nВведите артикул (или '-' для авто-генерации).",
        reply_markup=None,
    )


@router.message(AdminCreate.wait_article, F.text)
async def admin_new_article(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.AdminCreate.wait_article.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        return
    article_in = (m.text or "").strip()
    if article_in == "-" or not article_in:
        article = _gen_article()
    else:
        if not botmod._looks_like_article(article_in):
            await m.answer("Артикул выглядит некорректно. Укажите другой или '-' для авто.")
            return
        article = article_in
    data = await state.get_data()
    name = data.get("_new_name") or "Новый товар"
    conn = botmod.db()
    pid: Optional[int] = None
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO product(article, name, is_new) VALUES (?,?,1)",
                (article, name),
            )
            pid = cur.lastrowid
    except Exception:
        conn.close()
        await m.answer("Не удалось создать товар. Возможно, такой артикул уже существует.")
        return
    finally:
        conn.close()
    await state.update_data(_pid=pid)
    # Предложить выбрать локацию сразу
    # Собираем список локаций и рисуем клавиатуру 2 колонки
    from app.ui.keyboards import locations_2col_keyboard
    conn = botmod.db()
    try:
        rows = conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()
    finally:
        conn.close()
    codes = [r["code"] for r in rows]
    kb = locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"admin_new_loc|{pid}|{code}",
        back_cb=None,
    )
    await m.answer(
        f"Создан товар #{pid}: {name}\nАртикул: {article}\nВыберите локацию для добавления остатка:",
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )
    await state.clear()


@router.callback_query(F.data.startswith("admin_new_loc|"))
async def admin_new_loc(cb: CallbackQuery, state: FSMContext):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    await state.set_state(botmod.AdminCreate.wait_qty)
    await state.update_data(pid=pid, loc=code)
    await cb.message.edit_text(
        f"Введите количество для добавления на {code} (например: 1 или 2.5)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data=f"admin_item|{pid}")]]
        ),
    )
    await cb.answer()


def _parse_qty(s: str) -> Optional[float]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


@router.message(AdminCreate.wait_qty, F.text)
async def admin_new_qty(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.AdminCreate.wait_qty.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    data = await state.get_data()
    pid = int(data.get("pid"))
    loc = str(data.get("loc"))
    qty = _parse_qty(m.text or "")
    if qty is None or qty <= 0:
        await m.answer("Введите положительное число (например: 1 или 2.5)")
        return
    conn = botmod.db()
    ok, msg = botmod.adjust_location_qty(conn, pid, loc, qty)
    conn.close()
    if not ok:
        await m.answer(msg)
        return
    await m.answer(
        f"Добавлено {qty} на {loc} ✅",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← К товару", callback_data=f"admin_item|{pid}")]]
        ),
    )
    await state.clear()


@router.callback_query(F.data.startswith("admin_add_loc|"))
async def admin_add_loc(cb: CallbackQuery):
    """Из карточки админа — быстрый выбор локации для добавления."""
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    from app.ui.keyboards import locations_2col_keyboard
    conn = botmod.db()
    try:
        rows = conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()
    finally:
        conn.close()
    codes = [r["code"] for r in rows]
    kb = locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"admin_add_loc_chosen|{pid}|{code}",
        back_cb=f"admin_item|{pid}",
    )
    await cb.message.edit_text("Выберите локацию для добавления:", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_add_loc_chosen|"))
async def admin_add_loc_chosen(cb: CallbackQuery, state: FSMContext):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s, code = cb.data.split("|", 2)
    pid = int(pid_s)
    await state.set_state(botmod.AdminCreate.wait_qty)
    await state.update_data(pid=pid, loc=code)
    await cb.message.edit_text(
        f"Введите количество для добавления на {code} (например: 1 или 2.5)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data=f"admin_item|{pid}")]]
        ),
    )
    await cb.answer()


@router.message(F.text.regexp(r"^/admin_set\b"))
async def admin_set_field(m: Message):
    """Быстрая правка поля товара: /admin_set <id> <field>=<value>

    Примеры:
      /admin_set 123 name=Новая надпись
      /admin_set 123 local_name=Короткое имя
      /admin_set 123 brand_country=Производитель/страна
      /admin_set 123 article=ABC-123
      /admin_set 123 is_new=0
    """
    import marm_bot as botmod
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    text = (m.text or "").strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Формат: /admin_set <id> <поле>=<значение>")
        return
    try:
        pid = int(parts[1])
    except Exception:
        await m.answer("Некорректный id")
        return
    assign = parts[2]
    if "=" not in assign:
        await m.answer("Ожидалось <поле>=<значение>")
        return
    field, value = assign.split("=", 1)
    field = field.strip()
    value = value.strip()
    allowed = {"article", "name", "local_name", "brand_country", "is_new"}
    if field not in allowed:
        await m.answer("Можно менять: article, name, local_name, brand_country, is_new")
        return
    if field == "article" and not botmod._looks_like_article(value):
        await m.answer("Артикул выглядит некорректно")
        return
    conn = botmod.db()
    try:
        with conn:
            if field == "is_new":
                try:
                    ival = 1 if str(value).strip() in ("1", "true", "True", "да", "+") else 0
                except Exception:
                    ival = 0
                conn.execute("UPDATE product SET is_new=? WHERE id=?", (ival, pid))
            elif field == "name":
                conn.execute("UPDATE product SET name=? WHERE id=?", (value, pid))
                conn.execute("UPDATE stock SET name=? WHERE product_id=?", (value, pid))
            elif field == "local_name":
                conn.execute("UPDATE product SET local_name=? WHERE id=?", (value, pid))
                conn.execute("UPDATE stock SET local_name=? WHERE product_id=?", (value, pid))
            else:
                conn.execute(f"UPDATE product SET {field}=? WHERE id=?", (value, pid))
    except Exception as e:
        conn.close()
        await m.answer(f"Не удалось изменить: {e}")
        return
    finally:
        conn.close()
    await m.answer(
        "Сохранено ✅",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← К товару", callback_data=f"admin_item|{pid}")]]
        ),
    )
