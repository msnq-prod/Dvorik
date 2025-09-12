from __future__ import annotations

import html
from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from app.ui.states import AdminEdit

router = Router()


@router.callback_query(F.data.startswith("admin_edit|"))
async def admin_edit_menu(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    r = conn.execute(
        "SELECT article, name, local_name, brand_country FROM product WHERE id=?",
        (pid,),
    ).fetchone()
    conn.close()
    if not r:
        await cb.answer("Товар не найден", show_alert=True)
        return
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Артикул", callback_data=f"admin_edit_field|{pid}|article")
    b.button(text="✏️ Название", callback_data=f"admin_edit_field|{pid}|name")
    b.button(text="✏️ Локальное имя", callback_data=f"admin_edit_field|{pid}|local_name")
    b.button(text="✏️ Производитель/страна", callback_data=f"admin_edit_field|{pid}|brand_country")
    b.button(text="🖼️ Фото: заменить", callback_data=f"admin_edit_photo|{pid}")
    b.button(text="🧹 Фото: удалить", callback_data=f"admin_edit_clear_photo|{pid}")
    b.adjust(1)
    b.button(text="← Назад", callback_data=f"admin_item|{pid}")
    kb = b.as_markup()
    txt = (
        "Редактирование карточки:\n"
        f"Артикул: <b>{html.escape(r['article'] or '')}</b>\n"
        f"Название: <b>{html.escape(r['name'] or '')}</b>\n"
        f"Локальное имя: <b>{html.escape(r['local_name'] or '—')}</b>\n"
        f"Производитель/страна: <b>{html.escape(r['brand_country'] or '—')}</b>"
    )
    try:
        await cb.message.edit_text(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception:
        await cb.message.answer(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_edit_field|"))
async def admin_edit_field(cb: CallbackQuery, state: FSMContext):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s, field = cb.data.split("|", 2)
    pid = int(pid_s)
    fields = {
        "article": "артикул",
        "name": "название",
        "local_name": "локальное имя",
        "brand_country": "производитель/страна",
    }
    if field not in fields:
        await cb.answer("Неизвестное поле", show_alert=True)
        return
    await state.set_state(botmod.AdminEdit.wait_text)
    await state.update_data(pid=pid, field=field)
    await cb.message.answer(
        f"Введите новое значение для поля «{fields[field]}». Отправьте текст сообщением."
    )
    await cb.answer()


@router.message(AdminEdit.wait_text, F.text)
async def admin_edit_save_text(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.AdminEdit.wait_text.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    data = await state.get_data()
    pid = int(data.get("pid"))
    field = data.get("field")
    value = (m.text or "").strip()
    if not value:
        await m.answer("Пустое значение недопустимо.")
        return
    if field == "article" and not botmod._looks_like_article(value):
        await m.answer("Артикул выглядит некорректно. Попробуйте другой.")
        return
    conn = botmod.db()
    try:
        with conn:
            if field == "article":
                conn.execute("UPDATE product SET article=? WHERE id=?", (value, pid))
            elif field == "name":
                conn.execute("UPDATE product SET name=? WHERE id=?", (value, pid))
                conn.execute("UPDATE stock SET name=? WHERE product_id=?", (value, pid))
            elif field == "local_name":
                conn.execute("UPDATE product SET local_name=? WHERE id=?", (value, pid))
                conn.execute("UPDATE stock SET local_name=? WHERE product_id=?", (value, pid))
            elif field == "brand_country":
                conn.execute("UPDATE product SET brand_country=? WHERE id=?", (value, pid))
    except Exception:
        conn.close()
        await m.answer("Такой артикул уже существует. Значение не изменено.")
        return
    finally:
        conn.close()
    await m.answer(
        "Изменения сохранены ✅",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад к товару", callback_data=f"admin_item|{pid}")]]
        ),
    )
    await state.clear()


@router.callback_query(F.data.startswith("admin_edit_photo|"))
async def admin_edit_photo(cb: CallbackQuery, state: FSMContext):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await state.set_state(botmod.AdminEdit.wait_photo)
    await state.update_data(pid=pid)
    await cb.message.answer("Отправьте новую фотографию (как фото, не как файл).")
    await cb.answer()


@router.message(AdminEdit.wait_photo, F.photo)
async def admin_edit_save_photo(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.AdminEdit.wait_photo.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    data = await state.get_data()
    pid = int(data.get("pid"))
    file_id = m.photo[-1].file_id if m.photo else None
    rel_path = None
    try:
        if file_id:
            rel_path = await botmod._download_and_compress_photo(m.bot, file_id, pid)
    except Exception:
        rel_path = None
    conn = botmod.db()
    with conn:
        conn.execute(
            "UPDATE product SET photo_file_id=?, photo_path=? WHERE id=?",
            (file_id, rel_path, pid),
        )
    conn.close()
    await m.answer(
        "Фото обновлено ✅",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад к товару", callback_data=f"admin_item|{pid}")]]
        ),
    )
    await state.clear()


@router.callback_query(F.data.startswith("admin_edit_clear_photo|"))
async def admin_edit_clear_photo(cb: CallbackQuery):
    import marm_bot as botmod
    if not await botmod.require_admin(cb):
        return
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    conn = botmod.db()
    with conn:
        conn.execute(
            "UPDATE product SET photo_file_id=NULL, photo_path=NULL WHERE id=?",
            (pid,),
        )
    conn.close()
    await cb.answer("Фото удалено")
    await admin_edit_menu(cb)
