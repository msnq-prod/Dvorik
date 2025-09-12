from __future__ import annotations

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton

from app.db import db
from app.ui.keyboards import kb_cards_page
from app.ui.states import CardFill
from app.services.photos import download_and_compress_photo

router = Router()


@router.callback_query(F.data.startswith("complete_cards|"))
async def complete_cards(cb: CallbackQuery):
    _, page_s = cb.data.split("|", 1)
    page = max(1, int(page_s))
    conn = db()
    kb = kb_cards_page(conn, page)
    conn.close()
    # Безопасно обновляем: если текущее сообщение — фото, edit_text упадёт
    try:
        await cb.message.edit_text("Незавершённые карточки:", reply_markup=kb)
    except Exception:
        try:
            await cb.message.delete()
        except Exception:
            pass
        await cb.message.answer("Незавершённые карточки:", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("add_local_name|"))
async def add_local_name(cb: CallbackQuery, state: FSMContext):
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await state.set_state(CardFill.wait_local_name)
    await state.update_data(pid=pid)
    await cb.message.answer("Введите локальное название сообщением.")
    await cb.answer()


@router.message(CardFill.wait_local_name, F.text)
async def save_local_name(m: Message, state: FSMContext):
    st = await state.get_state()
    if st != CardFill.wait_local_name.state:
        return
    data = await state.get_data()
    pid = int(data["pid"])
    conn = db()
    with conn:
        conn.execute("UPDATE product SET local_name=? WHERE id=?", (m.text.strip(), pid))
        r = conn.execute(
            "SELECT COALESCE(local_name,'' ) AS ln, COALESCE(photo_file_id,'') AS pf, COALESCE(photo_path,'') AS pp, COALESCE(is_new,0) AS nw FROM product WHERE id=?",
            (pid,),
        ).fetchone()
        have_local = bool((r["ln"] or "").strip())
        have_photo = bool((r["pf"] or r["pp"]))
        if int(r["nw"] or 0) and have_local and have_photo:
            conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
    conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад к карточке", callback_data=f"open|{pid}")]
    ])
    await m.answer("Локальное название сохранено ✅", reply_markup=kb)
    await state.clear()


@router.callback_query(F.data.startswith("add_photo|"))
async def add_photo(cb: CallbackQuery, state: FSMContext):
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await state.set_state(CardFill.wait_photo)
    await state.update_data(pid=pid)
    await cb.message.answer("Отправьте фотографию (как фото, не как файл). Я сожму её вдвое для быстрого показа.")
    await cb.answer()


@router.message(CardFill.wait_photo, F.photo)
async def save_photo(m: Message, state: FSMContext):
    st = await state.get_state()
    if st != CardFill.wait_photo.state:
        return
    data = await state.get_data()
    pid = int(data["pid"])
    file_id = m.photo[-1].file_id if m.photo else None
    rel_path = None
    try:
        if file_id:
            rel_path = await download_and_compress_photo(m.bot, file_id, pid)
    except Exception:
        rel_path = None
    conn = db()
    with conn:
        conn.execute(
            "UPDATE product SET photo_file_id=?, photo_path=? WHERE id=?",
            (file_id, rel_path, pid),
        )
        r = conn.execute(
            "SELECT COALESCE(local_name,'') AS ln, COALESCE(photo_file_id,'') AS pf, COALESCE(photo_path,'') AS pp, COALESCE(is_new,0) AS nw FROM product WHERE id=?",
            (pid,),
        ).fetchone()
        have_local = bool((r["ln"] or "").strip())
        have_photo = bool((r["pf"] or r["pp"]))
        if int(r["nw"] or 0) and have_local and have_photo:
            conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
    conn.close()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад к карточке", callback_data=f"open|{pid}")]
    ])
    await m.answer("Фото сохранено ✅", reply_markup=kb)
    await state.clear()
