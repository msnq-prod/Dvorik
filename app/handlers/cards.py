from __future__ import annotations

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from app.ui.states import CardFill

router = Router()


@router.callback_query(F.data.startswith("complete_cards|"))
async def complete_cards(cb: CallbackQuery):
    import marm_bot as botmod
    _, page_s = cb.data.split("|", 1)
    page = max(1, int(page_s))
    conn = botmod.db()
    kb = botmod.kb_cards_page(conn, page)
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
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await state.set_state(botmod.CardFill.wait_local_name)
    await state.update_data(pid=pid)
    await cb.message.answer("Введите локальное название сообщением.")
    await cb.answer()


@router.message(CardFill.wait_local_name, F.text)
async def save_local_name(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.CardFill.wait_local_name.state:
        return
    # Делегируем в исходную реализацию для сохранения и ответа
    from marm_bot import save_local_name as _h
    return await _h(m, state)


@router.callback_query(F.data.startswith("add_photo|"))
async def add_photo(cb: CallbackQuery, state: FSMContext):
    import marm_bot as botmod
    _, pid_s = cb.data.split("|", 1)
    pid = int(pid_s)
    await state.set_state(botmod.CardFill.wait_photo)
    await state.update_data(pid=pid)
    await cb.message.answer("Отправьте фотографию (как фото, не как файл). Я сожму её вдвое для быстрого показа.")
    await cb.answer()


@router.message(CardFill.wait_photo, F.photo)
async def save_photo(m: Message, state: FSMContext):
    import marm_bot as botmod
    st = await state.get_state()
    if st != botmod.CardFill.wait_photo.state:
        return
    from marm_bot import save_photo as _h
    return await _h(m, state)
