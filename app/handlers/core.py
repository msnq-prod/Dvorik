from __future__ import annotations

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext

router = Router()


@router.message(CommandStart())
async def on_start(m: Message, state: FSMContext):
    import app.bot as botmod
    # Если пользователь уже есть в ролях по username, но без tg_id — обновим
    try:
        un = botmod._norm_username(m.from_user.username)
        if un:
            conn = botmod.db()
            with conn:
                conn.execute(
                    "UPDATE user_role SET tg_id=? WHERE tg_id IS NULL AND LOWER(username)=LOWER(?)",
                    (m.from_user.id, un),
                )
            conn.close()
    except Exception:
        pass
    # Проверка доступа
    if not botmod.is_allowed(m.from_user.id, m.from_user.username):
        # Запускаем онбординг регистрации: имя → фамилия → уведомление админам
        from app.ui.states import RegStates
        await state.set_state(RegStates.wait_first_name)
        await m.answer(
            "Добро пожаловать! Для доступа к боту отправьте заявку.\n"
            "Пожалуйста, введите ваше имя (например: Иван)."
        )
        return
    await m.answer("Главное меню:", reply_markup=botmod.kb_main(m.from_user.id, m.from_user.username))


@router.callback_query(lambda c: c.data == "home")
async def cb_home(cb: CallbackQuery):
    import app.bot as botmod
    await cb.message.edit_text(
        "Главное меню:", reply_markup=botmod.kb_main(cb.from_user.id, cb.from_user.username)
    )
    await cb.answer()


@router.callback_query(lambda c: c.data == "noop")
async def cb_noop(cb: CallbackQuery):
    # Ничего не делаем, просто закрываем "часики"
    try:
        await cb.answer()
    except Exception:
        pass
