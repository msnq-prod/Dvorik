from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery

router = Router()


@router.callback_query(F.data == "notify")
async def cb_notify(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    kb = botmod.kb_notify(cb.from_user.id)
    await cb.message.edit_text(botmod.notify_text(), reply_markup=kb, parse_mode=botmod.ParseMode.HTML)


@router.callback_query(F.data.startswith("notif|"))
async def cb_notif_set(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    _, t, mode = cb.data.split("|", 2)
    if t not in ("zero", "last", "to_skl") or mode not in ("off", "daily", "instant"):
        return
    botmod._set_notify_mode(cb.from_user.id, t, mode)
    kb = botmod.kb_notify(cb.from_user.id)
    await cb.message.edit_text(botmod.notify_text(), reply_markup=kb, parse_mode=botmod.ParseMode.HTML)


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    # Ничего не делаем, просто закрываем спиннер
    try:
        await cb.answer("Выберите один из вариантов", show_alert=False)
    except Exception:
        pass
