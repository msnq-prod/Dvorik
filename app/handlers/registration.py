from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from app.ui.states import RegStates

router = Router()


@router.message(F.text)
async def reg_entrypoint(m: Message, state: FSMContext):
    """Fallback-вход в регистрацию: если пользователь не авторизован и не в процессе регистрации,
    запускаем сбор имени/фамилии.
    """
    import app.bot as botmod
    from app.ui.states import RegStates

    # Обрабатываем только личные чаты с ботом
    if m.chat.type != "private":
        return
    # Если уже есть доступ — ничего не делаем
    if botmod.is_allowed(m.from_user.id, m.from_user.username):
        return
    st = await state.get_state()
    if st in (RegStates.wait_first_name.state, RegStates.wait_last_name.state):
        return  # уже в процессе
    # Запустить процесс регистрации
    await state.set_state(RegStates.wait_first_name)
    await m.answer("Добро пожаловать! Для доступа введите ваше имя (например: Иван).")

async def _notify_admins_of_request(bot, text: str, cb_approve_admin: str, cb_approve_seller: str, cb_reject: str) -> int:
    """Send a registration request notification to all admins and the super admin.

    Returns the number of admins notified.
    """
    import app.bot as botmod
    conn = botmod.db()
    # Collect admin tg_ids from DB
    rows = conn.execute(
        "SELECT DISTINCT tg_id FROM user_role WHERE role='admin' AND tg_id IS NOT NULL"
    ).fetchall()
    conn.close()
    admin_ids = {r["tg_id"] for r in rows if r["tg_id"]}
    # Always include super admin
    if botmod.SUPER_ADMIN_ID:
        admin_ids.add(botmod.SUPER_ADMIN_ID)
    if not admin_ids:
        return 0
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👑 Админ", callback_data=cb_approve_admin), InlineKeyboardButton(text="🛒 Продавец", callback_data=cb_approve_seller)],
            [InlineKeyboardButton(text="❌ Отказать", callback_data=cb_reject)],
        ]
    )
    success = 0
    for uid in list(admin_ids):
        try:
            await bot.send_message(uid, text, reply_markup=kb)
            success += 1
        except Exception:
            # user hasn't started bot or blocked it; ignore
            pass
    return success


@router.message(RegStates.wait_first_name, F.text)
async def on_reg_first_name(m: Message, state: FSMContext):
    import app.bot as botmod
    from app.ui.states import RegStates

    st = await state.get_state()
    if st != RegStates.wait_first_name.state:
        return

    first = (m.text or "").strip()
    if not first or len(first) < 2:
        await m.answer("Введите имя (не короче 2 символов)")
        return
    await state.update_data(first_name=first)
    await state.set_state(RegStates.wait_last_name)
    await m.answer("Спасибо. Теперь введите фамилию")


@router.message(RegStates.wait_last_name, F.text)
async def on_reg_last_name(m: Message, state: FSMContext):
    import app.bot as botmod
    from app.ui.states import RegStates

    st = await state.get_state()
    if st != RegStates.wait_last_name.state:
        return

    last = (m.text or "").strip()
    if not last or len(last) < 2:
        await m.answer("Введите фамилию (не короче 2 символов)")
        return

    data = await state.get_data()
    first = (data.get("first_name") or "").strip()
    display_name = (first + " " + last).strip()

    # Upsert pending request in DB
    conn = botmod.db()
    with conn:
        # ensure table exists (in case migrations applied late)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS registration_request(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                requested_role TEXT NOT NULL DEFAULT 'admin',
                status TEXT NOT NULL CHECK(status IN ('pending','approved','declined','cancelled')) DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now','localtime'))
            )
            """
        )
        # cancel previous pending if any for the same tg_id
        conn.execute(
            "UPDATE registration_request SET status='cancelled' WHERE tg_id=? AND status='pending'",
            (m.from_user.id,),
        )
        conn.execute(
            "INSERT INTO registration_request(tg_id, username, first_name, last_name, requested_role, status) VALUES(?,?,?,?, 'admin', 'pending')",
            (m.from_user.id, botmod._norm_username(m.from_user.username), first, last),
        )
    conn.close()

    # Notify admins
    text = (
        "Заявка на доступ\n"
        f"Имя: {display_name}\n"
        f"Username: {botmod._norm_username(m.from_user.username) or '—'}\n"
        f"ID: {m.from_user.id}\n\n"
        "Выберите роль для добавления."
    )
    cb_yes_admin = f"reg_approve|{m.from_user.id}|admin"
    cb_yes_seller = f"reg_approve|{m.from_user.id}|seller"
    cb_no = f"reg_reject|{m.from_user.id}"
    notified = await _notify_admins_of_request(m.bot, text, cb_yes_admin, cb_yes_seller, cb_no)
    await m.answer(
        "Спасибо! Заявка отправлена администраторам. Вы получите уведомление после решения."
        + ("\n(заметка: ни одному администратору не удалось доставить уведомление; откройте Админ → Заявки на доступ)" if notified == 0 else "")
    )
    await state.clear()


@router.callback_query(F.data.startswith("reg_approve|"))
async def on_reg_approve(cb: CallbackQuery):
    import app.bot as botmod
    # Only admins may act
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True)
        return
    try:
        _, uid_s, role = cb.data.split("|", 2)
        uid = int(uid_s)
    except Exception:
        await cb.answer("Некорректные данные", show_alert=True)
        return
    conn = botmod.db()
    try:
        # Fetch latest pending request
        req = conn.execute(
            "SELECT * FROM registration_request WHERE tg_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
            (uid,),
        ).fetchone()
        # Derive display name
        disp = None
        if req:
            f = (req["first_name"] or "").strip(); l = (req["last_name"] or "").strip()
            disp = (f + " " + l).strip() or None
        # Upsert role
        username = None
        try:
            # use current username from TG if possible by sending a dummy getChat? Not needed
            username = None
        except Exception:
            pass
        # We cannot fetch username here reliably; use saved one
        if req:
            username = req["username"]
        with conn:
            if role == "admin":
                conn.execute(
                    """
                    INSERT INTO user_role(tg_id, username, display_name, role)
                    VALUES(?, ?, ?, 'admin')
                    ON CONFLICT(tg_id, role) DO UPDATE SET
                        username=COALESCE(excluded.username, user_role.username),
                        display_name=COALESCE(excluded.display_name, user_role.display_name)
                    """,
                    (uid, username, disp),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO user_role(tg_id, username, display_name, role)
                    VALUES(?, ?, ?, 'seller')
                    ON CONFLICT(tg_id, role) DO UPDATE SET
                        username=COALESCE(excluded.username, user_role.username),
                        display_name=COALESCE(excluded.display_name, user_role.display_name)
                    """,
                    (uid, username, disp),
                )
            # Mark request approved
            conn.execute(
                "UPDATE registration_request SET status='approved' WHERE tg_id=? AND status='pending'",
                (uid,),
            )
    finally:
        conn.close()
    try:
        await cb.answer("Готово")
    except Exception:
        pass
    # Inform the user
    role_label = "администратор" if role == "admin" else "продавец"
    try:
        await cb.message.bot.send_message(uid, f"Ваша заявка одобрена. Роль: {role_label}. Нажмите /start")
    except Exception:
        pass
    # Update the admin’s message to reflect action
    try:
        await cb.message.edit_text(cb.message.text + f"\n\n✅ Одобрено ({role_label})")
    except Exception:
        pass


@router.callback_query(F.data.startswith("reg_reject|"))
async def on_reg_reject(cb: CallbackQuery):
    import app.bot as botmod
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True)
        return
    try:
        _, uid_s = cb.data.split("|", 1)
        uid = int(uid_s)
    except Exception:
        await cb.answer("Некорректные данные", show_alert=True)
        return
    conn = botmod.db()
    try:
        with conn:
            conn.execute(
                "UPDATE registration_request SET status='declined' WHERE tg_id=? AND status='pending'",
                (uid,),
            )
    finally:
        conn.close()
    try:
        await cb.answer("Отклонено")
    except Exception:
        pass
    try:
        await cb.message.bot.send_message(uid, "К сожалению, заявка отклонена.")
    except Exception:
        pass
    try:
        await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено")
    except Exception:
        pass
