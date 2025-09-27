from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.fsm.context import FSMContext
from app.ui.states import AdminStates

router = Router()


@router.callback_query(F.data == "admin")
async def cb_admin(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    rows = [
        [InlineKeyboardButton(text="🔎 Поиск админа", switch_inline_query_current_chat="ADM ")],
        [InlineKeyboardButton(text="👥 Продавцы", callback_data="admin_sellers")],
        [InlineKeyboardButton(text="👤 Заявки на доступ", callback_data="admin_reg_requests")],
        [InlineKeyboardButton(text="🔔 Уведомления", callback_data="notify")],
        [InlineKeyboardButton(text="📊 Отчёты", callback_data="reports")],
        [InlineKeyboardButton(text="📦 Поставка", callback_data="supply")],
    ]
    if botmod.is_super_admin(cb.from_user.id, cb.from_user.username):
        rows.insert(1, [InlineKeyboardButton(text="🛡️ Админы", callback_data="admin_admins")])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="home")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await cb.message.edit_text("Панель администратора:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Панель администратора:", reply_markup=kb)


@router.callback_query(F.data == "admin_reg_requests")
async def admin_reg_requests(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    conn = botmod.db()
    rows = conn.execute(
        "SELECT id, tg_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln, COALESCE(username,'') AS un, created_at FROM registration_request WHERE status='pending' ORDER BY id DESC LIMIT 50"
    ).fetchall()
    conn.close()
    if not rows:
        try:
            await cb.message.edit_text(
                "Заявок нет.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin")]]),
            )
        except Exception:
            await cb.message.answer(
                "Заявок нет.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin")]]),
            )
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{(r['fn']+' '+r['ln']).strip() or r['un'] or str(r['tg_id'])}", callback_data=f"admin_reg_pick|{int(r['tg_id'])}")] for r in rows
        ] + [[InlineKeyboardButton(text="← Назад", callback_data="admin")]]
    )
    try:
        await cb.message.edit_text("Ожидающие заявки на доступ:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Ожидающие заявки на доступ:", reply_markup=kb)


@router.callback_query(F.data.startswith("admin_reg_pick|"))
async def admin_reg_pick(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    try:
        _, uid_s = cb.data.split("|", 1)
        uid = int(uid_s)
    except Exception:
        await cb.answer("Некорректно", show_alert=True)
        return
    conn = botmod.db()
    row = conn.execute(
        "SELECT id, tg_id, COALESCE(first_name,'') AS fn, COALESCE(last_name,'') AS ln, COALESCE(username,'') AS un, created_at FROM registration_request WHERE status='pending' AND tg_id=? ORDER BY id DESC LIMIT 1",
        (uid,),
    ).fetchone()
    conn.close()
    if not row:
        await cb.answer("Заявка не найдена", show_alert=True); return
    name = (row["fn"] + " " + row["ln"]).strip() or row["un"] or str(row["tg_id"])
    text = (
        f"Заявка от: {name}\n"
        f"Username: {row['un'] or '—'}\n"
        f"ID: {row['tg_id']}\n\nВыберите действие:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Добавить админа", callback_data=f"reg_approve|{uid}|admin")],
        [InlineKeyboardButton(text="🛒 Добавить продавца", callback_data=f"reg_approve|{uid}|seller")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reg_reject|{uid}")],
        [InlineKeyboardButton(text="← Назад", callback_data="admin_reg_requests")],
    ])
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "admin_admins")
async def admin_admins(cb: CallbackQuery):
    import app.bot as botmod
    if not botmod.is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа (только главный админ)", show_alert=True)
        return
    await botmod._safe_cb_answer(cb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить админа", callback_data="admin_admin_add")],
        [InlineKeyboardButton(text="➖ Удалить админа", callback_data="admin_admin_del")],
        [InlineKeyboardButton(text="📋 Список админов", callback_data="admin_admin_list")],
        [InlineKeyboardButton(text="← Назад", callback_data="admin")],
    ])
    try:
        await cb.message.edit_text("Управление администраторами:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Управление администраторами:", reply_markup=kb)


@router.callback_query(F.data == "admin_sellers")
async def admin_sellers(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await botmod._safe_cb_answer(cb)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить продавца", callback_data="admin_seller_add")],
        [InlineKeyboardButton(text="📋 Список продавцов", callback_data="admin_seller_list")],
        [InlineKeyboardButton(text="← Назад", callback_data="admin")],
    ])
    try:
        await cb.message.edit_text("Управление продавцами:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Управление продавцами:", reply_markup=kb)


@router.callback_query(F.data == "admin_seller_add")
async def admin_seller_add(cb: CallbackQuery, state: FSMContext):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    await state.set_state(botmod.AdminStates.wait_seller_add)
    await cb.message.answer("Перешлите сообщение пользователя ИЛИ отправьте его тэг (@username)")
    await cb.answer()


@router.message(AdminStates.wait_seller_add, F.forward_from)
async def on_admin_seller_add_forward(m: Message, state: FSMContext):
    import app.bot as botmod
    # Обработчик будет вызван и на другие состояния; фильтрация по состоянию
    st = await state.get_state()
    if st != botmod.AdminStates.wait_seller_add.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        return
    if m.forward_from:
        fid = m.forward_from.id
        uname = botmod._norm_username(m.forward_from.username)
        conn = botmod.db()
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO user_role(tg_id, username, role) VALUES (?,?, 'seller')",
                (fid, uname),
            )
        conn.close()
        await m.answer(f"Добавлен продавец по пересланному сообщению: id={fid} {uname or ''}")
        await state.clear()


@router.message(AdminStates.wait_seller_add, F.text)
async def on_admin_seller_add_text(m: Message, state: FSMContext):
    import app.bot as botmod
    st = await state.get_state()
    if st != botmod.AdminStates.wait_seller_add.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа (только для админа)")
        return
    tag = botmod._norm_username(m.text)
    if not tag or not tag.startswith("@") or len(tag) < 2:
        await m.answer("Неверный тэг. Укажите @username")
        return
    conn = botmod.db()
    with conn:
        conn.execute("INSERT OR IGNORE INTO user_role(username, role) VALUES (?, 'seller')", (tag,))
    conn.close()
    await m.answer(f"Добавлен продавец: {tag}")
    await state.clear()


@router.callback_query(F.data == "admin_seller_list")
async def admin_seller_list(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    conn = botmod.db()
    rows = conn.execute(
        "SELECT id, COALESCE(display_name, '') AS dname, COALESCE(username, '') AS uname, COALESCE(tg_id, 0) AS tid "
        "FROM user_role WHERE role='seller' ORDER BY COALESCE(display_name, username)"
    ).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Список пуст", show_alert=True)
        return

    def _label(r):
        dn = (r['dname'] or '').strip()
        un = (r['uname'] or '').strip()
        if dn and un and dn != un:
            return f"{dn} ({un})"
        return dn or un or "(без имени)"

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=_label(r), callback_data=f"admin_seller_manage|{int(r['id'])}")] for r in rows
        ] + [[InlineKeyboardButton(text="← Назад", callback_data="admin_sellers")]]
    )
    try:
        await cb.message.edit_text("Выберите продавца:", reply_markup=kb)
    except Exception:
        await cb.message.answer("Выберите продавца:", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_seller_manage|"))
async def admin_seller_manage(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    try:
        _, sid_s = cb.data.split("|", 1)
        sid = int(sid_s)
    except Exception:
        await cb.answer("Некорректно", show_alert=True)
        return
    conn = botmod.db()
    row = conn.execute(
        "SELECT id, COALESCE(display_name, '') AS dname, COALESCE(username, '') AS uname, COALESCE(tg_id, 0) AS tid "
        "FROM user_role WHERE id=? AND role='seller'",
        (sid,),
    ).fetchone()
    conn.close()
    if not row:
        await cb.answer("Не найдено", show_alert=True)
        return

    dn = (row['dname'] or '').strip()
    un = (row['uname'] or '').strip()
    base = dn or un or "(без имени)"
    if dn and un and dn != un:
        base = f"{dn}\n@{un.lstrip('@')}"
    elif un and not un.startswith("@"):
        base = f"{base}\n@{un}"

    kb_rows = [[InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"admin_seller_rename|{sid}")]]
    kb_rows.append([InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"admin_seller_del_confirm|{sid}")])
    kb_rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin_seller_list")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = f"Продавец ID {sid}: {base}"
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("admin_seller_rename|"))
async def admin_seller_rename(cb: CallbackQuery, state: FSMContext):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    try:
        _, sid_s = cb.data.split("|", 1)
        sid = int(sid_s)
    except Exception:
        await cb.answer("Некорректно", show_alert=True)
        return
    conn = botmod.db()
    row = conn.execute(
        "SELECT id, COALESCE(display_name, '') AS dname FROM user_role WHERE id=? AND role='seller'",
        (sid,),
    ).fetchone()
    conn.close()
    if not row:
        await cb.answer("Не найдено", show_alert=True)
        return
    await state.set_state(botmod.AdminStates.wait_seller_rename_name)
    await state.update_data(seller_id=sid)
    await cb.message.answer("Отправьте новое отображаемое имя для продавца")
    await cb.answer()




@router.callback_query(F.data.startswith("admin_seller_del_confirm|"))
async def admin_seller_del_confirm(cb: CallbackQuery):
    import app.bot as botmod
    if not await botmod.require_admin(cb):
        return
    try:
        _, sid = cb.data.split("|", 1)
        sid = int(sid)
    except Exception:
        await cb.answer("Некорректно", show_alert=True)
        return
    conn = botmod.db()
    with conn:
        conn.execute("DELETE FROM user_role WHERE id=? AND role='seller'", (sid,))
    conn.close()
    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="← К списку", callback_data="admin_seller_list")]]
    )
    try:
        await cb.message.edit_text("Продавец удалён ✅", reply_markup=back_kb)
    except Exception:
        await cb.message.answer("Продавец удалён ✅", reply_markup=back_kb)
    await cb.answer()


@router.message(AdminStates.wait_seller_rename_name, F.text)
async def admin_seller_rename_name(m: Message, state: FSMContext):
    import app.bot as botmod
    st = await state.get_state()
    if st != botmod.AdminStates.wait_seller_rename_name.state:
        return
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        return
    data = await state.get_data()
    sid = int(data.get('seller_id'))
    new_name = (m.text or '').strip()
    if not new_name:
        await m.answer("Имя пустое. Повторите.")
        return
    conn = botmod.db()
    with conn:
        conn.execute("UPDATE user_role SET display_name=? WHERE id=? AND role='seller'", (new_name, sid))
    conn.close()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data=f"admin_seller_manage|{sid}")]]
    )
    await m.answer("Имя обновлено ✅", reply_markup=kb)
    await state.clear()


@router.callback_query(F.data == "admin_admin_add")
async def admin_admin_add(cb: CallbackQuery, state: FSMContext):
    import app.bot as botmod
    if not botmod.is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(botmod.AdminStates.wait_admin_add)
    await cb.message.answer("Отправьте тэг нового админа (@username)")
    await cb.answer()


@router.message(AdminStates.wait_admin_add, F.forward_from)
async def on_admin_admin_add_forward(m: Message, state: FSMContext):
    import app.bot as botmod
    st = await state.get_state()
    if st != botmod.AdminStates.wait_admin_add.state:
        return
    if not botmod.is_super_admin(m.from_user.id, m.from_user.username):
        return
    if m.forward_from:
        fid = m.forward_from.id
        uname = botmod._norm_username(m.forward_from.username)
        conn = botmod.db()
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO user_role(tg_id, username, role) VALUES (?,?, 'admin')",
                (fid, uname),
            )
        conn.close()
        await m.answer(f"Добавлен админ по пересланному сообщению: id={fid} {uname or ''}")
        await state.clear()
    else:
        await m.answer(
            "Не могу определить пользователя из пересланного сообщения. Попросите его написать боту /start или пришлите @тег."
        )


@router.message(AdminStates.wait_admin_add, F.text)
async def on_admin_admin_add_text(m: Message, state: FSMContext):
    import app.bot as botmod
    st = await state.get_state()
    if st != botmod.AdminStates.wait_admin_add.state:
        return
    if not botmod.is_super_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа")
        return
    tag = botmod._norm_username(m.text)
    if not tag or not tag.startswith("@"):
        await m.answer("Неверный тэг. Укажите @username")
        return
    conn = botmod.db()
    with conn:
        conn.execute("INSERT OR IGNORE INTO user_role(username, role) VALUES (?, 'admin')", (tag,))
    conn.close()
    await m.answer(f"Добавлен админ: {tag}")
    await state.clear()


@router.callback_query(F.data == "admin_admin_del")
async def admin_admin_del(cb: CallbackQuery, state: FSMContext):
    import app.bot as botmod
    if not botmod.is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(botmod.AdminStates.wait_admin_del)
    await cb.message.answer("Отправьте тэг админа для удаления (@username)")
    await cb.answer()


@router.message(AdminStates.wait_admin_del, F.text)
async def on_admin_admin_del_text(m: Message, state: FSMContext):
    import app.bot as botmod
    st = await state.get_state()
    if st != botmod.AdminStates.wait_admin_del.state:
        return
    if not botmod.is_super_admin(m.from_user.id, m.from_user.username):
        await m.answer("Нет доступа")
        return
    tag = botmod._norm_username(m.text)
    if not tag or not tag.startswith("@"):
        await m.answer("Неверный тэг. Укажите @username")
        return
    if tag.lower() == botmod.SUPER_ADMIN_USERNAME.lower():
        await m.answer("Нельзя удалять главного админа")
        return
    conn = botmod.db()
    with conn:
        conn.execute("DELETE FROM user_role WHERE role='admin' AND LOWER(username)=?", (tag,))
    conn.close()
    await m.answer(f"Удалён админ: {tag}")
    await state.clear()


@router.callback_query(F.data == "admin_admin_list")
async def admin_admin_list(cb: CallbackQuery):
    import app.bot as botmod
    if not botmod.is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True)
        return
    conn = botmod.db()
    rows = conn.execute(
        "SELECT COALESCE(username,'' ) AS uname, COALESCE(tg_id,'') AS tid FROM user_role WHERE role='admin' ORDER BY uname"
    ).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Список пуст", show_alert=True)
        return
    lines = [f"• {r['uname'] or '(без тега)'} {('['+str(r['tid'])+']') if r['tid'] else ''}" for r in rows]
    await cb.message.edit_text(
        "Админы:\n" + "\n".join(lines[: 4000 // 30]),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin_admins")]]
        ),
    )
    await cb.answer()
