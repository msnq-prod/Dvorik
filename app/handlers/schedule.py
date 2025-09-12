from __future__ import annotations

import datetime as dt
from typing import Optional, List, Tuple

from aiogram import Router, F
import html as _html
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder

import app.bot as botmod
from app.ui.keyboards import month_calendar_kb, admin_day_actions_kb
from app.ui.states import SchedStates, SchedTransfer, SchedAdmin
from app.services import schedule as sched

router = Router()


def _month_start(d: Optional[dt.date] = None) -> dt.date:
    d = d or dt.date.today()
    return dt.date(d.year, d.month, 1)


@router.message(Command(commands=["sched", "schedule"]))
async def cmd_schedule(m: Message):
    if not botmod.is_allowed(m.from_user.id, m.from_user.username):
        return
    d = _month_start()
    kb = month_calendar_kb(d.year, d.month, m.from_user.id, back_cb="home")
    await m.answer(
        f"Ваш график — {d.strftime('%B %Y')}\n\n"
        "Подсказка: выберите дату, затем сотрудника — чтобы предложить поменяться на этот день.\n"
        "Навигация по месяцам: ◀️/▶️. Кнопка ‘Таблица’ отправит общий отчёт на 2 месяца.",
        reply_markup=kb,
    )


@router.message(Command(commands=["sched_admin"]))
async def cmd_sched_admin(m: Message):
    if not botmod.is_admin(m.from_user.id, m.from_user.username):
        return
    b = InlineKeyboardBuilder()
    b.button(text="Календарь", callback_data="sched|admin|calendar")
    b.button(text="Создать график", callback_data="sched|admin|create")
    b.button(text="Поменять двух (все даты)", callback_data="sched|admin|swap_global")
    b.adjust(1)
    b.row(InlineKeyboardButton(text="← Назад", callback_data="home"))
    await m.answer("Управление расписанием (админ):", reply_markup=b.as_markup())


@router.callback_query(F.data.startswith("sched|month|"))
async def cb_month_nav(cb: CallbackQuery):
    if not botmod.is_allowed(cb.from_user.id, cb.from_user.username):
        await cb.answer(); return
    _, _, ym = cb.data.split("|", 2)
    y, m = map(int, ym.split("-"))
    kb = month_calendar_kb(y, m, cb.from_user.id, back_cb="home")
    await cb.message.edit_text(f"Ваш график — {dt.date(y,m,1).strftime('%B %Y')}", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "sched|table")
async def cb_table(cb: CallbackQuery):
    if not botmod.is_allowed(cb.from_user.id, cb.from_user.username):
        await cb.answer(); return
    today = dt.date.today()
    m1 = _month_start(today)
    # next month start
    m2 = dt.date(m1.year + (1 if m1.month == 12 else 0), 1 if m1.month == 12 else m1.month + 1, 1)
    # Generate PNG/PDF report
    from pathlib import Path
    from app.services.schedule_report import render_two_month_png, png_to_pdf
    from app import config as app_config
    ts = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
    png_path = app_config.REPORTS_DIR / f'schedule_{ts}.png'
    pdf_path = app_config.REPORTS_DIR / f'schedule_{ts}.pdf'
    render_two_month_png(m1, m2, png_path)
    try:
        png_to_pdf(png_path, pdf_path)
    except Exception:
        pdf_path = None
    # Send both + brief text fallback
    from aiogram.types import FSInputFile
    await cb.message.answer_document(FSInputFile(str(png_path)), caption=f"График на {m1.strftime('%B %Y')} и {m2.strftime('%B %Y')}")
    if pdf_path:
        await cb.message.answer_document(FSInputFile(str(pdf_path)))
    await cb.answer()


def _week_blocks_for_month(month_start: dt.date) -> List[Tuple[List[dt.date], str]]:
    # Return list of (week_dates[Mon..Sun], title) for 5 blocks starting from first Monday row of the month
    first = month_start
    start = first - dt.timedelta(days=first.weekday())  # Monday
    blocks = []
    for w in range(5):
        week = [start + dt.timedelta(days=w*7 + i) for i in range(7)]
        blocks.append((week, f"Неделя {w+1}"))
    return blocks


def _disp_name_for_tg(username: Optional[str], tg_id: int, display_name: Optional[str] = None) -> str:
    """Human label for buttons (no markup). Prefer display_name, then username, then id."""
    nm = (display_name or "").strip() if display_name else ""
    if nm:
        return nm
    if username and username.strip():
        return username
    return str(tg_id)


def _name_from_roles(tg_id: int) -> str:
    # Try to get display_name/username from user_role; fall back to tg_id
    conn = sched._conn()
    try:
        r = conn.execute(
            "SELECT COALESCE(display_name, MIN(username)) AS nm FROM user_role WHERE tg_id=?",
            (tg_id,),
        ).fetchone()
        return (r["nm"] or str(tg_id))
    finally:
        conn.close()


def _name_link(tg_id: int) -> str:
    nm = _name_from_roles(tg_id)
    return f'<a href="tg://user?id={tg_id}">{_html.escape(nm)}</a>'


def _format_two_month_report(m1: dt.date, m2: dt.date) -> str:
    conn = sched._conn()
    try:
        sellers = sched.list_sellers(conn)
        names = [_disp_name_for_tg(s.username, s.tg_id) for s in sellers][:5]  # up to 5 rows
        # header
        lines = []
        for month_start in (m1, m2):
            title = month_start.strftime("%B %Y").capitalize()
            lines.append(f"{title}")
            blocks = _week_blocks_for_month(month_start)
            for week, _ in blocks:
                # header with day numbers
                days_hdr = " ".join([f"{d.day:2d}" if d.month == month_start.month else "  " for d in week])
                lines.append("      | " + days_hdr)
                # rows: name + 7 cols with O/-/✖
                for i in range(max(1, len(names))):
                    name = names[i] if i < len(names) else "—"
                    cells = []
                    for d in week:
                        if d.month != month_start.month:
                            cells.append("  ")
                            continue
                        if not sched.is_open(d, conn):
                            cells.append("✖")
                        else:
                            assigned = sched.get_assignments(d, conn)
                            tg_id = sellers[i].tg_id if i < len(sellers) else None
                            if tg_id and tg_id in assigned:
                                cells.append("О")
                            else:
                                cells.append("–")
                    lines.append(f"{name:>5} | " + " ".join(cells))
                lines.append("")
        return "\n".join(lines)
    finally:
        conn.close()


async def _render_day_view(cb: CallbackQuery, day: dt.date):
    """Render combined day view: user swap options + inline admin actions (if admin)."""
    if not botmod.is_allowed(cb.from_user.id, cb.from_user.username):
        await cb.answer(); return
    conn = sched._conn()
    try:
        assigned = sched.get_assignments(day, conn)
        sellers = sched.list_sellers(conn)
        # Build display lists with hyperlinks in text
        on_links = [
            f'<a href="tg://user?id={s.tg_id}">{_html.escape(_disp_name_for_tg(s.username, s.tg_id, s.display_name))}</a>'
            for s in sellers if s.tg_id in assigned
        ]
        off_links = [
            f'<a href="tg://user?id={s.tg_id}">{_html.escape(_disp_name_for_tg(s.username, s.tg_id, s.display_name))}</a>'
            for s in sellers if s.tg_id not in assigned
        ]
        # Keyboard: swap targets first
        b = InlineKeyboardBuilder()
        user_on = cb.from_user.id in assigned
        if user_on:
            for s in sellers:
                if s.tg_id not in assigned and s.tg_id != cb.from_user.id:
                    b.button(text=_disp_name_for_tg(s.username, s.tg_id, s.display_name), callback_data=f"sched|target|{day.isoformat()}|{s.tg_id}")
        else:
            for s in sellers:
                if s.tg_id in assigned and s.tg_id != cb.from_user.id:
                    b.button(text=_disp_name_for_tg(s.username, s.tg_id, s.display_name), callback_data=f"sched|target|{day.isoformat()}|{s.tg_id}")
        b.adjust(2)
        # Inline admin actions appended directly
        if botmod.is_admin(cb.from_user.id, cb.from_user.username):
            is_open = sched.is_open(day, conn)
            b.row(InlineKeyboardButton(text="Добавить сотрудника", callback_data=f"sched|admin|add|{day.isoformat()}"))
            b.row(InlineKeyboardButton(text="Убрать сотрудника", callback_data=f"sched|admin|rem|{day.isoformat()}"))
            toggle_label = "Сделать день нерабочим" if is_open else "Сделать день рабочим"
            b.row(InlineKeyboardButton(text=toggle_label, callback_data=f"sched|admin|toggle|{day.isoformat()}"))
            b.row(InlineKeyboardButton(text="Поменять сотрудников", callback_data=f"sched|admin|swap_day|{day.isoformat()}"))
        b.row(InlineKeyboardButton(text="← Назад", callback_data="home"))
        txt = (
            f"{day.strftime('%d.%m.%Y')}\n"
            f"Работают: {', '.join(on_links) or '—'}\n"
            f"Свободны: {', '.join(off_links) or '—'}\n\n"
            "Подсказка: выберите сотрудника — ему придёт запрос на подтверждение. Нельзя меняться с напарником, если вы оба уже в смене."
        )
        await cb.message.edit_text(txt, reply_markup=b.as_markup(), parse_mode=ParseMode.HTML)
        await cb.answer()
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|day|"))
async def cb_day(cb: CallbackQuery):
    if not botmod.is_allowed(cb.from_user.id, cb.from_user.username):
        await cb.answer(); return
    _, _, date_iso = cb.data.split("|", 2)
    day = dt.date.fromisoformat(date_iso)
    await _render_day_view(cb, day)


@router.callback_query(F.data.startswith("sched|target|"))
async def cb_target(cb: CallbackQuery):
    if not botmod.is_allowed(cb.from_user.id, cb.from_user.username):
        await cb.answer(); return
    _, _, date_iso, target_id_s = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    target_id = int(target_id_s)
    conn = sched._conn()
    try:
        assigned = sched.get_assignments(day, conn)
        both_on = (cb.from_user.id in assigned) and (target_id in assigned)
        if both_on:
            await cb.answer("Нельзя меняться с напарником в день, когда вы оба работаете", show_alert=True); return
        ok, msg, rid = sched.propose_transfer(day, cb.from_user.id, target_id, conn)
        if not ok:
            await cb.answer(msg, show_alert=True); return
        # Notify target with accept/decline
        target_name = _name_link(target_id)
        init_name = _name_link(cb.from_user.id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Подтвердить", callback_data=f"sched|transfer|accept|{rid}"),
            InlineKeyboardButton(text="Отказаться", callback_data=f"sched|transfer|decline|{rid}"),
            InlineKeyboardButton(text="Посмотреть календарь", callback_data=f"sched|transfer|preview|{rid}")
        ]])
        try:
            await cb.bot.send_message(target_id, f"Предложение поменяться на {day.strftime('%d.%m.%Y')} от {init_name}", reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await cb.answer("Заявка отправлена")
    finally:
        conn.close()

def _disabled_calendar_kb(user_id: int, year: int, month: int, mark: Optional[List[dt.date]] = None) -> InlineKeyboardMarkup:
    # Disabled day buttons grid with nav handled separately
    mark_set = set([d.isoformat() for d in (mark or [])])
    return month_calendar_kb(year, month, user_id, back_cb="noop", table_cb="noop", is_admin=False, day_cb_prefix="noop", mark_dates=mark_set)


@router.callback_query(F.data.startswith("sched|transfer|preview"))
async def cb_transfer_preview(cb: CallbackQuery):
    parts = cb.data.split("|")
    rid = int(parts[3])
    ym = parts[4] if len(parts) > 4 else None
    conn = sched._conn()
    row = conn.execute("SELECT date FROM schedule_transfer_request WHERE id=?", (rid,)).fetchone()
    conn.close()
    if not row:
        await cb.answer("Заявка не найдена", show_alert=True); return
    day = dt.date.fromisoformat(row["date"])  # type: ignore
    cur = dt.date(day.year, day.month, 1)
    if ym:
        y, m = map(int, ym.split("-"))
        cur = dt.date(y, m, 1)
    assigned = sched.get_assignments(day)
    will_work = cb.from_user.id not in assigned
    caption = ("Предпросмотр: этот день будет рабочим для вас" if will_work else "Предпросмотр: этот день станет выходным для вас")
    kb = _disabled_calendar_kb(cb.from_user.id, cur.year, cur.month, [day])
    prev_month = (cur.replace(day=1) - dt.timedelta(days=1)).replace(day=1)
    next_month = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    nav = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️", callback_data=f"sched|transfer|preview|{rid}|{prev_month.strftime('%Y-%m')}"),
        InlineKeyboardButton(text="▶️", callback_data=f"sched|transfer|preview|{rid}|{next_month.strftime('%Y-%m')}")
    ], [
        InlineKeyboardButton(text="Подтвердить", callback_data=f"sched|transfer|accept|{rid}"),
        InlineKeyboardButton(text="Отказаться", callback_data=f"sched|transfer|decline|{rid}")
    ]])
    try:
        await cb.message.edit_text(f"{caption}\nДата изменения: {day.strftime('%d.%m.%Y')} (помечена ✖)", reply_markup=kb)
        await cb.message.answer("Навигация и действия:", reply_markup=nav)
    except Exception:
        await cb.message.answer(f"{caption}\nДата изменения: {day.strftime('%d.%m.%Y')} (помечена ✖)", reply_markup=kb)
        await cb.message.answer("Навигация и действия:", reply_markup=nav)
    await cb.answer()


@router.callback_query(F.data.startswith("sched|transfer|"))
async def cb_transfer_reply(cb: CallbackQuery):
    parts = cb.data.split("|")
    _, _, action, rid_s = parts
    rid = int(rid_s)
    accept = action == "accept"
    ok, msg, details = sched.apply_transfer(rid, accept)
    if not ok and msg:
        await cb.answer(msg, show_alert=True); return
    if accept and details:
        day, from_tg, to_tg = details
        try:
            await cb.bot.send_message(from_tg, f"Подтверждено: {_name_link(cb.from_user.id)} меняется на {day.strftime('%d.%m.%Y')}", parse_mode=ParseMode.HTML)
        except Exception:
            pass
    await cb.answer("Готово")


# ===== Admin day actions =====

@router.callback_query(F.data.startswith("sched|admin|menu|"))
async def cb_admin_day_menu(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    is_open = sched.is_open(day)
    kb = admin_day_actions_kb(date_iso, is_open)
    await cb.message.edit_text(f"Админ: {date_iso}", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "sched|admin|calendar")
async def cb_admin_calendar(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    d = _month_start()
    kb = month_calendar_kb(d.year, d.month, cb.from_user.id, back_cb="home")
    await cb.message.edit_text(f"Календарь — {d.strftime('%B %Y')}", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|toggle|"))
async def cb_admin_toggle_day(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    now_open = sched.toggle_day_open(day)
    # Re-render combined day view so labels and status reflect new state
    await _render_day_view(cb, day)
    await cb.answer("Сделано")


@router.callback_query(F.data.startswith("sched|admin|swap_day|"))
async def cb_admin_swap_day(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    conn = sched._conn()
    try:
        assigned = sched.get_assignments(day, conn)
        sellers = sched.list_sellers(conn)
        b = InlineKeyboardBuilder()
        for tid in assigned:
            b.button(text=_name_from_roles(tid), callback_data=f"sched|admin|swap_day_from|{date_iso}|{tid}")
        b.row(InlineKeyboardButton(text="← Назад", callback_data=f"sched|admin|menu|{date_iso}"))
        await cb.message.edit_text(f"Кого заменить на {date_iso}?", reply_markup=b.as_markup())
        await cb.answer()
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|admin|swap_day_from|"))
async def cb_admin_swap_day_from(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, from_s = cb.data.split("|", 4)
    day = dt.date.fromisoformat(date_iso)
    from_tid = int(from_s)
    conn = sched._conn()
    try:
        assigned = set(sched.get_assignments(day, conn))
        sellers = sched.list_sellers(conn)
        b = InlineKeyboardBuilder()
        for s in sellers:
            if s.tg_id not in assigned:
                b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|swap_day_to|{date_iso}|{from_tid}|{s.tg_id}")
        b.row(InlineKeyboardButton(text="← Назад", callback_data=f"sched|admin|swap_day|{date_iso}"))
        await cb.message.edit_text("На кого заменить?", reply_markup=b.as_markup())
        await cb.answer()
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|admin|swap_day_to|"))
async def cb_admin_swap_day_to(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, from_s, to_s = cb.data.split("|", 5)
    day = dt.date.fromisoformat(date_iso)
    from_tid = int(from_s); to_tid = int(to_s)
    conn = sched._conn()
    try:
        with conn:
            conn.execute("DELETE FROM schedule_assignment WHERE date=? AND tg_id=?", (date_iso, from_tid))
            conn.execute("INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES (?,?,?)", (date_iso, to_tid, 'admin'))
        # Notify both
        from_name = _name_from_roles(from_tid)
        to_name = _name_from_roles(to_tid)
        try:
            await cb.bot.send_message(from_tid, f"Администратор заменил вас на {day.strftime('%d.%m.%Y')} ({to_name} будет работать вместо вас)")
        except Exception:
            pass
        try:
            await cb.bot.send_message(to_tid, f"Администратор поставил вас на {day.strftime('%d.%m.%Y')} вместо {from_name}")
        except Exception:
            pass
        await cb.answer("Заменено", show_alert=True)
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|admin|add|"))
async def cb_admin_add(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    conn = sched._conn()
    try:
        assigned = set(sched.get_assignments(day, conn))
        sellers = sched.list_sellers(conn)
        b = InlineKeyboardBuilder()
        for s in sellers:
            if s.tg_id not in assigned:
                b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|add_do|{date_iso}|{s.tg_id}")
        b.row(InlineKeyboardButton(text="← Назад", callback_data=f"sched|admin|menu|{date_iso}"))
        await cb.message.edit_text(f"Добавить сотрудника на {date_iso}", reply_markup=b.as_markup())
        await cb.answer()
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|admin|add_do|"))
async def cb_admin_add_do(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, tg_s = cb.data.split("|", 4)
    day = dt.date.fromisoformat(date_iso)
    ok = sched.set_assignment(day, int(tg_s), source='admin')
    await cb.answer("Добавлено" if ok else "Не удалось (возможно, уже 2 человека)", show_alert=True)


@router.callback_query(F.data.startswith("sched|admin|rem|"))
async def cb_admin_rem(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    day = dt.date.fromisoformat(date_iso)
    conn = sched._conn()
    try:
        assigned = sched.get_assignments(day, conn)
        b = InlineKeyboardBuilder()
        for tid in assigned:
            b.button(text=_name_from_roles(tid), callback_data=f"sched|admin|rem_do|{date_iso}|{tid}")
        b.row(InlineKeyboardButton(text="← Назад", callback_data=f"sched|admin|menu|{date_iso}"))
        await cb.message.edit_text(f"Убрать сотрудника на {date_iso}", reply_markup=b.as_markup())
        await cb.answer()
    finally:
        conn.close()


@router.callback_query(F.data.startswith("sched|admin|rem_do|"))
async def cb_admin_rem_do(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, tg_s = cb.data.split("|", 4)
    day = dt.date.fromisoformat(date_iso)
    ok = sched.remove_assignment(day, int(tg_s))
    await cb.answer("Убрано" if ok else "Не найдено", show_alert=True)


@router.callback_query(F.data == "sched|admin|create")
async def cb_admin_create(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    # Step 1: pick first date
    d = _month_start()
    kb = month_calendar_kb(d.year, d.month, cb.from_user.id, back_cb="sched|admin|calendar", day_cb_prefix="sched|admin|start")
    await cb.message.edit_text("Создание графика: выберите дату начала (день 1)", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|start|"))
async def cb_admin_start(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso = cb.data.split("|", 3)
    # Normalize to first open day at/after selected date
    sel = dt.date.fromisoformat(date_iso)
    conn = sched._conn()
    try:
        day1 = sel
        while not sched.is_open(day1, conn):
            day1 = day1 + dt.timedelta(days=1)
        sellers = sched.list_sellers(conn)
    finally:
        conn.close()
    # Ask to pick primary employee who will work two days
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|primary|{day1.isoformat()}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|create"))
    await cb.message.edit_text(f"День 1/2: выберите первого сотрудника (работает оба дня) — {day1.isoformat()}", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|primary|"))
async def cb_admin_primary(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, tid_s = cb.data.split("|", 4)
    day1 = dt.date.fromisoformat(date_iso)
    primary_tid = int(tid_s)
    day2 = sched.next_open_day(day1)
    # Clear existing assignments on day1 and day2 (override anchor days)
    conn = sched._conn()
    try:
        with conn:
            conn.execute("DELETE FROM schedule_assignment WHERE date=?", (day1.isoformat(),))
            conn.execute("DELETE FROM schedule_assignment WHERE date=?", (day2.isoformat(),))
        # Assign primary on both days
        sched.set_assignment(day1, primary_tid, source='admin', conn=conn)
        sched.set_assignment(day2, primary_tid, source='admin', conn=conn)
    finally:
        conn.close()
    # Ask for second employee for day1
    conn = sched._conn(); sellers = [s for s in sched.list_sellers(conn) if s.tg_id != primary_tid]; conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|second1_add|{day1.isoformat()}|{primary_tid}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|create"))
    await cb.message.edit_text(f"День 1: выберите второго сотрудника ({day1.isoformat()})", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|second1_add|"))
async def cb_admin_second1_add(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, primary_s, second_s = cb.data.split("|", 5)
    day1 = dt.date.fromisoformat(date_iso)
    primary_tid = int(primary_s)
    second_tid = int(second_s)
    sched.set_assignment(day1, second_tid, source='admin')
    # Ask for second employee for day2
    day2 = sched.next_open_day(day1)
    conn = sched._conn(); sellers = [s for s in sched.list_sellers(conn) if s.tg_id != primary_tid]; conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|second2_add|{day1.isoformat()}|{primary_tid}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|create"))
    await cb.message.edit_text(f"День 2: выберите второго сотрудника ({day2.isoformat()})", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|second2_add|"))
async def cb_admin_second2_add(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, day1_iso, primary_s, second2_s = cb.data.split("|", 5)
    day1 = dt.date.fromisoformat(day1_iso)
    day2 = sched.next_open_day(day1)
    primary_tid = int(primary_s)
    second2_tid = int(second2_s)
    sched.set_assignment(day2, second2_tid, source='admin')
    # Finalize: save anchor at day1 and generate 30 days from selected start
    conn = sched._conn()
    try:
        sellers = sched.list_sellers(conn)
        if len(sellers) not in (3, 4):
            # Save anchor anyway to keep context, but do not generate
            with conn:
                conn.execute("INSERT OR IGNORE INTO schedule_anchor(start_date) VALUES (?)", (day1.isoformat(),))
            await cb.answer("Для автогенерации нужно 3 или 4 сотрудника", show_alert=True)
            d = _month_start(day1)
            kb = month_calendar_kb(d.year, d.month, cb.from_user.id, back_cb="home")
            await cb.message.edit_text(f"Календарь — {d.strftime('%B %Y')}", reply_markup=kb)
            return
        with conn:
            conn.execute("INSERT OR IGNORE INTO schedule_anchor(start_date) VALUES (?)", (day1.isoformat(),))
        sched.generate_schedule_range(day1, days=30, override=True, conn=conn)
    finally:
        conn.close()
    await cb.answer("График на 30 дней сгенерирован", show_alert=True)
    d = _month_start(day1)
    kb = month_calendar_kb(d.year, d.month, cb.from_user.id, back_cb="home")
    await cb.message.edit_text(f"Календарь — {d.strftime('%B %Y')}", reply_markup=kb)

@router.callback_query(F.data.startswith("sched|admin|pick1"))
async def cb_admin_pick1(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, date_iso = cb.data.split("|", 2)
    day = dt.date.fromisoformat(date_iso)
    # Ask to pick two employees for day1
    conn = sched._conn()
    sellers = sched.list_sellers(conn)
    conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|pick1_add|{date_iso}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|create"))
    await cb.message.edit_text(f"День 1: выберите двух сотрудников ({date_iso})", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|pick1_add|"))
async def cb_admin_pick1_add(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, tid_s = cb.data.split("|", 4)
    day1 = dt.date.fromisoformat(date_iso)
    tid = int(tid_s)
    # add assignment until 2 selected
    sched.set_assignment(day1, tid, source='admin')
    conn = sched._conn()
    ass = sched.get_assignments(day1, conn)
    conn.close()
    if len(ass) < 2:
        await cb.answer("Выбран 1", show_alert=True)
        return
    # Proceed to day2 (next open day)
    day2 = sched.next_open_day(day1)
    # clear possible leftovers
    # Ask pick two for day2
    conn = sched._conn(); sellers = sched.list_sellers(conn); conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|pick2_add|{day2.isoformat()}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|create"))
    await cb.message.edit_text(f"День 2: выберите двух сотрудников ({day2.isoformat()})", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|pick2_add|"))
async def cb_admin_pick2_add(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, date_iso, tid_s = cb.data.split("|", 4)
    day2 = dt.date.fromisoformat(date_iso)
    tid = int(tid_s)
    sched.set_assignment(day2, tid, source='admin')
    conn = sched._conn()
    ass2 = sched.get_assignments(day2, conn)
    # determine day1 as previous open day
    dprev = day2 - dt.timedelta(days=1)
    while not sched.is_open(dprev, conn):
        dprev -= dt.timedelta(days=1)
    ass1 = sched.get_assignments(dprev, conn)
    done = len(ass2) >= 2 and len(ass1) >= 2
    if done:
        # Save anchor and generate for two months
        with conn:
            conn.execute("INSERT OR IGNORE INTO schedule_anchor(start_date) VALUES (?)", (dprev.isoformat(),))
        sched.generate_schedule(_month_start(dprev), months=2, conn=conn)
        conn.close()
        await cb.answer("График сгенерирован", show_alert=True)
        # show calendar
        d = _month_start(dprev)
        kb = month_calendar_kb(d.year, d.month, cb.from_user.id, back_cb="home")
        await cb.message.edit_text(f"Календарь — {d.strftime('%B %Y')}", reply_markup=kb)
    else:
        conn.close()
        await cb.answer("Выбран 1", show_alert=True)


@router.callback_query(F.data.startswith("sched|admin|swap_global"))
async def cb_admin_swap_global(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    conn = sched._conn()
    sellers = sched.list_sellers(conn)
    conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|swapg_pick_a|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|calendar"))
    await cb.message.edit_text("Выберите первого сотрудника", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|swapg_pick_a|"))
async def cb_admin_swap_global_a(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, tid_s = cb.data.split("|", 3)
    a = int(tid_s)
    conn = sched._conn()
    sellers = [s for s in sched.list_sellers(conn) if s.tg_id != a]
    conn.close()
    b = InlineKeyboardBuilder()
    for s in sellers:
        b.button(text=(s.username or str(s.tg_id)), callback_data=f"sched|admin|swapg_do|{a}|{s.tg_id}")
    b.row(InlineKeyboardButton(text="← Назад", callback_data="sched|admin|calendar"))
    await cb.message.edit_text("Выберите второго сотрудника", reply_markup=b.as_markup())
    await cb.answer()


@router.callback_query(F.data.startswith("sched|admin|swapg_do|"))
async def cb_admin_swap_global_do(cb: CallbackQuery):
    if not botmod.is_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    _, _, _, a_s, b_s = cb.data.split("|", 4)
    a, b = int(a_s), int(b_s)
    cnt = sched.swap_employees_globally(a, b)
    await cb.answer(f"Заменено дат: {cnt}", show_alert=True)
