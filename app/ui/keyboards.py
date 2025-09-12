from typing import List, Tuple, Optional, Dict, Callable
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import datetime as dt

from app.services import schedule as sched


def grid_buttons(items: List[Tuple[str, str]], per_row: int = 2, back_cb: Optional[str] = None) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for text, cb in items:
        b.button(text=text, callback_data=cb)
    b.adjust(per_row)
    if back_cb:
        b.row(InlineKeyboardButton(text="← Назад", callback_data=back_cb))
    return b.as_markup()


def _all_location_codes() -> List[str]:
    codes = ["SKL-0"]
    codes += [f"SKL-{i}" for i in range(1, 5)]
    for home in range(2, 10):
        for shelf in (1, 2):
            codes.append(f"{home}.{shelf}")
    return codes


def locations_2col_keyboard(
    active_codes: List[str],
    cb_for: Callable[[str], str],
    label_for: Optional[Dict[str, str]] = None,
    back_cb: Optional[str] = None,
    hall_option: Optional[Tuple[str, str]] = None,
) -> InlineKeyboardMarkup:
    codes = [c for c in _all_location_codes() if c in set(active_codes)]
    rows: List[List[InlineKeyboardButton]] = []
    if hall_option:
        rows.append([InlineKeyboardButton(text=hall_option[0], callback_data=hall_option[1])])
    if "SKL-0" in codes:
        rows.append([
            InlineKeyboardButton(
                text=(label_for.get("SKL-0") if label_for else "SKL-0"),
                callback_data=cb_for("SKL-0"),
            )
        ])
        codes.remove("SKL-0")
    skl = [c for c in codes if c.startswith("SKL-")]
    dom = [c for c in codes if not c.startswith("SKL-")]
    i = 0
    while i < len(skl):
        row = []
        for _ in range(2):
            if i < len(skl):
                c = skl[i]
                i += 1
                row.append(
                    InlineKeyboardButton(
                        text=(label_for.get(c) if label_for else c), callback_data=cb_for(c)
                    )
                )
        rows.append(row)
    def _key(c: str):
        try:
            h, s = c.split('.')
            return (int(h), int(s))
        except Exception:
            return (999, 999)
    dom.sort(key=_key)
    i = 0
    while i < len(dom):
        row = []
        for _ in range(2):
            if i < len(dom):
                c = dom[i]
                i += 1
                row.append(
                    InlineKeyboardButton(
                        text=(label_for.get(c) if label_for else c), callback_data=cb_for(c)
                    )
                )
        rows.append(row)
    if back_cb:
        rows.append([InlineKeyboardButton(text="← Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def month_calendar_kb(
    year: int,
    month: int,
    user_tg_id: int,
    back_cb: str = "noop",
    table_cb: str = "sched|table",
    is_admin: bool = False,
    day_cb_prefix: str = "sched|day",
    mark_dates: Optional[set[str]] = None,
) -> InlineKeyboardMarkup:
    """Build 5x7 calendar grid (Mon..Sun) for given month with marks:
    - working day for user: add "✅"
    - closed day: add "✖"
    Buttons callback: sched|day|YYYY-MM-DD
    Bottom row: [Назад] [◀️/▶️] [Таблица]
    """
    b = InlineKeyboardBuilder()
    first = dt.date(year, month, 1)
    # start from Monday of the first week
    start = first - dt.timedelta(days=(first.weekday()))  # Monday is 0
    # 5 weeks = 35 days viewport
    viewport = [start + dt.timedelta(days=i) for i in range(35)]
    # For quick lookup, load assignments and open flags
    conn = sched._conn()
    try:
        for i, day in enumerate(viewport):
            in_month = (day.month == month)
            if not in_month:
                text = "·"
                cb = "noop"
            else:
                ass = sched.get_assignments(day, conn)
                closed = not sched.is_open(day, conn)
                text = f"{day.day}"
                if closed:
                    text += "✖"
                elif user_tg_id in ass:
                    text += "✅"
                if mark_dates and day.isoformat() in mark_dates:
                    text += "✖"
                cb = f"{day_cb_prefix}|{day.isoformat()}"
            b.button(text=text, callback_data=cb)
            if (i + 1) % 7 == 0:
                # new row
                pass
        # adjust to 7 columns per row
        b.adjust(7, 7, 7, 7, 7)
        # bottom row with prev/next and back
        prev_month = (dt.date(year, month, 1) - dt.timedelta(days=1)).replace(day=1)
        next_month = dt.date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
        b.row(
            InlineKeyboardButton(text="← Назад", callback_data=back_cb),
            InlineKeyboardButton(text="◀️", callback_data=f"sched|month|{prev_month.year:04d}-{prev_month.month:02d}"),
            InlineKeyboardButton(text="▶️", callback_data=f"sched|month|{next_month.year:04d}-{next_month.month:02d}"),
            InlineKeyboardButton(text="Таблица", callback_data=table_cb),
        )
        return b.as_markup()
    finally:
        conn.close()


def admin_day_actions_kb(date_iso: str, is_open: bool) -> InlineKeyboardMarkup:
    """Admin actions for a specific day with a proper back target and smart labels.

    - Toggle button label reflects current state (open/closed)
    - Swap label is "Поменять сотрудников"
    """
    toggle_label = "Сделать день нерабочим" if is_open else "Сделать день рабочим"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить сотрудника", callback_data=f"sched|admin|add|{date_iso}")],
        [InlineKeyboardButton(text="Убрать сотрудника", callback_data=f"sched|admin|rem|{date_iso}")],
        [InlineKeyboardButton(text=toggle_label, callback_data=f"sched|admin|toggle|{date_iso}")],
        [InlineKeyboardButton(text="Поменять сотрудников", callback_data=f"sched|admin|swap_day|{date_iso}")],
        [InlineKeyboardButton(text="← Назад", callback_data=f"sched|admin|menu|{date_iso}")],
    ])
