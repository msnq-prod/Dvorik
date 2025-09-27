from typing import List, Tuple, Optional, Dict, Callable
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import datetime as dt
import sqlite3

from app import constants as const
from app.services import schedule as sched
from app.services.auth import is_admin, is_seller
from app.db import db, get_default_supplier_id
from app import config as app_config
from app.services.notify import get_notify_mode
from app.utils_number import display_qty


def grid_buttons(items: List[Tuple[str, str]], per_row: int = 2, back_cb: Optional[str] = None) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for text, cb in items:
        b.button(text=text, callback_data=cb)
    b.adjust(per_row)
    if back_cb:
        b.row(InlineKeyboardButton(text="← Назад", callback_data=back_cb))
    return b.as_markup()


def _all_location_codes() -> List[str]:
    codes = ["COUNTER", const.HUB_LOCATION_CODE]
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
    labels = label_for or {}
    rows: List[List[InlineKeyboardButton]] = []
    if hall_option:
        rows.append([InlineKeyboardButton(text=hall_option[0], callback_data=hall_option[1])])
    if "COUNTER" in codes:
        rows.append([
            InlineKeyboardButton(
                text=labels.get("COUNTER", "за стойкой"),
                callback_data=cb_for("COUNTER"),
            )
        ])
        codes.remove("COUNTER")
    if const.HUB_LOCATION_CODE in codes:
        rows.append([
            InlineKeyboardButton(
                text=labels.get(const.HUB_LOCATION_CODE, const.HUB_LOCATION_CODE),
                callback_data=cb_for(const.HUB_LOCATION_CODE),
            )
        ])
        codes.remove(const.HUB_LOCATION_CODE)
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
                        text=labels.get(c, c), callback_data=cb_for(c)
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
                        text=labels.get(c, c), callback_data=cb_for(c)
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


def kb_main(user_id: Optional[int] = None, username: Optional[str] = None) -> InlineKeyboardMarkup:
    conn = db()
    b = InlineKeyboardBuilder()
    admin = is_admin(user_id or 0, username)
    seller = is_seller(user_id or 0, username)
    if admin:
        b.button(text="Наличие", callback_data="stock")
        b.adjust(1)
        today_ym = dt.date.today().strftime("%Y-%m")
        b.button(text="Расписание", callback_data=f"sched|month|{today_ym}")
        b.adjust(1)
        b.button(text="🛠️ Администрирование", callback_data="admin")
        b.adjust(1)
    else:
        b.button(text="Наличие", callback_data="stock")
        b.adjust(1)
        today_ym = dt.date.today().strftime("%Y-%m")
        b.button(text="Расписание", callback_data=f"sched|month|{today_ym}")
        b.adjust(1)
    b.row(InlineKeyboardButton(text="🔎 Поиск", switch_inline_query_current_chat=""))
    conn.close()
    return b.as_markup()


def kb_qty(pid: int, dest: str, current: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for d in (-1, +1):
        sign = "−" if d < 0 else "+"
        b.button(text=f"{sign}{abs(d)}", callback_data=f"qty|{pid}|{dest}|{d}")
    b.adjust(2)
    b.button(text=f"✅ {current}", callback_data=f"qty_ok|{pid}|{dest}|{current}")
    b.adjust(1)
    b.button(text="← Назад", callback_data=f"open|{pid}")
    return b.as_markup()


def kb_pick_src(conn: sqlite3.Connection, pid: int) -> InlineKeyboardMarkup:
    rows = conn.execute(
        "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
        (pid,),
    ).fetchall()
    codes = [r["location_code"] for r in rows]
    if not codes:
        return grid_buttons([("Нет доступных остатков", "noop")], per_row=1, back_cb=f"open|{pid}")
    label = {}
    for r in rows:
        q = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        label[r["location_code"]] = f"{r['location_code']} ({display_qty(q)})"
    return locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"src_chosen|{pid}|{code}",
        label_for=label,
        back_cb=f"open|{pid}",
    )


def kb_pick_dst(pid: int) -> InlineKeyboardMarkup:
    conn = db()
    have = {r["location_code"] for r in conn.execute(
        "SELECT location_code FROM stock WHERE product_id=? AND qty_pack>0",
        (pid,),
    ).fetchall()}
    all_codes = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    avail = [c for c in all_codes if c not in have and c != "HALL"]
    return locations_2col_keyboard(
        active_codes=avail,
        cb_for=lambda code: f"dst_chosen|{pid}|{code}",
        label_for=None,
        back_cb=f"open|{pid}",
        hall_option=("В ЗАЛ (списание)", f"dst_hall|{pid}"),
    )


def kb_route_src(conn: sqlite3.Connection, pid: int) -> InlineKeyboardMarkup:
    rows = conn.execute(
        "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
        (pid,),
    ).fetchall()
    codes = [r["location_code"] for r in rows]
    if not codes:
        return grid_buttons([("Нет доступных остатков", "noop")], per_row=1, back_cb=f"open|{pid}")
    label = {}
    for r in rows:
        q = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        label[r["location_code"]] = f"{r['location_code']} ({display_qty(q)})"
    return locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"route_src_chosen|{pid}|{code}",
        label_for=label,
        back_cb=f"open|{pid}",
    )


def kb_route_dst(pid: int) -> InlineKeyboardMarkup:
    conn = db()
    have = {r["location_code"] for r in conn.execute(
        "SELECT location_code FROM stock WHERE product_id=? AND qty_pack>0",
        (pid,),
    ).fetchall()}
    all_codes = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    avail = [c for c in all_codes if c not in have and c != "HALL"]
    return locations_2col_keyboard(
        active_codes=avail,
        cb_for=lambda code: f"route_dst_chosen|{pid}|{code}",
        label_for=None,
        back_cb=f"open|{pid}",
        hall_option=("В ЗАЛ (списание)", f"route_dst_chosen|{pid}|HALL"),
    )


def kb_supply_page(conn: sqlite3.Connection, page: int) -> InlineKeyboardMarkup:
    off = (page - 1) * app_config.PAGE_SIZE
    default_supplier_id = get_default_supplier_id(conn)
    rows = conn.execute(
        """
        SELECT p.id,
               COALESCE(
                   (SELECT code FROM supplier_sku WHERE product_id=p.id AND supplier_id=? AND active=1 ORDER BY id LIMIT 1),
                   (SELECT code FROM supplier_sku WHERE product_id=p.id AND active=1 ORDER BY id LIMIT 1),
                   p.article
               ) AS article,
               p.name
        FROM product p
        WHERE p.is_new=1 AND p.archived=0 ORDER BY p.id DESC LIMIT ? OFFSET ?
        """,
        (default_supplier_id, app_config.PAGE_SIZE, off),
    ).fetchall()
    items = [(f"{r['article']} | {r['name'][:40]}", f"open|{r['id']}") for r in rows]
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM product WHERE is_new=1 AND archived=0"
    ).fetchone()["c"]
    kb = grid_buttons(items, per_row=1, back_cb="supply")
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"supply_list|{page-1}"))
    if off + app_config.PAGE_SIZE < count:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"supply_list|{page+1}"))
    if nav:
        kb.inline_keyboard.append(nav)
    return kb


def _notify_button_row(user_id: int, notif_type: str, label: str) -> list[list[InlineKeyboardButton]]:
    mode = get_notify_mode(user_id, notif_type)

    def style(label_text: str, key: str) -> str:
        return ("✅ " + label_text) if mode == key else label_text

    mode_disp = {
        "daily": "В конце дня",
        "instant": "Сразу",
        "off": "Нет",
    }.get(mode, "Нет")

    return [[
        InlineKeyboardButton(text=f"{label}: {mode_disp}", callback_data="noop"),
        InlineKeyboardButton(text=style("В конце дня", "daily"), callback_data=f"notif|{notif_type}|daily"),
        InlineKeyboardButton(text=style("Сразу", "instant"), callback_data=f"notif|{notif_type}|instant"),
        InlineKeyboardButton(text=style("Нет", "off"), callback_data=f"notif|{notif_type}|off"),
    ]]


def kb_notify(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    rows += _notify_button_row(user_id, 'zero', 'Закончился')
    rows += _notify_button_row(user_id, 'last', 'Последняя пачка')
    rows += _notify_button_row(user_id, 'to_skl', 'На склад')
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
