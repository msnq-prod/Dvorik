# marm_bot.py
# Telegram-бот складского учёта на SQLite, aiogram 3.7.
# Функции:
# - Импорт поставок из Excel (два формата: Гордеева / Мармелэнд)
# - Складские локации (склад 0–4, домики 2.1–9.2, зал=списание)
# - Карточка товара: остатки по локациям, локальное имя, фото
# - Мастер перемещения (источник/назначение/кол-во) с мгновенным обновлением
# - Завершение карточек (добавление названия/фото)
# - Отчёты: заканчивающиеся (<2), нулевые
# - Наличие по локациям
# - Инвентаризация: ручные +/− в выбранной локации (без перемещений)
# - Поиск: inline (FTS5 если доступно, иначе LIKE)
#
# Конфиденциальные значения (токен/админ) выносятся в config.json

import asyncio
import datetime as dt
import os
import re
import sqlite3
import json
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import pandas as pd
import math
import csv
import html
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.session.aiohttp import AiohttpSession
import aiohttp
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    InlineQuery, InlineQueryResultArticle, InputTextMessageContent, FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
import time
from PIL import Image

# Modularized config/DB access
from app import config as app_config
from app import db as app_db
from app.ui.states import CardFill, AdminStates, AdminEdit
from app.ui.keyboards import grid_buttons, locations_2col_keyboard
from app.ui.keyboards import month_calendar_kb, admin_day_actions_kb
from app.ui.texts import product_caption, stocks_summary, notify_text
from app.services.photos import (
    compress_image_to_jpeg as _compress_image_to_jpeg,
    download_and_compress_photo as _download_and_compress_photo,
    ensure_local_photo as _ensure_local_photo,
)
from app.services.notify import (
    send_daily_digests,
    notify_instant_thresholds as _notify_instant_thresholds,
    notify_instant_to_skl as _notify_instant_to_skl,
    log_event_to_skl as _log_event_to_skl,
)
from app.services.stock import (
    move_specific as move_specific,
    adjust_location_qty as adjust_location_qty,
)
from app.services.imports import (
    excel_to_normalized_csv,
    csv_to_normalized_csv,
    import_supply_from_normalized_csv,
)
from app.services import schedule as sched

# ========= 1) НАСТРОЙКИ =========

CONFIG_PATH = app_config.CONFIG_PATH

# Токен бота и главный админ — из config.json или переменных окружения
BOT_TOKEN = app_config.BOT_TOKEN
if not BOT_TOKEN:
    raise RuntimeError(
        "Не задан BOT_TOKEN. Укажите его в config.json или переменной окружения BOT_TOKEN."
    )

# Главный админ: ID обязателен; username опционален
SUPER_ADMIN_ID = app_config.SUPER_ADMIN_ID
if SUPER_ADMIN_ID <= 0:
    raise RuntimeError(
        "Не задан SUPER_ADMIN_ID. Укажите его в config.json или переменной окружения SUPER_ADMIN_ID."
    )

SUPER_ADMIN_USERNAME = app_config.SUPER_ADMIN_USERNAME

DB_PATH = app_config.DB_PATH  # mutable via marm_bot for tests
UPLOAD_DIR = app_config.UPLOAD_DIR
NORMALIZED_DIR = app_config.NORMALIZED_DIR
REPORTS_DIR = app_config.REPORTS_DIR
PHOTOS_DIR = app_config.PHOTOS_DIR
PHOTO_QUALITY = app_config.PHOTO_QUALITY

PAGE_SIZE = app_config.PAGE_SIZE
CARDS_PAGE_SIZE = app_config.CARDS_PAGE_SIZE
STOCK_PAGE_SIZE = app_config.STOCK_PAGE_SIZE

# ========= РОУТЕР (объявляем до хендлеров) =========
router = Router()

# ========= 2) УТИЛИТЫ/БД =========

def db() -> sqlite3.Connection:
    # Delegate to modular db implementation
    return app_db.db()

def init_db():
    # Delegate to modular initializer
    return app_db.init_db()

# grid_buttons moved to app.ui.keyboards

# ===== РОЛИ/ПРАВА =====

def _norm_username(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if not u.startswith("@"):
        u = "@" + u
    return u.lower()

def is_super_admin(uid: int, username: Optional[str]) -> bool:
    if uid == SUPER_ADMIN_ID:
        return True
    un = _norm_username(username)
    return un == SUPER_ADMIN_USERNAME.lower()

def is_admin(uid: int, username: Optional[str]) -> bool:
    if is_super_admin(uid, username):
        return True
    conn = db()
    un = _norm_username(username)
    row = conn.execute(
        "SELECT 1 FROM user_role WHERE role='admin' AND (tg_id=? OR (username IS NOT NULL AND LOWER(username)=?)) LIMIT 1",
        (uid, un)
    ).fetchone()
    conn.close()
    return bool(row)

def is_seller(uid: int, username: Optional[str]) -> bool:
    conn = db()
    un = _norm_username(username)
    row = conn.execute(
        "SELECT 1 FROM user_role WHERE role='seller' AND (tg_id=? OR (username IS NOT NULL AND LOWER(username)=?)) LIMIT 1",
        (uid, un)
    ).fetchone()
    conn.close()
    return bool(row)

async def require_admin(cb: CallbackQuery) -> bool:
    if is_admin(cb.from_user.id, cb.from_user.username):
        return True
    await cb.answer("Нет доступа (только для админа)", show_alert=True)
    return False

def is_allowed(uid: int, username: Optional[str]) -> bool:
    return is_admin(uid, username) or is_seller(uid, username)

async def _safe_cb_answer(cb: CallbackQuery):
    try:
        await cb.answer()
    except Exception:
        pass

# ========= СОСТОЯНИЯ (FSM) =========
# States moved to app.ui.states
from app.ui.states import SchedStates, SchedTransfer, SchedAdmin

# ===== УВЕДОМЛЕНИЯ =====

def _get_notify_mode(user_id: int, notif_type: str) -> str:
    conn = db();
    try:
        row = conn.execute("SELECT mode FROM user_notify WHERE user_id=? AND notif_type=?", (user_id, notif_type)).fetchone()
        return row["mode"] if row else "off"
    finally:
        conn.close()

@router.callback_query(F.data.startswith("admin_edit|"))
async def admin_edit_menu(cb: CallbackQuery):
    from app.handlers.product_admin import admin_edit_menu as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("admin_edit_field|"))
async def admin_edit_field(cb: CallbackQuery, state: FSMContext):
    from app.handlers.product_admin import admin_edit_field as _h
    return await _h(cb, state)

@router.message(AdminEdit.wait_text, F.text.len() > 0)
async def admin_edit_save_text(m: Message, state: FSMContext):
    from app.handlers.product_admin import admin_edit_save_text as _h
    return await _h(m, state)

@router.callback_query(F.data.startswith("admin_edit_photo|"))
async def admin_edit_photo(cb: CallbackQuery, state: FSMContext):
    from app.handlers.product_admin import admin_edit_photo as _h
    return await _h(cb, state)

@router.message(AdminEdit.wait_photo, F.photo)
async def admin_edit_save_photo(m: Message, state: FSMContext):
    from app.handlers.product_admin import admin_edit_save_photo as _h
    return await _h(m, state)

@router.callback_query(F.data.startswith("admin_edit_clear_photo|"))
async def admin_edit_clear_photo(cb: CallbackQuery):
    from app.handlers.product_admin import admin_edit_clear_photo as _h
    return await _h(cb)

def _set_notify_mode(user_id: int, notif_type: str, mode: str):
    conn = db();
    try:
        with conn:
            conn.execute(
                "INSERT INTO user_notify(user_id, notif_type, mode) VALUES (?,?,?)\n                 ON CONFLICT(user_id, notif_type) DO UPDATE SET mode=excluded.mode",
                (user_id, notif_type, mode)
            )
    finally:
        conn.close()

def _notify_button_row(user_id: int, notif_type: str, label: str) -> list[list[InlineKeyboardButton]]:
    """Строка переключателей для одного типа уведомлений с понятной индикацией.

    Слева — подпись с текущим режимом, далее три варианта.
    Выбранный вариант помечаем "✅".
    """
    mode = _get_notify_mode(user_id, notif_type)

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
    # Заголовки как статический текст будут в самом сообщении, а здесь — только кнопки.
    # Порядок: zero, last, to_skl
    rows += _notify_button_row(user_id, 'zero', 'Закончился')
    rows += _notify_button_row(user_id, 'last', 'Последняя пачка')
    rows += _notify_button_row(user_id, 'to_skl', 'На склад')
    # Возврат в панель админа (уведомления доступны из неё)
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# notify_text moved to app.ui.texts

def total_stock(conn: sqlite3.Connection, pid: int) -> float:
    row = conn.execute("SELECT IFNULL(SUM(qty_pack),0) AS t FROM stock WHERE product_id=?", (pid,)).fetchone()
    return float(row["t"] or 0)

# Notification helpers moved to app.services.notify

# locations_2col_keyboard moved to app.ui.keyboards

def _has_incomplete(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM product WHERE local_name IS NULL OR (photo_file_id IS NULL AND COALESCE(photo_path,'')='') LIMIT 1").fetchone()
    return bool(row)

# ========= 3) ПОСТАВКА (Excel) =========

_ART_RX = re.compile(r'^\s*([A-Za-zА-Яа-я0-9\-\._/]+)\s+(.+)$')
_PACK_RX = re.compile(r'(\d+\s*(?:кг|гр|г)\s*[*xх]\s*\d+)', re.IGNORECASE)
COL_ART = {"артикул", "код", "артикул/код", "код товара"}
COL_NAME = {"наименование", "товар", "название",
            "товары (работы, услуги)", "товары (работы,услуги)",
            "товары(работы,услуги)", "товары(работы, услуги)"}
COL_QTY  = {"кол-во","количество","кол-во пачек","кол-во мест","мест","количество, шт"}

def _norm_header(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or "").strip().lower())

def _clean_name(raw_name: str) -> tuple[str, Optional[str]]:
    s = (raw_name or "").strip()
    brand = None
    if "/" in s:
        left, right = s.split("/", 1)
        s = left.strip()
        brand = right.strip() or None
    s = _PACK_RX.sub("", s)
    s = re.sub(r'\s{2,}', ' ', s).strip(" -–—\t")
    return s, brand

def _emptyish(val: Optional[str]) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s == "" or s in {"nan", "none", "null"}

def _detect_columns(df: pd.DataFrame):
    headers = {c: _norm_header(c) for c in df.columns}
    inv = {v: k for k, v in headers.items()}
    col_article = None
    for h in headers.values():
        if h in COL_ART:
            col_article = inv[h]; break
    col_name = None
    for h in headers.values():
        if h in COL_NAME:
            col_name = inv[h]; break
    if not col_name:
        col_name = df.columns[0]
    col_qty = None
    for h in headers.values():
        if h in COL_QTY:
            col_qty = inv[h]; break
    if not col_qty:
        num_cols = [c for c in df.columns if str(df[c].dtype).startswith(("int","float"))]
        col_qty = num_cols[0] if num_cols else None
    is_gordeeva = col_article is not None
    return col_article, col_name, col_qty, is_gordeeva

# (Удалено) _read_excel_any — больше не используется; все импорты идут через нормализацию CSV

def _iter_excel_sheets_raw(path: str):
    """Итерирует все листы Excel как (sheet_name, df без заголовка)."""
    ext = Path(path).suffix.lower()
    engines: List[Optional[str]]
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        engines = ["openpyxl"]
    elif ext == ".xls":
        engines = ["xlrd"]
    else:
        engines = [None, "openpyxl", "xlrd"]
    last_err = None
    for eng in engines:
        try:
            xls = pd.ExcelFile(path, engine=eng)
            for name in xls.sheet_names:
                df = xls.parse(name, header=None, dtype=object)
                yield name, df
            return
        except Exception as e:
            last_err = e
    if ext == ".xls":
        raise RuntimeError("Для чтения .xls установите xlrd==1.2.0 (в xlrd>=2 поддержка .xls удалена)") from last_err
    raise last_err

def _norm_cell(v) -> str:
    s = str(v if v is not None else "")
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _flat_ru(s: str) -> str:
    """Нижний регистр и удаление всего, кроме букв/цифр для грубого поиска."""
    s = (s or "").lower()
    s = s.replace("ё", "е")
    return re.sub(r"[^a-zа-я0-9]+", "", s)

def _simplify_query(s: str) -> str:
    s = (s or "").strip()
    # Простейшая нормализация для опечаток: ё→е, нижний регистр, схлопнуть пробелы
    s = s.replace("Ё", "Е").replace("ё", "е")
    s = re.sub(r"\s+", " ", s).lower()
    return s

def _find_header_triplet(cells: List[str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Ищет в строке индексы колонок заголовков: (Артикул, Товары..., Количество).
    Возвращает кортеж индексов или (None, None, None).
    """
    art_idx = name_idx = qty_idx = None
    for j, v in enumerate(cells):
        h = _norm_header(v)
        if h == "артикул" and art_idx is None:
            art_idx = j
        if (h in COL_NAME or "товар" in h) and name_idx is None:
            name_idx = j
        if (h in COL_QTY or h.startswith("кол-") or "кол" in h or "шт" in h) and qty_idx is None:
            qty_idx = j
    return art_idx, name_idx, qty_idx

def _looks_like_article(token: str) -> bool:
    t = _norm_cell(token)
    if not t:
        return False
    # должен содержать хотя бы одну цифру (исключаем служебные слова типа ИНН/ООО)
    if not re.search(r"\d", t):
        return False
    # допустимые символы: буквы/цифры и - _ / . и пробел
    if re.fullmatch(r"[A-Za-zА-Яа-я0-9\-_/\. ]{2,}", t):
        return True
    return False

def _locate_goods_section(df_raw: pd.DataFrame) -> tuple[Optional[int], Optional[int]]:
    """Ищем строку с «Товары (работы,услуги)» и рядом с ней возможную шапку.

    Возвращает (row_section, row_header) — row_header может быть None, если шапка не найдена.
    """
    tgt = "товарыработыуслуги"
    sec_row = None
    nrows = len(df_raw)
    for i in range(min(2000, nrows)):
        vals = df_raw.iloc[i].tolist()
        for v in vals:
            f = _flat_ru(_norm_cell(v))
            if tgt in f:
                sec_row = i
                break
        if sec_row is not None:
            break
    if sec_row is None:
        return None, None
    # Ищем шапку в пределах следующих 5 строк
    def row_has_headers(idx: int) -> bool:
        vals = [ _norm_header(_norm_cell(v)) for v in df_raw.iloc[idx].tolist() ]
        has_name = any(v in COL_NAME or ("товар" in v or "наимен" in v) for v in vals if v)
        has_qty  = any(v in COL_QTY or v.startswith("кол-") or "кол" in v or "мест" in v or "шт" in v for v in vals if v)
        return has_name and has_qty
    for h in range(sec_row, min(sec_row+6, nrows)):
        if row_has_headers(h):
            return sec_row, h
    return sec_row, None

def _unique_headers(cells: list[str]) -> list[str]:
    seen = {}
    out = []
    for idx, v in enumerate(cells):
        name = _norm_cell(v) or f"col{idx}"
        base = name
        k = 1
        while name in seen:
            k += 1
            name = f"{base}_{k}"
        seen[name] = True
        out.append(name)
    return out

def _infer_cols_no_header(df_block: pd.DataFrame) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Оцениваем индексы колонок: (name_idx, qty_idx, art_idx or None) без явной шапки.

    Выбираем пару (j, j+1), где j+1 чаще всего числовая в первых 50 строках.
    Проверяем слева (j-1) как возможный артикул.
    """
    if df_block.empty:
        return None, None, None
    sample = min(50, len(df_block))
    ncols = df_block.shape[1]
    best = (-1, -1, -1)  # (score, name_idx, qty_idx)
    for j in range(0, max(0, ncols - 1)):
        qty_col = df_block.columns[j+1]
        cnt_num = 0
        for i in range(sample):
            val = df_block.iloc[i][qty_col]
            if _to_float_qty(val) is not None:
                cnt_num += 1
        score = cnt_num
        if score > best[0]:
            best = (score, j, j+1)
    if best[0] <= 0:
        return None, None, None
    name_idx, qty_idx = best[1], best[2]
    art_idx = None
    if name_idx - 1 >= 0:
        art_col = df_block.columns[name_idx - 1]
        cnt_art = 0; cnt_nonempty = 0
        for i in range(sample):
            raw = _norm_cell(df_block.iloc[i][art_col])
            if raw:
                cnt_nonempty += 1
                if _looks_like_article(raw):
                    cnt_art += 1
        if cnt_nonempty > 0 and cnt_art / max(1, cnt_nonempty) >= 0.5:
            art_idx = name_idx - 1
    return name_idx, qty_idx, art_idx

# (Удалено) import_goods_section_xls — теперь импорт идёт только через нормализованный CSV

# ========= 3a) НОРМАЛИЗАЦИЯ В CSV =========

def _write_normalized_csv(rows: List[Tuple[str, str, float]], base_name: str) -> str:
    """Сохраняет нормализованные строки в CSV с колонками article,name,qty.

    Возвращает путь к CSV.
    """
    safe_base = re.sub(r"[^A-Za-zА-Яа-я0-9_.\-]+", "_", base_name)
    out_path = NORMALIZED_DIR / f"{safe_base}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["article", "name", "qty"])
        w.writeheader()
        for art, name, qty in rows:
            w.writerow({"article": art, "name": name, "qty": qty})
    return str(out_path)

def excel_to_normalized_csv(path: str) -> Tuple[Optional[str], dict]:
    """Конвертирует Excel в нормализованный CSV (article,name,qty).

    Возвращает (csv_path, stats). csv_path=None, если выделить товары не удалось.
    """
    stats = {"found": 0, "errors": []}
    rows_out: List[Tuple[str, str, float]] = []
    base_name = Path(path).stem
    try:
        selected = None
        sel_hdr = None
        sel_hdr_vals = None
        for sheet_name, raw in _iter_excel_sheets_raw(path):
            sec_row, hdr_row = _locate_goods_section(raw)
            if sec_row is None:
                continue
            start_data = (hdr_row + 1) if hdr_row is not None else (sec_row + 1)
            block = raw.iloc[start_data:].copy()
            block = block.dropna(how="all")
            if block.empty:
                continue
            selected = block
            sel_hdr = hdr_row
            sel_hdr_vals = raw.iloc[hdr_row].tolist() if hdr_row is not None else None
            break
        if selected is None:
            return None, stats

        if sel_hdr is not None:
            # Попробуем найти индексы заголовков по самой строке шапки (разряженная таблица)
            hdr_cells = [str(v) if v is not None else "" for v in (sel_hdr_vals or selected.iloc[0].tolist())]
            a_idx, n_idx, q_idx = _find_header_triplet(hdr_cells)
            if n_idx is not None and q_idx is not None:
                df = selected.reset_index(drop=True)
                name_col = df.columns[n_idx]
                qty_col = df.columns[q_idx]
                art_col = df.columns[a_idx] if a_idx is not None else None
            else:
                # Фоллбек к прежнему способу через _detect_columns
                headers = _unique_headers([_norm_cell(v) for v in hdr_cells])
                df = selected.iloc[:, :len(headers)].copy()
                df.columns = headers
                col_art, col_name, col_qty, _ = _detect_columns(df)
                if not (col_name and col_qty):
                    df = selected.reset_index(drop=True)
                    name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
                    if name_idx is None or qty_idx is None:
                        return None, stats
                    name_col = df.columns[name_idx]; qty_col = df.columns[qty_idx]
                    art_col = df.columns[art_idx] if art_idx is not None else None
                else:
                    name_col = col_name; qty_col = col_qty; art_col = col_art
        else:
            df = selected.reset_index(drop=True)
            name_idx, qty_idx, art_idx = _infer_cols_no_header(df)
            if name_idx is None or qty_idx is None:
                return None, stats
            name_col = df.columns[name_idx]; qty_col = df.columns[qty_idx]
            art_col = df.columns[art_idx] if art_idx is not None else None

        empty_streak = 0
        for _, row in df.iterrows():
            raw_name = _norm_cell(row.get(name_col, ""))
            if not raw_name:
                empty_streak += 1
                if empty_streak >= 10:
                    break
                continue
            empty_streak = 0
            low = raw_name.lower()
            if any(x in low for x in ("итог", "всего", "итого")):
                break
            qty = _to_float_qty(row.get(qty_col))
            if qty is None or qty <= 0:
                continue
            art = None
            if art_col is not None:
                art = _norm_cell(row.get(art_col, "")) or None
            # Если артикула нет или он не похож — попробуем извлечь из названия
            if not art or not _looks_like_article(art):
                m = _ART_RX.match(raw_name)
                if m:
                    art, raw_name = m.group(1), m.group(2)
            name, brand = _clean_name(raw_name)
            if _emptyish(art) or not _looks_like_article(art):
                continue
            # имя должно содержать буквы
            if _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                continue
            rows_out.append((art, name, float(qty)))
        stats["found"] = len(rows_out)
        if not rows_out:
            return None, stats
        out_csv = _write_normalized_csv(rows_out, base_name)
        return out_csv, stats
    except Exception as e:
        stats["errors"].append(str(e))
        return None, stats

def csv_to_normalized_csv(path: str) -> Tuple[Optional[str], dict]:
    """Приводит произвольный CSV к нормализованному article,name,qty.

    Пытается определить колонки по заголовкам/эвристике.
    """
    stats = {"found": 0, "errors": []}
    rows_out: List[Tuple[str, str, float]] = []
    base_name = Path(path).stem
    try:
        # Пробуем без заголовка и ищем разреженную шапку: Артикул / Товары / Количество
        df = pd.read_csv(path, header=None, dtype=object)
        df = df.dropna(how="all")
        if df.empty:
            return None, stats
        header_found = False
        a_idx = n_idx = q_idx = None
        start_row = 0
        for i in range(len(df)):
            cells = [str(df.iloc[i, j]) if j < df.shape[1] and df.iloc[i, j] is not None else '' for j in range(df.shape[1])]
            ai, ni, qi = _find_header_triplet(cells)
            # Требуем совпадения хотя бы по name и qty
            if ni is not None and qi is not None:
                header_found = True
                a_idx, n_idx, q_idx = ai, ni, qi
                start_row = i + 1
                break
        if not header_found:
            # Фоллбек: пробуем прочитать с header=0 и детектить по именам колонок
            dfh = pd.read_csv(path, dtype=object)
            dfh = dfh.dropna(how="all")
            if dfh.empty:
                return None, stats
            col_art, col_name, col_qty, _ = _detect_columns(dfh)
            if not col_name or not col_qty:
                return None, stats
            name_col = col_name; qty_col = col_qty; art_col = col_art
            # Перебор строк с обычным DataFrame (dfh)
            for _, row in dfh.iterrows():
                raw_name = _norm_cell(row.get(name_col, ""))
                qty = _to_float_qty(row.get(qty_col))
                art = _norm_cell(row.get(art_col, "")) if art_col is not None else None
                if not art or not _looks_like_article(art):
                    m = _ART_RX.match(raw_name)
                    if m:
                        art, raw_name = m.group(1), m.group(2)
                name, brand = _clean_name(raw_name)
                if qty is None or qty <= 0 or _emptyish(art) or not _looks_like_article(art) or _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                    continue
                rows_out.append((art, name, float(qty)))
        else:
            # Идём по строкам после шапки в разреженной таблице
            empty_streak = 0
            for i in range(start_row, len(df)):
                row = df.iloc[i]
                # повторная шапка или пустая строка
                cells = [str(row[j]) if j < df.shape[1] and row[j] is not None else '' for j in range(df.shape[1])]
                ai, ni, qi = _find_header_triplet(cells)
                if ni is not None and qi is not None:
                    empty_streak = 0
                    continue  # повторная шапка
                art = _norm_cell(row[a_idx]) if a_idx is not None and a_idx < df.shape[1] else ''
                name_cell = _norm_cell(row[n_idx]) if n_idx < df.shape[1] else ''
                qty_cell = row[q_idx] if q_idx < df.shape[1] else None
                if _emptyish(art) and _emptyish(name_cell) and _to_float_qty(qty_cell) in (None, 0):
                    empty_streak += 1
                    if empty_streak >= 3:
                        break
                    continue
                empty_streak = 0
                qty = _to_float_qty(qty_cell)
                if qty is None or qty <= 0:
                    continue
                raw_name = name_cell
                if _emptyish(art):
                    m = _ART_RX.match(raw_name)
                    if m:
                        art, raw_name = m.group(1), m.group(2)
                name, brand = _clean_name(raw_name)
                if _emptyish(art) or not _looks_like_article(art) or _emptyish(name) or not re.search(r"[A-Za-zА-Яа-я]", name):
                    continue
                rows_out.append((art, name, float(qty)))

        stats["found"] = len(rows_out)
        if not rows_out:
            return None, stats
        out_csv = _write_normalized_csv(rows_out, base_name)
        return out_csv, stats
    except Exception as e:
        stats["errors"].append(str(e))
        return None, stats

def import_supply_from_normalized_csv(path: str) -> dict:
    """Читает CSV формата article,name,qty и приходует в SKL-0.
    Возвращает статистику import/create/update.
    """
    stats = {"imported": 0, "created": 0, "updated": 0, "errors": [], "to_skl": {}}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if r.fieldnames is None or set(map(str.lower, r.fieldnames)) != {"article", "name", "qty"}:
                stats["errors"].append("CSV должен иметь колонки article,name,qty (utf-8)")
                return stats
            conn = db()
            row_idx = 1  # с учётом заголовка = 1, данные начнутся со 2
            for row in r:
                row_idx += 1
                try:
                    art = (row.get("article") or "").strip()
                    name = (row.get("name") or "").strip()
                    qty = _to_float_qty(row.get("qty"))
                    if _emptyish(art) or _emptyish(name) or qty is None or qty <= 0:
                        continue
                    if not _looks_like_article(art):
                        # Пропускаем строки с «непохожими» артикулами (часто служебные)
                        continue
                    with conn:
                        # Безопасно к гонкам: вставляем или игнорируем если уже есть
                        cur = conn.execute(
                            "INSERT OR IGNORE INTO product(article, name, is_new) VALUES (?,?,1)",
                            (art, name),
                        )
                        pid = conn.execute("SELECT id FROM product WHERE article=?", (art,)).fetchone()["id"]
                        if (cur.rowcount or 0) > 0:
                            stats["created"] += 1
                        else:
                            # Обновим имя, если оно пустое
                            conn.execute(
                                "UPDATE product SET name = COALESCE(NULLIF(name,''), ?) WHERE id=?",
                                (name, pid),
                            )
                            stats["updated"] += 1
                        # Получим имя/локальное имя для пользовательских полей в stock
                        prow = conn.execute(
                            "SELECT name, local_name FROM product WHERE id=?",
                            (pid,)
                        ).fetchone()
                        # добавим/обновим остаток с одновременной записью name/local_name
                        conn.execute(
                            """
                            INSERT INTO stock(product_id, location_code, qty_pack, name, local_name)
                            VALUES (?,?,?,?,?)
                            ON CONFLICT(product_id, location_code)
                            DO UPDATE SET
                                qty_pack = qty_pack + excluded.qty_pack,
                                name = excluded.name,
                                local_name = excluded.local_name
                            """,
                            (pid, "SKL-0", float(qty), prow["name"], prow["local_name"]),
                        )
                        # логируем для дневной сводки "ушёл на склад"
                        _log_event_to_skl(conn, pid, "SKL-0", float(qty))
                        # аккумулируем для возможных мгновенных уведомлений
                        stats["to_skl"][pid] = stats["to_skl"].get(pid, 0) + float(qty)
                    stats["imported"] += 1
                except Exception as e_row:
                    stats["errors"].append(f"CSV строка {row_idx}: {e_row}")
                    continue
            conn.close()
        return stats
    except Exception as e:
        stats["errors"].append(str(e))
        return stats

# (Удалено) _find_col_by_name — не используется

def _to_float_qty(val) -> Optional[float]:
    """Парсинг количества: только конечные (не NaN/inf) числа > 0.

    Возвращает float или None, если не число.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    try:
        f = float(s)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f

# Photo helpers moved to app.services.photos and imported above

# (Удалено) _clean_name_custom — не используется

# (Удалено) import_excel_by_rules — не используется, импорт уже универсальный через CSV

# (Удалено) import_supply_xls — заменён на конвейер нормализации в CSV

# ========= 4) КЛАВИАТУРЫ/ТЕКСТЫ =========

def kb_main(user_id: Optional[int] = None, username: Optional[str] = None) -> InlineKeyboardMarkup:
    conn = db()
    b = InlineKeyboardBuilder()
    admin = is_admin(user_id or 0, username)
    seller = is_seller(user_id or 0, username)
    # Главное меню без склад‑хаба
    if admin:
        b.button(text="Наличие", callback_data="stock")
        b.button(text="Инвентаризация", callback_data="inventory")
        b.adjust(2)
        # Расписание доступно всем
        today_ym = dt.date.today().strftime("%Y-%m")
        b.button(text="Расписание", callback_data=f"sched|month|{today_ym}")
        b.adjust(1)
        if _has_incomplete(conn):
            b.button(text="🧩 Заполнить карточки", callback_data="complete_cards|1")
            b.adjust(1)
        b.button(text="🛠️ Администрирование", callback_data="admin")
        b.adjust(1)
    else:
        # Продавец
        b.button(text="Наличие", callback_data="stock")
        b.adjust(1)
        today_ym = dt.date.today().strftime("%Y-%m")
        b.button(text="Расписание", callback_data=f"sched|month|{today_ym}")
        b.adjust(1)
    b.row(InlineKeyboardButton(text="🔎 Поиск", switch_inline_query_current_chat=""))
    conn.close()
    return b.as_markup()

# product_caption/stocks_summary moved to app.ui.texts

def _extract_pid_from_cbdata(data: str) -> Optional[int]:
    """Извлекает pid из callback_data разных форматов.
    Поддерживает: 'open|{pid}', 'qty_ok|{pid}|...', 'route_dst_chosen|{pid}|{code}' и т.д.
    """
    if not data:
        return None
    # Быстрый путь — строго 'open|pid'
    if data.startswith("open|"):
        try:
            return int(data.split("|", 1)[1])
        except Exception:
            return None
    # Иначе ищем первое числовое поле после первого разделителя
    parts = data.split("|")
    for token in parts[1:]:
        if token.isdigit():
            try:
                return int(token)
            except Exception:
                pass
    return None

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

# ========= 5) ДВИЖЕНИЯ (мастер) =========

def _sanitize_product_name(name: str) -> str:
    s = (name or "").strip()
    return re.sub(r"\s{2,}", " ", s)

# (user_id, product_id) -> {"src": Optional[str], "dst": Optional[str], "qty": int}
move_ctx: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}

def _ctx(uid: int, pid: int) -> Dict[str, Optional[str]]:
    c = move_ctx.get((uid, pid))
    if not c:
        c = {"src": None, "dst": None, "qty": 1}
        move_ctx[(uid, pid)] = c
    return c

def _ctx_badge(ctx: Dict[str, Optional[str]]) -> str:
    return f"(из: {ctx.get('src') or '—'} → в: {ctx.get('dst') or '—'}, кол-во: {ctx.get('qty') or 1})"

def kb_card(pid: int, uid: int, is_new: int = 0, need_local: bool = False, need_photo: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="🛒 В зал (−1)", callback_data=f"mv_hall|{pid}|1")
    b.adjust(1)

    # Спец-действие: если товар есть в SKL-0 и существует ровно одна другая локация (кроме SKL-0/HALL),
    # показать кнопку «Всё из SKL-0 → {эта_локация}»
    try:
        conn = db()
        rows = conn.execute(
            "SELECT location_code, qty_pack FROM stock WHERE product_id=? AND qty_pack>0",
            (pid,),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass
    try:
        has_skl0 = any((r["location_code"] == "SKL-0") for r in rows)
        non_skl0_dests = [r["location_code"] for r in rows if r["location_code"] not in ("SKL-0", "HALL")]
        if has_skl0 and len(non_skl0_dests) == 1:
            dst = non_skl0_dests[0]
            b.button(text=f"⇥ Всё из SKL-0 → {dst}", callback_data=f"skl0all|{pid}|{dst}")
            b.adjust(1)
    except Exception:
        pass

    b.button(text="🚚 Маршрут: откуда → куда", callback_data=f"route|{pid}")
    b.adjust(1)
    # Количество управляем прямо из карточки: −1 / +1 на одной линии
    # Текущее значение берём из контекста пользователя
    q = int(_ctx(uid, pid).get("qty") or 1)
    b.button(text="−1", callback_data=f"qty_card|{pid}|-1")
    b.button(text="+1", callback_data=f"qty_card|{pid}|1")
    b.adjust(2)
    b.button(text=f"Переместить {q}", callback_data=f"commit_move|{pid}")
    b.adjust(1)
    if is_new:
        b.button(text="✅ Готово (снять NEW)", callback_data=f"unset_new|{pid}")
        b.adjust(1)
    if need_local:
        b.button(text="📝 Добавить название", callback_data=f"add_local_name|{pid}")
        b.adjust(1)
    if need_photo:
        b.button(text="🖼️ Добавить фото", callback_data=f"add_photo|{pid}")
        b.adjust(1)
    b.button(text="← Назад к списку", callback_data="complete_cards|1")
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
        disp = int(q) if float(q).is_integer() else q
        label[r["location_code"]] = f"{r['location_code']} ({disp})"
    return locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"src_chosen|{pid}|{code}",
        label_for=label,
        back_cb=f"open|{pid}"
    )

def kb_pick_dst(pid: int) -> InlineKeyboardMarkup:
    conn = db()
    have = {r["location_code"] for r in conn.execute(
        "SELECT location_code FROM stock WHERE product_id=? AND qty_pack>0", (pid,)
    ).fetchall()}
    all_codes = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    avail = [c for c in all_codes if c not in have and c != "HALL"]
    return locations_2col_keyboard(
        active_codes=avail,
        cb_for=lambda code: f"dst_chosen|{pid}|{code}",
        label_for=None,
        back_cb=f"open|{pid}",
        hall_option=("В ЗАЛ (списание)", f"dst_hall|{pid}")
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
        disp = int(q) if float(q).is_integer() else q
        label[r["location_code"]] = f"{r['location_code']} ({disp})"
    return locations_2col_keyboard(
        active_codes=codes,
        cb_for=lambda code: f"route_src_chosen|{pid}|{code}",
        label_for=label,
        back_cb=f"open|{pid}"
    )

def kb_route_dst(pid: int) -> InlineKeyboardMarkup:
    conn = db()
    have = {r["location_code"] for r in conn.execute(
        "SELECT location_code FROM stock WHERE product_id=? AND qty_pack>0", (pid,)
    ).fetchall()}
    all_codes = [r["code"] for r in conn.execute("SELECT code FROM location ORDER BY kind, code").fetchall()]
    conn.close()
    avail = [c for c in all_codes if c not in have and c != "HALL"]
    return locations_2col_keyboard(
        active_codes=avail,
        cb_for=lambda code: f"route_dst_chosen|{pid}|{code}",
        label_for=None,
        back_cb=f"open|{pid}",
        hall_option=("В ЗАЛ (списание)", f"route_dst_chosen|{pid}|HALL")
    )

"""Stock movement helpers moved to app.services.stock"""

# ========= 6) ИНВЕНТАРИЗАЦИЯ (+/− в локации) =========

# inv_loc_ctx[user_id] = {"loc": "SKL-0"}
inv_loc_ctx: Dict[int, Dict[str, str]] = {}
# inv_qty_ctx[(user_id, product_id)] = {"loc": "SKL-0", "qty": 1}
inv_qty_ctx: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}

def _inv_loc_set(uid: int, code: str):
    inv_loc_ctx[uid] = {"loc": code}

def _inv_loc_get(uid: int) -> Optional[str]:
    d = inv_loc_ctx.get(uid)
    return d.get("loc") if d else None

def _inv_ctx(uid: int, pid: int) -> Dict[str, Optional[str]]:
    c = inv_qty_ctx.get((uid, pid))
    if not c:
        c = {"loc": _inv_loc_get(uid), "qty": 1}
        inv_qty_ctx[(uid, pid)] = c
    return c

"""Inventory adjustment moved to app.services.stock"""

# ========= 7) РОУТЕРЫ =========

@router.message(CommandStart())
async def on_start(m: Message):
    if not is_allowed(m.from_user.id, m.from_user.username):
        return
    try:
        await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id, m.from_user.username))
    except Exception:
        # На случай сетевой ошибки пробуем ещё раз через короткую паузу
        try:
            await asyncio.sleep(0.5)
            await m.answer("Главное меню:", reply_markup=kb_main(m.from_user.id, m.from_user.username))
        except Exception:
            pass

@router.callback_query(F.data == "home")
async def cb_home(cb: CallbackQuery):
    await cb.message.edit_text("Главное меню:", reply_markup=kb_main(cb.from_user.id, cb.from_user.username))
    await cb.answer()

# --- Поставка ---

@router.callback_query(F.data == "supply")
async def cb_supply(cb: CallbackQuery):
    from app.handlers.supply import cb_supply as _h
    return await _h(cb)

@router.callback_query(F.data == "supply_upload")
async def cb_supply_upload(cb: CallbackQuery, state: FSMContext):
    from app.handlers.supply import cb_supply_upload as _h
    return await _h(cb, state)

@router.message(F.document)
async def on_document(m: Message, state: FSMContext):
    from app.handlers.supply import on_document as _h
    return await _h(m, state)

def kb_supply_page(conn: sqlite3.Connection, page: int) -> InlineKeyboardMarkup:
    off = (page-1)*PAGE_SIZE
    rows = conn.execute("""
        SELECT id, article, name FROM product
        WHERE is_new=1 AND archived=0 ORDER BY id DESC LIMIT ? OFFSET ?
    """, (PAGE_SIZE, off)).fetchall()
    items = [(f"{r['article']} | {r['name'][:40]}", f"open|{r['id']}") for r in rows]
    count = conn.execute("SELECT COUNT(*) AS c FROM product WHERE is_new=1 AND archived=0").fetchone()["c"]
    kb = grid_buttons(items, per_row=1, back_cb="supply")
    nav=[]
    if page>1: nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"supply_list|{page-1}"))
    if off+PAGE_SIZE<count: nav.append(InlineKeyboardButton(text="➡️", callback_data=f"supply_list|{page+1}"))
    if nav: kb.inline_keyboard.append(nav)
    return kb

@router.callback_query(F.data.startswith("supply_list|"))
async def supply_list(cb: CallbackQuery):
    from app.handlers.supply import supply_list as _h
    return await _h(cb)

# --- Inline-поиск (NEW для новых, INV для инвентаризации, INC незаполненные, ADM админ) ---

@router.inline_query()
async def inline_query(iq: InlineQuery):
    from app.handlers.inline import inline_query as _h
    return await _h(iq)

    inv_mode = False
    if q.upper().startswith("INV "):
        inv_mode = True
        q = q[4:].strip()

    only_new = False
    if q.upper().startswith("NEW "):
        only_new = True
        q = q[4:].strip()

    only_incomplete = False
    if q.upper().startswith("INC "):
        only_incomplete = True
        q = q[4:].strip()

    admin_mode = False
    if q.upper().startswith("ADM "):
        admin_mode = True
        q = q[4:].strip()

    rows = []
    try:
        if q:
            if only_new:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ? AND p.is_new=1
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (q.replace(" ", "* ")+"*", 1 if admin_mode else 0)).fetchall()
            elif only_incomplete:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ?
                      AND (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (q.replace(" ", "* ")+"*", 1 if admin_mode else 0)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE product_fts MATCH ?
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (q.replace(" ", "* ")+"*", 1 if admin_mode else 0)).fetchall()
        else:
            if only_new:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE p.is_new=1 AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (1 if admin_mode else 0,)).fetchall()
            elif only_incomplete:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                      AND (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (1 if admin_mode else 0,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT p.id, p.article, p.name, p.local_name
                    FROM product p
                    LEFT JOIN (
                        SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                    ) t ON t.product_id=p.id
                    WHERE (COALESCE(t.total,0) > 0 OR ?=1)
                    ORDER BY p.id DESC LIMIT 50
                """, (1 if admin_mode else 0,)).fetchall()
    except sqlite3.OperationalError:
        like = f"%{q}%"
        if only_new:
            rows = conn.execute("""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE p.is_new=1
                  AND (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
            """, (like, like, like, 1 if admin_mode else 0)).fetchall()
        elif only_incomplete:
            rows = conn.execute("""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')=''))
                  AND (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
            """, (like, like, like, 1 if admin_mode else 0)).fetchall()
        else:
            rows = conn.execute("""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (
                    SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id
                ) t ON t.product_id=p.id
                WHERE (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                  AND (COALESCE(t.total,0) > 0 OR ?=1)
                ORDER BY p.id DESC LIMIT 50
            """, (like, like, like, 1 if admin_mode else 0)).fetchall()

    # Доп. поиск по LIKE (упрощённая строка: ё→е) — для ловли опечаток. Объединяем с основными результатами.
    if q:
        like_raw = f"%{q}%"
        sq = _simplify_query(q)
        like_simpl = f"%{sq}%"
        cond_total = "COALESCE(t.total,0) > 0 OR ?=1"
        extra = []
        if only_new:
            extra = conn.execute(f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE p.is_new=1 AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
            """, (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0)).fetchall()
        elif only_incomplete:
            extra = conn.execute(f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE (p.local_name IS NULL OR (p.photo_file_id IS NULL AND COALESCE(p.photo_path,'')='')) AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
            """, (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0)).fetchall()
        else:
            extra = conn.execute(f"""
                SELECT p.id, p.article, p.name, p.local_name
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                ) AND ({cond_total})
                ORDER BY p.id DESC LIMIT 50
            """, (like_raw, like_raw, like_raw, like_simpl, like_simpl, 1 if admin_mode else 0)).fetchall()

        by_id = {r["id"]: r for r in rows}
        for r in extra:
            if r["id"] not in by_id:
                by_id[r["id"]] = r
        rows = list(by_id.values())[:50]

    results=[]
    for r in rows:
        pid = r["id"]
        disp_name = r["local_name"] or r["name"]
        stock = stocks_summary(conn, pid)
        if admin_mode:
            cmd = f"/admin_{pid}"
        else:
            cmd = f"/inv_{pid}" if inv_mode else f"/open_{pid}"
        results.append(InlineQueryResultArticle(
            id=str(pid),
            title=f"{disp_name}",
            input_message_content=InputTextMessageContent(message_text=cmd),
            description=("Админ действия — " if admin_mode else ("")) + f"Остатки: {stock}"
        ))
    await iq.answer(results=results, cache_time=1, is_personal=True)
    conn.close()

@router.message(F.text.regexp(r"^/open_(\d+)$"))
async def cmd_open(m: Message):
    from app.handlers.product import cmd_open as _h
    return await _h(m)

@router.message(F.text.regexp(r"^/inv_(\d+)$"))
async def cmd_inv(m: Message):
    if not is_allowed(m.from_user.id, m.from_user.username):
        return
    pid = int(re.search(r"^/inv_(\d+)$", m.text).group(1))
    code = _inv_loc_get(m.from_user.id)
    if not code:
        await m.answer("Сначала выберите локацию в «Инвентаризация»."); return
    await inv_open_card_message(m, pid, code)

@router.message(F.text.regexp(r"^/admin_(\d+)$"))
async def cmd_admin_item(m: Message):
    from app.handlers.product import cmd_admin_item as _h
    return await _h(m)

# --- Карточка товара ---

@router.callback_query(F.data.startswith("open|"))
async def open_card(cb: CallbackQuery):
    from app.handlers.product import open_card as _h
    return await _h(cb)

def build_card_for_user(pid: int, uid: int, conn: Optional[sqlite3.Connection] = None, product_row: Optional[sqlite3.Row] = None):
    close_later = False
    if conn is None:
        conn = db(); close_later = True
    try:
        r = product_row or conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
        if not r:
            return None, None
        caption = product_caption(conn, r)
        # Определяем, нужны ли действия по заполнению
        local_val = (r["local_name"] or "") if "local_name" in r.keys() else ""
        photo_id = r["photo_file_id"] if "photo_file_id" in r.keys() else None
        photo_path = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else ""
        need_local = (local_val.strip() == "")
        need_photo = (not bool(photo_id) and not bool(photo_path))

        # Автоснятие NEW, если и локальное имя, и фото заполнены
        is_new = int(r["is_new"] or 0)
        if is_new and (not need_local) and (not need_photo):
            try:
                with conn:
                    conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
                is_new = 0
            except Exception:
                pass
        ctx = move_ctx.get((uid, pid))
        if ctx:
            caption += f"\n\n<i>Выбрано для перемещения: {_ctx_badge(ctx)}</i>"
        kb = kb_card(pid, uid, is_new, need_local, need_photo)
        return caption, kb
    finally:
        if close_later:
            conn.close()

@router.callback_query(F.data.startswith("unset_new|"))
async def unset_new(cb: CallbackQuery):
    _, pid_s = cb.data.split("|",1); pid = int(pid_s)
    conn = db()
    with conn:
        conn.execute("UPDATE product SET is_new=0 WHERE id=?", (pid,))
    conn.close()
    await cb.answer("Снята метка NEW")
    await open_card(cb)

# --- Выбор источника/назначения/количества и перемещение ---

@router.callback_query(F.data.startswith("pick_src|"))
async def pick_src(cb: CallbackQuery):
    from app.handlers.product import pick_src as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("src_chosen|"))
async def src_chosen(cb: CallbackQuery):
    from app.handlers.product import src_chosen as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("pick_dst|"))
async def pick_dst(cb: CallbackQuery):
    from app.handlers.product import pick_dst as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("route|"))
async def route_start(cb: CallbackQuery):
    from app.handlers.product import route_start as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("route_src_chosen|"))
async def route_src_chosen(cb: CallbackQuery):
    from app.handlers.product import route_src_chosen as _h
    return await _h(cb)
    

@router.callback_query(F.data.startswith("route_dst_chosen|"))
async def route_dst_chosen(cb: CallbackQuery):
    from app.handlers.product import route_dst_chosen as _h
    return await _h(cb)

# --- Админ: карточка выбора действий для товара ---

@router.callback_query(F.data.startswith("admin_item|"))
async def admin_item(cb: CallbackQuery):
    from app.handlers.product import admin_item as _h
    return await _h(cb)

def build_admin_item_card(pid: int) -> tuple[Optional[str], Optional[InlineKeyboardMarkup]]:
    conn = db()
    try:
        r = conn.execute("SELECT * FROM product WHERE id=?", (pid,)).fetchone()
        if not r:
            return None, None
        caption = product_caption(conn, r)
        b = InlineKeyboardBuilder()
        b.button(text="✏️ Редактировать", callback_data=f"admin_edit|{pid}")
        b.button(text="🗑️ Удалить товар", callback_data=f"admin_del|{pid}")
        b.button(text="➕ В SKL-0 (+1)", callback_data=f"admin_skl0|{pid}|add")
        b.button(text="➖ Из SKL-0 (−1)", callback_data=f"admin_skl0|{pid}|sub")
        b.button(text="➕ На локацию…", callback_data=f"admin_add_loc|{pid}")
        b.button(text="↔️ Переместить", callback_data=f"route|{pid}")
        b.button(text="📄 Открыть карточку", callback_data=f"open|{pid}")
        b.adjust(1)
        b.button(text="← Назад", callback_data="admin")
        return caption, b.as_markup()
    finally:
        conn.close()

@router.callback_query(F.data.startswith("admin_del|"))
async def admin_del(cb: CallbackQuery):
    from app.handlers.product import admin_del as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("admin_del_yes|"))
async def admin_del_yes(cb: CallbackQuery):
    from app.handlers.product import admin_del_yes as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("admin_skl0|"))
async def admin_skl0(cb: CallbackQuery):
    from app.handlers.product import admin_skl0 as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("dst_hall|"))
async def dst_hall(cb: CallbackQuery):
    from app.handlers.product import dst_hall as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("dst_chosen|"))
async def dst_chosen(cb: CallbackQuery):
    from app.handlers.product import dst_chosen as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("pick_qty|"))
async def pick_qty(cb: CallbackQuery):
    from app.handlers.product import pick_qty as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("qty|"))
async def qty_change(cb: CallbackQuery):
    from app.handlers.product import qty_change as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("qty_card|"))
async def qty_change_on_card(cb: CallbackQuery):
    from app.handlers.product import qty_change_on_card as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("qty_ok|"))
async def qty_ok(cb: CallbackQuery):
    from app.handlers.product import qty_ok as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("commit_move|"))
async def commit_move(cb: CallbackQuery):
    from app.handlers.product import commit_move as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("skl0all|"))
async def skl0_all_to_single(cb: CallbackQuery):
    from app.handlers.product import skl0_all_to_single as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("mv_hall|"))
async def mv_hall(cb: CallbackQuery):
    from app.handlers.product import mv_hall as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("mv_hall_from|"))
async def mv_hall_from(cb: CallbackQuery):
    from app.handlers.product import mv_hall_from as _h
    return await _h(cb)

# --- Инвентаризация UI ---

@router.callback_query(F.data == "inventory")
async def inv_root(cb: CallbackQuery):
    from app.handlers.inventory import inv_root as _h
    return await _h(cb)

def kb_inventory_location(conn: sqlite3.Connection, code: str, page: int = 1) -> InlineKeyboardMarkup:
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM stock WHERE location_code=? AND qty_pack>0",
        (code,)
    ).fetchone()["c"]
    off = (page - 1) * STOCK_PAGE_SIZE
    rows = conn.execute(
        """
        SELECT p.id, p.name, p.local_name, s.qty_pack
        FROM stock s JOIN product p ON p.id=s.product_id
        WHERE s.location_code=? AND s.qty_pack>0
        ORDER BY p.name
        LIMIT ? OFFSET ?
        """,
        (code, STOCK_PAGE_SIZE, off)
    ).fetchall()
    # Кастомная раскладка: строка с именем (noop), затем -1/+1
    rows_kb: list[list[InlineKeyboardButton]] = []
    rows_kb.append([InlineKeyboardButton(text="➕ Добавить товар", switch_inline_query_current_chat="INV ")])
    for r in rows:
        disp_name = (r["local_name"] or r["name"]).strip()
        qty_val = float(r["qty_pack"]) if r["qty_pack"] is not None else 0.0
        disp_qty = int(qty_val) if qty_val.is_integer() else qty_val
        rows_kb.append([InlineKeyboardButton(text=f"{disp_name[:35]} | {disp_qty}", callback_data="noop")])
        pid = int(r["id"]) if "id" in r.keys() else int(r[0])
        rows_kb.append([
            InlineKeyboardButton(text="−1", callback_data=f"inv_adj|{pid}|{code}|-1|{page}"),
            InlineKeyboardButton(text="+1", callback_data=f"inv_adj|{pid}|{code}|1|{page}"),
        ])
    # Навигация
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"inv_loc|{code}|{page-1}"))
    if off + STOCK_PAGE_SIZE < count:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"inv_loc|{code}|{page+1}"))
    if nav:
        rows_kb.append(nav)
    rows_kb.append([InlineKeyboardButton(text="← Назад", callback_data="inventory")])
    return InlineKeyboardMarkup(inline_keyboard=rows_kb)

@router.callback_query(F.data.startswith("inv_loc|"))
async def inv_loc(cb: CallbackQuery):
    from app.handlers.inventory import inv_loc as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("inv_open|"))
async def inv_open(cb: CallbackQuery):
    from app.handlers.inventory import inv_open as _h
    return await _h(cb)

async def inv_open_card(cb: CallbackQuery, pid: int, code: str):
    from app.handlers.inventory import inv_open_card as _h
    return await _h(cb, pid, code)

async def inv_open_card_message(m: Message, pid: int, code: str):
    from app.handlers.inventory import inv_open_card_message as _h
    return await _h(m, pid, code)

@router.callback_query(F.data.startswith("inv_qty|"))
async def inv_qty_change(cb: CallbackQuery):
    from app.handlers.inventory import inv_qty_change as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("inv_qty_ok|"))
async def inv_qty_ok(cb: CallbackQuery):
    from app.handlers.inventory import inv_qty_ok as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("inv_commit|"))
async def inv_commit(cb: CallbackQuery):
    from app.handlers.inventory import inv_commit as _h
    return await _h(cb)

# --- Завершить карточки (локальное имя/фото) ---

def kb_cards_page(conn: sqlite3.Connection, page: int) -> InlineKeyboardMarkup:
    off=(page-1)*CARDS_PAGE_SIZE
    rows = conn.execute("""
        SELECT id, article, name, local_name, photo_file_id, photo_path
        FROM product
        WHERE local_name IS NULL OR (photo_file_id IS NULL AND COALESCE(photo_path,'')='')
        ORDER BY id DESC LIMIT ? OFFSET ?
    """,(CARDS_PAGE_SIZE, off)).fetchall()
    items=[]
    for r in rows:
        miss=[]
        if not r["local_name"]: miss.append("название")
        if not r["photo_file_id"] and not (r["photo_path"] or "").strip():
            miss.append("фото")
        # Не показываем артикул здесь
        disp = _sanitize_product_name(r['name'])
        items.append((f"{disp[:40]} (нет: {', '.join(miss)})", f"open|{r['id']}"))
    count = conn.execute("""
        SELECT COUNT(*) AS c FROM product
        WHERE local_name IS NULL OR (photo_file_id IS NULL AND COALESCE(photo_path,'')='')
    """).fetchone()["c"]
    # Возврат в главное меню
    kb = grid_buttons(items, per_row=1, back_cb="home")
    # Инлайн-поиск по незаполненным карточкам
    kb.inline_keyboard.insert(0, [InlineKeyboardButton(text="🔎 Поиск незаполненных", switch_inline_query_current_chat="INC ")])
    nav=[]
    if page>1: nav.append(InlineKeyboardButton(text="⬅️",callback_data=f"complete_cards|{page-1}"))
    if off+CARDS_PAGE_SIZE<count: nav.append(InlineKeyboardButton(text="➡️",callback_data=f"complete_cards|{page+1}"))
    if nav: kb.inline_keyboard.append(nav)
    return kb

@router.callback_query(F.data.startswith("complete_cards|"))
async def complete_cards(cb: CallbackQuery):
    _, page_s = cb.data.split("|",1); page = max(1,int(page_s))
    conn = db()
    kb = kb_cards_page(conn, page)
    conn.close()
    await cb.message.edit_text("Незавершённые карточки:", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("add_local_name|"))
async def add_local_name(cb: CallbackQuery, state: FSMContext):
    _, pid_s = cb.data.split("|",1); pid=int(pid_s)
    await state.set_state(CardFill.wait_local_name)
    await state.update_data(pid=pid)
    await cb.message.answer("Введите локальное название сообщением.")
    await cb.answer()

@router.message(CardFill.wait_local_name, F.text.len() > 0)
async def save_local_name(m: Message, state: FSMContext):
    data = await state.get_data(); pid = int(data["pid"])
    conn = db()
    with conn:
        conn.execute("UPDATE product SET local_name=? WHERE id=?", (m.text.strip(), pid))
        # Если теперь есть и локальное имя, и фото — снимаем NEW автоматически
        r = conn.execute(
            "SELECT COALESCE(local_name,'') AS ln, COALESCE(photo_file_id,'') AS pf, COALESCE(photo_path,'') AS pp, COALESCE(is_new,0) AS nw FROM product WHERE id=?",
            (pid,)
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
    _, pid_s = cb.data.split("|",1); pid=int(pid_s)
    await state.set_state(CardFill.wait_photo)
    await state.update_data(pid=pid)
    await cb.message.answer("Отправьте фотографию (как фото, не как файл). Я сожму её вдвое для быстрого показа.")
    await cb.answer()

@router.message(CardFill.wait_photo, F.photo)
async def save_photo(m: Message, state: FSMContext):
    data = await state.get_data(); pid = int(data["pid"])
    file_id = m.photo[-1].file_id
    # Скачиваем и сжимаем фото вдвое, сохраняем в media/photos
    rel_path = None
    try:
        rel_path = await _download_and_compress_photo(m.bot, file_id, pid)
    except Exception:
        rel_path = None
    conn = db()
    with conn:
        conn.execute("UPDATE product SET photo_file_id=?, photo_path=? WHERE id=?", (file_id, rel_path, pid))
        # Если теперь есть и локальное имя, и фото — снимаем NEW автоматически
        r = conn.execute(
            "SELECT COALESCE(local_name,'') AS ln, COALESCE(photo_file_id,'') AS pf, COALESCE(photo_path,'') AS pp, COALESCE(is_new,0) AS nw FROM product WHERE id=?",
            (pid,)
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

# --- Отчёты (кратко в чат) ---

@router.callback_query(F.data == "reports")
async def cb_reports(cb: CallbackQuery):
    from app.handlers.reports import cb_reports as _h
    return await _h(cb)

@router.callback_query(F.data == "rpt_low")
async def rpt_low(cb: CallbackQuery):
    conn = db()
    rows = conn.execute("""
        SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total
        FROM product p LEFT JOIN stock s ON s.product_id=p.id
        GROUP BY p.id HAVING total>0 AND total<2
        ORDER BY total ASC, p.id DESC LIMIT 1000
    """).fetchall()
    conn.close()
    if not rows:
        await cb.answer("Нет заканчивающихся.", show_alert=True); return
    # Пишем CSV в reports/
    import time, csv
    ts = time.strftime('%Y%m%d-%H%M%S')
    path = REPORTS_DIR / f"low_{ts}.csv"
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["name", "article", "total"])
        for r in rows:
            tot = float(r['total'])
            disp_tot = int(tot) if tot.is_integer() else tot
            w.writerow([r['disp_name'], r['article'], disp_tot])
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="reports")]])
    await cb.message.answer_document(FSInputFile(path), caption="Заканчиваются (&lt;2)", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "rpt_zero")
async def rpt_zero(cb: CallbackQuery):
    from app.handlers.reports import rpt_zero as _h
    return await _h(cb)

@router.callback_query(F.data == "rpt_mid")
async def rpt_mid(cb: CallbackQuery):
    from app.handlers.reports import rpt_mid as _h
    return await _h(cb)

@router.callback_query(F.data == "rpt_all")
async def rpt_all(cb: CallbackQuery):
    from app.handlers.reports import rpt_all as _h
    return await _h(cb)

# --- Наличие по локациям ---

@router.callback_query(F.data == "stock")
async def cb_stock(cb: CallbackQuery):
    from app.handlers.stock import cb_stock as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("stock_loc|"))
async def stock_loc(cb: CallbackQuery):
    from app.handlers.stock import stock_loc as _h
    return await _h(cb)

# --- Админ (заглушка) ---

@router.callback_query(F.data == "admin")
async def cb_admin(cb: CallbackQuery):
    from app.handlers.admin import cb_admin as _h
    return await _h(cb)

@router.callback_query(F.data == "admin_admins")
async def admin_admins(cb: CallbackQuery):
    from app.handlers.admin import admin_admins as _h
    return await _h(cb)

@router.callback_query(F.data == "admin_sellers")
async def admin_sellers(cb: CallbackQuery):
    from app.handlers.admin import admin_sellers as _h
    return await _h(cb)

@router.callback_query(F.data == "admin_seller_add")
async def admin_seller_add(cb: CallbackQuery, state: FSMContext):
    from app.handlers.admin import admin_seller_add as _h
    return await _h(cb, state)

@router.callback_query(F.data == "notify")
async def cb_notify(cb: CallbackQuery):
    from app.handlers.notify_ui import cb_notify as _h
    return await _h(cb)

@router.callback_query(F.data.startswith("notif|"))
async def cb_notif_set(cb: CallbackQuery):
    from app.handlers.notify_ui import cb_notif_set as _h
    return await _h(cb)

@router.message(AdminStates.wait_seller_add)
async def on_admin_seller_add_forward(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_seller_add_forward as _h
    return await _h(m, state)

@router.message(AdminStates.wait_seller_add, F.text.len() > 0)
async def on_admin_seller_add_text(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_seller_add_text as _h
    return await _h(m, state)

@router.callback_query(F.data == "admin_seller_del")
async def admin_seller_del(cb: CallbackQuery, state: FSMContext):
    if not await require_admin(cb):
        return
    await state.set_state(AdminStates.wait_seller_del)
    await cb.message.answer("Отправьте тэг продавца для удаления (@username)")
    await cb.answer()

@router.message(AdminStates.wait_seller_del, F.text.len() > 0)
async def on_admin_seller_del_text(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_seller_del_text as _h
    return await _h(m, state)

@router.callback_query(F.data == "admin_seller_list")
async def admin_seller_list(cb: CallbackQuery):
    if not await require_admin(cb):
        return
    conn = db()
    rows = conn.execute("SELECT COALESCE(username,'' ) AS uname, COALESCE(tg_id,'') AS tid FROM user_role WHERE role='seller' ORDER BY uname").fetchall()
    conn.close()
    if not rows:
        await cb.answer("Список пуст", show_alert=True); return
    lines = [f"• {r['uname'] or '(без тега)'} {('['+str(r['tid'])+']') if r['tid'] else ''}" for r in rows]
    await cb.message.edit_text("Продавцы:\n" + "\n".join(lines[:4000//30]), reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin_sellers")]]
    ))
    await cb.answer()

# ==== Управление администраторами (только главный админ) ====

@router.callback_query(F.data == "admin_admin_add")
async def admin_admin_add(cb: CallbackQuery, state: FSMContext):
    if not is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(AdminStates.wait_admin_add)
    await cb.message.answer("Отправьте тэг нового админа (@username)")
    await cb.answer()

@router.message(AdminStates.wait_admin_add)
async def on_admin_admin_add_forward(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_admin_add_forward as _h
    return await _h(m, state)

@router.message(AdminStates.wait_admin_add, F.text.len() > 0)
async def on_admin_admin_add_text(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_admin_add_text as _h
    return await _h(m, state)

@router.callback_query(F.data == "admin_admin_del")
async def admin_admin_del(cb: CallbackQuery, state: FSMContext):
    if not is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(AdminStates.wait_admin_del)
    await cb.message.answer("Отправьте тэг админа для удаления (@username)")
    await cb.answer()

@router.message(AdminStates.wait_admin_del, F.text.len() > 0)
async def on_admin_admin_del_text(m: Message, state: FSMContext):
    from app.handlers.admin import on_admin_admin_del_text as _h
    return await _h(m, state)

@router.callback_query(F.data == "admin_admin_list")
async def admin_admin_list(cb: CallbackQuery):
    if not is_super_admin(cb.from_user.id, cb.from_user.username):
        await cb.answer("Нет доступа", show_alert=True); return
    conn = db()
    rows = conn.execute("SELECT COALESCE(username,'' ) AS uname, COALESCE(tg_id,'') AS tid FROM user_role WHERE role='admin' ORDER BY uname").fetchall()
    conn.close()
    if not rows:
        await cb.answer("Список пуст", show_alert=True); return
    lines = [f"• {r['uname'] or '(без тега)'} {('['+str(r['tid'])+']') if r['tid'] else ''}" for r in rows]
    await cb.message.edit_text("Админы:\n" + "\n".join(lines[:4000//30]), reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="admin_admins")]]
    ))
    await cb.answer()

# ========= 8) MAIN =========

async def main():
    # Делегируем запуск в модуль приложений, где регистрируются все роутеры.
    from app.main import main as _main
    await _main()

if __name__ == "__main__":
    asyncio.run(main())
