from __future__ import annotations

import math
from dataclasses import dataclass
import datetime as dt
import json
import os
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
    jsonify,
    send_from_directory,
)

from app import config as app_config
from app import db as adb
from app.services import schedule as sched
from app.services import stock as stock_svc
from app.services import imports as import_svc


@dataclass
class Column:
    name: str
    type: str
    notnull: bool
    pk_order: int  # 0 if not pk
    default: Optional[str]


_SAFE_NAME_RX = re.compile(r"[^A-Za-z0-9А-Яа-я_.\-]+")


def _sanitize_filename(name: str) -> str:
    basename = Path(name).name
    cleaned = _SAFE_NAME_RX.sub("_", basename)
    cleaned = cleaned.strip("._")
    return cleaned or "upload"


def _wants_json_response() -> bool:
    """Detect if the current request expects a JSON payload back."""
    accept = (request.headers.get("Accept") or "").lower()
    if "application/json" in accept:
        return True
    xrw = (request.headers.get("X-Requested-With") or "").lower()
    if xrw in {"fetch", "xmlhttprequest"}:
        return True
    if request.headers.get("HX-Request"):
        return True
    return False


def _columns(conn, table: str) -> List[Column]:
    # Escape single quotes in table name to avoid breaking the PRAGMA query
    escaped = table.replace("'", "''")
    info = conn.execute(f"PRAGMA table_info('{escaped}')").fetchall()
    cols = [
        Column(
            name=row[1],
            type=(row[2] or ""),
            notnull=bool(row[3]),
            pk_order=int(row[5] or 0),
            default=row[4],
        )
        for row in info
    ]
    # PRAGMA table_info returns pk order starting at 1
    return cols


def _pk_cols(cols: List[Column]) -> List[Column]:
    return sorted([c for c in cols if c.pk_order > 0], key=lambda c: c.pk_order)


def _is_virtual_or_view(conn, table: str) -> Tuple[bool, str]:
    row = conn.execute(
        "SELECT type, sql FROM sqlite_master WHERE name=?",
        (table,),
    ).fetchone()
    if not row:
        return False, "table"
    typ = (row[0] or "table").lower()
    sql = (row[1] or "").upper()
    is_virtual = "VIRTUAL TABLE" in sql
    return (is_virtual or typ == "view"), typ


def _list_tables(conn) -> List[Tuple[str, str]]:
    rows = conn.execute(
        "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    out: List[Tuple[str, str]] = []
    for name, typ, sql in rows:
        sqlu = (sql or "").upper()
        # Скрываем представления и виртуальные таблицы (read-only)
        if typ == "view" or "VIRTUAL TABLE" in sqlu:
            continue
        out.append((name, "table"))
    return out

# Подписи/переводы на русский
TABLE_LABELS: Dict[str, str] = {
    "product": "Товары",
    "location": "Локации",
    "stock": "Наличие",
    "user_role": "Пользователи и роли",
    "user_notify": "Настройки уведомлений",
    "event_log": "Журнал событий",
    "schedule_day": "График: календарь",
    "schedule_assignment": "График: назначения",
    "schedule_transfer_request": "График: переносы",
    "schedule_anchor": "График: якорь",
    "registration_request": "Заявки на регистрацию",
}

COLUMN_LABELS: Dict[str, Dict[str, str]] = {
    "product": {
        "id": "ID",
        "article": "Артикул",
        "name": "Название",
        "brand_country": "Бренд/Страна",
        "local_name": "Локальное имя",
        "photo_file_id": "Фото (file_id)",
        "photo_path": "Фото (путь)",
        "is_new": "Новинка",
        "archived": "Архив",
        "archived_at": "Дата архивации",
        "last_restock_at": "Последнее поступление",
        "created_at": "Создано",
    },
    "location": {
        "code": "Код",
        "kind": "Тип",
        "title": "Название",
    },
    "stock": {
        "product_id": "Товар (ID)",
        "location_code": "Локация",
        "qty_pack": "Количество (уп.)",
        "name": "Название",
        "local_name": "Локальное имя",
    },
    "user_role": {
        "id": "ID",
        "tg_id": "Telegram ID",
        "username": "Логин",
        "display_name": "Имя",
        "role": "Роль",
        "created_at": "Создано",
    },
    "user_notify": {
        "user_id": "Пользователь (ID)",
        "notif_type": "Тип уведомления",
        "mode": "Режим",
    },
    "event_log": {
        "id": "ID",
        "ts": "Время",
        "type": "Тип",
        "product_id": "Товар (ID)",
        "location_code": "Локация",
        "delta": "Изменение",
    },
    "schedule_day": {
        "date": "Дата",
        "is_open": "Открыто",
        "notes": "Заметки",
    },
    "schedule_assignment": {
        "date": "Дата",
        "tg_id": "Telegram ID",
        "source": "Источник",
        "created_at": "Создано",
    },
    "schedule_transfer_request": {
        "id": "ID",
        "date": "Дата",
        "from_tg_id": "От (TG)",
        "to_tg_id": "К (TG)",
        "status": "Статус",
        "created_at": "Создано",
        "expires_at": "Истекает",
    },
    "schedule_anchor": {
        "id": "ID",
        "start_date": "Начало",
    },
    "registration_request": {
        "id": "ID",
        "tg_id": "Telegram ID",
        "username": "Логин",
        "first_name": "Имя",
        "last_name": "Фамилия",
        "requested_role": "Запрошенная роль",
        "status": "Статус",
        "created_at": "Создано",
    },
}

# Колонки, которые скрываем из списка при просмотре таблиц
BROWSE_HIDDEN_COLUMNS: Dict[str, Set[str]] = {
    "product": {
        "brand_country",
        "photo_file_id",
        "photo_path",
        "is_new",
        "archived",
        "archived_at",
        "created_at",
    }
}


def _visible_columns(table: str, cols: Sequence[Column]) -> List[Column]:
    hidden = BROWSE_HIDDEN_COLUMNS.get(table, set())
    return [c for c in cols if c.name not in hidden]

LOCATION_KIND_LABEL = {
    "SKL": "Склад",
    "DOMIK": "Домик",
    "HALL": "Зал",
    "COUNTER": "за стойкой",
}

BOOL_COLS = {"is_new", "archived", "is_open"}

ENUM_TRANSLATIONS = {
    ("user_notify", "notif_type"): {
        "zero": "нулевые",
        "last": "заканчиваются",
        "to_skl": "в склад",
    },
    ("user_notify", "mode"): {
        "off": "выкл",
        "daily": "ежедневно",
        "instant": "мгновенно",
    },
    ("schedule_transfer_request", "status"): {
        "pending": "ожидает",
        "accepted": "принято",
        "declined": "отклонено",
        "cancelled": "отменено",
        "expired": "истекло",
    },
    ("user_role", "role"): {
        "admin": "админ",
        "seller": "продавец",
    },
}

def table_title(name: str) -> str:
    return TABLE_LABELS.get(name, name)

def col_title(table: str, name: str) -> str:
    return COLUMN_LABELS.get(table, {}).get(name, name)

def value_label(table: str, col: str, value: Any) -> str:
    if value is None:
        return ""
    if col in BOOL_COLS:
        return "Да" if str(value) in ("1", "True", "true", "on") else "Нет"
    if table == "location" and col == "kind":
        return LOCATION_KIND_LABEL.get(str(value), str(value))
    tr = ENUM_TRANSLATIONS.get((table, col))
    if tr:
        return tr.get(str(value), str(value))
    return str(value)


def _detect_input_type(table: str, col: Column) -> Tuple[str, Dict[str, Any], Optional[List[Tuple[str, str]]]]:
    t = (col.type or "").upper()
    attrs: Dict[str, Any] = {}
    enum_map = ENUM_TRANSLATIONS.get((table, col.name))
    if enum_map:
        choices = list(enum_map.items())
        return "select", attrs, choices
    # Имя столбца подсказывает булево-поле
    if col.name in ("is_new", "archived", "is_open"):
        return "checkbox", attrs, None
    if any(x in t for x in ("INT",)):
        attrs["step"] = 1
        return "number", attrs, None
    if any(x in t for x in ("REAL", "FLOA", "DOUB")):
        attrs["step"] = "any"
        return "number", attrs, None
    # Booleans stored as int
    if "BOOL" in t:
        return "checkbox", attrs, None
    # Default to text
    return "text", attrs, None


def _coerce_value(col: Column, val: Optional[str]) -> Any:
    if val is None:
        return None
    v = val.strip()
    if v == "":
        return None
    t = col.type.upper()
    try:
        if any(x in t for x in ("INT",)):
            return int(v)
        if any(x in t for x in ("REAL", "FLOA", "DOUB")):
            return float(v)
        if "BOOL" in t:
            return 1 if v in ("1", "true", "on", "yes") else 0
    except ValueError:
        # Fallback to raw string if coercion fails
        return v
    return v


def _build_where_for_pk(pkcols: List[Column]) -> Tuple[str, List[str]]:
    parts = []
    keys = []
    for c in pkcols:
        parts.append(f"{c.name}=?")
        keys.append(c.name)
    return " AND ".join(parts), keys


def _safe_ident(name: str) -> str:
    # Very conservative
    if not name:
        abort(400)
    if any(ch in name for ch in ("`", "\"", "'", ";", "/", "\\")):
        abort(400)
    return name


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "dev-local-admin"
    # Ensure DB schema exists so admin can run standalone
    try:
        adb.init_db()
    except Exception:
        pass
    app.config.setdefault("MAX_CONTENT_LENGTH", 20 * 1024 * 1024)  # 20 MB uploads limit

    RU_MONTHS = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
        7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
    }

    PRIMARY_TABLES = {"product", "user_role"}
    HIDDEN_TABLES = {"stock"}

    SUPPLY_ALLOWED_EXTS = {".csv", ".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}
    SUPPLY_EXCEL_EXTS = {".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}
    SUPPLY_SESSION_TTL = 60 * 30  # 30 минут на подтверждение
    supply_sessions: Dict[str, Dict[str, Any]] = {}

    def _discard_supply_session(token: str, *, keep_files: bool = False) -> None:
        data = supply_sessions.pop(token, None)
        if not data:
            return
        if keep_files:
            data["committed"] = True
            return
        for key in ("stored_path", "preview_normalized_path"):
            path = data.get(key)
            if not path:
                continue
            try:
                Path(path).unlink()
            except FileNotFoundError:
                continue
            except Exception:
                continue

    def _purge_supply_sessions() -> None:
        now = time.time()
        for token, data in list(supply_sessions.items()):
            if data.get("committed"):
                continue
            created = data.get("created_at", now)
            if now - created > SUPPLY_SESSION_TTL:
                _discard_supply_session(token)

    def _parse_qty(val: Any) -> Optional[float]:
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        s = s.replace(" ", "").replace(",", ".")
        try:
            num = float(s)
        except Exception:
            return None
        if not math.isfinite(num):
            return None
        return num

    def _supply_error(message: str, status: int = 400, **extra):
        payload: Dict[str, Any] = {"success": False, "message": message}
        if extra:
            payload.update(extra)
        return jsonify(payload), status

    @app.context_processor
    def inject_tables():
        with adb.db() as conn:
            all_tables = [t for t in _list_tables(conn) if t[0] not in HIDDEN_TABLES]
            primary = [t for t in all_tables if t[0] in PRIMARY_TABLES]
            technical = [t for t in all_tables if t[0] not in PRIMARY_TABLES]
            return {
                "primary_tables": primary,
                "tech_tables": technical,
                "table_title": table_title,
                "col_title": col_title,
            }

    @app.template_global()
    def edit_url(table: str, row: Any, pkcols: Sequence[Column]) -> str:
        params = {pk.name: row[pk.name] for pk in pkcols}
        return url_for("table_edit", table=table, **params)

    @app.template_global()
    def month_ru(d: dt.date) -> str:
        try:
            return RU_MONTHS.get(int(d.month), d.strftime("%B"))
        except Exception:
            return d.strftime("%B")

    def _load_stock_groups(conn, *, include_hall: bool = True) -> List[Dict[str, Any]]:
        """Return stock rows grouped by location with consistent ordering."""

        groups: List[Dict[str, Any]] = []
        grows = conn.execute(
            """
            SELECT l.code AS location_code,
                   COALESCE(l.title, l.code) AS title,
                   COALESCE(l.kind,'OTHER') AS kind
            FROM location l
            ORDER BY
              CASE
                WHEN kind='SKL' AND l.code LIKE 'SKL-%' AND CAST(substr(l.code,5) AS INTEGER) BETWEEN 1 AND 4 THEN 1
                WHEN kind='DOMIK' THEN 2
                WHEN kind='COUNTER' THEN 3
                WHEN kind='HALL' THEN 4
                WHEN kind='SKL' AND l.code='SKL-0' THEN 5
                ELSE 6
              END,
              CASE
                WHEN kind='SKL' AND l.code LIKE 'SKL-%' THEN CAST(substr(l.code,5) AS INTEGER)
                WHEN kind='DOMIK' AND instr(l.code,'.')>0 THEN CAST(substr(l.code,1, instr(l.code,'.')-1) AS INTEGER)
                ELSE 0
              END,
              CASE
                WHEN kind='DOMIK' AND instr(l.code,'.')>0 THEN CAST(substr(l.code, instr(l.code,'.')+1) AS INTEGER)
                ELSE 0
              END
            """
        ).fetchall()
        for g in grows:
            code = g["location_code"]
            if not include_hall and code == "HALL":
                continue
            rows = conn.execute(
                """
                SELECT product_id, name, local_name, qty_pack
                FROM stock
                WHERE location_code=?
                ORDER BY COALESCE(local_name, name), product_id
                """,
                (code,),
            ).fetchall()
            groups.append(
                {
                    "code": code,
                    "title": g["title"],
                    "kind": g["kind"],
                    "rows": rows,
                }
            )
        return groups

    def _load_locations(conn) -> List[Dict[str, Any]]:
        rows = conn.execute(
            "SELECT code, title, kind FROM location ORDER BY kind, code"
        ).fetchall()
        return [
            {"code": row["code"], "title": row["title"], "kind": row["kind"]}
            for row in rows
        ]

    def _build_schedule_data(year: int, month: int):
        ms, me = _month_range(year, month)
        sellers = sched.list_sellers()
        day_infos: List[Optional[Dict[str, Any]]] = []
        with adb.db() as conn:
            d = ms
            while d <= me:
                info = {"date": d, "assignments": sched.get_assignments(d, conn)}
                day_infos.append(info)
                d += dt.timedelta(days=1)
        first_weekday = ms.weekday()
        weeks: List[List[Optional[Dict[str, Any]]]] = []
        week: List[Optional[Dict[str, Any]]] = [None] * first_weekday
        for info in day_infos:
            week.append(info)
            if len(week) == 7:
                weeks.append(week); week = []
        if week:
            week.extend([None] * (7 - len(week)))
            weeks.append(week)
        return ms, me, sellers, weeks

    def _get_signature_exceptions() -> List[Dict[str, Any]]:
        with adb.db() as conn:
            rows = conn.execute(
                "SELECT id, phrase FROM display_name_exception ORDER BY lower(phrase)"
            ).fetchall()
            return [{"id": row["id"], "phrase": row["phrase"]} for row in rows]

    @app.route("/supply")
    def supply_page():
        max_size = app.config.get("MAX_CONTENT_LENGTH")
        last_import = None
        with adb.db() as conn:
            row = conn.execute(
                """
                SELECT id, original_name, supplier, invoice, created_at, items_count, items_json
                FROM import_log
                WHERE reverted_at IS NULL
                ORDER BY datetime(created_at) DESC
                LIMIT 1
                """
            ).fetchone()
            if row:
                try:
                    items_data = json.loads(row["items_json"]) if row["items_json"] else []
                except Exception:
                    items_data = []
                last_import = {
                    "id": row["id"],
                    "original_name": row["original_name"],
                    "supplier": row["supplier"],
                    "invoice": row["invoice"],
                    "created_at": row["created_at"],
                    "items_count": row["items_count"],
                    "items": items_data,
                }
        return render_template(
            "supply.html",
            last_import=last_import,
            max_upload_size=max_size,
            allowed_exts=sorted(SUPPLY_ALLOWED_EXTS),
            signature_exceptions=_get_signature_exceptions(),
        )

    @app.route("/supply/signature-exceptions", methods=["POST"])
    def supply_add_signature_exception():
        payload = request.get_json(silent=True) or {}
        phrase = (payload.get("phrase") or "").strip()
        if not phrase:
            return _supply_error("Введите исключение для отображения")

        created = True
        try:
            with adb.db() as conn:
                with conn:
                    conn.execute(
                        "INSERT INTO display_name_exception(phrase) VALUES (?)",
                        (phrase,),
                    )
        except sqlite3.IntegrityError:
            # Уже есть — не считаем ошибкой, просто вернём текущий список
            created = False
        except Exception as exc:
            return _supply_error(f"Не удалось сохранить исключение: {exc}")

        exceptions = _get_signature_exceptions()
        return jsonify({"success": True, "created": created, "exceptions": exceptions})

    @app.route("/supply/preview", methods=["POST"])
    def supply_preview():
        _purge_supply_sessions()
        file = request.files.get("file")
        if file is None or not file.filename:
            return _supply_error("Выберите файл поставки")

        original_name = file.filename
        suffix = Path(original_name).suffix.lower()
        if suffix not in SUPPLY_ALLOWED_EXTS:
            return _supply_error("Поддерживаются файлы CSV и Excel (.xls/.xlsx)")

        safe_name = _sanitize_filename(original_name)
        base_name = Path(safe_name).stem or "upload"
        unique_suffix = secrets.token_hex(4)
        stored_filename = f"{base_name}_{unique_suffix}{suffix}"
        dest_path = app_config.UPLOAD_DIR / stored_filename

        try:
            app_config.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            file.save(dest_path)
        except Exception as exc:
            return _supply_error(f"Не удалось сохранить файл: {exc}")

        try:
            source_hash = import_svc.compute_sha256(str(dest_path))
            duplicate = import_svc.check_import_duplicate(source_hash)
            if duplicate:
                try:
                    Path(dest_path).unlink()
                except FileNotFoundError:
                    pass
                details: List[str] = ["Этот файл уже импортирован."]
                if duplicate.get("created_at"):
                    details.append(f"Дата импорта: {duplicate['created_at']}.")
                if duplicate.get("supplier"):
                    details.append(f"Поставщик: {duplicate['supplier']}.")
                if duplicate.get("invoice"):
                    details.append(f"Счёт: {duplicate['invoice']}.")
                details.append("Во избежание дублирования импорт остановлен.")
                return _supply_error(" ".join(details), status=409, duplicate=True)

            if suffix == ".csv":
                norm_csv_path, stats = import_svc.csv_to_normalized_csv(str(dest_path))
                import_type = "csv"
            else:
                norm_csv_path, stats = import_svc.excel_to_normalized_csv(str(dest_path))
                import_type = "excel"
        except Exception as exc:
            try:
                Path(dest_path).unlink()
            except FileNotFoundError:
                pass
            return _supply_error(f"Ошибка обработки файла: {exc}")

        errors = stats.get("errors") if isinstance(stats, dict) else []
        warnings_list = stats.get("warnings") if isinstance(stats, dict) else []
        rows = list(stats.get("items", [])) if isinstance(stats, dict) else []
        if not rows:
            if norm_csv_path:
                try:
                    Path(norm_csv_path).unlink()
                except FileNotFoundError:
                    pass
            try:
                Path(dest_path).unlink()
            except FileNotFoundError:
                pass
            message = "Не удалось распознать товары в файле"
            if errors:
                message += ": " + "; ".join(errors)
            return _supply_error(message)

        preview_payload = stats.get("preview") if isinstance(stats, dict) else None
        if not preview_payload:
            preview_payload = {
                "headers": ["Артикул", "Название", "Количество"],
                "rows": [[a, n, str(q)] for a, n, q in rows[:20]],
                "total_rows": len(rows),
                "total_cols": 3,
            }

        normalized_rows: List[Dict[str, Any]] = []
        for art, name, qty in rows:
            qty_val = _parse_qty(qty)
            normalized_rows.append(
                {
                    "article": str(art or "").strip(),
                    "name": str(name or "").strip(),
                    "qty": qty_val if qty_val is not None else qty,
                }
            )

        token = secrets.token_urlsafe(16)
        supply_sessions[token] = {
            "created_at": time.time(),
            "original_name": original_name,
            "stored_path": str(dest_path),
            "source_hash": source_hash,
            "import_type": import_type,
            "supplier": stats.get("supplier"),
            "invoice": stats.get("invoice"),
            "preview_normalized_path": norm_csv_path,
            "base_name": base_name,
            "initial_rows": normalized_rows,
        }

        response_payload = {
            "success": True,
            "token": token,
            "original": preview_payload,
            "normalized": normalized_rows,
            "found": int(stats.get("found", len(rows))) if isinstance(stats, dict) else len(rows),
            "supplier": stats.get("supplier"),
            "invoice": stats.get("invoice"),
            "warnings": warnings_list,
            "source_hash": source_hash,
            "original_name": original_name,
        }
        return jsonify(response_payload)

    @app.route("/supply/confirm", methods=["POST"])
    def supply_confirm():
        payload = request.get_json(silent=True) or {}
        token = payload.get("token")
        rows_payload = payload.get("rows") or []
        if not token or not isinstance(rows_payload, list):
            return _supply_error("Неизвестная сессия поставки")

        session = supply_sessions.get(token)
        if not session:
            return _supply_error("Сессия поставки не найдена или устарела", status=410)

        def normalize_name_key(value: str) -> str:
            return re.sub(r"\s+", " ", (value or "").strip().lower())

        rows_map: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for item in rows_payload:
            art = str(item.get("article", "")).strip()
            name = str(item.get("name", "")).strip()
            qty_val = _parse_qty(item.get("qty"))
            if not name or qty_val is None or qty_val <= 0:
                continue
            key = normalize_name_key(name)
            if not key:
                continue
            row = rows_map.get(key)
            if row is None:
                row = {
                    "article": art,
                    "name": name,
                    "qty": 0.0,
                    "articles": set(),
                }
                rows_map[key] = row
                order.append(key)
            if art:
                row["articles"].add(art)
                if not row["article"]:
                    row["article"] = art
            if name and (not row["name"] or len(name) > len(row["name"] or "")):
                row["name"] = name
            row["qty"] += qty_val

        final_rows: List[Tuple[str, str, float]] = []
        for key in order:
            row = rows_map[key]
            article = (row.get("article") or "").strip()
            if not article:
                continue
            final_rows.append((article, row.get("name", ""), row.get("qty", 0.0)))

        if not final_rows:
            return _supply_error("Нет строк для импорта")

        duplicate = import_svc.check_import_duplicate(session["source_hash"])
        if duplicate:
            _discard_supply_session(token)
            details = ["Этот файл уже импортирован другим пользователем."]
            if duplicate.get("created_at"):
                details.append(f"Дата: {duplicate['created_at']}.")
            return _supply_error(" ".join(details), status=409, duplicate=True)

        stats = import_svc.import_supply_rows(final_rows)
        if stats.get("errors"):
            return _supply_error("Ошибки при импорте: " + "; ".join(stats["errors"]))

        preview_norm = session.get("preview_normalized_path")
        supplier = payload.get("supplier") or session.get("supplier")
        invoice = payload.get("invoice") or session.get("invoice")

        base_name = session.get("base_name") or Path(session["stored_path"]).stem
        unique_base = f"{base_name}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}_{token[:6]}"
        normalized_csv_path = import_svc._write_normalized_csv(final_rows, unique_base)
        normalized_hash = import_svc.compute_sha256(normalized_csv_path)

        try:
            import_svc.record_import_log(
                original_name=session["original_name"],
                stored_path=session["stored_path"],
                import_type=session["import_type"],
                source_hash=session["source_hash"],
                items=final_rows,
                normalized_csv=normalized_csv_path,
                normalized_hash=normalized_hash,
                supplier=supplier,
                invoice=invoice,
            )
        finally:
            if preview_norm and Path(preview_norm) != Path(normalized_csv_path):
                try:
                    Path(preview_norm).unlink()
                except FileNotFoundError:
                    pass
            _discard_supply_session(token, keep_files=True)

        response_payload = {
            "success": True,
            "stats": {
                "imported": stats.get("imported", 0),
                "created": stats.get("created", 0),
                "updated": stats.get("updated", 0),
            },
            "supplier": supplier,
            "invoice": invoice,
            "normalized_csv": normalized_csv_path,
        }
        return jsonify(response_payload)

    @app.route("/supply/cancel", methods=["POST"])
    def supply_cancel():
        payload = request.get_json(silent=True) or {}
        token = payload.get("token")
        if token and token in supply_sessions:
            _discard_supply_session(token)
        return jsonify({"success": True})

    @app.route("/supply/revert", methods=["POST"])
    def supply_revert():
        with adb.db() as conn:
            row = conn.execute(
                """
                SELECT id, items_json
                FROM import_log
                WHERE reverted_at IS NULL
                ORDER BY datetime(created_at) DESC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return _supply_error("Нет поставок для отмены", status=404)
            try:
                items = json.loads(row["items_json"]) if row["items_json"] else []
            except Exception:
                items = []

            insufficient: List[Dict[str, Any]] = []
            adjustments: List[Tuple[int, float]] = []
            for item in items:
                art = str(item.get("article", "")).strip()
                qty_val = _parse_qty(item.get("qty"))
                if not art or qty_val is None or qty_val <= 0:
                    continue
                prow = conn.execute(
                    "SELECT id, COALESCE(local_name, name) AS disp_name FROM product WHERE article=?",
                    (art,),
                ).fetchone()
                if not prow:
                    insufficient.append({"article": art, "reason": "товар удалён"})
                    continue
                stock_row = conn.execute(
                    "SELECT qty_pack FROM stock WHERE product_id=? AND location_code='SKL-0'",
                    (prow["id"],),
                ).fetchone()
                current_qty = stock_row["qty_pack"] if stock_row else 0.0
                if current_qty is None:
                    current_qty = 0.0
                if current_qty + 1e-6 < qty_val:
                    insufficient.append(
                        {
                            "article": art,
                            "name": prow["disp_name"],
                            "have": current_qty,
                            "need": qty_val,
                        }
                    )
                    continue
                adjustments.append((prow["id"], qty_val))

            if insufficient:
                return _supply_error(
                    "Товар уже разложен, верните его в склад - 0 и повторите попытку",
                    status=409,
                    details=insufficient,
                )

            with conn:
                for pid, qty_val in adjustments:
                    conn.execute(
                        "UPDATE stock SET qty_pack = qty_pack - ? WHERE product_id=? AND location_code='SKL-0'",
                        (float(qty_val), pid),
                    )
                    conn.execute(
                        "DELETE FROM stock WHERE product_id=? AND location_code='SKL-0' AND qty_pack<=0.000001",
                        (pid,),
                    )
                conn.execute(
                    "UPDATE import_log SET reverted_at=datetime('now','localtime') WHERE id=?",
                    (row["id"],),
                )

        return jsonify({"success": True})

    @app.route("/")
    def index():
        # Главная панель: компактный график выхода сотрудников + лоусток + сводка по локациям
        today = dt.date.today()
        ym = request.args.get("ym")
        if ym:
            try:
                y, m = map(int, ym.split("-")); year, month = y, m
            except Exception:
                year, month = today.year, today.month
        else:
            year = int(request.args.get("year", today.year))
            month = int(request.args.get("month", today.month))

        ms, me, sellers, weeks = _build_schedule_data(year, month)

        with adb.db() as conn:
            # Low stock report (like reports 'low'): total in (0,2]
            low_rows = conn.execute(
                """
                SELECT p.article,
                       COALESCE(p.local_name,p.name) AS disp_name,
                       IFNULL(SUM(s.qty_pack),0) AS total
                FROM product p
                LEFT JOIN stock s ON s.product_id=p.id
                WHERE p.archived=0
                GROUP BY p.id
                HAVING total>0 AND total<=2
                ORDER BY total ASC, p.id DESC
                LIMIT 100
                """
            ).fetchall()
            # Totals by location for summary table (kept for reference)
            loc_rows = conn.execute(
                """
                SELECT s.location_code AS code,
                       COALESCE(l.title, s.location_code) AS title,
                       COALESCE(l.kind, 'OTHER') AS kind,
                       IFNULL(SUM(s.qty_pack),0) AS total
                FROM stock s
                LEFT JOIN location l ON l.code = s.location_code
                GROUP BY s.location_code
                ORDER BY l.kind, s.location_code
                """
            ).fetchall()

            # Build detailed groups for the compact interactive widget on the home page
            # Custom order: SKL-1..4 first, DOMIK 2.1..9.2 next, за стойкой, hall, and SKL-0.
            groups = _load_stock_groups(conn, include_hall=False)

            # Locations for move dropdowns
            locs = _load_locations(conn)

        return render_template(
            "home.html",
            low_rows=low_rows,
            loc_rows=loc_rows,
            groups=groups,
            locations=locs,
            ms=ms, me=me, sellers=sellers, weeks=weeks,
        )

    # Публичная раздача локальных медиа-файлов (фото товаров)
    @app.route("/media/<path:subpath>")
    def serve_media(subpath: str):
        base = Path("media").resolve()
        target = (base / subpath).resolve()
        if not str(target).startswith(str(base)):
            abort(403)
        if not target.exists():
            abort(404)
        return send_from_directory(str(base), subpath)

    # ====== Страница карточек товаров (маркетплейс) ======
    @app.route("/cards")
    def cards_page():
        # Начальная загрузка страницы — без данных; фронт подтянет через API с пустым q
        with adb.db() as conn:
            locs = [
                {
                    "code": row["code"],
                    "title": row["title"],
                }
                for row in conn.execute(
                    "SELECT code, title FROM location ORDER BY kind, code"
                ).fetchall()
            ]
        return render_template("cards.html", locations=locs)

    @app.route("/table/<table>")
    def table_browse(table: str):
        table = _safe_ident(table)
        page = max(int(request.args.get("page", 1)), 1)
        per_page = min(max(int(request.args.get("per_page", 50)), 1), 500)
        q = request.args.get("q", "").strip()
        sort = request.args.get("sort")
        direction = request.args.get("dir", "asc")
        with adb.db() as conn:
            # Validate table exists
            tables = dict(_list_tables(conn))
            if table not in tables:
                abort(404)
            # Legacy stock table view removed in favor of dashboard widget
            if table == "stock":
                abort(404)
            is_readonly, typ = _is_virtual_or_view(conn, table)
            cols = _columns(conn, table)
            display_cols = _visible_columns(table, cols)
            colnames = [c.name for c in cols]
            pkcols = _pk_cols(cols)
            order_col = sort if sort in colnames else (pkcols[0].name if pkcols else colnames[0])
            order_dir = "DESC" if direction.lower() == "desc" else "ASC"

            # Filtering
            params: List[Any] = []
            where = ""
            if q:
                text_cols = [c.name for c in cols if (c.type or "").upper() in ("TEXT", "CHAR", "CLOB", "") or "CHAR" in (c.type or "").upper()]
                if text_cols:
                    like = " OR ".join([f"{name} LIKE ?" for name in text_cols])
                    where = f"WHERE ({like})"
                    params.extend([f"%{q}%" for _ in text_cols])

            # Count
            count_sql = f"SELECT COUNT(*) FROM {table} {where}"
            total = conn.execute(count_sql, params).fetchone()[0]

            # Page
            offset = (page - 1) * per_page
            if table == "product":
                page = 1
                offset = 0
                per_page = max(total, 1)
            select_cols = ", ".join(colnames)
            sql = f"SELECT {select_cols} FROM {table} {where} ORDER BY {order_col} {order_dir} LIMIT ? OFFSET ?"
            rows = conn.execute(sql, (*params, per_page, offset)).fetchall()
            pages = max(1, math.ceil(total / per_page))
            if table == "product":
                pages = 1

        tmpl = "table.html"
        context = dict(
            table=table,
            cols=cols,
            display_cols=display_cols,
            rows=rows,
            pkcols=pkcols,
            q=q,
            page=page,
            per_page=per_page,
            pages=pages,
            total=total,
            sort=order_col,
            dir=order_dir.lower(),
            is_readonly=is_readonly,
            table_title=table_title,
            col_title=col_title,
            value_label=value_label,
        )
        if table == "product":
            with adb.db() as conn2:
                locs = conn2.execute("SELECT code, title FROM location ORDER BY kind, code").fetchall()
            context["locations"] = locs
        return render_template(tmpl, **context)

    @app.route("/table/<table>/add", methods=["GET", "POST"])
    def table_add(table: str):
        table = _safe_ident(table)
        with adb.db() as conn:
            tables = dict(_list_tables(conn))
            if table not in tables:
                abort(404)
            is_readonly, _ = _is_virtual_or_view(conn, table)
            if is_readonly:
                abort(400, "Только чтение")
            cols = _columns(conn, table)
            pkcols = _pk_cols(cols)
            # Prepare form metadata
            fields_meta = [(c, *_detect_input_type(table, c)) for c in cols]

            if request.method == "POST":
                form = request.form
                names: List[str] = []
                values: List[Any] = []
                for c in cols:
                    raw = form.get(c.name)
                    if c.name in ("is_new", "archived", "is_open") and raw is None:
                        raw = "0"
                    # If PK is integer and not provided -> let SQLite assign
                    if c in pkcols and (raw is None or raw.strip() == "") and "INT" in c.type.upper():
                        continue
                    value = _coerce_value(c, raw)
                    enum_map = ENUM_TRANSLATIONS.get((table, c.name))
                    if enum_map and value is not None and str(value) not in enum_map:
                        flash(f"Недопустимое значение для поля \u00ab{col_title(table, c.name)}\u00bb", "danger")
                        return redirect(request.url)
                    if value is None:
                        if c.default is not None:
                            # Defer to database default if defined
                            continue
                        if c.notnull:
                            flash(f"Заполните поле \u00ab{col_title(table, c.name)}\u00bb", "danger")
                            return redirect(request.url)
                    names.append(c.name)
                    values.append(value)
                if not names:
                    flash("Нет данных для вставки", "warning")
                    return redirect(url_for("table_browse", table=table))
                placeholders = ",".join(["?"] * len(names))
                cols_sql = ", ".join(names)
                with conn:
                    conn.execute(
                        f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})",
                        values,
                    )
                flash("Строка добавлена", "success")
                return redirect(url_for("table_browse", table=table))

        return render_template("form.html", table=table, cols=cols, fields_meta=fields_meta, mode="add", table_title=table_title, col_title=col_title)

    @app.route("/table/<table>/edit", methods=["GET", "POST"])
    def table_edit(table: str):
        table = _safe_ident(table)
        with adb.db() as conn:
            tables = dict(_list_tables(conn))
            if table not in tables:
                abort(404)
            is_readonly, _ = _is_virtual_or_view(conn, table)
            if is_readonly:
                abort(400, "Только чтение")
            cols = _columns(conn, table)
            pkcols = _pk_cols(cols)
            if not pkcols:
                abort(400, "Изменение возможно только для таблиц с PK")
            where_sql, keys = _build_where_for_pk(pkcols)
            # Extract PK values from query args
            args = request.args
            pk_values: List[Any] = []
            for k in keys:
                if k not in args:
                    abort(400, f"Не передан ключ {k}")
                pk_values.append(args.get(k))

            if request.method == "POST":
                # Build SET for non-PK columns
                form = request.form
                set_parts: List[str] = []
                set_values: List[Any] = []
                for c in cols:
                    if c in pkcols:
                        continue
                    raw = form.get(c.name)
                    if raw is None:
                        if c.name in ("is_new", "archived", "is_open"):
                            raw = "0"
                        elif c.name not in form:
                            continue
                    if c.name in ("is_new", "archived", "is_open") and raw is None:
                        raw = "0"
                    value = _coerce_value(c, raw)
                    enum_map = ENUM_TRANSLATIONS.get((table, c.name))
                    if enum_map and value is not None and str(value) not in enum_map:
                        flash(f"Недопустимое значение для поля \u00ab{col_title(table, c.name)}\u00bb", "danger")
                        return redirect(request.url)
                    set_parts.append(f"{c.name}=?")
                    set_values.append(value)
                if set_parts:
                    sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {where_sql}"
                    with conn:
                        conn.execute(sql, (*set_values, *pk_values))
                    flash("Изменения сохранены", "success")
                return redirect(url_for("table_browse", table=table))

            # GET -> load row
            row = conn.execute(
                f"SELECT * FROM {table} WHERE {where_sql}",
                pk_values,
            ).fetchone()
            if not row:
                abort(404)
            fields_meta = [(c, *_detect_input_type(table, c)) for c in cols]
        return render_template("form.html", table=table, cols=cols, fields_meta=fields_meta, mode="edit", row=row, pkcols=pkcols, table_title=table_title, col_title=col_title)

    @app.route("/table/<table>/delete", methods=["POST"])
    def table_delete(table: str):
        table = _safe_ident(table)
        with adb.db() as conn:
            tables = dict(_list_tables(conn))
            if table not in tables:
                abort(404)
            is_readonly, _ = _is_virtual_or_view(conn, table)
            if is_readonly:
                abort(400, "Только чтение")
            cols = _columns(conn, table)
            pkcols = _pk_cols(cols)
            if not pkcols:
                abort(400, "Удаление возможно только по PK")
            where_sql, keys = _build_where_for_pk(pkcols)
            form = request.form
            pk_values: List[Any] = []
            for k in keys:
                val = form.get(k)
                if val is None:
                    abort(400)
                pk_values.append(val)
            with conn:
                conn.execute(f"DELETE FROM {table} WHERE {where_sql}", pk_values)
            flash("Строка удалена", "success")
        return redirect(url_for("table_browse", table=table))

    @app.route("/inventory")
    def inventory_page():
        with adb.db() as conn:
            groups = _load_stock_groups(conn)
            locations = _load_locations(conn)
        return render_template("inventory.html", groups=groups, locations=locations)

    # ===== Stock actions =====
    @app.route("/table/stock/adjust", methods=["POST"])
    def stock_adjust():
        pid = int(request.form.get("product_id", "0"))
        loc = request.form.get("location_code", "").strip()
        try:
            delta = int(request.form.get("delta", "0"))
        except Exception:
            delta = 0
        if not pid or not loc or delta == 0:
            abort(400)
        wants_json = _wants_json_response()
        new_qty: Optional[float] = None
        with adb.db() as conn:
            ok, msg = stock_svc.adjust_with_hub(conn, pid, loc, delta)
            if ok and wants_json:
                row = conn.execute(
                    "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
                    (pid, loc),
                ).fetchone()
                new_qty = float(row["qty_pack"]) if row and row["qty_pack"] is not None else 0.0
        if wants_json:
            if ok:
                qty_val = new_qty if new_qty is not None else 0.0
                return jsonify({
                    "ok": True,
                    "qty": qty_val,
                    "qty_display": format(qty_val, "g"),
                })
            status = 400 if msg else 500
            return jsonify({"ok": False, "error": msg or "Не удалось изменить остаток"}), status
        if not ok:
            flash(msg or "Не удалось изменить остаток", "danger")
        # Optional redirect back target
        nxt = request.form.get("next") or request.args.get("next")
        if nxt:
            return redirect(nxt)
        return redirect(url_for("index"))

    @app.route("/table/stock/add", methods=["POST"])
    def stock_add():
        pid = int(request.form.get("product_id", "0"))
        loc = request.form.get("location_code", "").strip()
        try:
            qty = int(float(request.form.get("qty", "1")))
        except Exception:
            qty = 1
        if not pid or not loc or qty <= 0:
            abort(400)
        with adb.db() as conn:
            stock_svc.adjust_location_qty(conn, pid, loc, qty)
        nxt = request.form.get("next") or request.args.get("next")
        if nxt:
            return redirect(nxt)
        return redirect(url_for("index") + f"#loc-{loc}")

    @app.route("/stock/move", methods=["POST"])
    def stock_move():
        pid = int(request.form.get("product_id", "0"))
        src = request.form.get("src", "").strip()
        dst = request.form.get("dst", "").strip()
        try:
            qty = int(float(request.form.get("qty", "1")))
        except Exception:
            qty = 1
        if not pid or not src or not dst or qty <= 0:
            abort(400)
        wants_json = _wants_json_response()
        src_qty: Optional[float] = None
        dst_qty: Optional[float] = None
        src_remaining = False
        dst_present = False
        with adb.db() as conn:
            ok, msg = stock_svc.move_specific(conn, pid, src, dst, qty)
            if ok and wants_json:
                row_src = conn.execute(
                    "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
                    (pid, src),
                ).fetchone()
                if row_src and row_src["qty_pack"] is not None:
                    src_qty = float(row_src["qty_pack"])
                    src_remaining = True
                row_dst = conn.execute(
                    "SELECT qty_pack FROM stock WHERE product_id=? AND location_code=?",
                    (pid, dst),
                ).fetchone()
                if row_dst and row_dst["qty_pack"] is not None:
                    dst_qty = float(row_dst["qty_pack"])
                    dst_present = True
        if wants_json:
            if ok:
                def _fmt(val: Optional[float]) -> Optional[str]:
                    if val is None:
                        return None
                    return format(val, "g")

                return jsonify({
                    "ok": True,
                    "product_id": pid,
                    "src": src,
                    "dst": dst,
                    "qty": qty,
                    "src_exists": src_remaining,
                    "src_qty": src_qty,
                    "src_qty_display": _fmt(src_qty),
                    "dst_exists": dst_present,
                    "dst_qty": dst_qty,
                    "dst_qty_display": _fmt(dst_qty),
                })
            status = 400 if msg else 500
            return jsonify({"ok": False, "error": msg or "Не удалось переместить"}), status
        nxt = request.form.get("next") or request.args.get("next")
        if not ok:
            flash(msg or "Не удалось переместить", "danger")
            if nxt:
                return redirect(nxt)
            return redirect(url_for("index") + f"#loc-{src}")
        if nxt:
            return redirect(nxt)
        # anchor back to source location block
        return redirect(url_for("index") + f"#loc-{src}")

    @app.get("/api/products/search")
    def api_products_search():
        q = (request.args.get("q") or "").strip()
        limit = int(request.args.get("limit", "10"))
        items: List[Dict[str, Any]] = []
        if not q:
            return jsonify(items)
        with adb.db() as conn:
            has_fts = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_fts'"
            ).fetchone() is not None
            if has_fts:
                query = q.replace("'", "''").strip()
                # Simple prefix matching: tokenize and add *
                tokens = [t for t in query.split() if t]
                match = " ".join(t + "*" for t in tokens) or query + "*"
                sql = (
                    "SELECT p.id, p.article, p.name, p.local_name "
                    "FROM product p JOIN product_fts f ON p.id=f.rowid "
                    "WHERE product_fts MATCH ? LIMIT ?"
                )
                rows = conn.execute(sql, (match, limit)).fetchall()
            else:
                like = f"%{q}%"
                rows = conn.execute(
                    "SELECT id, article, name, local_name FROM product "
                    "WHERE article LIKE ? OR name LIKE ? OR local_name LIKE ? "
                    "ORDER BY id DESC LIMIT ?",
                    (like, like, like, limit),
                ).fetchall()
            for r in rows:
                items.append(
                    {
                        "id": r["id"],
                        "article": r["article"],
                        "name": r["name"],
                        "local_name": r["local_name"],
                        "display": (r["local_name"] or r["name"] or "") + (f" · {r['article']}" if r["article"] else ""),
                    }
                )
        return jsonify(items)

    # ====== API для карточек товаров ======
    def _cards_search(conn, q: str, limit: int = 60) -> List[Dict[str, Any]]:
        q = (q or "").strip()
        rows: List[Any] = []
        try:
            if q:
                has_fts = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_fts'"
                ).fetchone() is not None
                if has_fts:
                    match = (q.replace(" ", "* ") + "*").strip()
                    rows = conn.execute(
                        """
                        SELECT p.id, p.article, p.name, p.local_name, p.photo_path
                        FROM product_fts f
                        JOIN product p ON p.id=f.rowid
                        LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                        WHERE product_fts MATCH ? AND p.archived=0
                        ORDER BY (COALESCE(t.total,0) > 0) DESC, p.id DESC
                        LIMIT ?
                        """,
                        (match, limit),
                    ).fetchall()
                else:
                    like = f"%{q}%"
                    rows = conn.execute(
                        """
                        SELECT p.id, p.article, p.name, p.local_name, p.photo_path,
                               COALESCE(t.total,0) AS total
                        FROM product p
                        LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                        WHERE p.archived=0 AND (p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)
                        ORDER BY (COALESCE(t.total,0) > 0) DESC, p.id DESC
                        LIMIT ?
                        """,
                        (like, like, like, limit),
                    ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT p.id, p.article, p.name, p.local_name, p.photo_path,
                           COALESCE(t.total,0) AS total
                    FROM product p
                    LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                    WHERE p.archived=0
                    ORDER BY (COALESCE(t.total,0) > 0) DESC, p.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        except Exception:
            # Fallback with simplified LIKE: ё->е
            like_raw = f"%{q}%"
            sq = (q or "").replace("Ё", "Е").replace("ё", "е").strip().lower()
            like_simpl = f"%{sq}%"
            rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name, p.photo_path,
                       COALESCE(t.total,0) AS total
                FROM product p
                LEFT JOIN (SELECT product_id, SUM(qty_pack) AS total FROM stock GROUP BY product_id) t ON t.product_id=p.id
                WHERE p.archived=0 AND (
                    p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?
                    OR REPLACE(LOWER(p.name),'ё','е') LIKE ?
                    OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?
                )
                ORDER BY (COALESCE(t.total,0) > 0) DESC, p.id DESC
                LIMIT ?
                """,
                (like_raw, like_raw, like_raw, like_simpl, like_simpl, limit),
            ).fetchall()

        ids = [r["id"] for r in rows]
        stocks_map: Dict[int, List[Dict[str, Any]]] = {}
        if ids:
            qmarks = ",".join(["?"] * len(ids))
            srows = conn.execute(
                f"""
                SELECT s.product_id, s.location_code, COALESCE(l.title, s.location_code) AS title, SUM(s.qty_pack) AS qty
                FROM stock s LEFT JOIN location l ON l.code=s.location_code
                WHERE s.product_id IN ({qmarks})
                GROUP BY s.product_id, s.location_code
                HAVING ABS(SUM(s.qty_pack))>0.000001
                ORDER BY s.location_code
                """,
                ids,
            ).fetchall()
            for sr in srows:
                stocks_map.setdefault(int(sr["product_id"]), []).append(
                    {"code": sr["location_code"], "title": sr["title"], "qty": sr["qty"]}
                )

        items: List[Dict[str, Any]] = []
        for r in rows:
            pid = int(r["id"])
            ppath = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else ""
            purl = None
            if ppath and os.path.isfile(ppath):
                # Build media URL relative to media/
                try:
                    rel = os.path.relpath(ppath, "media")
                except Exception:
                    rel = None
                if rel and not rel.startswith(".."):
                    purl = url_for("serve_media", subpath=rel)
            items.append(
                {
                    "id": pid,
                    "article": r["article"],
                    "name": r["name"],
                    "local_name": r["local_name"],
                    "photo_url": purl,
                    "stocks": stocks_map.get(pid, []),
                }
            )
        return items

    @app.get("/api/cards/search")
    def api_cards_search():
        q = request.args.get("q", "").strip()
        limit = int(request.args.get("limit", "60"))
        with adb.db() as conn:
            items = _cards_search(conn, q, limit)
        return jsonify(items)

    @app.post("/api/stock/adjust")
    def api_stock_adjust():
        try:
            pid = int(request.form.get("product_id") or request.json.get("product_id"))
            loc = (request.form.get("location_code") or request.json.get("location_code") or "").strip()
            delta = int(float(request.form.get("delta") or request.json.get("delta")))
        except Exception:
            abort(400)
        if not pid or not loc or delta == 0:
            abort(400)
        with adb.db() as conn:
            ok, msg = stock_svc.adjust_with_hub(conn, pid, loc, delta)
            new_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, loc),
            ).fetchone()[0]
            hub_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, "SKL-0"),
            ).fetchone()[0]
        return jsonify({"ok": bool(ok), "qty": new_qty, "hub_qty": hub_qty, "message": msg or ""})

    @app.post("/api/stock/set_qty")
    def api_stock_set_qty():
        payload = request.form if request.form else request.json or {}
        try:
            pid = int(payload.get("product_id"))
            loc = (payload.get("location_code") or "").strip()
            qty = float(payload.get("qty"))
        except Exception:
            abort(400)

        if not pid or not loc:
            abort(400)

        with adb.db() as conn:
            ok, msg = stock_svc.set_location_qty(conn, pid, loc, qty)
            if not ok:
                return jsonify({"ok": False, "error": msg}), 400
            new_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, loc),
            ).fetchone()[0]
        return jsonify({"ok": True, "qty": new_qty})

    @app.post("/api/stock/move")
    def api_stock_move():
        try:
            pid = int(request.form.get("product_id") or request.json.get("product_id"))
            src = (request.form.get("src") or request.json.get("src") or "").strip()
            dst = (request.form.get("dst") or request.json.get("dst") or "").strip()
            qty = int(float(request.form.get("qty") or request.json.get("qty") or 1))
        except Exception:
            abort(400)
        if not pid or not src or not dst or qty <= 0:
            abort(400)
        with adb.db() as conn:
            ok, msg = stock_svc.move_specific(conn, pid, src, dst, qty)
            src_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, src),
            ).fetchone()[0]
            dst_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, dst),
            ).fetchone()[0]
        return jsonify({"ok": bool(ok), "src_qty": src_qty, "dst_qty": dst_qty, "message": msg or ""})

    @app.post("/api/product/set_local_name")
    def api_product_set_local_name():
        try:
            pid = int(request.form.get("product_id") or request.json.get("product_id"))
            name = (request.form.get("local_name") or request.json.get("local_name") or "").strip()
        except Exception:
            abort(400)
        if not pid:
            abort(400)
        with adb.db() as conn:
            with conn:
                conn.execute("UPDATE product SET local_name=? WHERE id=?", (name or None, pid))
        return jsonify({"ok": True, "local_name": name})

    @app.post("/api/product/upload_photo")
    def api_product_upload_photo():
        try:
            pid = int(request.form.get("product_id"))
        except Exception:
            abort(400)
        if not pid or "photo" not in request.files:
            abort(400)
        file = request.files["photo"]
        if not file or file.filename == "":
            abort(400)
        # Save to temp and compress into media/photos
        photos_dir = Path("media/photos")
        photos_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = photos_dir / f"tmp_upload_{pid}"
        file.save(tmp_path)
        ts = int(dt.datetime.now().timestamp())
        dest = photos_dir / f"p_{pid}_{ts}.jpg"
        try:
            from app.services.photos import compress_image_to_jpeg
            compress_image_to_jpeg(tmp_path, dest, 85)
        except Exception:
            # If compression fails, just move original
            try:
                os.replace(tmp_path, dest)
            except Exception:
                pass
        try:
            if tmp_path.exists():
                os.remove(tmp_path)
        except Exception:
            pass
        rel = str(dest)
        with adb.db() as conn:
            with conn:
                conn.execute("UPDATE product SET photo_path=?, photo_file_id=NULL WHERE id=?", (rel, pid))
        # Build URL
        purl = url_for("serve_media", subpath=os.path.relpath(rel, "media")) if os.path.isfile(rel) else None
        return jsonify({"ok": True, "photo_url": purl})

    # ===== Schedule admin =====
    def _month_range(year: int, month: int) -> Tuple[dt.date, dt.date]:
        start = dt.date(year, month, 1)
        if month == 12:
            end = dt.date(year, 12, 31)
        else:
            end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
        return start, end

    @app.route("/schedule", methods=["GET"]) 
    def schedule_view():
        today = dt.date.today()
        # Allow direct jump via ?ym=YYYY-MM
        ym = request.args.get("ym")
        if ym:
            try:
                y, m = map(int, ym.split("-"))
                year, month = y, m
            except Exception:
                year, month = today.year, today.month
        else:
            year = int(request.args.get("year", today.year))
            month = int(request.args.get("month", today.month))
        ms, me, sellers, weeks = _build_schedule_data(year, month)

        return render_template(
            "schedule.html",
            year=year,
            month=month,
            ms=ms,
            me=me,
            weeks=weeks,
            sellers=sellers,
        )

    # Non-working day toggle removed

    @app.route("/schedule/toggle_cell", methods=["POST"]) 
    def schedule_toggle_cell():
        date = dt.date.fromisoformat(request.form.get("date"))
        tg_id = int(request.form.get("tg_id"))
        wants_json = _wants_json_response()
        new_state = False
        total_assigned = 0
        with adb.db() as conn:
            assigned = sched.get_assignments(date, conn)
            if tg_id in assigned:
                sched.remove_assignment(date, tg_id, conn)
            else:
                sched.set_assignment(date, tg_id, source='admin', conn=conn)
            if wants_json:
                updated = sched.get_assignments(date, conn)
                new_state = tg_id in updated
                total_assigned = len(updated)
        if wants_json:
            return jsonify({
                "ok": True,
                "assigned": new_state,
                "count": total_assigned,
                "date": date.isoformat(),
                "tg_id": tg_id,
            })
        if (request.form.get('from') or '').strip() == 'index':
            return redirect(url_for("index", year=date.year, month=date.month))
        return redirect(url_for("schedule_view", year=date.year, month=date.month))

    # Non-working days marking removed

    # Обнулить график на выбранный месяц (удалить назначения)
    @app.route("/schedule/clear_month", methods=["POST"]) 
    def schedule_clear_month():
        year = int(request.form.get("year"))
        month = int(request.form.get("month"))
        ms, me = _month_range(year, month)
        with adb.db() as conn:
            with conn:
                conn.execute("DELETE FROM schedule_assignment WHERE date BETWEEN ? AND ?", (ms.isoformat(), me.isoformat()))
        if (request.form.get('from') or '').strip() == 'index':
            return redirect(url_for("index", year=year, month=month))
        return redirect(url_for("schedule_view", year=year, month=month))

    # Снять все крестики (вернуть до 'нерабочих'): очищаем schedule_day
    @app.route("/schedule/reset_all_open", methods=["POST"]) 
    def schedule_reset_all_open():
        with adb.db() as conn:
            with conn:
                conn.execute("DELETE FROM schedule_day")
        today = dt.date.today()
        return redirect(url_for("schedule_view", year=today.year, month=today.month))

    @app.route("/schedule/assign", methods=["POST"]) 
    def schedule_assign():
        date = dt.date.fromisoformat(request.form.get("date"))
        tg_id = int(request.form.get("tg_id"))
        with adb.db() as conn:
            sched.set_assignment(date, tg_id, source='admin', conn=conn)
        return redirect(url_for("schedule_view", year=date.year, month=date.month))

    @app.route("/schedule/unassign", methods=["POST"]) 
    def schedule_unassign():
        date = dt.date.fromisoformat(request.form.get("date"))
        tg_id = int(request.form.get("tg_id"))
        with adb.db() as conn:
            sched.remove_assignment(date, tg_id, conn)
        return redirect(url_for("schedule_view", year=date.year, month=date.month))

    # Generation/anchor/swap endpoints removed

    # Страница отчётов
    @app.route("/reports")
    def reports_page():
        allowed_reports = {"low", "zero", "mid", "all", "arch"}
        report = request.args.get('report') or 'low'
        if report not in allowed_reports:
            report = 'low'

        report_rows: List[Dict[str, Any]] | None = None
        report_kind = report

        with adb.db() as conn:
            if report == 'low':
                sql = (
                    "SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total\n"
                    "FROM product p LEFT JOIN stock s ON s.product_id=p.id\n"
                    "WHERE p.archived=0\n"
                    "GROUP BY p.id HAVING total>0 AND total<2\n"
                    "ORDER BY total ASC, p.id DESC LIMIT 300"
                )
                rows = conn.execute(sql).fetchall()
                report_rows = [dict(article=r['article'], name=r['disp_name'], total=r['total']) for r in rows]
            elif report == 'zero':
                sql = (
                    "SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name\n"
                    "FROM product p LEFT JOIN stock s ON s.product_id=p.id\n"
                    "WHERE p.archived=0\n"
                    "GROUP BY p.id HAVING IFNULL(SUM(s.qty_pack),0)=0\n"
                    "ORDER BY p.id DESC LIMIT 1000"
                )
                rows = conn.execute(sql).fetchall()
                report_rows = [dict(article=r['article'], name=r['disp_name'], total=None) for r in rows]
            elif report == 'mid':
                sql = (
                    "SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total\n"
                    "FROM product p LEFT JOIN stock s ON s.product_id=p.id\n"
                    "WHERE p.archived=0\n"
                    "GROUP BY p.id HAVING total>=3 AND total<=5\n"
                    "ORDER BY total DESC, disp_name ASC LIMIT 1000"
                )
                rows = conn.execute(sql).fetchall()
                report_rows = [dict(article=r['article'], name=r['disp_name'], total=r['total']) for r in rows]
            elif report == 'all':
                sql = (
                    "SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, IFNULL(SUM(s.qty_pack),0) AS total\n"
                    "FROM product p LEFT JOIN stock s ON s.product_id=p.id\n"
                    "WHERE p.archived=0\n"
                    "GROUP BY p.id\n"
                    "ORDER BY disp_name ASC LIMIT 2000"
                )
                rows = conn.execute(sql).fetchall()
                report_rows = [dict(article=r['article'], name=r['disp_name'], total=r['total']) for r in rows]
            elif report == 'arch':
                sql = (
                    "SELECT p.article, COALESCE(p.local_name,p.name) AS disp_name, p.archived_at, p.last_restock_at\n"
                    "FROM product p WHERE p.archived=1\n"
                    "ORDER BY (p.archived_at IS NULL) ASC, p.archived_at DESC, disp_name ASC LIMIT 2000"
                )
                rows = conn.execute(sql).fetchall()
                report_rows = [dict(article=r['article'], name=r['disp_name'], archived_at=r['archived_at'], last_restock_at=r['last_restock_at']) for r in rows]

        return render_template("reports.html", report_kind=report_kind, report_rows=report_rows)

    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("ADMIN_HOST", "127.0.0.1")
    try:
        port = int(os.getenv("ADMIN_PORT", "8000"))
    except Exception:
        port = 8000
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)
