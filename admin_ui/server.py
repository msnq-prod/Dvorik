from __future__ import annotations

import math
from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, List, Optional, Sequence, Tuple
import os
from pathlib import Path

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

from app import db as adb
from app.services import stock as stock_svc
from app.services import schedule as sched


@dataclass
class Column:
    name: str
    type: str
    notnull: bool
    pk_order: int  # 0 if not pk
    default: Optional[str]


def _columns(conn, table: str) -> List[Column]:
    info = conn.execute(f"PRAGMA table_info('{table.replace("'", "''")}')").fetchall()
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
    "stock": "Остатки",
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

LOCATION_KIND_LABEL = {
    "SKL": "Склад",
    "DOMIK": "Домик",
    "HALL": "Зал",
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


def _detect_input_type(col: Column) -> Tuple[str, Dict[str, Any]]:
    t = col.type.upper()
    attrs: Dict[str, Any] = {}
    # Имя столбца подсказывает булево-поле
    if col.name in ("is_new", "archived", "is_open"):
        return "checkbox", attrs
    if any(x in t for x in ("INT",)):
        attrs["step"] = 1
        return "number", attrs
    if any(x in t for x in ("REAL", "FLOA", "DOUB")):
        attrs["step"] = "any"
        return "number", attrs
    # Booleans stored as int
    if "BOOL" in t:
        return "checkbox", attrs
    # Default to text
    return "text", attrs


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

    PRIMARY_TABLES = {"product", "stock", "user_role"}

    @app.context_processor
    def inject_tables():
        with adb.db() as conn:
            all_tables = _list_tables(conn)
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

    @app.route("/")
    def index():
        # Главное меню с карточками
        return render_template("home.html")

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
            locs = conn.execute("SELECT code, title FROM location ORDER BY kind, code").fetchall()
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
            # Special UI for stock table
            if table == "stock":
                # Build groups by location with rows
                groups: List[Dict[str, Any]] = []
                grows = conn.execute(
                    """
                    SELECT s.location_code, COALESCE(l.title, s.location_code) AS title, l.kind
                    FROM stock s
                    LEFT JOIN location l ON l.code=s.location_code
                    GROUP BY s.location_code
                    ORDER BY l.kind, s.location_code
                    """
                ).fetchall()
                for g in grows:
                    code = g["location_code"]
                    rows = conn.execute(
                        """
                        SELECT product_id, name, local_name, qty_pack
                        FROM stock
                        WHERE location_code=?
                        ORDER BY COALESCE(local_name, name), product_id
                        """,
                        (code,),
                    ).fetchall()
                    groups.append({
                        "code": code,
                        "title": g["title"],
                        "kind": g["kind"],
                        "rows": rows,
                    })
                # locations for transfer dropdowns
                locs = conn.execute("SELECT code, title FROM location ORDER BY kind, code").fetchall()
                return render_template(
                    "stock.html",
                    groups=groups,
                    locations=locs,
                )
            is_readonly, typ = _is_virtual_or_view(conn, table)
            cols = _columns(conn, table)
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
            select_cols = ", ".join(colnames)
            sql = f"SELECT {select_cols} FROM {table} {where} ORDER BY {order_col} {order_dir} LIMIT ? OFFSET ?"
            rows = conn.execute(sql, (*params, per_page, offset)).fetchall()
            pages = max(1, math.ceil(total / per_page))

        tmpl = "table.html"
        context = dict(
            table=table,
            cols=cols,
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
            fields_meta = [(c, *_detect_input_type(c)) for c in cols]

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
                    names.append(c.name)
                    values.append(_coerce_value(c, raw))
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
                    if c.name in ("is_new", "archived", "is_open") and raw is None:
                        raw = "0"
                    set_parts.append(f"{c.name}=?")
                    set_values.append(_coerce_value(c, raw))
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
            fields_meta = [(c, *_detect_input_type(c)) for c in cols]
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
        with adb.db() as conn:
            ok, msg = stock_svc.adjust_location_qty(conn, pid, loc, delta)
        # Optional redirect back target
        nxt = request.form.get("next") or request.args.get("next")
        if nxt:
            return redirect(nxt)
        return redirect(url_for("table_browse", table="stock"))

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
        return redirect(url_for("table_browse", table="stock") + f"#loc-{loc}")

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
        with adb.db() as conn:
            stock_svc.move_specific(conn, pid, src, dst, qty)
        nxt = request.form.get("next") or request.args.get("next")
        if nxt:
            return redirect(nxt)
        # anchor back to source location block
        return redirect(url_for("table_browse", table="stock") + f"#loc-{src}")

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
            ok, msg = stock_svc.adjust_location_qty(conn, pid, loc, delta)
            new_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, loc),
            ).fetchone()[0]
        return jsonify({"ok": bool(ok), "qty": new_qty})

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
            stock_svc.move_specific(conn, pid, src, dst, qty)
            src_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, src),
            ).fetchone()[0]
            dst_qty = conn.execute(
                "SELECT IFNULL(SUM(qty_pack),0) FROM stock WHERE product_id=? AND location_code=?",
                (pid, dst),
            ).fetchone()[0]
        return jsonify({"ok": True, "src_qty": src_qty, "dst_qty": dst_qty})

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
        ms, me = _month_range(year, month)
        sellers = sched.list_sellers()
        day_infos: List[Optional[Dict[str, Any]]] = []
        with adb.db() as conn:
            d = ms
            while d <= me:
                info = {
                    "date": d,
                    "assignments": sched.get_assignments(d, conn),
                }
                day_infos.append(info)
                d += dt.timedelta(days=1)
        # Weeks (list of 7 day dicts or None)
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
        with adb.db() as conn:
            assigned = sched.get_assignments(date, conn)
            if tg_id in assigned:
                sched.remove_assignment(date, tg_id, conn)
            else:
                sched.set_assignment(date, tg_id, source='admin', conn=conn)
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
        report = request.args.get('report')
        report_rows: List[Dict[str, Any]] | None = None
        report_kind = report
        if report:
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
