from __future__ import annotations

import math
import sqlite3
from typing import Any, Dict, List, Optional, Sequence

from flask import (
    Blueprint,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from app import db as adb
from admin_ui.blueprints import utils as bp_utils


Column = bp_utils.Column


bp = Blueprint("tables", __name__)


@bp.route("/table/<table>", endpoint="table_browse")
def table_browse(table: str):
    table = bp_utils.safe_ident(table)
    page = max(int(request.args.get("page", 1)), 1)
    per_page = min(max(int(request.args.get("per_page", 50)), 1), 500)
    q = request.args.get("q", "").strip()
    sort = request.args.get("sort")
    direction = request.args.get("dir", "asc")
    with adb.db() as conn:
        tables = dict(bp_utils.list_tables(conn))
        if table not in tables:
            abort(404)
        if table == "stock":
            abort(404)
        is_readonly, _ = bp_utils.is_virtual_or_view(conn, table)
        cols = bp_utils.columns(conn, table)
        display_cols = bp_utils.visible_columns(table, cols)
        colnames = [c.name for c in cols]
        pkcols = bp_utils.pk_cols(cols)
        column_expr: Dict[str, str] = {c.name: c.name for c in cols}
        extra_expr: Dict[str, str] = {}
        table_sql = table
        extra_display_cols: List[Column] = []
        if table == "product":
            table_sql = "product p LEFT JOIN manufacturer m ON m.id=p.manufacturer_id"
            column_expr = {c.name: f"p.{c.name}" for c in cols}
            extra_expr = {
                "manufacturer_name": "m.name",
                "manufacturer_country": "m.country",
            }
            extra_display_cols = [
                Column("manufacturer_name", "TEXT", False, 0, None),
                Column("manufacturer_country", "TEXT", False, 0, None),
            ]
            display_cols = list(display_cols) + extra_display_cols
        all_expr = {**column_expr, **extra_expr}
        sortable_names = colnames + [c.name for c in extra_display_cols]
        order_col = sort if sort in sortable_names else (pkcols[0].name if pkcols else colnames[0])
        order_dir = "DESC" if direction.lower() == "desc" else "ASC"

        params: List[Any] = []
        where = ""
        if q:
            text_cols = [
                c.name
                for c in cols
                if (c.type or "").upper() in ("TEXT", "CHAR", "CLOB", "")
                or "CHAR" in (c.type or "").upper()
            ]
            if table == "product":
                text_cols.extend(["manufacturer_name", "manufacturer_country"])
            if text_cols:
                like_parts = [f"{all_expr.get(name, name)} LIKE ?" for name in text_cols]
                where = f"WHERE ({' OR '.join(like_parts)})"
                params.extend([f"%{q}%" for _ in text_cols])

        count_sql = f"SELECT COUNT(*) FROM {table_sql} {where}"
        total = conn.execute(count_sql, params).fetchone()[0]

        offset = (page - 1) * per_page
        if table == "product":
            page = 1
            offset = 0
            per_page = max(total, 1)
        select_parts = [f"{all_expr[name]} AS {name}" for name in colnames]
        if table == "product":
            select_parts.extend([
                "m.name AS manufacturer_name",
                "m.country AS manufacturer_country",
            ])
        select_cols = ", ".join(select_parts)
        sql = (
            f"SELECT {select_cols} FROM {table_sql} {where} "
            f"ORDER BY {all_expr.get(order_col, order_col)} {order_dir} LIMIT ? OFFSET ?"
        )
        rows = conn.execute(sql, (*params, per_page, offset)).fetchall()
        pages = max(1, math.ceil(total / per_page))
        if table == "product":
            pages = 1

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
        table_title=bp_utils.table_title,
        col_title=bp_utils.col_title,
        value_label=bp_utils.value_label,
    )
    if table == "product":
        with adb.db() as conn2:
            locs = conn2.execute("SELECT code, title FROM location ORDER BY kind, code").fetchall()
        context["locations"] = locs
    return render_template("table.html", **context)


@bp.route("/table/<table>/add", methods=["GET", "POST"], endpoint="table_add")
def table_add(table: str):
    table = bp_utils.safe_ident(table)
    with adb.db() as conn:
        tables = dict(bp_utils.list_tables(conn))
        if table not in tables:
            abort(404)
        is_readonly, _ = bp_utils.is_virtual_or_view(conn, table)
        if is_readonly:
            abort(400, "Только чтение")
        cols = bp_utils.columns(conn, table)
        pkcols = bp_utils.pk_cols(cols)
        fields_meta = [(c, *bp_utils.detect_input_type(table, c)) for c in cols]

        if request.method == "POST":
            form = request.form
            names: List[str] = []
            values: List[Any] = []
            for c in cols:
                raw = form.get(c.name)
                if c.name in ("is_new", "archived", "is_open") and raw is None:
                    raw = "0"
                if c in pkcols and (raw is None or raw.strip() == "") and "INT" in c.type.upper():
                    continue
                value = bp_utils.coerce_value(c, raw)
                enum_map = bp_utils.ENUM_TRANSLATIONS.get((table, c.name))
                if enum_map and value is not None and str(value) not in enum_map:
                    flash(
                        f"Недопустимое значение для поля \u00ab{bp_utils.col_title(table, c.name)}\u00bb",
                        "danger",
                    )
                    return redirect(request.url)
                if value is None:
                    if c.default is not None:
                        continue
                    if c.notnull:
                        flash(
                            f"Заполните поле \u00ab{bp_utils.col_title(table, c.name)}\u00bb",
                            "danger",
                        )
                        return redirect(request.url)
                names.append(c.name)
                values.append(value)
            if not names:
                flash("Нет данных для вставки", "warning")
                return redirect(url_for("tables.table_browse", table=table))
            placeholders = ",".join(["?"] * len(names))
            cols_sql = ", ".join(names)
            with conn:
                conn.execute(
                    f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})",
                    values,
                )
            flash("Строка добавлена", "success")
            return redirect(url_for("tables.table_browse", table=table))

    return render_template(
        "form.html",
        table=table,
        cols=cols,
        fields_meta=fields_meta,
        mode="add",
        pkcols=pkcols,
        table_title=bp_utils.table_title,
        col_title=bp_utils.col_title,
    )


@bp.route("/table/<table>/edit", methods=["GET", "POST"], endpoint="table_edit")
def table_edit(table: str):
    table = bp_utils.safe_ident(table)
    with adb.db() as conn:
        tables = dict(bp_utils.list_tables(conn))
        if table not in tables:
            abort(404)
        is_readonly, _ = bp_utils.is_virtual_or_view(conn, table)
        if is_readonly:
            abort(400, "Только чтение")
        cols = bp_utils.columns(conn, table)
        pkcols = bp_utils.pk_cols(cols)
        if not pkcols:
            abort(400, "Нет первичного ключа")
        where_sql, keys = bp_utils.build_where_for_pk(pkcols)
        form_keys = {k: request.values.get(k) for k in keys}
        if any(v is None for v in form_keys.values()):
            abort(400)
        row = conn.execute(
            f"SELECT * FROM {table} WHERE {where_sql}",
            tuple(form_keys[k] for k in keys),
        ).fetchone()
        if not row:
            abort(404)
        fields_meta = [(c, *bp_utils.detect_input_type(table, c)) for c in cols]

        if request.method == "POST":
            form = request.form
            updates: List[str] = []
            params: List[Any] = []
            for c in cols:
                if c in pkcols:
                    continue
                raw = form.get(c.name)
                value = bp_utils.coerce_value(c, raw)
                enum_map = bp_utils.ENUM_TRANSLATIONS.get((table, c.name))
                if enum_map and value is not None and str(value) not in enum_map:
                    flash(
                        f"Недопустимое значение для поля \u00ab{bp_utils.col_title(table, c.name)}\u00bb",
                        "danger",
                    )
                    return redirect(request.url)
                if value is None and c.notnull and c.default is None:
                    flash(
                        f"Заполните поле \u00ab{bp_utils.col_title(table, c.name)}\u00bb",
                        "danger",
                    )
                    return redirect(request.url)
                updates.append(f"{c.name}=?")
                params.append(value)
            if updates:
                params.extend(form_keys[k] for k in keys)
                with conn:
                    conn.execute(
                        f"UPDATE {table} SET {', '.join(updates)} WHERE {where_sql}",
                        params,
                    )
                flash("Изменения сохранены", "success")
            else:
                flash("Нет изменений", "info")
            return redirect(url_for("tables.table_browse", table=table))

    return render_template(
        "form.html",
        table=table,
        cols=cols,
        row=row,
        fields_meta=fields_meta,
        mode="edit",
        pkcols=pkcols,
        table_title=bp_utils.table_title,
        col_title=bp_utils.col_title,
    )


@bp.route("/table/<table>/delete", methods=["POST"], endpoint="table_delete")
def table_delete(table: str):
    table = bp_utils.safe_ident(table)
    with adb.db() as conn:
        tables = dict(bp_utils.list_tables(conn))
        if table not in tables:
            abort(404)
        is_readonly, _ = bp_utils.is_virtual_or_view(conn, table)
        if is_readonly:
            abort(400, "Только чтение")
        cols = bp_utils.columns(conn, table)
        pkcols = bp_utils.pk_cols(cols)
        if not pkcols:
            abort(400, "Удаление возможно только по PK")
        where_sql, keys = bp_utils.build_where_for_pk(pkcols)
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
    return redirect(url_for("tables.table_browse", table=table))


__all__ = [
    "bp",
    "table_browse",
    "table_add",
    "table_edit",
    "table_delete",
]
