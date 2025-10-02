from __future__ import annotations

from dataclasses import dataclass
import logging
import re
import sqlite3
from typing import Iterable, Mapping, Sequence

from flask import Blueprint, Response, abort, redirect, render_template, request, url_for

from dvorik.admin.csrf import CSRFError, validate_csrf_request

from dvorik.db.conn import db

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TableMeta:
    """Metadata about a browsable database table."""

    name: str
    row_count: int


@dataclass(frozen=True, slots=True)
class TableColumn:
    """Description of a table column derived from PRAGMA information."""

    name: str
    declared_type: str
    notnull: bool
    default: str | None
    primary_key: bool


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

blueprint = Blueprint("tables", __name__, url_prefix="/tables")


@blueprint.get("/")
def list_tables() -> str:
    """Display available database tables."""

    tables = _fetch_browsable_tables()
    status, message = _extract_status()
    return render_template(
        "tables/table.html",
        tables=tables,
        active_table=None,
        columns=(),
        rows=(),
        page=1,
        pages=1,
        status=status,
        message=message,
    )


@blueprint.get("/<table_name>/")
def browse_table(table_name: str) -> str:
    """Render rows for a specific database table."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)

    page = _parse_positive_int(request.args.get("page"), default=1)
    per_page = 50
    offset = (page - 1) * per_page

    columns = _fetch_table_columns(table.name)
    rows = _fetch_table_rows(table.name, columns, limit=per_page, offset=offset)

    total_pages = max((table.row_count + per_page - 1) // per_page, 1)
    has_prev = page > 1
    has_next = page < total_pages
    status, message = _extract_status()

    return render_template(
        "tables/table.html",
        tables=tables,
        active_table=table,
        columns=columns,
        rows=rows,
        page=page,
        pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        prev_page=page - 1 if has_prev else 1,
        next_page=page + 1 if has_next else total_pages,
        status=status,
        message=message,
    )


@blueprint.get("/<table_name>/new")
def new_row_form(table_name: str) -> str:
    """Render the creation form for ``table_name``."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)
    columns = _fetch_table_columns(table.name)

    return _render_form(
        table=table,
        columns=columns,
        values={col.name: "" for col in columns},
        errors={},
        mode="create",
    )


@blueprint.post("/<table_name>/new")
def create_row(table_name: str) -> Response | str:
    """Insert a new record into ``table_name``."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)
    columns = _fetch_table_columns(table.name)

    csrf_failure = _ensure_csrf(table.name)
    if csrf_failure is not None:
        return csrf_failure

    form_values = {col.name: request.form.get(col.name, "") for col in columns}
    errors: dict[str, str] = {}
    payload = []
    field_names: list[str] = []

    for column in columns:
        raw_value = form_values[column.name]

        if column.primary_key and not raw_value:
            # Skip auto-generated primary keys when no value provided.
            continue

        value = _normalise_value(raw_value)
        if value is None and column.notnull and column.default is None:
            errors[column.name] = "Value is required."
            continue

        field_names.append(column.name)
        payload.append(value)

    if errors:
        return _render_form(table, columns, form_values, errors, mode="create")

    if field_names:
        insert_sql = _build_insert_sql(table.name, field_names)
    else:
        insert_sql = f"INSERT INTO {_quote_identifier(table.name)} DEFAULT VALUES"

    conn = db()
    try:
        with conn:
            conn.execute(insert_sql, payload if field_names else ())
    except sqlite3.Error as exc:
        logger.exception("Failed to insert row into table %s", table.name)
        errors["__form__"] = _format_error(exc)
        return _render_form(table, columns, form_values, errors, mode="create")
    finally:
        conn.close()

    return _redirect_to_table(table.name, status="created")


@blueprint.get("/<table_name>/<int:rowid>/edit")
def edit_row_form(table_name: str, rowid: int) -> str | Response:
    """Render the edit form for ``rowid`` in ``table_name``."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)
    columns = _fetch_table_columns(table.name)

    row = _fetch_single_row(table.name, rowid, columns)
    if row is None:
        return _redirect_to_table(table.name, status="error", message="Row was not found.")

    values = {column.name: row[column.name] for column in columns}
    return _render_form(table, columns, values, errors={}, mode="edit", rowid=rowid)


@blueprint.post("/<table_name>/<int:rowid>/edit")
def update_row(table_name: str, rowid: int) -> Response | str:
    """Persist modifications to an existing row."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)
    columns = _fetch_table_columns(table.name)

    csrf_failure = _ensure_csrf(table.name)
    if csrf_failure is not None:
        return csrf_failure

    form_values = {col.name: request.form.get(col.name, "") for col in columns}
    assignments: list[str] = []
    payload: list[object] = []
    errors: dict[str, str] = {}

    for column in columns:
        if column.primary_key:
            continue

        raw_value = form_values[column.name]
        value = _normalise_value(raw_value)
        if value is None and column.notnull and column.default is None:
            errors[column.name] = "Value is required."
            continue

        assignments.append(f"{_quote_identifier(column.name)} = ?")
        payload.append(value)

    if errors:
        return _render_form(table, columns, form_values, errors, mode="edit", rowid=rowid)

    if not assignments:
        errors["__form__"] = "There are no editable columns for this table."
        return _render_form(table, columns, form_values, errors, mode="edit", rowid=rowid)

    payload.append(rowid)
    update_sql = f"UPDATE {_quote_identifier(table.name)} SET {', '.join(assignments)} WHERE rowid = ?"

    conn = db()
    try:
        with conn:
            cursor = conn.execute(update_sql, payload)
            if cursor.rowcount == 0:
                return _redirect_to_table(table.name, status="error", message="Row was not found.")
    except sqlite3.Error as exc:
        logger.exception("Failed to update row %s in table %s", rowid, table.name)
        errors["__form__"] = _format_error(exc)
        return _render_form(table, columns, form_values, errors, mode="edit", rowid=rowid)
    finally:
        conn.close()

    return _redirect_to_table(table.name, status="updated")


@blueprint.post("/<table_name>/<int:rowid>/delete")
def delete_row(table_name: str, rowid: int) -> Response:
    """Remove a row from ``table_name`` identified by ``rowid``."""

    tables = _fetch_browsable_tables()
    table = _get_table_meta(tables, table_name)

    csrf_failure = _ensure_csrf(table.name)
    if csrf_failure is not None:
        return csrf_failure

    conn = db()
    try:
        with conn:
            cursor = conn.execute(
                f"DELETE FROM {_quote_identifier(table.name)} WHERE rowid = ?",
                (rowid,),
            )
            if cursor.rowcount == 0:
                return _redirect_to_table(table.name, status="error", message="Row was not found.")
    except sqlite3.Error as exc:
        logger.exception("Failed to delete row %s in table %s", rowid, table.name)
        return _redirect_to_table(table.name, status="error", message=_format_error(exc))
    finally:
        conn.close()

    return _redirect_to_table(table.name, status="deleted")


def _ensure_csrf(table: str) -> Response | None:
    try:
        validate_csrf_request()
    except CSRFError as exc:
        logger.warning("CSRF validation failed for table %s: %s", table, exc)
        return _redirect_to_table(
            table,
            status="error",
            message="Your session has expired. Please refresh the page and try again.",
        )
    return None


def _render_form(
    table: TableMeta,
    columns: Sequence[TableColumn],
    values: Mapping[str, object | None],
    errors: Mapping[str, str],
    *,
    mode: str,
    rowid: int | None = None,
) -> str:
    """Render the create/edit form template."""

    return render_template(
        "tables/form.html",
        table=table,
        columns=columns,
        values=values,
        errors=errors,
        mode=mode,
        rowid=rowid,
    )


def _fetch_browsable_tables() -> tuple[TableMeta, ...]:
    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """,
        ).fetchall()

        tables: list[TableMeta] = []
        for row in rows:
            name = str(row["name"])
            definition = (row["sql"] or "").upper()
            if "VIRTUAL TABLE" in definition:
                continue

            count = _count_rows(conn, name)
            tables.append(TableMeta(name=name, row_count=count))

        return tuple(tables)
    finally:
        conn.close()


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    try:
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(table)}",
        )
    except sqlite3.Error as exc:  # pragma: no cover - defensive guard
        logger.exception("Unable to count rows for table %s", table)
        raise

    result = cursor.fetchone()
    return int(result[0]) if result else 0


def _fetch_table_columns(table: str) -> tuple[TableColumn, ...]:
    conn = db()
    try:
        pragma_sql = f"PRAGMA table_info({_quote_identifier(table)})"
        rows = conn.execute(pragma_sql).fetchall()
    finally:
        conn.close()

    if not rows:
        abort(404)

    columns: list[TableColumn] = []
    for row in rows:
        columns.append(
            TableColumn(
                name=str(row["name"]),
                declared_type=str(row["type"] or ""),
                notnull=bool(row["notnull"]),
                default=str(row["dflt_value"]) if row["dflt_value"] is not None else None,
                primary_key=bool(row["pk"]),
            )
        )

    return tuple(columns)


def _fetch_table_rows(
    table: str,
    columns: Sequence[TableColumn],
    *,
    limit: int,
    offset: int,
) -> tuple[sqlite3.Row, ...]:
    conn = db()
    try:
        column_list = ", ".join(_quote_identifier(col.name) for col in columns)
        sql = (
            f"SELECT rowid AS __rowid__, {column_list} "
            f"FROM {_quote_identifier(table)} ORDER BY rowid LIMIT ? OFFSET ?"
        )
        rows = conn.execute(sql, (limit, offset)).fetchall()
        return tuple(rows)
    finally:
        conn.close()


def _fetch_single_row(
    table: str,
    rowid: int,
    columns: Sequence[TableColumn],
) -> sqlite3.Row | None:
    conn = db()
    try:
        column_list = ", ".join(_quote_identifier(col.name) for col in columns)
        sql = (
            f"SELECT rowid AS __rowid__, {column_list} "
            f"FROM {_quote_identifier(table)} WHERE rowid = ?"
        )
        row = conn.execute(sql, (rowid,)).fetchone()
        return row
    finally:
        conn.close()


def _get_table_meta(tables: Iterable[TableMeta], name: str) -> TableMeta:
    for table in tables:
        if table.name == name:
            return table
    abort(404)


def _build_insert_sql(table: str, columns: Sequence[str]) -> str:
    quoted_columns = ", ".join(_quote_identifier(col) for col in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {_quote_identifier(table)} ({quoted_columns}) VALUES ({placeholders})"


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):  # pragma: no cover - defensive guard
        raise ValueError(f"Unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def _normalise_value(value: str | None) -> object | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped != "" else None


def _parse_positive_int(raw_value: str | None, *, default: int) -> int:
    try:
        value = int(raw_value) if raw_value is not None else default
    except ValueError:
        return default
    return value if value > 0 else default


def _redirect_to_table(table: str, *, status: str, message: str | None = None) -> Response:
    params: dict[str, str] = {"status": status}
    if message:
        params["message"] = message
    return redirect(url_for("tables.browse_table", table_name=table, **params))


def _extract_status() -> tuple[str | None, str | None]:
    status = request.args.get("status")
    message = request.args.get("message")
    if status == "error" and message is None:
        message = "An unexpected error occurred."
    return status, message


def _format_error(exc: sqlite3.Error) -> str:
    return str(exc).strip() or exc.__class__.__name__


__all__ = ["blueprint"]
