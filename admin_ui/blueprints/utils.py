from __future__ import annotations

import datetime as dt
import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from flask import abort, request, url_for

from app import constants as const
from app import db as adb
from app.services import products_display as display_svc
from app.services import schedule as sched


primary_product_name = display_svc.primary_product_name
strip_display_exceptions = display_svc.strip_display_exceptions

# TODO[2024-09-30]: drop transitional aliases once callers stop using underscored names.
_primary_product_name = display_svc.primary_product_name
_strip_display_exceptions = display_svc.strip_display_exceptions


@dataclass
class Column:
    """Thin wrapper around PRAGMA column metadata used by admin forms."""

    name: str
    type: str
    notnull: bool
    pk_order: int  # 0 if not part of the primary key
    default: Optional[str]


TABLE_LABELS = const.TABLE_LABELS
COLUMN_LABELS = const.COLUMN_LABELS
BROWSE_HIDDEN_COLUMNS = const.BROWSE_HIDDEN_COLUMNS
LOCATION_KIND_LABEL = const.LOCATION_KIND_LABEL
BOOL_COLS = const.BOOL_COLS
ENUM_TRANSLATIONS = const.ENUM_TRANSLATIONS
HUB_LOCATION_CODE = const.HUB_LOCATION_CODE


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


def wants_json_response() -> bool:
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


def columns(conn: sqlite3.Connection, table: str) -> List[Column]:
    """Load column metadata for ``table`` preserving PRAGMA semantics."""

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
    return cols


def pk_cols(cols: Sequence[Column]) -> List[Column]:
    return sorted([c for c in cols if c.pk_order > 0], key=lambda c: c.pk_order)


def is_virtual_or_view(conn: sqlite3.Connection, table: str) -> Tuple[bool, str]:
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


def list_tables(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    rows = conn.execute(
        "SELECT name, type, sql FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    out: List[Tuple[str, str]] = []
    for name, typ, sql in rows:
        sqlu = (sql or "").upper()
        if typ == "view" or "VIRTUAL TABLE" in sqlu:
            continue
        out.append((name, "table"))
    return out


def safe_ident(name: str) -> str:
    if not name:
        abort(400)
    if any(ch in name for ch in ("`", '"', "'", ";", "/", "\\")):
        abort(400)
    return name


def build_where_for_pk(pkcols: Sequence[Column]) -> Tuple[str, List[str]]:
    parts = []
    keys = []
    for c in pkcols:
        parts.append(f"{c.name}=?")
        keys.append(c.name)
    return " AND ".join(parts), keys


def visible_columns(table: str, cols: Sequence[Column]) -> List[Column]:
    hidden = BROWSE_HIDDEN_COLUMNS.get(table, set())
    return [c for c in cols if c.name not in hidden]


def detect_input_type(
    table: str, col: Column
) -> Tuple[str, Dict[str, Any], Optional[List[Tuple[str, str]]]]:
    t = (col.type or "").upper()
    attrs: Dict[str, Any] = {}
    enum_map = ENUM_TRANSLATIONS.get((table, col.name))
    if enum_map:
        choices = list(enum_map.items())
        return "select", attrs, choices
    if col.name in ("is_new", "archived", "is_open"):
        return "checkbox", attrs, None
    if any(x in t for x in ("INT",)):
        attrs["step"] = 1
        return "number", attrs, None
    if any(x in t for x in ("REAL", "FLOA", "DOUB")):
        attrs["step"] = "any"
        return "number", attrs, None
    if "BOOL" in t:
        return "checkbox", attrs, None
    return "text", attrs, None


def coerce_value(col: Column, val: Optional[str]) -> Any:
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
        return v
    return v


def load_stock_groups(
    conn: sqlite3.Connection, *, include_hall: bool = True
) -> List[Dict[str, Any]]:
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
            WHEN kind='SKL' AND l.code=? THEN 5
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
        """,
        (HUB_LOCATION_CODE,),
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


def load_locations(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT code, title, kind FROM location ORDER BY kind, code"
    ).fetchall()
    return [
        {"code": row["code"], "title": row["title"], "kind": row["kind"]}
        for row in rows
    ]


def month_range(year: int, month: int) -> Tuple[dt.date, dt.date]:
    start = dt.date(year, month, 1)
    if month == 12:
        end = dt.date(year, 12, 31)
    else:
        end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    return start, end


def build_schedule_data(year: int, month: int):
    ms, me = month_range(year, month)
    sellers = sched.list_sellers()
    day_infos: List[Optional[Dict[str, Any]]] = []
    with adb.db() as conn:  # type: ignore[name-defined]
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
            weeks.append(week)
            week = []
    if week:
        week.extend([None] * (7 - len(week)))
        weeks.append(week)
    return ms, me, sellers, weeks


def product_detail(conn: sqlite3.Connection, pid: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT p.id,
               p.article,
               p.name,
               p.brand_country,
               p.local_name,
               p.photo_file_id,
               p.photo_path,
               p.archived,
               p.manufacturer_id,
               m.name AS manufacturer_name,
               m.country AS manufacturer_country
        FROM product p
        LEFT JOIN manufacturer m ON m.id = p.manufacturer_id
        WHERE p.id=?
        """,
        (pid,),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["nomenclature_name"] = primary_product_name(row["name"])
    ppath = (row["photo_path"] or "").strip()
    photo_url = None
    if ppath and os.path.isfile(ppath):
        try:
            rel = os.path.relpath(ppath, "media")
        except Exception:
            rel = None
        if rel and not rel.startswith(".."):
            photo_url = url_for("home.serve_media", subpath=rel)
    data["photo_url"] = photo_url
    stocks = conn.execute(
        """
        SELECT s.location_code AS code,
               COALESCE(l.title, s.location_code) AS title,
               SUM(s.qty_pack) AS qty
        FROM stock s
        LEFT JOIN location l ON l.code=s.location_code
        WHERE s.product_id=?
        GROUP BY s.location_code
        HAVING ABS(SUM(s.qty_pack))>0.000001
        ORDER BY s.location_code
        """,
        (pid,),
    ).fetchall()
    data["manufacturer"] = None
    if row["manufacturer_id"]:
        data["manufacturer"] = {
            "id": int(row["manufacturer_id"]),
            "name": row["manufacturer_name"],
            "country": row["manufacturer_country"],
        }
    data["stocks"] = [
        {
            "code": r["code"],
            "title": r["title"],
            "qty": float(r["qty"] or 0.0),
        }
        for r in stocks
    ]
    return data


__all__ = [
    "Column",
    "table_title",
    "col_title",
    "value_label",
    "wants_json_response",
    "columns",
    "pk_cols",
    "is_virtual_or_view",
    "list_tables",
    "safe_ident",
    "build_where_for_pk",
    "visible_columns",
    "detect_input_type",
    "coerce_value",
    "load_stock_groups",
    "load_locations",
    "month_range",
    "build_schedule_data",
    "product_detail",
    "TABLE_LABELS",
    "COLUMN_LABELS",
    "BROWSE_HIDDEN_COLUMNS",
    "LOCATION_KIND_LABEL",
    "ENUM_TRANSLATIONS",
]
