from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import sqlite3
from flask import Blueprint, abort, jsonify, render_template, request, send_from_directory

from app import db as adb
from admin_ui.blueprints import utils as bp_utils


bp = Blueprint("home", __name__)


def _prepare_low_stock_rows(
    rows: Sequence[sqlite3.Row], phrases: Sequence[str]
) -> List[Dict[str, Any]]:
    prepared: List[Dict[str, Any]] = []
    strip_exceptions = bp_utils.strip_display_exceptions
    for row in rows:
        data = dict(row)
        cleaned_name = strip_exceptions(data.get("disp_name"), phrases)
        if not cleaned_name:
            article = data.get("article")
            cleaned_name = str(article).strip() if article else ""
        data["disp_name"] = cleaned_name
        prepared.append(data)
    return prepared


@bp.route("/", endpoint="index")
def index():
    today = dt.date.today()
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

    ms, me, sellers, weeks = bp_utils.build_schedule_data(year, month)

    with adb.db() as conn:
        low_rows_raw = conn.execute(
            """
            SELECT p.article,
                   COALESCE(p.local_name,p.name) AS disp_name,
                   IFNULL(SUM(s.qty_pack),0) AS total
            FROM product p
            LEFT JOIN stock s ON s.product_id=p.id
            WHERE p.archived=0
            GROUP BY p.id
            HAVING total>0 AND total<2
            ORDER BY total ASC, p.id DESC
            LIMIT 100
            """
        ).fetchall()
        exception_rows = conn.execute(
            "SELECT phrase FROM display_name_exception ORDER BY lower(phrase)"
        ).fetchall()
        exception_phrases = [row["phrase"] for row in exception_rows if row["phrase"] is not None]
        low_rows = _prepare_low_stock_rows(low_rows_raw, exception_phrases)

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

        groups = bp_utils.load_stock_groups(conn, include_hall=False)
        locs = bp_utils.load_locations(conn)

    return render_template(
        "home.html",
        low_rows=low_rows,
        loc_rows=loc_rows,
        groups=groups,
        locations=locs,
        ms=ms,
        me=me,
        sellers=sellers,
        weeks=weeks,
    )


@bp.route("/ipad", endpoint="ipad_page")
def ipad_page():
    with adb.db() as conn:
        groups = bp_utils.load_stock_groups(conn, include_hall=False)
        locs = bp_utils.load_locations(conn)

    return render_template("ipad.html", groups=groups, locations=locs)
=======

    return render_template("ipad.html", groups=groups)



@bp.get("/ipad/api/product/<int:pid>/locations", endpoint="ipad_product_locations")
def ipad_product_locations(pid: int):
    with adb.db() as conn:
        rows = conn.execute(
            """
            SELECT s.location_code AS code,
                   COALESCE(l.title, s.location_code) AS title,
                   IFNULL(s.qty_pack, 0) AS qty
            FROM stock s
            LEFT JOIN location l ON l.code = s.location_code
            WHERE s.product_id=?
            ORDER BY qty DESC, s.location_code
            """,
            (pid,),
        ).fetchall()

    locations: List[Dict[str, Any]] = []
    for row in rows:
        code = row["code"]
        title = row["title"]
        qty_val = row["qty"]
        try:
            qty_float = float(qty_val)
        except (TypeError, ValueError):
            qty_float = 0.0
        locations.append(
            {
                "code": code,
                "title": title,
                "quantity": qty_float,
            }
        )

    preferred: Optional[Dict[str, Any]] = None
    for item in locations:
        if item.get("quantity", 0.0) > 0:
            preferred = item
            break
    if preferred is None and locations:
        preferred = locations[0]

    payload = {"locations": locations, "preferred": preferred}
    return jsonify(payload)


@bp.route("/media/<path:subpath>", endpoint="serve_media")
def serve_media(subpath: str):
    base = Path("media").resolve()
    target = (base / subpath).resolve()
    if not str(target).startswith(str(base)):
        abort(403)
    if not target.exists():
        abort(404)
    return send_from_directory(str(base), subpath)


__all__ = ["bp", "index", "serve_media"]
