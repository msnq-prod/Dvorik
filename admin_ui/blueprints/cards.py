from __future__ import annotations

import datetime as dt
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from flask import (
    Blueprint,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from app import db as adb
from app.services import product_merge as merge_svc
from app.services import search as search_svc
from admin_ui.blueprints import utils as bp_utils


bp = Blueprint("cards", __name__)


def _hydrate_card_rows(
    conn: sqlite3.Connection,
    rows: Sequence[sqlite3.Row],
    *,
    extras: Optional[Dict[int, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    extras = extras or {}
    ids = [int(r["id"]) for r in rows]
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
                {
                    "code": sr["location_code"],
                    "title": sr["title"],
                    "qty": sr["qty"],
                }
            )

    items: List[Dict[str, Any]] = []
    for r in rows:
        pid = int(r["id"])
        ppath = (r["photo_path"] or "").strip() if "photo_path" in r.keys() else ""
        purl = None
        if ppath and os.path.isfile(ppath):
            try:
                rel = os.path.relpath(ppath, "media")
            except Exception:
                rel = None
            if rel and not rel.startswith(".."):
                purl = url_for("home.serve_media", subpath=rel)
        data: Dict[str, Any] = {
            "id": pid,
            "article": r["article"],
            "name": r["name"],
            "nomenclature_name": bp_utils.primary_product_name(r["name"]),
            "local_name": r["local_name"],
            "brand_country": r["brand_country"] if "brand_country" in r.keys() else None,
            "manufacturer_id": r["manufacturer_id"] if "manufacturer_id" in r.keys() else None,
            "manufacturer_name": r["manufacturer_name"] if "manufacturer_name" in r.keys() else None,
            "manufacturer_country": r["manufacturer_country"] if "manufacturer_country" in r.keys() else None,
            "photo_url": purl,
            "stocks": stocks_map.get(pid, []),
        }
        if data.get("manufacturer_id"):
            data["manufacturer"] = {
                "id": int(data["manufacturer_id"]),
                "name": data.get("manufacturer_name"),
                "country": data.get("manufacturer_country"),
            }
        else:
            data["manufacturer"] = None
        extra = extras.get(pid)
        if extra:
            data.update(extra)
        items.append(data)
    return items


def _cards_search(
    conn: sqlite3.Connection,
    q: str,
    limit: int = 60,
    *,
    without_local: bool = False,
    hide_empty: bool = False,
    only_empty: bool = False,
    location_codes: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    rows = search_svc.cards_search(
        conn,
        q,
        limit,
        without_local=without_local,
        hide_empty=hide_empty,
        only_empty=only_empty,
        location_codes=location_codes,
    )
    return _hydrate_card_rows(conn, rows)


def _find_similar_cards(
    conn: sqlite3.Connection,
    product_id: int,
    *,
    limit: int = 30,
    threshold: float = 0.7,
) -> List[Dict[str, Any]]:
    rows, extras = search_svc.find_similar_cards(
        conn,
        product_id,
        limit=limit,
        threshold=threshold,
    )
    return _hydrate_card_rows(conn, rows, extras=extras)


def _find_similar_groups(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    threshold: float = 0.7,
    use_exceptions: bool = True,
) -> List[Dict[str, Any]]:
    groups_data = search_svc.find_similar_groups(
        conn,
        limit=limit,
        threshold=threshold,
        use_exceptions=use_exceptions,
    )
    groups: List[Dict[str, Any]] = []
    for group in groups_data:
        items = _hydrate_card_rows(conn, group["rows"], extras=group["extras"])
        groups.append(
            {
                "group_id": group["group_id"],
                "size": group["size"],
                "score": group["score"],
                "items": items,
            }
        )
    return groups


@bp.get("/api/manufacturers", endpoint="api_manufacturers_list")
def api_manufacturers_list():
    q = (request.args.get("q") or "").strip()
    try:
        limit = int(request.args.get("limit", "200"))
    except (TypeError, ValueError):
        limit = 200
    limit = max(1, min(limit, 500))
    like = f"%{q}%" if q else None
    with adb.db() as conn:
        if like:
            rows = conn.execute(
                """
                SELECT id, name, country
                FROM manufacturer
                WHERE lower(name) LIKE lower(?) OR lower(country) LIKE lower(?)
                ORDER BY lower(name)
                LIMIT ?
                """,
                (like, like, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, name, country
                FROM manufacturer
                ORDER BY lower(name)
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    items = [
        {"id": int(r["id"]), "name": r["name"], "country": r["country"]}
        for r in rows
    ]
    return jsonify(items)


@bp.get("/api/cards/search", endpoint="api_cards_search")
def api_cards_search():
    q = request.args.get("q", "").strip()
    try:
        limit = int(request.args.get("limit", "60"))
    except (TypeError, ValueError):
        limit = 60
    limit = max(min(limit, 500), 1)

    def _as_bool(value: Optional[str]) -> bool:
        if value is None:
            return False
        value = value.strip().lower()
        return value in {"1", "true", "yes", "on"}

    raw_locations = request.args.getlist("locations")
    seen_locations: Set[str] = set()
    selected_locations: List[str] = []
    for raw_code in raw_locations:
        code = (raw_code or "").strip()
        if code and code not in seen_locations:
            seen_locations.add(code)
            selected_locations.append(code)

    with adb.db() as conn:
        items = _cards_search(
            conn,
            q,
            limit,
            without_local=_as_bool(request.args.get("no_local")),
            hide_empty=_as_bool(request.args.get("hide_empty")),
            only_empty=_as_bool(request.args.get("only_empty")),
            location_codes=selected_locations,
        )
    return jsonify(items)


@bp.get("/api/cards/similar", endpoint="api_cards_similar")
def api_cards_similar():
    raw_pid = (request.args.get("product_id") or "").strip()
    if not raw_pid:
        abort(400)
    try:
        pid = int(raw_pid)
    except Exception:
        abort(400)
    try:
        limit = int(request.args.get("limit", "30"))
    except (TypeError, ValueError):
        limit = 30
    limit = max(1, min(limit, 200))
    threshold_param = request.args.get("threshold")
    try:
        threshold = float(threshold_param) if threshold_param else 0.7
    except (TypeError, ValueError):
        threshold = 0.7
    threshold = max(0.0, min(threshold, 1.0))
    with adb.db() as conn:
        items = _find_similar_cards(conn, pid, limit=limit, threshold=threshold)
    return jsonify(items)


@bp.get("/api/cards/similar-groups", endpoint="api_cards_similar_groups")
def api_cards_similar_groups():
    try:
        limit = int(request.args.get("limit", "20"))
    except (TypeError, ValueError):
        limit = 20
    limit = max(1, min(limit, 200))
    threshold_param = request.args.get("threshold")
    try:
        threshold = float(threshold_param) if threshold_param else 0.7
    except (TypeError, ValueError):
        threshold = 0.7
    threshold = max(0.0, min(threshold, 1.0))
    disable_exceptions = False
    for raw in (
        request.args.get("no_exceptions"),
        request.args.get("ignore_exceptions"),
    ):
        if not raw:
            continue
        if raw.strip().lower() in {"1", "true", "yes", "on"}:
            disable_exceptions = True
            break
    with adb.db() as conn:
        groups = _find_similar_groups(
            conn,
            limit=limit,
            threshold=threshold,
            use_exceptions=not disable_exceptions,
        )
    return jsonify(groups)


@bp.get("/api/products/search", endpoint="api_products_search")
def api_products_search():
    q = (request.args.get("q") or "").strip()
    try:
        limit = int(request.args.get("limit", "10"))
    except (TypeError, ValueError):
        limit = 10
    items: List[Dict[str, Any]] = []
    if not q:
        return jsonify(items)
    with adb.db() as conn:
        has_fts = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_fts'"
        ).fetchone() is not None
        if has_fts:
            query = q.replace("'", "''").strip()
            tokens = [t for t in query.split() if t]
            match = " ".join(t + "*" for t in tokens) or query + "*"
            sql = (
                "SELECT p.id, p.article, p.name, p.local_name "
                "FROM product p JOIN product_fts f ON p.id=f.rowid "
                "WHERE product_fts MATCH ? LIMIT ?"
            )
            rows = conn.execute(sql, (match, limit)).fetchall()
            if not rows:
                like = f"%{q}%"
                rows = conn.execute(
                    "SELECT id, article, name, local_name FROM product "
                    "WHERE article LIKE ? OR name LIKE ? OR COALESCE(local_name,'') LIKE ? "
                    "ORDER BY id DESC LIMIT ?",
                    (like, like, like, limit),
                ).fetchall()
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
                    "display": (r["local_name"] or r["name"] or "")
                    + (f" · {r['article']}" if r["article"] else ""),
                }
            )
    return jsonify(items)


@bp.get("/api/merge/product/<int:pid>", endpoint="api_merge_product")
def api_merge_product(pid: int):
    with adb.db() as conn:
        detail = bp_utils.product_detail(conn, pid)
        if not detail:
            abort(404)
    return jsonify(detail)


@bp.post("/api/merge/apply", endpoint="api_merge_apply")
def api_merge_apply():
    payload = request.get_json(silent=True) or {}
    try:
        source_a = int(payload.get("source_a_id"))
        source_b = int(payload.get("source_b_id"))
    except Exception:
        abort(400)
    raw_modes = payload.get("field_modes") or {}
    if not isinstance(raw_modes, dict):
        raw_modes = {}
    field_modes = {str(k): str(v) for k, v in raw_modes.items()}
    stock_mode = str(payload.get("stock_mode") or "merge")
    with adb.db() as conn:
        try:
            result = merge_svc.apply_merge(
                conn,
                source_a,
                source_b,
                field_modes=field_modes,
                stock_mode=stock_mode,
            )
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify(result)


@bp.get("/api/merge/history", endpoint="api_merge_history")
def api_merge_history():
    raw_limit = request.args.get("limit", "")
    try:
        limit = int(raw_limit) if raw_limit else 20
    except Exception:
        limit = 20
    limit = max(1, min(limit, 200))
    with adb.db() as conn:
        items = merge_svc.list_history(conn, limit=limit)
    return jsonify(items)


@bp.post("/api/merge/undo", endpoint="api_merge_undo")
def api_merge_undo():
    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict()
    try:
        merge_id = int(payload.get("merge_id"))
    except Exception:
        abort(400)
    with adb.db() as conn:
        result = merge_svc.undo_merge(conn, merge_id)
    status = 200 if result.get("ok") else 400
    return jsonify(result), status


@bp.post("/api/product/set_local_name", endpoint="api_product_set_local_name")
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


@bp.post("/api/product/set_manufacturer", endpoint="api_product_set_manufacturer")
def api_product_set_manufacturer():
    payload = request.form if request.form else request.json or {}
    try:
        pid = int(payload.get("product_id"))
    except Exception:
        abort(400)
    if not pid:
        abort(400)
    raw_mid = payload.get("manufacturer_id")
    manufacturer_id: Optional[int]
    if raw_mid is None or str(raw_mid).strip() in {"", "null", "none"}:
        manufacturer_id = None
    else:
        try:
            manufacturer_id = int(raw_mid)
        except Exception:
            abort(400)
    with adb.db() as conn:
        with conn:
            if manufacturer_id is None:
                conn.execute(
                    "UPDATE product SET manufacturer_id=NULL, brand_country=NULL WHERE id=?",
                    (pid,),
                )
                manufacturer = None
                brand = None
            else:
                row = conn.execute(
                    "SELECT id, name, country FROM manufacturer WHERE id=?",
                    (manufacturer_id,),
                ).fetchone()
                if not row:
                    abort(400)
                brand = f"{row['name']} ({row['country']})"
                conn.execute(
                    "UPDATE product SET manufacturer_id=?, brand_country=? WHERE id=?",
                    (manufacturer_id, brand, pid),
                )
                manufacturer = {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "country": row["country"],
                }
    return jsonify({"ok": True, "manufacturer": manufacturer, "brand_country": brand})


@bp.post("/api/product/upload_photo", endpoint="api_product_upload_photo")
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
            conn.execute(
                "UPDATE product SET photo_path=?, photo_file_id=NULL WHERE id=?",
                (rel, pid),
            )
    purl = None
    if os.path.isfile(rel):
        try:
            media_rel = os.path.relpath(rel, "media")
        except Exception:
            media_rel = None
        if media_rel and not media_rel.startswith(".."):
            purl = url_for("home.serve_media", subpath=media_rel)
    return jsonify({"ok": True, "photo_url": purl})


__all__ = [
    "bp",
    "edit_page",
    "cards_page",
    "api_cards_search",
    "api_cards_similar",
    "api_cards_similar_groups",
    "api_products_search",
    "api_merge_product",
    "api_merge_apply",
    "api_merge_history",
    "api_merge_undo",
    "api_product_set_local_name",
    "api_product_set_manufacturer",
    "api_product_upload_photo",
]

@bp.route("/edit", endpoint="edit_page")
def edit_page():
    return render_template("edit.html")


@bp.route("/cards", endpoint="cards_page")
def cards_page():
    with adb.db() as conn:
        locs = [
            {"code": row["code"], "title": row["title"]}
            for row in conn.execute(
                "SELECT code, title FROM location ORDER BY kind, code"
            ).fetchall()
        ]
    return render_template("cards.html", locations=locs)
