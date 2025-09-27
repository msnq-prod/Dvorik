from __future__ import annotations

from typing import Any, Dict, List, Optional

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from app import constants as const
from app import db as adb
from app.services import stock as stock_svc
from app.utils_number import display_qty
from admin_ui.blueprints import utils as bp_utils


bp = Blueprint("inventory", __name__)


@bp.route("/inventory", endpoint="inventory_page")
def inventory_page():
    with adb.db() as conn:
        groups = bp_utils.load_stock_groups(conn)
        locations = bp_utils.load_locations(conn)
    return render_template("inventory.html", groups=groups, locations=locations)


@bp.route("/table/stock/adjust", methods=["POST"], endpoint="stock_adjust")
def stock_adjust():
    pid = int(request.form.get("product_id", "0"))
    loc = request.form.get("location_code", "").strip()
    try:
        delta = int(request.form.get("delta", "0"))
    except Exception:
        delta = 0
    if not pid or not loc or delta == 0:
        abort(400)
    wants_json = bp_utils.wants_json_response()
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
                "qty_display": display_qty(qty_val),
            })
        status = 400 if msg else 500
        return jsonify({"ok": False, "error": msg or "Не удалось изменить остаток"}), status
    if not ok:
        flash(msg or "Не удалось изменить остаток", "danger")
    nxt = request.form.get("next") or request.args.get("next")
    if nxt:
        return redirect(nxt)
    return redirect(url_for("home.index"))


@bp.route("/table/stock/add", methods=["POST"], endpoint="stock_add")
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
    return redirect(url_for("home.index") + f"#loc-{loc}")


@bp.route("/stock/move", methods=["POST"], endpoint="stock_move")
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
    wants_json = bp_utils.wants_json_response()
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
                return display_qty(val)

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
        return redirect(url_for("home.index") + f"#loc-{src}")
    if nxt:
        return redirect(nxt)
    return redirect(url_for("home.index") + f"#loc-{src}")


@bp.post("/api/stock/adjust", endpoint="api_stock_adjust")
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
            (pid, const.HUB_LOCATION_CODE),
        ).fetchone()[0]
    return jsonify({"ok": bool(ok), "qty": new_qty, "hub_qty": hub_qty, "message": msg or ""})


@bp.post("/api/stock/set_qty", endpoint="api_stock_set_qty")
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


@bp.post("/api/stock/move", endpoint="api_stock_move")
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


__all__ = [
    "bp",
    "inventory_page",
    "stock_adjust",
    "stock_add",
    "stock_move",
    "api_stock_adjust",
    "api_stock_set_qty",
    "api_stock_move",
]
