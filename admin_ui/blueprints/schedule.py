from __future__ import annotations

import datetime as dt
from typing import Dict

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
from app.services import schedule as sched
from admin_ui.blueprints import utils as bp_utils


bp = Blueprint("schedule", __name__)


@bp.route("/schedule", methods=["GET"], endpoint="schedule_view")
def schedule_view():
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

    return render_template(
        "schedule.html",
        year=year,
        month=month,
        ms=ms,
        me=me,
        weeks=weeks,
        sellers=sellers,
    )


@bp.route("/schedule/toggle_cell", methods=["POST"], endpoint="schedule_toggle_cell")
def schedule_toggle_cell():
    date = dt.date.fromisoformat(request.form.get("date"))
    tg_id = int(request.form.get("tg_id"))
    wants_json = bp_utils.wants_json_response()
    new_state = False
    total_assigned = 0
    with adb.db() as conn:
        assigned = sched.get_assignments(date, conn)
        if tg_id in assigned:
            sched.remove_assignment(date, tg_id, conn)
        else:
            sched.set_assignment(date, tg_id, source="admin", conn=conn)
        if wants_json:
            updated = sched.get_assignments(date, conn)
            new_state = tg_id in updated
            total_assigned = len(updated)
    if wants_json:
        return jsonify(
            {
                "ok": True,
                "assigned": new_state,
                "count": total_assigned,
                "date": date.isoformat(),
                "tg_id": tg_id,
            }
        )
    if (request.form.get("from") or "").strip() == "index":
        return redirect(url_for("home.index", year=date.year, month=date.month))
    return redirect(url_for("schedule.schedule_view", year=date.year, month=date.month))


@bp.route("/schedule/clear_month", methods=["POST"], endpoint="schedule_clear_month")
def schedule_clear_month():
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))
    ms, me = bp_utils.month_range(year, month)
    with adb.db() as conn:
        with conn:
            conn.execute(
                "DELETE FROM schedule_assignment WHERE date BETWEEN ? AND ?",
                (ms.isoformat(), me.isoformat()),
            )
    if (request.form.get("from") or "").strip() == "index":
        return redirect(url_for("home.index", year=year, month=month))
    return redirect(url_for("schedule.schedule_view", year=year, month=month))


@bp.route("/schedule/reset_all_open", methods=["POST"], endpoint="schedule_reset_all_open")
def schedule_reset_all_open():
    with adb.db() as conn:
        with conn:
            conn.execute("DELETE FROM schedule_day")
    today = dt.date.today()
    return redirect(url_for("schedule.schedule_view", year=today.year, month=today.month))


@bp.route("/schedule/assign", methods=["POST"], endpoint="schedule_assign")
def schedule_assign():
    date = dt.date.fromisoformat(request.form.get("date"))
    tg_id = int(request.form.get("tg_id"))
    with adb.db() as conn:
        sched.set_assignment(date, tg_id, source="admin", conn=conn)
    return redirect(url_for("schedule.schedule_view", year=date.year, month=date.month))


@bp.route("/schedule/unassign", methods=["POST"], endpoint="schedule_unassign")
def schedule_unassign():
    date = dt.date.fromisoformat(request.form.get("date"))
    tg_id = int(request.form.get("tg_id"))
    with adb.db() as conn:
        sched.remove_assignment(date, tg_id, conn)
    return redirect(url_for("schedule.schedule_view", year=date.year, month=date.month))


__all__ = [
    "bp",
    "schedule_view",
    "schedule_toggle_cell",
    "schedule_clear_month",
    "schedule_reset_all_open",
    "schedule_assign",
    "schedule_unassign",
]
