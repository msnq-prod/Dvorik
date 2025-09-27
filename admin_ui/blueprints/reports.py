from __future__ import annotations

from typing import Any, Dict, List

from flask import Blueprint, render_template, request

from app import db as adb
from app.services import reports as reports_svc


bp = Blueprint("reports", __name__)


@bp.route("/reports", endpoint="reports_page")
def reports_page():
    allowed_reports = {"low", "zero", "mid", "all", "arch"}
    report = request.args.get("report") or "low"
    if report not in allowed_reports:
        report = "low"

    report_rows: List[Dict[str, Any]] | None = None
    report_kind = report

    with adb.db() as conn:
        if report == "low":
            report_rows = reports_svc.low_stock(conn, limit=300)
        elif report == "zero":
            report_rows = reports_svc.zero_stock(conn, limit=1000)
        elif report == "mid":
            report_rows = reports_svc.mid_stock(conn, limit=1000)
        elif report == "all":
            report_rows = reports_svc.all_stock(conn, limit=2000)
        elif report == "arch":
            report_rows = reports_svc.archived_stock(conn, limit=2000)

    return render_template(
        "reports.html", report_kind=report_kind, report_rows=report_rows
    )


__all__ = ["bp", "reports_page"]
