from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Any, Dict, List

from flask import Blueprint, render_template, request

from app import db as adb
from admin_ui.blueprints import utils as bp_utils


bp = Blueprint("labels", __name__)


@bp.route("/labels", endpoint="labels_page")
def labels_page():
    return render_template("labels.html")


@bp.get("/labels/print", endpoint="labels_print")
def labels_print():
    raw_ids = (request.args.get("ids") or "").strip()
    raw_titles = (request.args.get("titles") or "").strip()
    ids: List[int] = []
    if raw_ids:
        for chunk in raw_ids.split(","):
            part = chunk.strip()
            if not part:
                continue
            try:
                pid = int(part)
            except Exception:
                continue
            ids.append(pid)
    title_overrides: Dict[int, str] = {}
    if raw_titles:
        try:
            padding = -len(raw_titles) % 4
            payload = raw_titles + ("=" * padding)
            decoded = base64.b64decode(payload).decode("utf-8")
            parsed = json.loads(decoded)
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    try:
                        pid = int(key)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(value, str):
                        clean = value.strip()
                        if clean:
                            title_overrides[pid] = clean
        except Exception:
            title_overrides = {}

    items: List[Dict[str, Any]] = []
    if ids:
        with adb.db() as conn:
            for pid in ids:
                detail = bp_utils.product_detail(conn, pid)
                if not detail:
                    continue
                detail["id"] = pid
                override = title_overrides.get(pid)
                if override:
                    detail["print_title"] = override
                items.append(detail)

    today = dt.date.today()
    first_day = today.replace(day=1)
    prev_month_last = first_day - dt.timedelta(days=1)
    manufacture_date = prev_month_last.strftime("%d.%m.%Y")
    open_date = today.strftime("%d.%m.%Y")
    storage_text = "в сухом, прохладном месте"
    expiry_months = 12

    return render_template(
        "labels_print.html",
        items=items,
        manufacture_date=manufacture_date,
        open_date=open_date,
        storage_text=storage_text,
        expiry_months=expiry_months,
    )


__all__ = ["bp", "labels_page", "labels_print"]
