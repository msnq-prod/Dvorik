from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Sequence

from flask import Flask, url_for

from app import constants as const
from app import db as adb
from admin_ui.blueprints import utils as bp_utils


def register(app: Flask) -> None:
    @app.context_processor
    def inject_tables() -> Dict[str, Any]:
        with adb.db() as conn:
            all_tables = [
                t for t in bp_utils.list_tables(conn) if t[0] not in const.HIDDEN_TABLES
            ]
            primary = [t for t in all_tables if t[0] in const.PRIMARY_TABLES]
            technical = [t for t in all_tables if t[0] not in const.PRIMARY_TABLES]
        return {
            "primary_tables": primary,
            "tech_tables": technical,
            "table_title": bp_utils.table_title,
            "col_title": bp_utils.col_title,
            "hub_code": const.HUB_LOCATION_CODE,
            "enum_translations": bp_utils.ENUM_TRANSLATIONS,
        }

    @app.template_global()
    def edit_url(table: str, row: Any, pkcols: Sequence[bp_utils.Column]) -> str:
        params = {pk.name: row[pk.name] for pk in pkcols}
        return url_for("tables.table_edit", table=table, **params)

    @app.template_global()
    def month_ru(d: dt.date) -> str:
        try:
            return const.RU_MONTHS.get(int(d.month), d.strftime("%B"))
        except Exception:
            return d.strftime("%B")


__all__ = ["register"]
