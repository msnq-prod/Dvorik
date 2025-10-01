from __future__ import annotations

from flask import Blueprint

blueprint = Blueprint("tables", __name__, url_prefix="/tables")


@blueprint.get("/")
def list_tables() -> str:
    return "Tables placeholder"
