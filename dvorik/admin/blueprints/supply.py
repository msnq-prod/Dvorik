from __future__ import annotations

from flask import Blueprint

blueprint = Blueprint("supply", __name__, url_prefix="/supply")


@blueprint.get("/")
def supply_home() -> str:
    return "Supply placeholder"
