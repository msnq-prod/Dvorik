from __future__ import annotations

from flask import Blueprint

blueprint = Blueprint("home", __name__)


@blueprint.get("/")
def index() -> str:
    return "Home placeholder"
