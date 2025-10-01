from __future__ import annotations

from flask import Blueprint

blueprint = Blueprint("superadmin", __name__, url_prefix="/superadmin")


@blueprint.get("/")
def dashboard() -> str:
    return "Superadmin placeholder"
