import sqlite3
from pathlib import Path
from typing import Iterable

import pytest

from dvorik.admin.blueprints import menus
from dvorik.db.migrations import init_db


@pytest.fixture
def menu_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "menu.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_db(connection=conn)
    conn.close()

    def _db() -> sqlite3.Connection:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        return connection

    monkeypatch.setattr(menus, "db", _db)
    return db_path


def _seed_menu(db_path: Path, entries: Iterable[dict[str, object | None]]) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            conn.execute("DELETE FROM ui_menu")
            for index, entry in enumerate(entries, start=1):
                slug = str(entry.get("slug"))
                title = str(entry.get("title") or slug.title())
                url = str(entry.get("url") or f"/{slug}")
                position = int(entry.get("position") or index)
                visible = 1 if entry.get("visible", True) else 0
                required_role = entry.get("required_role")
                if isinstance(required_role, str):
                    required_role = required_role.strip() or None
                conn.execute(
                    """
                    INSERT INTO ui_menu(
                        slug, title, url, icon, parent_id, position, target, visible, required_role
                    )
                    VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?)
                    """,
                    (
                        slug,
                        title,
                        url,
                        None,
                        position,
                        visible,
                        required_role,
                    ),
                )
    finally:
        conn.close()


def _extract_slugs(entries: Iterable[menus.MenuEntry]) -> set[str]:
    return {entry.slug for entry in entries}


def test_menu_entries_filtered_for_superadmin(menu_database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_menu(
        menu_database,
        [
            {"slug": "common", "required_role": None},
            {"slug": "only-super", "required_role": "superadmin"},
            {"slug": "seller", "required_role": "seller"},
        ],
    )

    monkeypatch.setattr(menus, "_resolve_user_role", lambda: "superadmin")

    entries, is_dynamic = menus._load_menu_entries()

    assert is_dynamic is True
    slugs = _extract_slugs(entries)
    assert slugs == {"common", "only-super"}


def test_menu_entries_hidden_from_other_roles(menu_database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_menu(
        menu_database,
        [
            {"slug": "common", "required_role": None},
            {"slug": "only-super", "required_role": "superadmin"},
            {"slug": "seller", "required_role": "seller"},
        ],
    )

    monkeypatch.setattr(menus, "_resolve_user_role", lambda: "seller")

    entries, is_dynamic = menus._load_menu_entries()

    assert is_dynamic is True
    slugs = _extract_slugs(entries)
    assert slugs == {"common", "seller"}


def test_menu_entries_hidden_when_no_role(menu_database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_menu(
        menu_database,
        [
            {"slug": "common", "required_role": None},
            {"slug": "only-super", "required_role": "superadmin"},
        ],
    )

    monkeypatch.setattr(menus, "_resolve_user_role", lambda: None)

    entries, is_dynamic = menus._load_menu_entries()

    assert is_dynamic is True
    slugs = _extract_slugs(entries)
    assert slugs == {"common"}
