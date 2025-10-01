from __future__ import annotations

import logging
import sqlite3
from typing import Iterable

from .conn import db

logger = logging.getLogger(__name__)

# Schema definition for Ticket 2.3.  The database is created from scratch
# using a fixed set of idempotent DDL statements.  Each script groups related
# tables and indexes so they can be executed independently.
_SCHEMA_SCRIPTS: Iterable[str] = (
    """
    CREATE TABLE IF NOT EXISTS manufacturer(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        country TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS supplier(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        contact TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS product(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article TEXT,
        barcode TEXT,
        name TEXT NOT NULL,
        local_name TEXT,
        description TEXT,
        unit TEXT,
        manufacturer_id INTEGER,
        price REAL,
        vat_rate REAL,
        is_new INTEGER NOT NULL DEFAULT 0,
        archived INTEGER NOT NULL DEFAULT 0,
        archived_at TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT,
        last_restock_at TEXT,
        photo_file_id TEXT,
        photo_path TEXT,
        FOREIGN KEY(manufacturer_id) REFERENCES manufacturer(id) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_product_article ON product(article);
    CREATE INDEX IF NOT EXISTS idx_product_name ON product(name);
    """,
    """
    CREATE TABLE IF NOT EXISTS location(
        code TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS stock(
        product_id INTEGER NOT NULL,
        location_code TEXT NOT NULL,
        qty_pack REAL NOT NULL DEFAULT 0,
        reserved_pack REAL NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now','localtime')),
        PRIMARY KEY(product_id, location_code),
        FOREIGN KEY(product_id) REFERENCES product(id) ON DELETE CASCADE,
        FOREIGN KEY(location_code) REFERENCES location(code) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_stock_location ON stock(location_code);
    """,
    """
    CREATE TABLE IF NOT EXISTS user_role(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER,
        username TEXT,
        display_name TEXT,
        role TEXT NOT NULL CHECK(role IN ('admin','seller')),
        created_at TEXT DEFAULT (datetime('now','localtime')),
        UNIQUE(username, role),
        UNIQUE(tg_id, role)
    );

    CREATE TABLE IF NOT EXISTS user_notify(
        user_id INTEGER NOT NULL,
        notif_type TEXT NOT NULL CHECK (notif_type IN ('zero','last','to_skl','new_type')),
        mode TEXT NOT NULL CHECK (mode IN ('off','daily','instant')) DEFAULT 'off',
        updated_at TEXT DEFAULT (datetime('now','localtime')),
        PRIMARY KEY(user_id, notif_type)
    );

    CREATE TABLE IF NOT EXISTS event_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT DEFAULT (datetime('now','localtime')),
        event_type TEXT NOT NULL,
        product_id INTEGER,
        location_code TEXT,
        user_id INTEGER,
        delta REAL,
        payload_json TEXT,
        FOREIGN KEY(product_id) REFERENCES product(id) ON DELETE SET NULL,
        FOREIGN KEY(location_code) REFERENCES location(code) ON DELETE SET NULL
    );

    CREATE INDEX IF NOT EXISTS idx_event_log_ts ON event_log(ts);
    CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
    """,
    """
    CREATE TABLE IF NOT EXISTS import_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_name TEXT NOT NULL,
        stored_path TEXT NOT NULL,
        import_type TEXT NOT NULL CHECK(import_type IN ('csv','excel')),
        source_hash TEXT NOT NULL UNIQUE,
        normalized_csv TEXT,
        normalized_hash TEXT UNIQUE,
        supplier TEXT,
        invoice TEXT,
        items_count INTEGER NOT NULL DEFAULT 0,
        items_json TEXT,
        reverted_at TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE INDEX IF NOT EXISTS idx_import_log_created ON import_log(created_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS schedule_day(
        date TEXT PRIMARY KEY,
        is_open INTEGER NOT NULL DEFAULT 1,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS schedule_assignment(
        date TEXT NOT NULL,
        tg_id INTEGER NOT NULL,
        source TEXT NOT NULL DEFAULT 'auto',
        created_at TEXT DEFAULT (datetime('now','localtime')),
        PRIMARY KEY(date, tg_id),
        FOREIGN KEY(date) REFERENCES schedule_day(date) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS schedule_transfer_request(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        from_tg_id INTEGER NOT NULL,
        to_tg_id INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending','accepted','declined','cancelled','expired')) DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now','localtime')),
        expires_at TEXT,
        UNIQUE(date, from_tg_id, to_tg_id),
        FOREIGN KEY(date) REFERENCES schedule_day(date) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS schedule_anchor(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_date TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS registration_request(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER NOT NULL,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        requested_role TEXT NOT NULL DEFAULT 'admin',
        status TEXT NOT NULL CHECK(status IN ('pending','approved','declined','cancelled')) DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS supplier_sku(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER NOT NULL,
        supplier_id INTEGER NOT NULL,
        code TEXT NOT NULL,
        barcode TEXT,
        pack_qty REAL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        updated_at TEXT,
        UNIQUE(supplier_id, code),
        FOREIGN KEY(product_id) REFERENCES product(id) ON DELETE CASCADE,
        FOREIGN KEY(supplier_id) REFERENCES supplier(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_supplier_sku_product ON supplier_sku(product_id);
    CREATE INDEX IF NOT EXISTS idx_supplier_sku_supplier_active ON supplier_sku(supplier_id, active);

    CREATE TABLE IF NOT EXISTS display_name_exception(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phrase TEXT NOT NULL UNIQUE,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS product_fts USING fts5(
        article,
        name,
        local_name,
        content='product',
        content_rowid='id'
    );

    CREATE TRIGGER IF NOT EXISTS product_ai AFTER INSERT ON product BEGIN
        INSERT INTO product_fts(rowid, article, name, local_name)
        VALUES (new.id, new.article, new.name, new.local_name);
    END;

    CREATE TRIGGER IF NOT EXISTS product_ad AFTER DELETE ON product BEGIN
        INSERT INTO product_fts(product_fts, rowid, article, name, local_name)
        VALUES ('delete', old.id, old.article, old.name, old.local_name);
    END;

    CREATE TRIGGER IF NOT EXISTS product_au AFTER UPDATE ON product BEGIN
        INSERT INTO product_fts(product_fts, rowid, article, name, local_name)
        VALUES ('delete', old.id, old.article, old.name, old.local_name);
        INSERT INTO product_fts(rowid, article, name, local_name)
        VALUES (new.id, new.article, new.name, new.local_name);
    END;
    """,
    """
    CREATE TABLE IF NOT EXISTS query_registry(
        key TEXT PRIMARY KEY,
        sql TEXT NOT NULL,
        description TEXT,
        updated_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE TABLE IF NOT EXISTS ui_widget(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT NOT NULL,
        name TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        entrypoint TEXT,
        config_schema TEXT,
        UNIQUE(module, name)
    );

    CREATE TABLE IF NOT EXISTS ui_widget_instance(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        widget_id INTEGER NOT NULL,
        zone TEXT NOT NULL,
        position INTEGER NOT NULL DEFAULT 0,
        config_json TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        UNIQUE(zone, position),
        FOREIGN KEY(widget_id) REFERENCES ui_widget(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS ui_menu(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slug TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        url TEXT,
        icon TEXT,
        parent_id INTEGER,
        position INTEGER NOT NULL DEFAULT 0,
        target TEXT,
        visible INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(parent_id) REFERENCES ui_menu(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS scheduled_job(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        schedule_type TEXT NOT NULL CHECK(schedule_type IN ('daily','cron')),
        schedule_expression TEXT,
        next_run_at TEXT,
        last_run_at TEXT,
        task_module TEXT NOT NULL,
        task_name TEXT NOT NULL,
        config_json TEXT,
        enabled INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT DEFAULT (datetime('now','localtime')),
        actor_id INTEGER,
        actor_username TEXT,
        action TEXT NOT NULL,
        entity TEXT,
        entity_id TEXT,
        payload_json TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);
    """,
)


def _run_script(conn: sqlite3.Connection, script: str) -> None:
    """Execute a migration script handling common SQLite errors."""

    if not script.strip():
        return

    try:
        conn.executescript(script)
    except sqlite3.OperationalError as exc:  # pragma: no cover - defensive
        logger.warning("Skipping migration script due to sqlite error: %s", exc)


def init_db(connection: sqlite3.Connection | None = None) -> None:
    """Initialise the SQLite schema ensuring idempotency."""

    owns_connection = connection is None
    conn = connection if connection is not None else db()

    try:
        with conn:
            for script in _SCHEMA_SCRIPTS:
                _run_script(conn, script)
    finally:
        if owns_connection:
            conn.close()


__all__ = ["init_db"]
