from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Tuple
import importlib

from app import config as _config


def _current_db_path() -> str:
    """Return current DB path. Prefer app.bot.DB_PATH if available (for tests),
    otherwise fall back to app.config.DB_PATH.
    """
    try:
        botmod = importlib.import_module("app.bot")
        p = getattr(botmod, "DB_PATH", None)
        if p:
            return str(p)
    except Exception:
        pass
    return _config.DB_PATH


def db() -> sqlite3.Connection:
    """Create a new SQLite connection with concurrency-friendly pragmas."""
    path = _current_db_path()
    conn = sqlite3.connect(path, timeout=30.0, isolation_level="DEFERRED")
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn


def _execmany(conn: sqlite3.Connection, sql: str, rows: List[Tuple]):
    with conn:
        conn.executemany(sql, rows)


def init_db():
    Path("data").mkdir(exist_ok=True)
    conn = db()
    with conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS product(
            id INTEGER PRIMARY KEY,
            article TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            brand_country TEXT,
            local_name TEXT,
            photo_file_id TEXT,
            is_new INTEGER NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            archived_at TEXT,
            last_restock_at TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS location(
            code TEXT PRIMARY KEY,
            kind TEXT NOT NULL,          -- 'SKL', 'DOMIK', 'HALL'
            title TEXT NOT NULL
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS stock(
            product_id INTEGER NOT NULL,
            location_code TEXT NOT NULL,
            qty_pack REAL NOT NULL DEFAULT 0,
            -- Пользовательские поля для удобства в админке
            name TEXT,
            local_name TEXT,
            PRIMARY KEY (product_id, location_code),
            FOREIGN KEY (product_id) REFERENCES product(id) ON DELETE CASCADE,
            FOREIGN KEY (location_code) REFERENCES location(code) ON DELETE CASCADE
        );
        """)
        # FTS5 (если доступно)
        try:
            conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS product_fts USING fts5(
                article, name, local_name, content='product', content_rowid='id'
            );
            """)
            conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS product_ai AFTER INSERT ON product BEGIN
                INSERT INTO product_fts(rowid, article,name,local_name)
                VALUES (new.id, new.article, new.name, new.local_name);
            END;
            CREATE TRIGGER IF NOT EXISTS product_ad AFTER DELETE ON product BEGIN
                INSERT INTO product_fts(product_fts, rowid, article,name,local_name)
                VALUES('delete', old.id, old.article, old.name, old.local_name);
            END;
            CREATE TRIGGER IF NOT EXISTS product_au AFTER UPDATE ON product BEGIN
                INSERT INTO product_fts(product_fts, rowid, article,name,local_name)
                VALUES('delete', old.id, old.article, old.name, old.local_name);
                INSERT INTO product_fts(rowid, article,name,local_name)
                VALUES (new.id, new.article, new.name, new.local_name);
            END;
            """)
        except sqlite3.OperationalError:
            pass

        # Роли пользователей: admin / seller
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_role(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                username TEXT,
                display_name TEXT,
                role TEXT NOT NULL CHECK(role IN ('admin','seller')),
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(username, role)
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_role_tg_role
            ON user_role(tg_id, role)
            """
        )
        # Настройки уведомлений: по пользователю и типу
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_notify(
                user_id INTEGER NOT NULL,
                notif_type TEXT NOT NULL CHECK (notif_type IN ('zero','last','to_skl')),
                mode TEXT NOT NULL CHECK (mode IN ('off','daily','instant')) DEFAULT 'off',
                PRIMARY KEY (user_id, notif_type)
            )
            """
        )
        # Журнал событий (для отчётов конца дня)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT DEFAULT (datetime('now','localtime')),
                type TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                location_code TEXT,
                delta REAL
            )
            """
        )
        # ===== Scheduling tables =====
        # Days (open/closed)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_day(
                date TEXT PRIMARY KEY,        -- YYYY-MM-DD
                is_open INTEGER NOT NULL DEFAULT 1,
                notes TEXT
            )
            """
        )
        # Assignments: exactly two tg_id per open date (enforced at app layer)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_assignment(
                date TEXT NOT NULL,
                tg_id INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'auto',
                created_at TEXT DEFAULT (datetime('now','localtime')),
                PRIMARY KEY(date, tg_id)
            )
            """
        )
        # Swap/transfer requests on a single date (no day-to-day swap)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_transfer_request(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                from_tg_id INTEGER NOT NULL,
                to_tg_id INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending','accepted','declined','cancelled','expired')) DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now','localtime')),
                expires_at TEXT,
                UNIQUE(date, from_tg_id, to_tg_id)
            )
            """
        )
        # Anchor pair for generation: two consecutive open days with 2 workers each
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_anchor(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_date TEXT NOT NULL,   -- first open date
                UNIQUE(start_date)
            )
            """
        )
        # Registration requests (admin onboarding)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS registration_request(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                requested_role TEXT NOT NULL DEFAULT 'admin',
                status TEXT NOT NULL CHECK(status IN ('pending','approved','declined','cancelled')) DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now','localtime'))
            )
            """
        )
    # Сидируем локации, если пусто
    cur = conn.execute("SELECT COUNT(*) AS c FROM location").fetchone()["c"]
    if cur == 0:
        seeds = []
        for i in range(5):  # склад 0..4
            seeds.append((f"SKL-{i}", "SKL", f"Склад {i}"))
        for home in range(2, 10):  # домики 2.1..9.2
            for shelf in (1, 2):
                seeds.append((f"{home}.{shelf}", "DOMIK", f"Домик {home}.{shelf}"))
        seeds.append(("HALL", "HALL", "Зал (списание)"))
        _execmany(conn, "INSERT INTO location(code,kind,title) VALUES (?,?,?)", seeds)
    # Миграции: добавить колонку photo_path, если её нет (однократно)
    try:
        conn.execute("ALTER TABLE product ADD COLUMN photo_path TEXT")
    except sqlite3.OperationalError:
        # колонка есть или таблица ещё не создана — игнорируем
        pass
    # Миграция: добавить display_name в user_role
    try:
        conn.execute("ALTER TABLE user_role ADD COLUMN display_name TEXT")
    except sqlite3.OperationalError:
        pass
    # Миграции: архивные поля (archived, archived_at, last_restock_at)
    try:
        conn.execute("ALTER TABLE product ADD COLUMN archived INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE product ADD COLUMN archived_at TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE product ADD COLUMN last_restock_at TEXT")
    except sqlite3.OperationalError:
        pass
    # Миграция: столбцы name/local_name в stock (для внешней админки)
    try:
        conn.execute("ALTER TABLE stock ADD COLUMN name TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("ALTER TABLE stock ADD COLUMN local_name TEXT")
    except sqlite3.OperationalError:
        pass
    # Бэкофилл из product, если пусто
    try:
        with conn:
            conn.execute(
                """
                UPDATE stock SET name=(
                    SELECT p.name FROM product p WHERE p.id=stock.product_id
                )
                WHERE name IS NULL OR name=''
                """
            )
            conn.execute(
                """
                UPDATE stock SET local_name=(
                    SELECT p.local_name FROM product p WHERE p.id=stock.product_id
                )
                WHERE local_name IS NULL OR local_name=''
                """
            )
    except sqlite3.OperationalError:
        # На случай, если таблицы ещё не готовы — пропускаем
        pass
    conn.close()
