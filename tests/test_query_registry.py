import sqlite3

import pytest

from dvorik.db.query_registry import get_query, set_query


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE query_registry (
            key TEXT PRIMARY KEY,
            sql TEXT,
            description TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    yield connection
    connection.close()


def test_get_query_returns_default_when_missing(conn):
    default_sql = "SELECT 1"
    assert get_query(conn, "missing", default_sql) == default_sql


def test_set_query_inserts_and_updates(conn):
    set_query(conn, "stock.low", "SELECT * FROM stock", "Low stock listing")

    assert get_query(conn, "stock.low", "SELECT 1") == "SELECT * FROM stock"

    set_query(conn, "stock.low", "SELECT * FROM stock WHERE qty_pack < 5")

    assert get_query(conn, "stock.low", "SELECT 1") == "SELECT * FROM stock WHERE qty_pack < 5"

    row = conn.execute("SELECT description FROM query_registry WHERE key = ?", ("stock.low",)).fetchone()
    assert row["description"] == "Low stock listing"
