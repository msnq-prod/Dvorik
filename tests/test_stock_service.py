import asyncio
import json
import sqlite3

import pytest

from dvorik.core import events
from dvorik.services import stock


@pytest.fixture(autouse=True)
def clear_event_registry():
    events._clear_subscribers()
    yield
    events._clear_subscribers()


@pytest.fixture()
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE stock (
            product_id INTEGER,
            location_code TEXT,
            qty_pack REAL,
            updated_at TEXT,
            PRIMARY KEY (product_id, location_code)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            product_id INTEGER,
            location_code TEXT,
            user_id INTEGER,
            delta REAL,
            payload_json TEXT
        )
        """
    )
    yield connection
    connection.close()


def test_set_location_qty_updates_stock_and_emits_event(conn):
    async def main():
        captured: list[dict[str, object]] = []

        async def handler(**payload):
            captured.append(payload)

        events.subscribe("stock.adjusted", handler)

        change = await stock.set_location_qty(
            conn,
            product_id=1,
            location_code="A",
            qty_pack=5,
            user_id=99,
        )

        assert change.delta == pytest.approx(5)

        row = conn.execute(
            "SELECT qty_pack FROM stock WHERE product_id = ? AND location_code = ?",
            (1, "A"),
        ).fetchone()
        assert row["qty_pack"] == 5

        log_row = conn.execute("SELECT event_type, payload_json FROM event_log").fetchone()
        assert log_row["event_type"] == "stock.set"
        assert json.loads(log_row["payload_json"]) == {
            "product_id": 1,
            "location_code": "A",
            "qty_before": 0.0,
            "qty_after": 5.0,
            "delta": 5.0,
            "user_id": 99,
        }

        assert captured == [
            {
                "product_id": 1,
                "location_code": "A",
                "qty_before": 0.0,
                "qty_after": 5.0,
                "delta": 5.0,
                "user_id": 99,
            }
        ]

    asyncio.run(main())


def test_set_location_qty_no_change_skips_writes(conn):
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "A", 3.0),
    )

    async def main():
        change = await stock.set_location_qty(conn, product_id=1, location_code="A", qty_pack=3.0)
        assert change.delta == pytest.approx(0)

    asyncio.run(main())

    rows = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
    assert rows == 0


def test_move_specific_transfers_between_locations(conn):
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "SRC", 10.0),
    )
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "DST", 2.0),
    )

    async def main():
        moved_events: list[str] = []
        adjustments: list[tuple[str, float]] = []

        async def moved(**payload):
            moved_events.append(payload["from_location"])

        async def adjusted(**payload):
            adjustments.append((payload["location_code"], payload["delta"]))

        events.subscribe("stock.moved", moved)
        events.subscribe("stock.adjusted", adjusted)

        result = await stock.move_specific(conn, 1, "SRC", "DST", 4)

        assert result["from"].qty_after == pytest.approx(6)
        assert result["to"].qty_after == pytest.approx(6)

        src_qty = conn.execute(
            "SELECT qty_pack FROM stock WHERE product_id = 1 AND location_code = 'SRC'"
        ).fetchone()["qty_pack"]
        dst_qty = conn.execute(
            "SELECT qty_pack FROM stock WHERE product_id = 1 AND location_code = 'DST'"
        ).fetchone()["qty_pack"]

        assert src_qty == pytest.approx(6)
        assert dst_qty == pytest.approx(6)

        assert moved_events == ["SRC"]
        assert ("SRC", -4.0) in adjustments
        assert ("DST", 4.0) in adjustments

    asyncio.run(main())


def test_adjust_with_hub_balances_quantities(conn):
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "LOC", 1.0),
    )
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "SKL-0", 20.0),
    )

    async def main():
        result = await stock.adjust_with_hub(conn, 1, "LOC", 6, hub_code="SKL-0")

        assert result["from"].location_code == "SKL-0"
        assert result["to"].location_code == "LOC"

    asyncio.run(main())

    hub_qty = conn.execute(
        "SELECT qty_pack FROM stock WHERE product_id = 1 AND location_code = 'SKL-0'"
    ).fetchone()["qty_pack"]
    loc_qty = conn.execute(
        "SELECT qty_pack FROM stock WHERE product_id = 1 AND location_code = 'LOC'"
    ).fetchone()["qty_pack"]

    assert hub_qty == pytest.approx(15)
    assert loc_qty == pytest.approx(6)


def test_adjust_with_hub_noop_returns_single_change(conn):
    conn.execute(
        "INSERT INTO stock (product_id, location_code, qty_pack, updated_at) VALUES (?, ?, ?, 'now')",
        (1, "LOC", 3.0),
    )

    async def main():
        change = await stock.adjust_with_hub(conn, 1, "LOC", 3.0, hub_code="SKL-0")
        assert isinstance(change, stock.StockChange)
        assert change.delta == pytest.approx(0)

    asyncio.run(main())
