from __future__ import annotations

import asyncio
import sqlite3
import sys
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dvorik.core import events
from dvorik.db.migrations import init_db
from dvorik.domain.models import ImportLogEntry, Location, LowStockRecord, Product
from dvorik.services import notify
from dvorik.services import stock as stock_service
from dvorik.services.imports import log_completed_import


@pytest.fixture(autouse=True)
def clear_event_bus():
    events._clear_subscribers()
    yield
    events._clear_subscribers()


def _make_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def test_stock_adjustment_triggers_threshold_notification():
    conn = _make_connection()
    with conn:
        conn.execute("INSERT INTO product(id, name) VALUES (1, 'Widget')")
        conn.execute(
            "INSERT INTO location(code, kind, title) VALUES ('LOC-1', 'SHOP', 'Main Shop')"
        )

    product = Product(id=1, name="Widget")
    location = Location(code="LOC-1", kind="SHOP", title="Main Shop")
    record = LowStockRecord(product=product, location=location, qty_pack=0.5, threshold=1.0)

    class StubStockRepo:
        def __init__(self) -> None:
            self.requests: list[tuple[float, int]] = []

        def low_stock(self, *, threshold: float | None = None, limit: int = 20):
            self.requests.append((float(threshold or 0), limit))
            return (record,)

    notifications: list[dict[str, object]] = []

    async def callback(payload):
        notifications.append(dict(payload))

    repo = StubStockRepo()
    unsubscribe = notify.notify_instant_thresholds(repo, callback, threshold=1.0, limit=5)

    asyncio.run(stock_service.set_location_qty(conn, 1, "LOC-1", 0.5))

    try:
        assert notifications, "Expected notification to be emitted"
        message = notifications[0]
        assert message["type"] == "threshold"
        assert message["product_id"] == 1
        assert message["location_code"] == "LOC-1"
        assert pytest.approx(message["qty_after"], rel=1e-9) == 0.5
        assert message["record"] == record
        assert repo.requests == [(1.0, 5)]
    finally:
        unsubscribe()


def test_log_completed_import_publishes_event():
    entry = ImportLogEntry(
        original_name="latest.xlsx",
        stored_path="/tmp/latest.xlsx",
        import_type="excel",
        source_hash="abc123",
        items_count=7,
    )
    stored_entry = replace(
        entry,
        id=42,
        normalized_hash="def456",
        supplier="ACME",
        invoice="INV-7",
    )

    class StubImportRepo:
        def __init__(self) -> None:
            self.entries: list[ImportLogEntry] = []

        def add(self, log_entry: ImportLogEntry) -> ImportLogEntry:
            self.entries.append(log_entry)
            return stored_entry

    received: list[dict[str, object]] = []

    async def handler(**payload):
        received.append(payload)

    events.subscribe("import.completed", handler)

    repo = StubImportRepo()
    result = asyncio.run(log_completed_import(repo, entry, metadata={"extra": "value"}))

    assert result is stored_entry
    assert repo.entries == [entry]
    assert received, "Import completion event was not published"
    payload = received[0]
    assert payload["entry"] is stored_entry
    assert payload["import_id"] == 42
    assert payload["original_name"] == "latest.xlsx"
    assert payload["items_count"] == 7
    assert payload["supplier"] == "ACME"
    assert payload["invoice"] == "INV-7"
    assert payload["normalized_hash"] == "def456"
    assert payload["extra"] == "value"

