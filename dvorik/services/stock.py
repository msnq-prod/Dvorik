from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict

from dvorik.core import events

_STOCK_ADJUSTED_EVENT = "stock.adjusted"
_STOCK_MOVED_EVENT = "stock.moved"


@dataclass(slots=True)
class StockChange:
    """Represents a stock adjustment performed by the service."""

    product_id: int
    location_code: str
    qty_before: float
    qty_after: float
    user_id: int | None = None

    @property
    def delta(self) -> float:
        return self.qty_after - self.qty_before


async def set_location_qty(
    conn: sqlite3.Connection,
    product_id: int,
    location_code: str,
    qty_pack: float,
    *,
    user_id: int | None = None,
) -> StockChange:
    """Set ``qty_pack`` for the product at the location.

    The operation is idempotent.  A ``ValueError`` is raised if the provided
    quantity is negative.  After updating the database the function publishes a
    ``stock.adjusted`` event describing the delta.
    """

    if qty_pack < 0:
        raise ValueError("Quantity must be non-negative")

    qty_before = _get_current_qty(conn, product_id, location_code)
    change = StockChange(
        product_id=product_id,
        location_code=location_code,
        qty_before=qty_before,
        qty_after=float(qty_pack),
        user_id=user_id,
    )

    if abs(change.delta) < 1e-9:
        # No change required — skip writes but still return the delta object.
        return change

    payload = {
        "product_id": product_id,
        "location_code": location_code,
        "qty_before": change.qty_before,
        "qty_after": change.qty_after,
        "delta": change.delta,
        "user_id": user_id,
    }

    with conn:
        _upsert_stock(conn, product_id, location_code, change.qty_after)
        _log_event(conn, "stock.set", payload)

    await events.publish(_STOCK_ADJUSTED_EVENT, **payload)
    return change


async def move_specific(
    conn: sqlite3.Connection,
    product_id: int,
    from_location: str,
    to_location: str,
    qty_pack: float,
    *,
    user_id: int | None = None,
) -> Dict[str, StockChange]:
    """Move ``qty_pack`` units of ``product_id`` between locations.

    Returns a mapping containing the changes for both the origin and the
    destination.  A ``ValueError`` is raised if the quantity is not positive or
    if the source location does not have enough stock.
    """

    if qty_pack <= 0:
        raise ValueError("Quantity to move must be positive")
    if from_location == to_location:
        raise ValueError("Source and destination locations must differ")

    available = _get_current_qty(conn, product_id, from_location)
    if available < qty_pack - 1e-9:
        raise ValueError("Insufficient stock at source location")

    from_before = available
    to_before = _get_current_qty(conn, product_id, to_location)
    from_after = from_before - qty_pack
    to_after = to_before + qty_pack

    changes = {
        "from": StockChange(
            product_id=product_id,
            location_code=from_location,
            qty_before=from_before,
            qty_after=from_after,
            user_id=user_id,
        ),
        "to": StockChange(
            product_id=product_id,
            location_code=to_location,
            qty_before=to_before,
            qty_after=to_after,
            user_id=user_id,
        ),
    }

    payload = {
        "product_id": product_id,
        "from_location": from_location,
        "to_location": to_location,
        "qty": float(qty_pack),
        "from_before": from_before,
        "from_after": from_after,
        "to_before": to_before,
        "to_after": to_after,
        "user_id": user_id,
    }

    with conn:
        _upsert_stock(conn, product_id, from_location, from_after)
        _upsert_stock(conn, product_id, to_location, to_after)
        _log_event(conn, "stock.move", payload)

    await asyncio.gather(
        events.publish(
            _STOCK_ADJUSTED_EVENT,
            product_id=product_id,
            location_code=from_location,
            qty_before=from_before,
            qty_after=from_after,
            delta=changes["from"].delta,
            user_id=user_id,
        ),
        events.publish(
            _STOCK_ADJUSTED_EVENT,
            product_id=product_id,
            location_code=to_location,
            qty_before=to_before,
            qty_after=to_after,
            delta=changes["to"].delta,
            user_id=user_id,
        ),
        events.publish(_STOCK_MOVED_EVENT, **payload),
    )

    return changes


async def adjust_with_hub(
    conn: sqlite3.Connection,
    product_id: int,
    location_code: str,
    qty_pack: float,
    *,
    hub_code: str = "SKL-0",
    user_id: int | None = None,
) -> Dict[str, StockChange] | StockChange:
    """Adjust a location quantity by balancing against the hub location.

    If the requested quantity is higher than the current quantity, items are
    moved from ``hub_code`` to the target location.  Otherwise the difference is
    moved back to the hub.  When the target location already has the desired
    quantity the hub remains untouched and the return value is a single
    :class:`StockChange` describing the no-op.
    """

    if hub_code == location_code:
        raise ValueError("Hub code must differ from target location")

    current_qty = _get_current_qty(conn, product_id, location_code)
    delta = float(qty_pack) - current_qty

    if abs(delta) < 1e-9:
        return StockChange(
            product_id=product_id,
            location_code=location_code,
            qty_before=current_qty,
            qty_after=current_qty,
            user_id=user_id,
        )

    if delta > 0:
        return await move_specific(
            conn,
            product_id,
            hub_code,
            location_code,
            delta,
            user_id=user_id,
        )

    return await move_specific(
        conn,
        product_id,
        location_code,
        hub_code,
        abs(delta),
        user_id=user_id,
    )


def _get_current_qty(conn: sqlite3.Connection, product_id: int, location_code: str) -> float:
    cursor = conn.execute(
        """
        SELECT qty_pack
        FROM stock
        WHERE product_id = :product_id AND location_code = :location_code
        """,
        {"product_id": product_id, "location_code": location_code},
    )
    row = cursor.fetchone()
    if not row:
        return 0.0
    value = row[0] if isinstance(row, tuple) else row["qty_pack"]
    return float(value or 0)


def _upsert_stock(
    conn: sqlite3.Connection,
    product_id: int,
    location_code: str,
    qty_pack: float,
) -> None:
    conn.execute(
        """
        INSERT INTO stock (product_id, location_code, qty_pack, updated_at)
        VALUES (:product_id, :location_code, :qty_pack, datetime('now','localtime'))
        ON CONFLICT(product_id, location_code)
        DO UPDATE SET
            qty_pack = excluded.qty_pack,
            updated_at = datetime('now','localtime')
        """,
        {
            "product_id": product_id,
            "location_code": location_code,
            "qty_pack": float(qty_pack),
        },
    )


def _log_event(conn: sqlite3.Connection, event_type: str, payload: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO event_log (event_type, product_id, location_code, user_id, delta, payload_json)
        VALUES (:event_type, :product_id, :location_code, :user_id, :delta, :payload_json)
        """,
        {
            "event_type": event_type,
            "product_id": payload.get("product_id"),
            "location_code": payload.get("location_code")
            or payload.get("from_location")
            or payload.get("to_location"),
            "user_id": payload.get("user_id"),
            "delta": payload.get("delta") or payload.get("qty"),
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
        },
    )


__all__ = [
    "StockChange",
    "adjust_with_hub",
    "move_specific",
    "set_location_qty",
]
