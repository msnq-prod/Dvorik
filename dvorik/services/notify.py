from __future__ import annotations

import inspect
import logging
from typing import Awaitable, Callable, Mapping, MutableMapping, Sequence

from dvorik.core import events
from dvorik.domain.models import LowStockRecord
from dvorik.domain.ports import StockRepo

logger = logging.getLogger(__name__)

_NotifyCallback = Callable[[Mapping[str, object]], Awaitable[None] | None]

_STOCK_ADJUSTED_EVENT = "stock.adjusted"
_STOCK_MOVED_EVENT = "stock.moved"
_DAILY_TICK_EVENT = "scheduler.daily"


def _execute_callback(callback: _NotifyCallback, payload: Mapping[str, object]) -> Awaitable[None] | None:
    try:
        result = callback(payload)
        if inspect.isawaitable(result):
            return result  # type: ignore[return-value]
        return None
    except Exception:  # pragma: no cover - notification failures must be logged
        logger.exception("Notification callback failed", extra={"payload": dict(payload)})
        return None


def notify_instant_thresholds(
    stock_repo: StockRepo,
    callback: _NotifyCallback,
    *,
    threshold: float,
    limit: int = 50,
) -> Callable[[], None]:
    """Subscribe to stock adjustments and notify when below ``threshold``.

    Returns a callable that unsubscribes the listener.
    """

    async def _handler(**payload: object) -> None:
        qty_after = float(payload.get("qty_after", 0))
        if qty_after > threshold:
            return

        product_id = payload.get("product_id")
        location_code = payload.get("location_code")
        if not isinstance(product_id, int) or not isinstance(location_code, str):
            return

        low_stock_records = stock_repo.low_stock(threshold=threshold, limit=limit)
        record = next(
            (
                item
                for item in low_stock_records
                if item.product.id == product_id and item.location.code == location_code
            ),
            None,
        )
        if record is None:
            return

        notification_payload: MutableMapping[str, object] = {
            "type": "threshold",
            "product_id": product_id,
            "location_code": location_code,
            "qty_after": qty_after,
            "threshold": threshold,
            "record": record,
        }
        awaitable = _execute_callback(callback, notification_payload)
        if awaitable is not None:
            await awaitable

    events.subscribe(_STOCK_ADJUSTED_EVENT, _handler)

    def _unsubscribe() -> None:
        events.unsubscribe(_STOCK_ADJUSTED_EVENT, _handler)

    return _unsubscribe


def notify_instant_to_skl(
    callback: _NotifyCallback,
    *,
    hub_code: str = "SKL-0",
) -> Callable[[], None]:
    """Subscribe to stock move events that target the hub location."""

    async def _handler(**payload: object) -> None:
        if payload.get("to_location") != hub_code:
            return

        notification_payload: MutableMapping[str, object] = {
            "type": "to_skl",
            "product_id": payload.get("product_id"),
            "from_location": payload.get("from_location"),
            "to_location": payload.get("to_location"),
            "qty": payload.get("qty"),
        }
        awaitable = _execute_callback(callback, notification_payload)
        if awaitable is not None:
            await awaitable

    events.subscribe(_STOCK_MOVED_EVENT, _handler)

    def _unsubscribe() -> None:
        events.unsubscribe(_STOCK_MOVED_EVENT, _handler)

    return _unsubscribe


def send_daily_digests(
    stock_repo: StockRepo,
    callback: _NotifyCallback,
    *,
    threshold: float,
    limit: int = 200,
) -> Callable[[], None]:
    """Subscribe to the scheduler tick and emit daily stock digests."""

    async def _handler(**_: object) -> None:
        low_stock_records: Sequence[LowStockRecord] = stock_repo.low_stock(
            threshold=threshold, limit=limit
        )
        if not low_stock_records:
            return

        payload: Mapping[str, object] = {
            "type": "daily_digest",
            "threshold": threshold,
            "records": tuple(low_stock_records),
        }
        awaitable = _execute_callback(callback, payload)
        if awaitable is not None:
            await awaitable

    events.subscribe(_DAILY_TICK_EVENT, _handler)

    def _unsubscribe() -> None:
        events.unsubscribe(_DAILY_TICK_EVENT, _handler)

    return _unsubscribe


__all__ = [
    "notify_instant_thresholds",
    "notify_instant_to_skl",
    "send_daily_digests",
]
