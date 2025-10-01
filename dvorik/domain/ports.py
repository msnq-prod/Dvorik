"""Interfaces describing repository contracts for the domain."""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from .models import (
    ImportLogEntry,
    LowStockRecord,
    Product,
    ProductDetail,
    ScheduleAssignment,
    ScheduleDay,
    ScheduleTransferRequest,
    StockItem,
    StockSnapshot,
)


@runtime_checkable
class ProductRepo(Protocol):
    """Read-only operations for product catalogue data."""

    def get(self, product_id: int) -> Product | None:
        """Return a single product by identifier."""

    def list(self, *, include_archived: bool = False) -> Sequence[Product]:
        """Return all products filtered by archive state."""

    def search_fts(self, query: str, *, limit: int = 20) -> Sequence[Product]:
        """Full-text search over product attributes."""

    def product_detail(self, product_id: int) -> ProductDetail | None:
        """Return a composite view combining product and related data."""


@runtime_checkable
class StockRepo(Protocol):
    """Access stock balances and inventory insights."""

    def get_item(self, product_id: int, location_code: str) -> StockItem | None:
        """Return stock information for a product at a location."""

    def stock_by_location(self, location_code: str | None = None) -> Sequence[StockSnapshot]:
        """Return stock grouped by location, optionally filtered by code."""

    def low_stock(self, *, threshold: float | None = None, limit: int = 20) -> Sequence[LowStockRecord]:
        """Return stock entries considered below the provided threshold."""


@runtime_checkable
class ScheduleRepo(Protocol):
    """Schedule read model used by admin and bot services."""

    def day(self, date: str) -> ScheduleDay | None:
        """Return metadata for a single day."""

    def assignments(self, start_date: str, end_date: str | None = None) -> Sequence[ScheduleAssignment]:
        """Return assignments in the inclusive range between the provided dates."""

    def assignments_for_month(self, month: str) -> Sequence[ScheduleAssignment]:
        """Return assignments for a specific month (``YYYY-MM``)."""

    def transfer_requests(self, *, status: str | None = None) -> Sequence[ScheduleTransferRequest]:
        """Return transfer requests optionally filtered by status."""


@runtime_checkable
class ImportLogRepo(Protocol):
    """Import log inspection interface."""

    def get(self, import_id: int) -> ImportLogEntry | None:
        """Return a single import log by identifier."""

    def latest(self, limit: int = 20) -> Sequence[ImportLogEntry]:
        """Return the latest import logs ordered by recency."""

    def add(self, entry: ImportLogEntry) -> ImportLogEntry:
        """Persist a new import log entry and return the stored representation."""

    def mark_reverted(self, import_id: int) -> None:
        """Mark an import as reverted."""


__all__ = ["ProductRepo", "StockRepo", "ScheduleRepo", "ImportLogRepo"]
