"""SQLite-backed implementation of :class:`~dvorik.domain.ports.StockRepo`."""

from __future__ import annotations

import sqlite3
from typing import Sequence

from dvorik.db.query_registry import get_query
from dvorik.domain.models import (
    Location,
    LowStockRecord,
    Product,
    StockItem,
    StockSnapshot,
)
from dvorik.domain.ports import StockRepo


class SQLiteStockRepo(StockRepo):
    """Repository exposing stock read models backed by SQLite."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_item(self, product_id: int, location_code: str) -> StockItem | None:
        sql = get_query(
            self._conn,
            "repo.stock.get_item",
            """
            SELECT
                product_id,
                location_code,
                qty_pack,
                reserved_pack,
                updated_at
            FROM stock
            WHERE product_id = :product_id AND location_code = :location_code
            """,
        )
        cursor = self._conn.execute(
            sql,
            {"product_id": product_id, "location_code": location_code},
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return StockItem(
            product_id=row["product_id"],
            location_code=row["location_code"],
            qty_pack=float(row["qty_pack"] or 0),
            reserved_pack=float(row["reserved_pack"] or 0),
            updated_at=row["updated_at"],
        )

    def stock_by_location(self, location_code: str | None = None) -> Sequence[StockSnapshot]:
        sql = get_query(
            self._conn,
            "repo.stock.by_location",
            """
            SELECT
                p.id AS product_id,
                p.article AS product_article,
                p.barcode AS product_barcode,
                p.name AS product_name,
                p.local_name AS product_local_name,
                p.description AS product_description,
                p.unit AS product_unit,
                p.manufacturer_id AS product_manufacturer_id,
                p.price AS product_price,
                p.vat_rate AS product_vat_rate,
                p.is_new AS product_is_new,
                p.archived AS product_archived,
                p.archived_at AS product_archived_at,
                p.created_at AS product_created_at,
                p.updated_at AS product_updated_at,
                p.last_restock_at AS product_last_restock_at,
                p.photo_file_id AS product_photo_file_id,
                p.photo_path AS product_photo_path,
                l.code AS location_code,
                l.kind AS location_kind,
                l.title AS location_title,
                l.created_at AS location_created_at,
                s.qty_pack AS stock_qty_pack,
                s.reserved_pack AS stock_reserved_pack,
                s.updated_at AS stock_updated_at
            FROM stock AS s
            JOIN product AS p ON p.id = s.product_id
            JOIN location AS l ON l.code = s.location_code
            WHERE (:location_code IS NULL OR l.code = :location_code)
            ORDER BY l.code, p.name COLLATE NOCASE
            """,
        )
        cursor = self._conn.execute(sql, {"location_code": location_code})
        rows = cursor.fetchall()
        return [_row_to_snapshot(row) for row in rows]

    def low_stock(
        self,
        *,
        threshold: float | None = None,
        limit: int = 20,
    ) -> Sequence[LowStockRecord]:
        threshold_value = float(threshold) if threshold is not None else 0.0

        sql = get_query(
            self._conn,
            "repo.stock.low_stock",
            """
            SELECT
                p.id AS product_id,
                p.article AS product_article,
                p.barcode AS product_barcode,
                p.name AS product_name,
                p.local_name AS product_local_name,
                p.description AS product_description,
                p.unit AS product_unit,
                p.manufacturer_id AS product_manufacturer_id,
                p.price AS product_price,
                p.vat_rate AS product_vat_rate,
                p.is_new AS product_is_new,
                p.archived AS product_archived,
                p.archived_at AS product_archived_at,
                p.created_at AS product_created_at,
                p.updated_at AS product_updated_at,
                p.last_restock_at AS product_last_restock_at,
                p.photo_file_id AS product_photo_file_id,
                p.photo_path AS product_photo_path,
                l.code AS location_code,
                l.kind AS location_kind,
                l.title AS location_title,
                l.created_at AS location_created_at,
                s.qty_pack AS stock_qty_pack,
                s.reserved_pack AS stock_reserved_pack
            FROM stock AS s
            JOIN product AS p ON p.id = s.product_id
            JOIN location AS l ON l.code = s.location_code
            WHERE s.qty_pack <= :threshold
            ORDER BY s.qty_pack ASC, p.name COLLATE NOCASE
            LIMIT :limit
            """,
        )
        cursor = self._conn.execute(
            sql,
            {"threshold": threshold_value, "limit": max(1, int(limit))},
        )
        rows = cursor.fetchall()
        snapshots = [_row_to_snapshot(row) for row in rows]
        return [
            LowStockRecord(
                product=record.product,
                location=record.location,
                qty_pack=record.qty_pack,
                threshold=threshold_value,
            )
            for record in snapshots
        ]


def _row_to_snapshot(row: sqlite3.Row) -> StockSnapshot:
    product = Product(
        id=row["product_id"],
        article=row["product_article"],
        barcode=row["product_barcode"],
        name=row["product_name"],
        local_name=row["product_local_name"],
        description=row["product_description"],
        unit=row["product_unit"],
        manufacturer_id=row["product_manufacturer_id"],
        price=row["product_price"],
        vat_rate=row["product_vat_rate"],
        is_new=bool(row["product_is_new"]),
        archived=bool(row["product_archived"]),
        archived_at=row["product_archived_at"],
        created_at=row["product_created_at"],
        updated_at=row["product_updated_at"],
        last_restock_at=row["product_last_restock_at"],
        photo_file_id=row["product_photo_file_id"],
        photo_path=row["product_photo_path"],
    )

    location = Location(
        code=row["location_code"],
        kind=row["location_kind"],
        title=row["location_title"],
        created_at=row["location_created_at"],
    )

    return StockSnapshot(
        product=product,
        location=location,
        qty_pack=float(row["stock_qty_pack"] or 0),
        reserved_pack=float(row["stock_reserved_pack"] or 0),
    )


__all__ = ["SQLiteStockRepo"]
