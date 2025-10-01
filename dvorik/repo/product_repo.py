"""SQLite-backed implementation of :class:`~dvorik.domain.ports.ProductRepo`."""

from __future__ import annotations

import sqlite3
from typing import Any, List, Mapping, Sequence

from dvorik.db.query_registry import get_query
from dvorik.domain.models import (
    Manufacturer,
    Product,
    ProductDetail,
    StockItem,
    SupplierSku,
)
from dvorik.domain.ports import ProductRepo


class SQLiteProductRepo(ProductRepo):
    """Read-only repository for product catalogue data."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    # ------------------------------------------------------------------
    # Product listing and retrieval
    # ------------------------------------------------------------------
    def get(self, product_id: int) -> Product | None:
        sql = get_query(
            self._conn,
            "repo.product.get",
            """
            SELECT
                id,
                article,
                barcode,
                name,
                local_name,
                description,
                unit,
                manufacturer_id,
                price,
                vat_rate,
                is_new,
                archived,
                archived_at,
                created_at,
                updated_at,
                last_restock_at,
                photo_file_id,
                photo_path
            FROM product
            WHERE id = :product_id
            """,
        )
        cursor = self._conn.execute(sql, {"product_id": product_id})
        row = cursor.fetchone()
        if row is None:
            return None
        return _row_to_product(row)

    def list(self, *, include_archived: bool = False) -> Sequence[Product]:
        sql = get_query(
            self._conn,
            "repo.product.list",
            """
            SELECT
                id,
                article,
                barcode,
                name,
                local_name,
                description,
                unit,
                manufacturer_id,
                price,
                vat_rate,
                is_new,
                archived,
                archived_at,
                created_at,
                updated_at,
                last_restock_at,
                photo_file_id,
                photo_path
            FROM product
            WHERE (:include_archived = 1) OR archived = 0
            ORDER BY name COLLATE NOCASE
            """,
        )
        cursor = self._conn.execute(
            sql,
            {"include_archived": 1 if include_archived else 0},
        )
        return [_row_to_product(row) for row in cursor.fetchall()]

    def search_fts(self, query: str, *, limit: int = 20) -> Sequence[Product]:
        if not query.strip():
            return []

        sql = get_query(
            self._conn,
            "repo.product.search_fts",
            """
            SELECT
                p.id,
                p.article,
                p.barcode,
                p.name,
                p.local_name,
                p.description,
                p.unit,
                p.manufacturer_id,
                p.price,
                p.vat_rate,
                p.is_new,
                p.archived,
                p.archived_at,
                p.created_at,
                p.updated_at,
                p.last_restock_at,
                p.photo_file_id,
                p.photo_path
            FROM product AS p
            JOIN product_fts AS fts ON fts.rowid = p.id
            WHERE fts MATCH :match
            ORDER BY p.name COLLATE NOCASE
            LIMIT :limit
            """,
        )
        cursor = self._conn.execute(
            sql,
            {
                "match": query,
                "limit": max(1, int(limit)),
            },
        )
        return [_row_to_product(row) for row in cursor.fetchall()]

    def product_detail(self, product_id: int) -> ProductDetail | None:
        sql = get_query(
            self._conn,
            "repo.product.detail",
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
                m.id AS manufacturer_id,
                m.name AS manufacturer_name,
                m.country AS manufacturer_country,
                m.created_at AS manufacturer_created_at
            FROM product AS p
            LEFT JOIN manufacturer AS m ON m.id = p.manufacturer_id
            WHERE p.id = :product_id
            """,
        )
        cursor = self._conn.execute(sql, {"product_id": product_id})
        row = cursor.fetchone()
        if row is None:
            return None

        product = _row_to_product(
            {
                "id": row["product_id"],
                "article": row["product_article"],
                "barcode": row["product_barcode"],
                "name": row["product_name"],
                "local_name": row["product_local_name"],
                "description": row["product_description"],
                "unit": row["product_unit"],
                "manufacturer_id": row["product_manufacturer_id"],
                "price": row["product_price"],
                "vat_rate": row["product_vat_rate"],
                "is_new": row["product_is_new"],
                "archived": row["product_archived"],
                "archived_at": row["product_archived_at"],
                "created_at": row["product_created_at"],
                "updated_at": row["product_updated_at"],
                "last_restock_at": row["product_last_restock_at"],
                "photo_file_id": row["product_photo_file_id"],
                "photo_path": row["product_photo_path"],
            }
        )

        manufacturer = None
        if row["manufacturer_id"] is not None:
            manufacturer = Manufacturer(
                id=row["manufacturer_id"],
                name=row["manufacturer_name"],
                country=row["manufacturer_country"],
                created_at=row["manufacturer_created_at"],
            )

        supplier_skus = self._fetch_supplier_skus(product_id)
        stock_items = self._fetch_stock_items(product_id)

        return ProductDetail(
            product=product,
            manufacturer=manufacturer,
            supplier_skus=tuple(supplier_skus),
            stock_items=tuple(stock_items),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_supplier_skus(self, product_id: int) -> List[SupplierSku]:
        sql = get_query(
            self._conn,
            "repo.product.detail_supplier_skus",
            """
            SELECT
                id,
                product_id,
                supplier_id,
                code,
                barcode,
                pack_qty,
                active,
                created_at,
                updated_at
            FROM supplier_sku
            WHERE product_id = :product_id
            ORDER BY supplier_id, code COLLATE NOCASE
            """,
        )
        cursor = self._conn.execute(sql, {"product_id": product_id})
        return [_row_to_supplier_sku(row) for row in cursor.fetchall()]

    def _fetch_stock_items(self, product_id: int) -> List[StockItem]:
        sql = get_query(
            self._conn,
            "repo.product.detail_stock",
            """
            SELECT
                product_id,
                location_code,
                qty_pack,
                reserved_pack,
                updated_at
            FROM stock
            WHERE product_id = :product_id
            ORDER BY location_code
            """,
        )
        cursor = self._conn.execute(sql, {"product_id": product_id})
        return [_row_to_stock_item(row) for row in cursor.fetchall()]


# ----------------------------------------------------------------------
# Row conversion helpers
# ----------------------------------------------------------------------
def _row_to_product(row: sqlite3.Row | Mapping[str, Any]) -> Product:
    data = dict(row)
    return Product(
        id=data["id"],
        article=data.get("article"),
        barcode=data.get("barcode"),
        name=data["name"],
        local_name=data.get("local_name"),
        description=data.get("description"),
        unit=data.get("unit"),
        manufacturer_id=data.get("manufacturer_id"),
        price=data.get("price"),
        vat_rate=data.get("vat_rate"),
        is_new=bool(data.get("is_new", 0)),
        archived=bool(data.get("archived", 0)),
        archived_at=data.get("archived_at"),
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        last_restock_at=data.get("last_restock_at"),
        photo_file_id=data.get("photo_file_id"),
        photo_path=data.get("photo_path"),
    )


def _row_to_supplier_sku(row: sqlite3.Row) -> SupplierSku:
    return SupplierSku(
        id=row["id"],
        product_id=row["product_id"],
        supplier_id=row["supplier_id"],
        code=row["code"],
        barcode=row["barcode"],
        pack_qty=row["pack_qty"],
        active=bool(row["active"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_stock_item(row: sqlite3.Row) -> StockItem:
    return StockItem(
        product_id=row["product_id"],
        location_code=row["location_code"],
        qty_pack=float(row["qty_pack"] or 0),
        reserved_pack=float(row["reserved_pack"] or 0),
        updated_at=row["updated_at"],
    )


__all__ = ["SQLiteProductRepo"]
