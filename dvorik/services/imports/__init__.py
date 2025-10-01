from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from dvorik.core import events
from dvorik.domain.models import ImportLogEntry
from dvorik.domain.ports import ImportLogRepo
from dvorik.repo.stock_repo import SQLiteStockRepo
from dvorik.services import stock

from .strategies import ColumnMapping, detect_columns, parse_csv, parse_sheet

__all__ = [
    "ImportBatch",
    "ImportFacade",
    "ImportBatchApplier",
    "ImportMoveRecord",
    "ImportOperationRecord",
    "ImportProcessResult",
    "ImportRowError",
    "NormalisedRow",
    "log_completed_import",
    "normalise_rows",
]


_IMPORT_COMPLETED_EVENT = "import.completed"


logger = logging.getLogger(__name__)


NormalisedRow = Mapping[str, Any]


@dataclass(slots=True)
class ImportBatch:
    """Result of parsing an import file."""

    import_type: str
    original_name: str
    rows: List[Mapping[str, Any]]
    columns: ColumnMapping
    source_hash: str

    def normalised_rows(self) -> List[NormalisedRow]:
        return list(normalise_rows(self.rows, self.columns))

    def to_csv(self, *, delimiter: str = ";") -> str:
        normalised = self.normalised_rows()
        if not normalised:
            return ""
        headers = sorted(normalised[0].keys())
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=headers, delimiter=delimiter)
        writer.writeheader()
        for row in normalised:
            writer.writerow({key: row.get(key) for key in headers})
        buffer.seek(0)
        return buffer.read()


class ImportFacade:
    """High level entry point for import processing."""

    def __init__(self, storage_dir: str | Path | None = None) -> None:
        self._storage_dir = Path(storage_dir) if storage_dir else None
        if self._storage_dir is not None:
            self._storage_dir.mkdir(parents=True, exist_ok=True)

    def from_csv(
        self,
        source: Any,
        *,
        original_name: str,
        encoding: str = "utf-8",
        delimiter: str | None = None,
    ) -> ImportBatch:
        rows = parse_csv(source, encoding=encoding, delimiter=delimiter)
        columns = detect_columns(rows)
        data_hash = _hash_rows(rows)
        return ImportBatch(
            import_type="csv",
            original_name=original_name,
            rows=rows,
            columns=columns,
            source_hash=data_hash,
        )

    def from_excel(
        self,
        source: Any,
        *,
        original_name: str,
        sheet_name: str | None = None,
    ) -> ImportBatch:
        rows = parse_sheet(source, sheet_name=sheet_name)
        columns = detect_columns(rows)
        data_hash = _hash_rows(rows)
        return ImportBatch(
            import_type="excel",
            original_name=original_name,
            rows=rows,
            columns=columns,
            source_hash=data_hash,
        )

    def store_normalised(self, batch: ImportBatch, *, filename: str | None = None) -> Path:
        if self._storage_dir is None:
            raise RuntimeError("ImportFacade was not configured with storage directory")

        target_name = filename or f"{batch.source_hash}.csv"
        target_path = self._storage_dir / target_name
        normalised = list(batch.normalised_rows())
        if not normalised:
            target_path.write_text("", encoding="utf-8")
            return target_path

        headers = sorted(normalised[0].keys())
        with target_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
            writer.writeheader()
            for row in normalised:
                writer.writerow({key: row.get(key) for key in headers})
        return target_path


async def log_completed_import(
    repo: ImportLogRepo,
    entry: ImportLogEntry,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> ImportLogEntry:
    """Persist ``entry`` and publish an ``import.completed`` event.

    Parameters
    ----------
    repo:
        Repository responsible for storing the import log entry.
    entry:
        Dataclass describing the processed import.
    metadata:
        Optional additional values to merge into the published event payload.

    Returns
    -------
    ImportLogEntry
        The stored representation returned by the repository.
    """

    saved_entry = repo.add(entry)

    payload: Dict[str, Any] = {
        "entry": saved_entry,
        "import_id": saved_entry.id,
        "original_name": saved_entry.original_name,
        "import_type": saved_entry.import_type,
        "source_hash": saved_entry.source_hash,
        "items_count": saved_entry.items_count,
    }
    if saved_entry.supplier:
        payload["supplier"] = saved_entry.supplier
    if saved_entry.invoice:
        payload["invoice"] = saved_entry.invoice
    if saved_entry.normalized_hash:
        payload["normalized_hash"] = saved_entry.normalized_hash
    if metadata:
        payload.update(metadata)

    await events.publish(_IMPORT_COMPLETED_EVENT, **payload)
    return saved_entry


def normalise_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: ColumnMapping,
) -> Iterable[NormalisedRow]:
    mapping = columns.as_dict()
    if not mapping:
        for row in rows:
            yield row
        return

    for row in rows:
        projected = {canonical: row.get(source_column) for canonical, source_column in mapping.items()}
        yield projected


@dataclass(slots=True)
class ImportOperationRecord:
    """Result of adjusting stock at a specific location."""

    row_index: int
    product_id: int
    location_code: str
    qty_before: float
    qty_after: float
    delta: float
    article: str | None = None
    name: str | None = None


@dataclass(slots=True)
class ImportMoveRecord:
    """Result of moving stock between locations."""

    row_index: int
    product_id: int
    from_location: str
    to_location: str
    qty: float
    from_before: float
    from_after: float
    to_before: float
    to_after: float
    article: str | None = None
    name: str | None = None


@dataclass(slots=True)
class ImportRowError:
    """Error raised while processing a row from the import batch."""

    row_index: int
    message: str
    row: Mapping[str, Any]


@dataclass(slots=True)
class ImportProcessResult:
    """Aggregated outcome of applying an import batch or reverting it."""

    adjustments: List[ImportOperationRecord]
    moves: List[ImportMoveRecord]
    errors: List[ImportRowError]

    @property
    def total_operations(self) -> int:
        return len(self.adjustments) + len(self.moves)

    @property
    def affected_products(self) -> set[int]:
        product_ids = {record.product_id for record in self.adjustments}
        product_ids.update(record.product_id for record in self.moves)
        return product_ids

    @property
    def affected_locations(self) -> set[str]:
        locations = {record.location_code for record in self.adjustments}
        for record in self.moves:
            locations.add(record.from_location)
            locations.add(record.to_location)
        return {code for code in locations if code}

    def snapshot(self, limit: int | None = None) -> List[Dict[str, Any]]:
        """Return a serialisable representation of the processed operations."""

        entries: List[Dict[str, Any]] = []
        for record in self.adjustments:
            entries.append(
                {
                    "type": "adjust",
                    "row_index": record.row_index,
                    "product_id": record.product_id,
                    "location_code": record.location_code,
                    "qty_before": record.qty_before,
                    "qty_after": record.qty_after,
                    "delta": record.delta,
                    "article": record.article,
                    "name": record.name,
                }
            )

        for record in self.moves:
            entries.append(
                {
                    "type": "move",
                    "row_index": record.row_index,
                    "product_id": record.product_id,
                    "from_location": record.from_location,
                    "to_location": record.to_location,
                    "qty": record.qty,
                    "from_before": record.from_before,
                    "from_after": record.from_after,
                    "to_before": record.to_before,
                    "to_after": record.to_after,
                    "article": record.article,
                    "name": record.name,
                }
            )

        if limit is not None:
            return entries[: max(0, int(limit))]
        return entries


class ImportBatchApplier:
    """Execute stock operations described by :class:`ImportBatch` rows."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        user_id: int | None = None,
        actor_id: int | None = None,
        actor_username: str | None = None,
        remote_addr: str | None = None,
    ) -> None:
        self._conn = conn
        self._user_id = user_id
        self._actor_id = actor_id
        self._actor_username = actor_username
        self._remote_addr = remote_addr
        self._stock_repo = SQLiteStockRepo(conn)

    async def apply(
        self,
        batch: ImportBatch,
        *,
        import_id: int | None = None,
    ) -> ImportProcessResult:
        rows = list(batch.normalised_rows())
        adjustments: List[ImportOperationRecord] = []
        moves: List[ImportMoveRecord] = []
        errors: List[ImportRowError] = []

        for index, row in enumerate(rows, start=1):
            try:
                instruction = self._parse_instruction(row)
            except ValueError as exc:
                errors.append(
                    ImportRowError(
                        row_index=index,
                        message=str(exc),
                        row=row,
                    )
                )
                continue

            if instruction["type"] == "move":
                try:
                    changes = await stock.move_specific(
                        self._conn,
                        instruction["product_id"],
                        instruction["from_location"],
                        instruction["to_location"],
                        instruction["qty"],
                        user_id=self._user_id,
                    )
                except Exception as exc:  # pragma: no cover - logged in audit payload
                    errors.append(
                        ImportRowError(
                            row_index=index,
                            message=str(exc),
                            row=row,
                        )
                    )
                    continue

                moves.append(
                    ImportMoveRecord(
                        row_index=index,
                        product_id=instruction["product_id"],
                        from_location=instruction["from_location"],
                        to_location=instruction["to_location"],
                        qty=float(instruction["qty"]),
                        from_before=changes["from"].qty_before,
                        from_after=changes["from"].qty_after,
                        to_before=changes["to"].qty_before,
                        to_after=changes["to"].qty_after,
                        article=instruction.get("article"),
                        name=instruction.get("name"),
                    )
                )
                continue

            product_id = instruction["product_id"]
            location_code = instruction["location_code"]
            delta = float(instruction["delta"])

            current = self._stock_repo.get_item(product_id, location_code)
            base_qty = current.qty_pack if current is not None else 0.0
            target_qty = base_qty + delta

            try:
                change = await stock.set_location_qty(
                    self._conn,
                    product_id,
                    location_code,
                    target_qty,
                    user_id=self._user_id,
                )
            except Exception as exc:  # pragma: no cover - logged in audit payload
                errors.append(
                    ImportRowError(
                        row_index=index,
                        message=str(exc),
                        row=row,
                    )
                )
                continue

            adjustments.append(
                ImportOperationRecord(
                    row_index=index,
                    product_id=product_id,
                    location_code=location_code,
                    qty_before=change.qty_before,
                    qty_after=change.qty_after,
                    delta=change.delta,
                    article=instruction.get("article"),
                    name=instruction.get("name"),
                )
            )

        result = ImportProcessResult(adjustments=adjustments, moves=moves, errors=errors)
        self._log_audit(
            action="import.apply",
            import_id=import_id,
            result=result,
            metadata={
                "original_name": batch.original_name,
                "import_type": batch.import_type,
                "rows": len(rows),
            },
        )
        return result

    async def revert(
        self,
        snapshot: Sequence[Mapping[str, Any]],
        *,
        import_id: int | None = None,
    ) -> ImportProcessResult:
        adjustments: List[ImportOperationRecord] = []
        moves: List[ImportMoveRecord] = []
        errors: List[ImportRowError] = []

        for index, entry in enumerate(snapshot, start=1):
            entry_type = str(entry.get("type") or "").lower()
            try:
                product_id = self._parse_product_id(entry)
            except ValueError as exc:
                errors.append(
                    ImportRowError(
                        row_index=index,
                        message=str(exc),
                        row=entry,
                    )
                )
                continue
            article = _clean_text(entry.get("article"))
            name = _clean_text(entry.get("name"))

            if entry_type == "move":
                from_location = _clean_location(entry.get("from_location"))
                to_location = _clean_location(entry.get("to_location"))
                qty = _parse_qty_value(entry.get("qty"))
                if from_location is None or to_location is None or qty is None:
                    errors.append(
                        ImportRowError(
                            row_index=index,
                            message="Snapshot entry is incomplete.",
                            row=entry,
                        )
                    )
                    continue

                try:
                    changes = await stock.move_specific(
                        self._conn,
                        product_id,
                        to_location,
                        from_location,
                        qty,
                        user_id=self._user_id,
                    )
                except Exception as exc:  # pragma: no cover - logged in audit payload
                    errors.append(
                        ImportRowError(
                            row_index=index,
                            message=str(exc),
                            row=entry,
                        )
                    )
                    continue

                moves.append(
                    ImportMoveRecord(
                        row_index=index,
                        product_id=product_id,
                        from_location=to_location,
                        to_location=from_location,
                        qty=float(qty),
                        from_before=changes["from"].qty_before,
                        from_after=changes["from"].qty_after,
                        to_before=changes["to"].qty_before,
                        to_after=changes["to"].qty_after,
                        article=article,
                        name=name,
                    )
                )
                continue

            location_code = _clean_location(entry.get("location_code"))
            qty_before = entry.get("qty_before")
            if location_code is None or qty_before is None:
                errors.append(
                    ImportRowError(
                        row_index=index,
                        message="Snapshot entry is incomplete.",
                        row=entry,
                    )
                )
                continue

            try:
                change = await stock.set_location_qty(
                    self._conn,
                    product_id,
                    location_code,
                    float(qty_before),
                    user_id=self._user_id,
                )
            except Exception as exc:  # pragma: no cover - logged in audit payload
                errors.append(
                    ImportRowError(
                        row_index=index,
                        message=str(exc),
                        row=entry,
                    )
                )
                continue

            adjustments.append(
                ImportOperationRecord(
                    row_index=index,
                    product_id=product_id,
                    location_code=location_code,
                    qty_before=change.qty_before,
                    qty_after=change.qty_after,
                    delta=change.delta,
                    article=article,
                    name=name,
                )
            )

        result = ImportProcessResult(adjustments=adjustments, moves=moves, errors=errors)
        self._log_audit(
            action="import.revert",
            import_id=import_id,
            result=result,
            metadata={"snapshot_size": len(snapshot)},
        )
        return result

    def _parse_instruction(self, row: Mapping[str, Any]) -> Mapping[str, Any]:
        product_id = self._parse_product_id(row)
        article = _clean_text(row.get("article"))
        name = _clean_text(row.get("name"))

        from_location = _clean_location(row.get("from_location"))
        to_location = _clean_location(row.get("to_location"))
        operation = str(row.get("operation") or row.get("type") or "").lower()

        if from_location and to_location or operation in {"move", "transfer"}:
            qty = _parse_qty_value(
                row.get("qty")
                or row.get("quantity")
                or row.get("qty_pack")
                or row.get("move_qty")
            )
            if qty is None or qty <= 0:
                raise ValueError("Move quantity must be positive.")
            if not from_location or not to_location:
                raise ValueError(
                    "Move operation requires both source and destination locations."
                )
            return {
                "type": "move",
                "product_id": product_id,
                "from_location": from_location,
                "to_location": to_location,
                "qty": float(qty),
                "article": article,
                "name": name,
            }

        location_code = _clean_location(
            row.get("location_code")
            or row.get("location")
            or row.get("target_location")
            or row.get("to_location")
        )
        if not location_code:
            raise ValueError("Location code is required for stock adjustment.")

        delta = _parse_qty_value(
            row.get("delta")
            or row.get("qty")
            or row.get("quantity")
            or row.get("qty_pack")
        )
        if delta is None or delta == 0:
            raise ValueError("Quantity delta must be non-zero.")

        return {
            "type": "adjust",
            "product_id": product_id,
            "location_code": location_code,
            "delta": float(delta),
            "article": article,
            "name": name,
        }

    def _parse_product_id(self, row: Mapping[str, Any]) -> int:
        value = row.get("product_id") or row.get("product") or row.get("id")
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("Invalid product_id value") from exc

        article = _clean_text(row.get("article"))
        if not article:
            raise ValueError("Product identifier is required")

        cursor = self._conn.execute(
            "SELECT id FROM product WHERE article = ?",
            (article,),
        )
        row_data = cursor.fetchone()
        if not row_data:
            raise ValueError(f"Product with article '{article}' was not found")
        return int(row_data["id"] if isinstance(row_data, sqlite3.Row) else row_data[0])

    def _log_audit(
        self,
        *,
        action: str,
        import_id: int | None,
        result: ImportProcessResult,
        metadata: MutableMapping[str, Any] | None = None,
    ) -> None:
        payload: MutableMapping[str, Any] = {
            "status": _resolve_status(result),
            "operations": {
                "adjustments": len(result.adjustments),
                "moves": len(result.moves),
            },
            "affected_products": sorted(result.affected_products),
            "affected_locations": sorted(result.affected_locations),
        }
        if result.errors:
            payload["errors"] = [
                {
                    "row_index": error.row_index,
                    "message": error.message,
                }
                for error in result.errors
            ]
        if metadata:
            payload.update(metadata)
        if self._remote_addr:
            payload.setdefault("remote_addr", self._remote_addr)

        try:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO audit_log(actor_id, actor_username, action, entity, entity_id, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._actor_id,
                        self._actor_username,
                        action,
                        "import",
                        str(import_id) if import_id is not None else None,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
        except sqlite3.DatabaseError as exc:  # pragma: no cover - logging only
            logger.warning("Failed to write audit log entry: %s", exc)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_location(value: Any) -> str | None:
    text = _clean_text(value)
    if text:
        return text.upper()
    return None


def _parse_qty_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    normalised = text.replace(" ", "").replace(",", ".")
    try:
        return float(normalised)
    except ValueError:
        return None


def _resolve_status(result: ImportProcessResult) -> str:
    if result.total_operations == 0 and result.errors:
        return "error"
    if result.errors:
        return "partial"
    return "success"


def _hash_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        for key in sorted(row.keys()):
            value = row.get(key)
            hasher.update(str(key).encode("utf-8"))
            hasher.update(str(value).encode("utf-8"))
    return hasher.hexdigest()
