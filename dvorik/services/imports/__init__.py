from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence

from .strategies import ColumnMapping, detect_columns, parse_csv, parse_sheet

__all__ = ["ImportBatch", "ImportFacade", "NormalisedRow", "normalise_rows"]


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


def _hash_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        for key in sorted(row.keys()):
            value = row.get(key)
            hasher.update(str(key).encode("utf-8"))
            hasher.update(str(value).encode("utf-8"))
    return hasher.hexdigest()
