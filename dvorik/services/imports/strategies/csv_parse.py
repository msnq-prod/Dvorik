from __future__ import annotations

import csv
import io
from collections.abc import Iterable
from typing import Any, List, Mapping

__all__ = ["parse_csv", "sniff_delimiter"]


def sniff_delimiter(sample: str) -> str:
    """Try to detect a delimiter from the provided ``sample`` text."""

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|:")
        return dialect.delimiter
    except csv.Error:
        return ","


def _prepare_stream(source: Any, *, encoding: str) -> io.TextIOBase:
    if isinstance(source, io.TextIOBase):
        source.seek(0)
        return source
    if isinstance(source, (bytes, bytearray)):
        text = source.decode(encoding)
        return io.StringIO(text)
    if hasattr(source, "read"):
        data = source.read()
        if isinstance(data, bytes):
            text = data.decode(encoding)
        else:
            text = str(data)
        return io.StringIO(text)
    if isinstance(source, str):
        return io.StringIO(source)
    raise TypeError("Unsupported CSV source type")


def parse_csv(
    source: Any,
    *,
    encoding: str = "utf-8",
    delimiter: str | None = None,
    normalize_headers: bool = True,
) -> List[Mapping[str, Any]]:
    """Parse CSV data returning a list of dictionaries."""

    stream = _prepare_stream(source, encoding=encoding)
    sample = stream.read(2048)
    stream.seek(0)
    csv_delimiter = delimiter or sniff_delimiter(sample)

    reader = csv.DictReader(stream, delimiter=csv_delimiter)
    rows: List[Mapping[str, Any]] = []

    headers: Iterable[str] = reader.fieldnames or []
    if normalize_headers:
        header_map = {
            header: header.strip() if isinstance(header, str) else header
            for header in headers
        }
    else:
        header_map = {header: header for header in headers}

    for raw_row in reader:
        cleaned_row = {
            header_map.get(key, key): value
            for key, value in raw_row.items()
            if key is not None
        }
        rows.append(cleaned_row)

    stream.seek(0)
    return rows
