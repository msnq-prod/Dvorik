from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping
from uuid import uuid4

from flask import (
    Blueprint,
    Response,
    current_app,
    redirect,
    render_template,
    request,
    url_for,
)
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from dvorik.core.config import Config
from dvorik.admin.auth import require_superadmin
from dvorik.db.conn import db
from dvorik.domain.models import ImportLogEntry
from dvorik.repo.import_repo import SQLiteImportLogRepo
from dvorik.services.imports import (
    ImportBatch,
    ImportBatchApplier,
    ImportFacade,
    ImportProcessResult,
    log_completed_import,
)

blueprint = Blueprint("supply", __name__, url_prefix="/supply")

logger = logging.getLogger(__name__)

_PREVIEW_SAMPLE_ROWS = 15
_SNAPSHOT_ROWS = 50


@dataclass(slots=True)
class PreviewContext:
    """Data required to render the preview confirmation panel."""

    original_name: str
    stored_path: str
    stored_filename: str
    import_type: str
    source_hash: str
    row_count: int
    headers: tuple[str, ...]
    sample_rows: tuple[Mapping[str, object], ...]
    column_mapping: Mapping[str, str]
    supplier: str
    invoice: str
    delimiter: str | None
    sheet_name: str | None


@blueprint.get("/")
@require_superadmin
def supply_home() -> str:
    """Render the supply management landing page."""

    status, message = _extract_status()
    imports = _load_recent_imports()
    return render_template(
        "supply.html",
        preview=None,
        imports=imports,
        status=status,
        message=message,
    )


@blueprint.post("/preview")
@require_superadmin
def preview_import() -> str | Response:
    """Accept an uploaded file and display a preview before importing."""

    upload = request.files.get("file")
    if upload is None or not upload.filename:
        return _redirect_with_status("error", "Please choose a file to upload.")

    supplier = (request.form.get("supplier") or "").strip()
    invoice = (request.form.get("invoice") or "").strip()
    sheet_name = (request.form.get("sheet_name") or "").strip() or None
    delimiter = (request.form.get("delimiter") or "").strip() or None
    requested_type = (request.form.get("import_type") or "auto").strip().lower()

    config = _get_config()

    try:
        stored_path = _persist_upload(upload, config)
        import_type = _resolve_import_type(requested_type, stored_path)
        facade = _get_facade(config)
        batch = _parse_batch(
            facade,
            import_type,
            stored_path,
            original_name=upload.filename,
            delimiter=delimiter,
            sheet_name=sheet_name,
        )
        preview = _build_preview_context(
            batch,
            stored_path=stored_path,
            import_type=import_type,
            supplier=supplier,
            invoice=invoice,
            delimiter=delimiter,
            sheet_name=sheet_name,
        )
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to build import preview")
        return _redirect_with_status("error", "Failed to analyse the uploaded file.")

    imports = _load_recent_imports()
    return render_template(
        "supply.html",
        preview=preview,
        imports=imports,
        status=None,
        message=None,
    )


@blueprint.post("/confirm")
@require_superadmin
def confirm_import() -> Response:
    """Persist a processed import into the database."""

    stored_ref = request.form.get("stored_path") or ""
    original_name = request.form.get("original_name") or ""
    import_type = request.form.get("import_type") or ""
    source_hash = request.form.get("source_hash") or ""
    supplier = (request.form.get("supplier") or "").strip()
    invoice = (request.form.get("invoice") or "").strip()
    delimiter = (request.form.get("delimiter") or "").strip() or None
    sheet_name = (request.form.get("sheet_name") or "").strip() or None

    if not stored_ref or not original_name or not import_type or not source_hash:
        return _redirect_with_status("error", "Import context is incomplete. Please start over.")

    config = _get_config()

    try:
        stored_path = _resolve_uploaded_path(stored_ref, config)
    except ValueError:
        return _redirect_with_status("error", "The referenced upload is no longer available.")

    facade = _get_facade(config)

    try:
        batch = _parse_batch(
            facade,
            import_type,
            stored_path,
            original_name=original_name,
            delimiter=delimiter,
            sheet_name=sheet_name,
        )
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to re-parse uploaded file for confirmation")
        return _redirect_with_status("error", "Could not re-read the uploaded file. Please upload again.")

    if batch.source_hash != source_hash:
        logger.warning(
            "Source hash mismatch during confirmation (expected %s, got %s)",
            source_hash,
            batch.source_hash,
        )
        return _redirect_with_status(
            "error",
            "The uploaded file has changed. Please upload and preview again.",
        )

    normalised_rows = batch.normalised_rows()
    normalised_csv = batch.to_csv()
    normalised_hash = (
        hashlib.sha256(normalised_csv.encode("utf-8")).hexdigest() if normalised_csv else None
    )

    try:
        facade.store_normalised(batch, filename=f"{batch.source_hash}.csv")
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to persist normalised CSV")
        return _redirect_with_status("error", "Could not store the normalised data.")

    entry = ImportLogEntry(
        original_name=original_name,
        stored_path=str(stored_path),
        import_type=import_type,
        source_hash=source_hash,
        normalized_csv=normalised_csv or None,
        normalized_hash=normalised_hash,
        supplier=supplier or None,
        invoice=invoice or None,
        items_count=len(normalised_rows),
        items_json=None,
    )

    conn = None
    saved_entry: ImportLogEntry | None = None
    apply_result: ImportProcessResult | None = None
    try:
        conn = db()
        repo = SQLiteImportLogRepo(conn)
        saved_entry = asyncio.run(
            log_completed_import(
                repo,
                entry,
                metadata={"stored_path": entry.stored_path},
            )
        )

        try:
            applier = _make_import_applier(conn)
            apply_result = asyncio.run(
                applier.apply(
                    batch,
                    import_id=saved_entry.id,
                )
            )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception("Failed to apply import batch to stock")
            return _redirect_with_status(
                "error",
                (
                    f"Import #{saved_entry.id} recorded but could not be applied to stock."
                    if saved_entry and saved_entry.id
                    else "Import could not be applied to stock."
                ),
            )

        if saved_entry.id is not None and apply_result is not None:
            snapshot_payload = apply_result.snapshot()
            try:
                _update_import_snapshot(conn, saved_entry.id, snapshot_payload)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to persist import snapshot for revert")
            else:
                saved_entry.items_json = json.dumps(snapshot_payload, ensure_ascii=False)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to persist import log entry")
        return _redirect_with_status("error", "Failed to record the import in the log.")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover - best effort
                logger.warning("Could not close database connection after import confirmation")

    if saved_entry is None:
        return _redirect_with_status("error", "Import was not recorded.")

    if apply_result is None:
        status = "error"
        message = (
            f"Import #{saved_entry.id} recorded but no stock changes were applied."
            if saved_entry.id
            else "Import recorded but no stock changes were applied."
        )
    elif apply_result.errors or apply_result.total_operations == 0:
        status = "error"
        if apply_result.errors:
            first_error = apply_result.errors[0]
            details = f"row {first_error.row_index}: {first_error.message}"
        else:
            details = "no applicable rows found"
        message = (
            f"Import #{saved_entry.id} applied with issues ({details})."
            if saved_entry.id
            else f"Import applied with issues ({details})."
        )
        if apply_result.total_operations:
            message += f" {apply_result.total_operations} stock updates were executed before the error."
    else:
        status = "applied"
        affected_products = len(apply_result.affected_products)
        affected_locations = len(apply_result.affected_locations)
        message = (
            f"Import #{saved_entry.id} applied successfully: "
            f"{apply_result.total_operations} operations across {affected_products} products and "
            f"{affected_locations} locations."
            if saved_entry.id
            else (
                f"Import applied successfully: {apply_result.total_operations} operations across "
                f"{affected_products} products and {affected_locations} locations."
            )
    return redirect(url_for("supply.supply_home", status=status, message=message))


@blueprint.post("/<int:import_id>/revert")
@require_superadmin
def revert_import(import_id: int) -> Response:
    """Revert a previously applied import using the stored snapshot."""

    conn: sqlite3.Connection | None = None
    revert_result: ImportProcessResult | None = None
    try:
        conn = db()
        repo = SQLiteImportLogRepo(conn)
        entry = repo.get(import_id)
        if entry is None:
            return _redirect_with_status("error", "Import entry was not found.")
        if entry.reverted_at:
            return _redirect_with_status("error", "Import has already been reverted.")

        try:
            snapshot = _snapshot_from_entry(entry)
        except ValueError as exc:
            return _redirect_with_status("error", f"Cannot revert import #{import_id}: {exc}")

        applier = _make_import_applier(conn)
        revert_result = asyncio.run(applier.revert(snapshot, import_id=import_id))
        if revert_result.errors:
            first_error = revert_result.errors[0]
            details = f"row {first_error.row_index}: {first_error.message}"
            return _redirect_with_status(
                "error",
                f"Failed to revert import #{import_id}: {details}",
            )

        repo.mark_reverted(import_id)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to revert import %s", import_id)
        return _redirect_with_status("error", "Could not revert the selected import.")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover - best effort
                logger.warning("Could not close database connection after revert operation")

    if revert_result is None:
        return _redirect_with_status("error", "Revert result is unavailable.")

    operations = revert_result.total_operations
    affected_products = len(revert_result.affected_products)
    affected_locations = len(revert_result.affected_locations)
    if operations == 0:
        message = f"Import #{import_id} marked as reverted. No stock adjustments were required."
    else:
        message = (
            f"Import #{import_id} reverted: {operations} operations across {affected_products} products "
            f"and {affected_locations} locations."
        )

    return redirect(url_for("supply.supply_home", status="reverted", message=message))


def _build_preview_context(
    batch: ImportBatch,
    *,
    stored_path: Path,
    import_type: str,
    supplier: str,
    invoice: str,
    delimiter: str | None,
    sheet_name: str | None,
) -> PreviewContext:
    rows = batch.normalised_rows()
    sample = tuple(rows[:_PREVIEW_SAMPLE_ROWS])
    headers: Iterable[str]
    if sample:
        headers = sample[0].keys()
    else:
        headers = batch.columns.as_dict().keys()

    config = _get_config()
    stored_ref = _relativise_path(stored_path, config.uploads_dir)

    return PreviewContext(
        original_name=batch.original_name,
        stored_path=stored_ref,
        stored_filename=stored_path.name,
        import_type=import_type,
        source_hash=batch.source_hash,
        row_count=len(rows),
        headers=tuple(headers),
        sample_rows=sample,
        column_mapping=batch.columns.as_dict(),
        supplier=supplier,
        invoice=invoice,
        delimiter=delimiter,
        sheet_name=sheet_name,
    )


def _resolve_import_type(requested: str, stored_path: Path) -> str:
    if requested in {"csv", "excel"}:
        return requested

    suffix = stored_path.suffix.lower()
    if suffix in {".xls", ".xlsx", ".xlsm", ".xlsb"}:
        return "excel"
    return "csv"


def _parse_batch(
    facade: ImportFacade,
    import_type: str,
    stored_path: Path,
    *,
    original_name: str,
    delimiter: str | None,
    sheet_name: str | None,
) -> ImportBatch:
    if import_type == "excel":
        return facade.from_excel(str(stored_path), original_name=original_name, sheet_name=sheet_name)
    if import_type == "csv":
        return facade.from_csv(str(stored_path), original_name=original_name, delimiter=delimiter)
    raise ValueError(f"Unsupported import type: {import_type}")


def _persist_upload(upload: FileStorage, config: Config) -> Path:
    filename = secure_filename(upload.filename or "upload")
    if not filename:
        filename = "upload"
    unique_name = f"{uuid4().hex}_{filename}"
    target = config.uploads_dir / unique_name
    target.parent.mkdir(parents=True, exist_ok=True)
    upload.save(target)
    return target


def _resolve_uploaded_path(reference: str, config: Config) -> Path:
    candidate = (config.uploads_dir / reference).resolve()
    uploads_root = config.uploads_dir.resolve()
    if uploads_root not in candidate.parents and candidate != uploads_root:
        raise ValueError("Upload reference escapes the uploads directory")
    if not candidate.exists():
        raise ValueError("Uploaded file no longer exists")
    return candidate


def _relativise_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.name)


def _get_config() -> Config:
    return current_app.config["DVORIK_CONFIG"]


def _get_facade(config: Config) -> ImportFacade:
    return ImportFacade(storage_dir=config.normalized_uploads_dir)


def _load_recent_imports(limit: int = 15):
    conn = None
    try:
        conn = db()
        repo = SQLiteImportLogRepo(conn)
        entries = list(repo.latest(limit))
        return entries
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to load recent import entries")
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover - best effort
                logger.warning("Could not close database connection after loading imports")


def _make_import_applier(conn: sqlite3.Connection) -> ImportBatchApplier:
    config = _get_config()
    actor_username = request.headers.get("X-Actor") or config.super_admin_username
    return ImportBatchApplier(
        conn,
        user_id=config.super_admin_id,
        actor_id=config.super_admin_id,
        actor_username=actor_username,
        remote_addr=request.remote_addr,
    )


def _update_import_snapshot(
    conn: sqlite3.Connection, import_id: int, snapshot: Iterable[Mapping[str, Any]]
) -> None:
    payload = json.dumps(list(snapshot), ensure_ascii=False)
    with conn:
        conn.execute(
            """
            UPDATE import_log
            SET items_json = :items_json
            WHERE id = :import_id
            """,
            {"items_json": payload, "import_id": import_id},
        )


def _snapshot_from_entry(entry: ImportLogEntry) -> list[Mapping[str, Any]]:
    if not entry.items_json:
        raise ValueError("snapshot is not available")
    try:
        data = json.loads(entry.items_json)
    except json.JSONDecodeError as exc:
        raise ValueError("snapshot data is corrupted") from exc
    if not isinstance(data, list):
        raise ValueError("snapshot data has unexpected format")
    snapshot: list[Mapping[str, Any]] = []
    for item in data:
        if isinstance(item, Mapping):
            snapshot.append(dict(item))
        else:
            raise ValueError("snapshot contains unexpected values")
    return snapshot


def _redirect_with_status(status: str, message: str | None = None) -> Response:
    return redirect(url_for("supply.supply_home", status=status, message=message))


def _extract_status() -> tuple[str | None, str | None]:
    status = request.args.get("status")
    message = request.args.get("message")
    if status == "error" and not message:
        message = "An unexpected error occurred."
    return status, message


__all__ = ["blueprint"]
