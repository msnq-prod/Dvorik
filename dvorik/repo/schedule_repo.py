"""SQLite-backed implementation of :class:`~dvorik.domain.ports.ScheduleRepo`."""

from __future__ import annotations

import sqlite3
from typing import Sequence

from dvorik.db.query_registry import get_query
from dvorik.domain.models import (
    ScheduleAssignment,
    ScheduleDay,
    ScheduleTransferRequest,
)
from dvorik.domain.ports import ScheduleRepo


class SQLiteScheduleRepo(ScheduleRepo):
    """Repository exposing schedule read models."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def day(self, date: str) -> ScheduleDay | None:
        sql = get_query(
            self._conn,
            "repo.schedule.day",
            """
            SELECT
                date,
                is_open,
                notes
            FROM schedule_day
            WHERE date = :date
            """,
        )
        cursor = self._conn.execute(sql, {"date": date})
        row = cursor.fetchone()
        if row is None:
            return None
        return ScheduleDay(
            date=row["date"],
            is_open=bool(row["is_open"]),
            notes=row["notes"],
        )

    def assignments(self, start_date: str, end_date: str | None = None) -> Sequence[ScheduleAssignment]:
        sql = get_query(
            self._conn,
            "repo.schedule.assignments_range",
            """
            SELECT
                date,
                tg_id,
                source,
                created_at
            FROM schedule_assignment
            WHERE date BETWEEN :start_date AND COALESCE(:end_date, :start_date)
            ORDER BY date, tg_id
            """,
        )
        cursor = self._conn.execute(
            sql,
            {"start_date": start_date, "end_date": end_date},
        )
        rows = cursor.fetchall()
        return [
            ScheduleAssignment(
                date=row["date"],
                tg_id=row["tg_id"],
                source=row["source"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def assignments_for_month(self, month: str) -> Sequence[ScheduleAssignment]:
        sql = get_query(
            self._conn,
            "repo.schedule.assignments_month",
            """
            SELECT
                date,
                tg_id,
                source,
                created_at
            FROM schedule_assignment
            WHERE substr(date, 1, 7) = :month
            ORDER BY date, tg_id
            """,
        )
        cursor = self._conn.execute(sql, {"month": month})
        rows = cursor.fetchall()
        return [
            ScheduleAssignment(
                date=row["date"],
                tg_id=row["tg_id"],
                source=row["source"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def transfer_requests(self, *, status: str | None = None) -> Sequence[ScheduleTransferRequest]:
        sql = get_query(
            self._conn,
            "repo.schedule.transfer_requests",
            """
            SELECT
                id,
                date,
                from_tg_id,
                to_tg_id,
                status,
                created_at,
                expires_at
            FROM schedule_transfer_request
            WHERE (:status IS NULL OR status = :status)
            ORDER BY created_at DESC
            """,
        )
        cursor = self._conn.execute(sql, {"status": status})
        rows = cursor.fetchall()
        return [
            ScheduleTransferRequest(
                id=row["id"],
                date=row["date"],
                from_tg_id=row["from_tg_id"],
                to_tg_id=row["to_tg_id"],
                status=row["status"],
                created_at=row["created_at"],
                expires_at=row["expires_at"],
            )
            for row in rows
        ]


__all__ = ["SQLiteScheduleRepo"]
