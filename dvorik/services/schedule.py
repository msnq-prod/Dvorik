from __future__ import annotations

import datetime as dt
import sqlite3
from collections.abc import Sequence
from typing import Dict, List, Mapping, MutableMapping

from dvorik.domain.models import ScheduleAssignment, ScheduleDay
from dvorik.domain.ports import ScheduleRepo

__all__ = ["ScheduleService"]


class ScheduleService:
    """Provides helpers to manage the staff schedule."""

    def __init__(self, conn: sqlite3.Connection, repo: ScheduleRepo) -> None:
        self._conn = conn
        self._repo = repo

    # ------------------------------------------------------------------
    # Day management
    # ------------------------------------------------------------------
    def upsert_day(self, date: str, *, is_open: bool = True, notes: str | None = None) -> ScheduleDay:
        """Create or update the ``schedule_day`` record for ``date``."""

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO schedule_day(date, is_open, notes)
                VALUES (:date, :is_open, :notes)
                ON CONFLICT(date) DO UPDATE SET
                    is_open = excluded.is_open,
                    notes = excluded.notes
                """,
                {"date": date, "is_open": 1 if is_open else 0, "notes": notes},
            )
        day = self._repo.day(date)
        if day is None:  # pragma: no cover - defensive fallback
            day = ScheduleDay(date=date, is_open=is_open, notes=notes)
        return day

    # ------------------------------------------------------------------
    # Assignment management
    # ------------------------------------------------------------------
    def assign_user(self, date: str, tg_id: int, *, source: str = "manual") -> None:
        """Assign ``tg_id`` to ``date`` with the provided ``source`` label."""

        self.upsert_day(date)
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO schedule_assignment(date, tg_id, source)
                VALUES (:date, :tg_id, :source)
                ON CONFLICT(date, tg_id) DO UPDATE SET source = excluded.source
                """,
                {"date": date, "tg_id": tg_id, "source": source},
            )

    def remove_assignment(self, date: str, tg_id: int) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM schedule_assignment WHERE date = :date AND tg_id = :tg_id",
                {"date": date, "tg_id": tg_id},
            )

    def clear_day(self, date: str) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM schedule_assignment WHERE date = :date",
                {"date": date},
            )

    def assignments(self, start_date: str, end_date: str | None = None) -> Sequence[ScheduleAssignment]:
        return self._repo.assignments(start_date, end_date)

    def assignments_for_month(self, month: str) -> Sequence[ScheduleAssignment]:
        return self._repo.assignments_for_month(month)

    # ------------------------------------------------------------------
    # Generation helpers
    # ------------------------------------------------------------------
    def generate_period(
        self,
        start_date: str,
        *,
        days: int,
        pattern: Mapping[int, Sequence[int]],
        source: str = "auto",
    ) -> Sequence[ScheduleAssignment]:
        """Generate assignments for ``days`` using a weekday ``pattern``."""

        if days <= 0:
            raise ValueError("days must be positive")

        start = dt.date.fromisoformat(start_date)
        for offset in range(days):
            current = start + dt.timedelta(days=offset)
            weekday = current.weekday()
            target_date = current.isoformat()
            tg_ids = pattern.get(weekday, ())

            self.upsert_day(target_date)
            with self._conn:
                self._conn.execute(
                    "DELETE FROM schedule_assignment WHERE date = :date AND source = :source",
                    {"date": target_date, "source": source},
                )
                for tg_id in tg_ids:
                    self._conn.execute(
                        """
                        INSERT OR REPLACE INTO schedule_assignment(date, tg_id, source)
                        VALUES (:date, :tg_id, :source)
                        """,
                        {"date": target_date, "tg_id": tg_id, "source": source},
                    )
        end_date = (start + dt.timedelta(days=max(days - 1, 0))).isoformat()
        return self._repo.assignments(start.isoformat(), end_date)

    def ensure_anchor(self, start_date: str) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO schedule_anchor(start_date)
                VALUES (:start_date)
                """,
                {"start_date": start_date},
            )

    def remove_anchor(self, start_date: str) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM schedule_anchor WHERE start_date = :start_date",
                {"start_date": start_date},
            )

    # ------------------------------------------------------------------
    # Aggregated helpers
    # ------------------------------------------------------------------
    def overview(self, start_date: str, end_date: str | None = None) -> MutableMapping[str, Sequence[int]]:
        """Return assignments grouped by date between the provided bounds."""

        assignments = self._repo.assignments(start_date, end_date)
        grouped: Dict[str, List[int]] = {}
        for item in assignments:
            grouped.setdefault(item.date, []).append(item.tg_id)
        return grouped
