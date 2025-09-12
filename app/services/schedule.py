from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict

import sqlite3

from app import db as app_db


@dataclass
class Seller:
    tg_id: int
    username: Optional[str]
    display_name: Optional[str]


def _conn() -> sqlite3.Connection:
    return app_db.db()


def list_sellers(conn: Optional[sqlite3.Connection] = None) -> List[Seller]:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        # Distinct by tg_id if duplicates
        rows = conn.execute(
            """
            SELECT MIN(id) as id, tg_id,
                   MIN(username) AS username,
                   COALESCE(MAX(display_name), MIN(username)) AS display_name
            FROM user_role
            WHERE role='seller' AND tg_id IS NOT NULL
            GROUP BY tg_id
            ORDER BY MIN(id)
            """
        ).fetchall()
        return [Seller(int(r["tg_id"]), r["username"], r["display_name"]) for r in rows if r["tg_id"] is not None]
    finally:
        if own:
            conn.close()


def is_open(date: dt.date, conn: Optional[sqlite3.Connection] = None) -> bool:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        r = conn.execute("SELECT is_open FROM schedule_day WHERE date=?", (date.isoformat(),)).fetchone()
        if r is None:
            return True
        return bool(r[0])
    finally:
        if own:
            conn.close()


def set_open(date: dt.date, val: bool, conn: Optional[sqlite3.Connection] = None):
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        with conn:
            conn.execute(
                "INSERT INTO schedule_day(date,is_open) VALUES(?,?)\n                 ON CONFLICT(date) DO UPDATE SET is_open=excluded.is_open",
                (date.isoformat(), 1 if val else 0),
            )
    finally:
        if own:
            conn.close()


def get_assignments(date: dt.date, conn: Optional[sqlite3.Connection] = None) -> List[int]:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        rows = conn.execute(
            "SELECT tg_id FROM schedule_assignment WHERE date=? ORDER BY tg_id",
            (date.isoformat(),)
        ).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        if own:
            conn.close()


def set_assignment(date: dt.date, tg_id: int, source: str = 'admin', conn: Optional[sqlite3.Connection] = None) -> bool:
    """Add assignment for a date. No limit on the number of people per day.
    Returns True if operation attempted (idempotent via OR IGNORE)."""
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES(?,?,?)",
                (date.isoformat(), tg_id, source)
            )
        return True
    finally:
        if own:
            conn.close()


def remove_assignment(date: dt.date, tg_id: int, conn: Optional[sqlite3.Connection] = None) -> bool:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        with conn:
            cur = conn.execute(
                "DELETE FROM schedule_assignment WHERE date=? AND tg_id=?",
                (date.isoformat(), tg_id)
            )
        return cur.rowcount > 0
    finally:
        if own:
            conn.close()


def _open_days_between(start: dt.date, end: dt.date, conn: sqlite3.Connection) -> List[dt.date]:
    days: List[dt.date] = []
    d = start
    while d <= end:
        if is_open(d, conn):
            days.append(d)
        d += dt.timedelta(days=1)
    return days


def compute_anchor(conn: sqlite3.Connection) -> Optional[Tuple[dt.date, List[int], dt.date, List[int]]]:
    """Try to detect the latest anchor (two consecutive open days having exactly 2 workers)."""
    row = conn.execute("SELECT start_date FROM schedule_anchor ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        return None
    d0 = dt.date.fromisoformat(row[0])
    # Find next open date >= d0
    d = d0
    while not is_open(d, conn):
        d += dt.timedelta(days=1)
    a0 = get_assignments(d, conn)
    if len(a0) != 2:
        return None
    # Next open day
    d1 = d + dt.timedelta(days=1)
    while not is_open(d1, conn):
        d1 += dt.timedelta(days=1)
    a1 = get_assignments(d1, conn)
    if len(a1) != 2:
        return None
    return (d, a0, d1, a1)


def generate_schedule(month_start: dt.date, months: int = 1, conn: Optional[sqlite3.Connection] = None) -> Tuple[int, int]:
    """Generate/refresh assignments for given months using current anchor.
    Respects manual edits: does not overwrite existing assignments for a date
    if there are already 2 people assigned; fills only missing ones.
    Returns: (days_filled, assignments_added)
    """
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        anchor = compute_anchor(conn)
        if not anchor:
            return (0, 0)
        d0, a0, d1, a1 = anchor
        sellers = list_sellers(conn)
        seller_ids = [s.tg_id for s in sellers]
        n = len(seller_ids)
        if n not in (3, 4):
            return (0, 0)
        # Determine period
        year = month_start.year
        month = month_start.month
        # End date is last day of (month_start + months - 1)
        def _last_day(y: int, m: int) -> int:
            if m == 12:
                return 31
            return (dt.date(y, m + 1, 1) - dt.timedelta(days=1)).day
        # Compute end date across months
        end = month_start
        for i in range(months - 1):
            # jump to first day of next month
            if end.month == 12:
                end = dt.date(end.year + 1, 1, 1)
            else:
                end = dt.date(end.year, end.month + 1, 1)
        end = dt.date(end.year, end.month, _last_day(end.year, end.month))

        open_days = _open_days_between(month_start, end, conn)
        if not open_days:
            return (0, 0)

        # Establish index i for open days starting at anchor start day d0 index 0
        # Build a mapping date->i offset relative to d0
        # Iterate from min(d0, first open day in period) backward/forward to compute offsets
        # For simplicity: iterate from the earliest among (d0, month_start)
        # and build sequence onward; for i<0 we won't fill.
        days_filled = 0
        assigns_added = 0

        # Create a generator for who works at open-day index i
        if n == 3:
            # Off-cycle order from anchor
            all_ids = set(seller_ids)
            off0 = list(all_ids - set(a0))
            off1 = list(all_ids - set(a1))
            if len(off0) != 1 or len(off1) != 1:
                return (0, 0)
            # Determine off2 as the remaining one
            off2 = list(all_ids - {off0[0], off1[0]})
            if len(off2) != 1:
                return (0, 0)
            off_cycle = [off0[0], off1[0], off2[0]]

            def on_for_index(i: int) -> List[int]:
                off = off_cycle[i % 3]
                return [x for x in seller_ids if x != off]

        else:  # n == 4
            lane1 = list(a0)
            # Prefer a disjoint lane2; if a1 overlaps, use complement of lane1
            if len(set(a0).intersection(a1)) == 0 and len(set(a1)) == 2:
                lane2 = list(a1)
            else:
                # Build lane2 from the remaining sellers to satisfy 2/2 rotation
                lane2 = [x for x in seller_ids if x not in set(lane1)]

            def on_for_index(i: int) -> List[int]:
                # blocks of two days per lane
                block = (i // 2) % 2
                return lane1 if block == 0 else lane2

        # Determine i for each open day in the period: i=0 on d0, i increments by 1 per open day
        # First find the first open day on/after d0
        # Build a list of open days covering from min(d0, month_start) up to end
        # and compute i for each.
        # Find i0 for the first open day in sequence at/after d0
        # We'll compute offsets by counting open days between d0 and target day.
        def open_day_index(target: dt.date) -> Optional[int]:
            # count open days from d0 to target (inclusive), increasing i by 1 per open day, with i=0 at d0_open
            # Find first open day at/after d0
            d = d0
            # move to first open day >= d0
            while not is_open(d, conn):
                d += dt.timedelta(days=1)
            i = 0
            while d < target:
                d += dt.timedelta(days=1)
                if is_open(d, conn):
                    i += 1
            if d == target:
                return i
            # If target is before d0, return None (we don't backfill before anchor)
            return None

        for day in open_days:
            idx = open_day_index(day)
            if idx is None:
                continue
            current = get_assignments(day, conn)
            want = on_for_index(idx)
            # If already assigned two, respect manual/admin edits and skip
            if len(current) >= 2:
                continue
            # Fill missing ones from desired list in order
            to_add = [x for x in want if x not in current]
            for tg in to_add:
                if len(current) >= 2:
                    break
                if set_assignment(day, tg, source='auto', conn=conn):
                    current.append(tg)
                    assigns_added += 1
            days_filled += 1

        return (days_filled, assigns_added)
    finally:
        if own:
            conn.close()


def generate_schedule_range(
    start_date: dt.date,
    days: int = 30,
    override: bool = True,
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[int, int]:
    """Generate or refresh assignments for a fixed range starting from start_date.
    Uses the current anchor (two consecutive open days with two workers each) to
    deduce rotation rules: 2/1 for 3 sellers, 2/2 for 4 sellers.

    - When override=True: for each open day in range (except the two anchor days),
      set assignments to exactly the desired pair (delete existing then insert).
    - When override=False: fill only missing assignments; do not alter days with 2.

    Returns: (days_processed, assignments_changed)
    """
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        anchor = compute_anchor(conn)
        if not anchor:
            return (0, 0)
        d0, a0, d1, a1 = anchor
        sellers = list_sellers(conn)
        seller_ids = [s.tg_id for s in sellers]
        n = len(seller_ids)
        if n not in (3, 4):
            return (0, 0)

        end_date = start_date + dt.timedelta(days=max(0, days - 1))
        open_days = _open_days_between(start_date, end_date, conn)
        if not open_days:
            return (0, 0)

        # Rotation rules
        if n == 3:
            all_ids = set(seller_ids)
            off0 = list(all_ids - set(a0))
            off1 = list(all_ids - set(a1))
            if len(off0) != 1 or len(off1) != 1:
                return (0, 0)
            off2 = list(all_ids - {off0[0], off1[0]})
            if len(off2) != 1:
                return (0, 0)
            off_cycle = [off0[0], off1[0], off2[0]]

            def on_for_index(i: int) -> List[int]:
                off = off_cycle[i % 3]
                return [x for x in seller_ids if x != off]
        else:  # n == 4
            lane1 = list(a0)
            # Build lane2 as disjoint from lane1; if anchor day2 already disjoint, accept it
            if len(set(a0).intersection(a1)) == 0 and len(set(a1)) == 2:
                lane2 = list(a1)
            else:
                lane2 = [x for x in seller_ids if x not in set(lane1)]

            def on_for_index(i: int) -> List[int]:
                block = (i // 2) % 2
                return lane1 if block == 0 else lane2

        # Determine index of open days starting from first open day at/after d0 (index 0)
        def open_day_index(target: dt.date) -> Optional[int]:
            d = d0
            while not is_open(d, conn):
                d += dt.timedelta(days=1)
            i = 0
            while d < target:
                d += dt.timedelta(days=1)
                if is_open(d, conn):
                    i += 1
            if d == target:
                return i
            return None

        days_proc = 0
        changed = 0
        anchor_days = {d0, d1}
        for day in open_days:
            idx = open_day_index(day)
            if idx is None:
                continue
            want = on_for_index(idx)

            # For anchor days, never override admin selection
            if day in anchor_days:
                if not override:
                    # may fill if missing
                    current = get_assignments(day, conn)
                    to_add = [x for x in want if x not in current]
                    for tg in to_add:
                        if len(current) >= 2:
                            break
                        if set_assignment(day, tg, source='auto', conn=conn):
                            current.append(tg)
                            changed += 1
                days_proc += 1
                continue

            if override:
                with conn:
                    conn.execute("DELETE FROM schedule_assignment WHERE date=?", (day.isoformat(),))
                for tg in want:
                    if set_assignment(day, tg, source='auto', conn=conn):
                        changed += 1
                days_proc += 1
            else:
                current = get_assignments(day, conn)
                if len(current) >= 2:
                    days_proc += 1
                    continue
                to_add = [x for x in want if x not in current]
                for tg in to_add:
                    if len(current) >= 2:
                        break
                    if set_assignment(day, tg, source='auto', conn=conn):
                        current.append(tg)
                        changed += 1
                days_proc += 1

        return (days_proc, changed)
    finally:
        if own:
            conn.close()


def propose_transfer(date: dt.date, initiator_tg: int, target_tg: int, conn: Optional[sqlite3.Connection] = None) -> Tuple[bool, str, Optional[int]]:
    """Create a transfer (single-day replace) request. Returns (ok, msg, request_id)."""
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        if not is_open(date, conn):
            return False, "День помечен как нерабочий", None
        ass = get_assignments(date, conn)
        both_on = initiator_tg in ass and target_tg in ass
        if both_on:
            return False, "Нельзя меняться с напарником в день, когда вы оба работаете", None
        # Allowed if exactly one of them is ON
        if (initiator_tg in ass) == (target_tg in ass):
            # both OFF or both ON rejected
            return False, "Можно предлагать только тому, кто в этот день не работает (или работает вместо вас)", None
        expires = (dt.datetime.now() + dt.timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
        with conn:
            cur = conn.execute(
                "INSERT INTO schedule_transfer_request(date, from_tg_id, to_tg_id, expires_at) VALUES (?,?,?,?)",
                (date.isoformat(), initiator_tg, target_tg, expires)
            )
            rid = cur.lastrowid
        return True, "Заявка отправлена", int(rid)
    finally:
        if own:
            conn.close()


def apply_transfer(req_id: int, accept: bool, conn: Optional[sqlite3.Connection] = None) -> Tuple[bool, str, Optional[Tuple[dt.date, int, int]]]:
    """Accept/decline transfer request. If accepted, perform replacement and return details (date, from, to)."""
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        row = conn.execute(
            "SELECT id, date, from_tg_id, to_tg_id, status FROM schedule_transfer_request WHERE id=?",
            (req_id,)
        ).fetchone()
        if not row:
            return False, "Заявка не найдена", None
        if row["status"] != "pending":
            return False, "Заявка уже обработана", None
        date = dt.date.fromisoformat(row["date"])  # type: ignore
        if not is_open(date, conn):
            with conn:
                conn.execute("UPDATE schedule_transfer_request SET status='cancelled' WHERE id=?", (req_id,))
            return False, "День стал нерабочим", None
        if not accept:
            with conn:
                conn.execute("UPDATE schedule_transfer_request SET status='declined' WHERE id=?", (req_id,))
            return True, "Отклонено", None
        # Accept: perform replacement
        from_tg = int(row["from_tg_id"])  # the initiator
        to_tg = int(row["to_tg_id"])      # the responder
        ass = get_assignments(date, conn)
        # validate invariant: exactly one of them must be ON now
        if (from_tg in ass) == (to_tg in ass):
            with conn:
                conn.execute("UPDATE schedule_transfer_request SET status='cancelled' WHERE id=?", (req_id,))
            return False, "Состав на дату изменился, заявка отменена", None
        with conn:
            if from_tg in ass:
                conn.execute(
                    "DELETE FROM schedule_assignment WHERE date=? AND tg_id=?",
                    (date.isoformat(), from_tg)
                )
                conn.execute(
                    "INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES (?,?,?)",
                    (date.isoformat(), to_tg, 'transfer')
                )
            else:
                conn.execute(
                    "DELETE FROM schedule_assignment WHERE date=? AND tg_id=?",
                    (date.isoformat(), to_tg)
                )
                conn.execute(
                    "INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES (?,?,?)",
                    (date.isoformat(), from_tg, 'transfer')
                )
            conn.execute("UPDATE schedule_transfer_request SET status='accepted' WHERE id=?", (req_id,))
        return True, "Готово", (date, from_tg, to_tg)
    finally:
        if own:
            conn.close()


def swap_employees_globally(emp_a: int, emp_b: int, start_date: Optional[dt.date] = None, conn: Optional[sqlite3.Connection] = None) -> int:
    """Swap all future assignments between two tg_id from start_date (default today). Returns count of affected rows."""
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        if start_date is None:
            start_date = dt.date.today()
        d = start_date.isoformat()
        # For each date with either emp assigned, swap presence.
        rows = conn.execute(
            "SELECT date FROM schedule_assignment WHERE date>=? AND (tg_id=? OR tg_id=?) GROUP BY date",
            (d, emp_a, emp_b)
        ).fetchall()
        changed = 0
        with conn:
            for r in rows:
                day = r["date"]
                a_on = conn.execute("SELECT 1 FROM schedule_assignment WHERE date=? AND tg_id=?", (day, emp_a)).fetchone() is not None
                b_on = conn.execute("SELECT 1 FROM schedule_assignment WHERE date=? AND tg_id=?", (day, emp_b)).fetchone() is not None
                if a_on and not b_on:
                    conn.execute("DELETE FROM schedule_assignment WHERE date=? AND tg_id=?", (day, emp_a))
                    conn.execute("INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES (?,?,?)", (day, emp_b, 'admin'))
                    changed += 1
                elif b_on and not a_on:
                    conn.execute("DELETE FROM schedule_assignment WHERE date=? AND tg_id=?", (day, emp_b))
                    conn.execute("INSERT OR IGNORE INTO schedule_assignment(date,tg_id,source) VALUES (?,?,?)", (day, emp_a, 'admin'))
                    changed += 1
                # if both on or both off, no change
        return changed
    finally:
        if own:
            conn.close()


def toggle_day_open(date: dt.date, conn: Optional[sqlite3.Connection] = None) -> bool:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        open_now = is_open(date, conn)
        set_open(date, not open_now, conn)
        return not open_now
    finally:
        if own:
            conn.close()


def next_open_day(after: dt.date, conn: Optional[sqlite3.Connection] = None) -> dt.date:
    own = False
    if conn is None:
        conn = _conn(); own = True
    try:
        d = after + dt.timedelta(days=1)
        while not is_open(d, conn):
            d += dt.timedelta(days=1)
        return d
    finally:
        if own:
            conn.close()
