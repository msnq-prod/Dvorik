"""Lightweight cooperative scheduler used by the new Dvorik stack.

The scheduler keeps an in-memory registry of jobs and periodically
executes due callbacks in the running asyncio event loop.  Jobs can be
registered either as "daily" tasks (triggered once a day at the provided
time) or via a cron expression with minute-level precision.

The implementation favours simplicity and determinism: schedules are
calculated eagerly when jobs are registered and after each execution.
Errors inside job callbacks are caught and logged so that one faulty job
does not break the scheduler loop.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import logging
from typing import Awaitable, Callable, Dict, Iterable, Optional, Sequence

logger = logging.getLogger(__name__)

Callback = Callable[[], Awaitable[object] | object]


def _ensure_timezone(dt: _dt.datetime, tz: _dt.tzinfo | None) -> _dt.datetime:
    """Return ``dt`` as an aware datetime bound to ``tz``.

    If ``dt`` is naive it will be assigned the supplied timezone (or UTC
    if ``tz`` is ``None``).  When ``dt`` already carries timezone
    information it will be converted to ``tz`` when provided.
    """

    tz = tz or _dt.timezone.utc
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


class Schedule:
    """Base class for supported schedules."""

    def next_after(self, moment: _dt.datetime) -> _dt.datetime:  # pragma: no cover - interface
        raise NotImplementedError


@dataclasses.dataclass(slots=True)
class DailySchedule(Schedule):
    """Run a job once a day at a specific local time."""

    at: _dt.time
    tzinfo: _dt.tzinfo | None = None

    def next_after(self, moment: _dt.datetime) -> _dt.datetime:
        tz = self.tzinfo or _dt.timezone.utc
        local_now = _ensure_timezone(moment, tz)
        candidate = _dt.datetime.combine(local_now.date(), self.at, tz)
        if candidate <= local_now:
            candidate += _dt.timedelta(days=1)
        return candidate.astimezone(_dt.timezone.utc)


def _expand_range(
    base: str, *, minimum: int, maximum: int, step: int
) -> Iterable[int]:
    if base in {"*", ""}:
        start, end = minimum, maximum
    elif "-" in base:
        left, right = base.split("-", 1)
        start, end = int(left), int(right)
    else:
        value = int(base)
        start = end = value
    if start < minimum or end > maximum:
        raise ValueError("cron field outside supported range")
    if step <= 0:
        raise ValueError("cron step must be positive")
    return range(start, end + 1, step)


def _parse_field(spec: str, *, minimum: int, maximum: int, allow_seven: bool = False) -> Optional[Sequence[int]]:
    spec = spec.strip()
    if spec == "*":
        return None
    values: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            raise ValueError("empty cron token")
        if "/" in part:
            base, step_str = part.split("/", 1)
            step = int(step_str)
        else:
            base, step = part, 1
        for value in _expand_range(base, minimum=minimum, maximum=maximum, step=step):
            if allow_seven and value == 7:
                values.add(0)
            else:
                values.add(value)
    return tuple(sorted(values))


@dataclasses.dataclass(slots=True)
class CronSchedule(Schedule):
    """Cron-style minute precision schedule."""

    minute: Sequence[int] | None
    hour: Sequence[int] | None
    day: Sequence[int] | None
    month: Sequence[int] | None
    weekday: Sequence[int] | None
    tzinfo: _dt.tzinfo | None = None
    _max_iterations: int = dataclasses.field(default=525_600, init=False, repr=False)

    @classmethod
    def from_expression(cls, expression: str, tzinfo: _dt.tzinfo | None = None) -> "CronSchedule":
        tokens = expression.split()
        if len(tokens) != 5:
            raise ValueError("cron expression must contain 5 fields")
        minute = _parse_field(tokens[0], minimum=0, maximum=59)
        hour = _parse_field(tokens[1], minimum=0, maximum=23)
        day = _parse_field(tokens[2], minimum=1, maximum=31)
        month = _parse_field(tokens[3], minimum=1, maximum=12)
        weekday = _parse_field(tokens[4], minimum=0, maximum=6, allow_seven=True)
        return cls(minute, hour, day, month, weekday, tzinfo)

    def _matches(self, dt: _dt.datetime) -> bool:
        minute = dt.minute
        hour = dt.hour
        day = dt.day
        month = dt.month
        weekday = dt.weekday()
        if self.minute is not None and minute not in self.minute:
            return False
        if self.hour is not None and hour not in self.hour:
            return False
        if self.day is not None and day not in self.day:
            return False
        if self.month is not None and month not in self.month:
            return False
        if self.weekday is not None and weekday not in self.weekday:
            return False
        return True

    def next_after(self, moment: _dt.datetime) -> _dt.datetime:
        tz = self.tzinfo or _dt.timezone.utc
        local_now = _ensure_timezone(moment, tz)
        candidate = local_now.replace(second=0, microsecond=0)
        if candidate <= local_now:
            candidate += _dt.timedelta(minutes=1)
        for _ in range(self._max_iterations):
            if self._matches(candidate):
                return candidate.astimezone(_dt.timezone.utc)
            candidate += _dt.timedelta(minutes=1)
        raise RuntimeError("unable to resolve next cron execution within a year")


@dataclasses.dataclass(slots=True)
class ScheduledJob:
    """Internal representation of a scheduled job."""

    name: str
    callback: Callback
    schedule: Schedule
    next_run: _dt.datetime | None = None

    def compute_next_run(self, *, reference: _dt.datetime | None = None) -> None:
        reference = reference or _dt.datetime.now(_dt.timezone.utc)
        self.next_run = self.schedule.next_after(reference)


_jobs: Dict[str, ScheduledJob] = {}
_heartbeat: _dt.datetime | None = None


def _update_heartbeat(moment: _dt.datetime | None = None) -> None:
    """Record the most recent moment the scheduler loop was observed alive."""

    global _heartbeat
    _heartbeat = moment or _dt.datetime.now(_dt.timezone.utc)


def heartbeat() -> _dt.datetime | None:
    """Return the timestamp of the last observed scheduler activity."""

    return _heartbeat


def heartbeat_age(reference: _dt.datetime | None = None) -> _dt.timedelta | None:
    """Return how long ago the scheduler loop was last seen alive."""

    if _heartbeat is None:
        return None
    reference = reference or _dt.datetime.now(_dt.timezone.utc)
    return reference - _heartbeat


def register_daily(
    name: str, callback: Callback, at: _dt.time | str, *, tzinfo: _dt.tzinfo | None = None
) -> ScheduledJob:
    """Register a new job executed once per day at the specified time."""

    if isinstance(at, str):
        at = _dt.time.fromisoformat(at)
    schedule = DailySchedule(at=at, tzinfo=tzinfo)
    job = ScheduledJob(name=name, callback=callback, schedule=schedule)
    job.compute_next_run()
    _jobs[name] = job
    logger.debug("Registered daily job %s -> %s", name, job.next_run)
    return job


def register_cron(
    name: str,
    callback: Callback,
    expression: str,
    *,
    tzinfo: _dt.tzinfo | None = None,
) -> ScheduledJob:
    """Register a cron-style job with minute precision."""

    schedule = CronSchedule.from_expression(expression, tzinfo=tzinfo)
    job = ScheduledJob(name=name, callback=callback, schedule=schedule)
    job.compute_next_run()
    _jobs[name] = job
    logger.debug("Registered cron job %s -> %s (%s)", name, job.next_run, expression)
    return job


async def _execute_job(job: ScheduledJob) -> None:
    try:
        result = job.callback()
        if asyncio.iscoroutine(result):
            await result
    except Exception:  # noqa: BLE001 - we want to guard the scheduler loop
        logger.exception("Scheduled job %s raised an exception", job.name)


async def run_forever(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Run the scheduler loop indefinitely."""

    if loop is None:
        loop = asyncio.get_running_loop()

    logger.info("Scheduler loop started")
    try:
        while True:
            now = _dt.datetime.now(_dt.timezone.utc)
            _update_heartbeat(now)

            jobs = list(_jobs.values())

            if not jobs:
                await asyncio.sleep(1)
                continue

            due_jobs = [job for job in jobs if job.next_run and job.next_run <= now]

            if due_jobs:
                for job in due_jobs:
                    await _execute_job(job)
                    _update_heartbeat()
                    job.compute_next_run(reference=now)
                continue

            next_run = min(job.next_run for job in jobs if job.next_run is not None)
            sleep_for = max((next_run - now).total_seconds(), 0.1)
            await asyncio.sleep(min(sleep_for, 60))
    except asyncio.CancelledError:  # pragma: no cover - forwarded cancellation
        logger.info("Scheduler loop cancelled")
        raise
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Scheduler loop terminated unexpectedly")
        raise


def registered_jobs() -> Dict[str, ScheduledJob]:
    """Return a snapshot of registered jobs."""

    return dict(_jobs)


def is_alive(max_age: _dt.timedelta | float | int | None = None) -> bool:
    """Return ``True`` when the scheduler heartbeat is within ``max_age``."""

    hb = heartbeat()
    if hb is None:
        return False

    if max_age is None:
        return True

    if isinstance(max_age, (int, float)):
        max_age = _dt.timedelta(seconds=float(max_age))

    age = heartbeat_age()
    if age is None:
        return False

    return age <= max_age

