from __future__ import annotations

import datetime as dt

from app import config as app_config


def local_now() -> dt.datetime:
    """Return current datetime in the configured local timezone."""
    tz = getattr(app_config, "LOCAL_TZ", None)
    if tz is None:
        # Fallback to system local time if timezone is missing in config
        return dt.datetime.now().astimezone()
    return dt.datetime.now(tz)


def local_today() -> dt.date:
    """Return today's date in the configured local timezone."""
    return local_now().date()
