from __future__ import annotations

"""Composition root wiring shared infrastructure for the Dvorik project."""

import datetime as dt
import logging
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, Mapping, MutableMapping, Sequence

from dvorik.core import events
from dvorik.core.config import Config, get_config
 codex/add-logging-bootstrap-module-with-context
from dvorik.core.logging import bootstrap_logging
from dvorik.core.plugins import load_plugins
from dvorik.core.plugins import PluginDescriptor, load_plugins
from dvorik.core.registry import JobRegistry
from dvorik.core.scheduler import register_daily
from dvorik.db import db, init_db

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from flask import Flask

logger = logging.getLogger(__name__)

AdminAppFactory = Callable[[], "Flask"]
AdminRunner = Callable[..., None]
BotRunner = Callable[[], Awaitable[None]]

_DAILY_TICK_JOB_KEY = "builtin.scheduler.daily_tick"
_DAILY_TICK_EVENT = "scheduler.daily"
_DAILY_TICK_TIME = dt.time(hour=9, minute=0)
_NOTIFICATION_EVENT = "bot.notifications.generated"
_NOTIFICATION_STATE: MutableMapping[str, object] = {}


@dataclass(slots=True)
class DvorikSystem:
    """Container exposing factories for the admin UI and Telegram bot."""

    config: Config
    create_admin_app: AdminAppFactory
    run_admin: AdminRunner
    run_bot: BotRunner


def create_system(*, config: Config | None = None) -> DvorikSystem:
    """Initialise shared services and return runnable application factories."""

    bootstrap_logging()
    config = config or get_config()

    logger.debug("Initialising Dvorik system")
    init_db()


    if config.plugin_disabled:
        plugins = tuple()
        logger.info("Plugin loading disabled via configuration")

    plugins = load_plugins()
    if plugins:
        _run_plugin_migrations(plugins)
        logger.info("Loaded %d plugin(s)", len(plugins))
    else:
        plugins = load_plugins(*config.plugin_paths)
        if plugins:
            logger.info("Loaded %d plugin(s)", len(plugins))
        else:
            logger.info("No plugins discovered")

    _register_admin_components()
    _register_bot_components()
    _register_scheduler_jobs()
    _register_notifications(config)

    return DvorikSystem(
        config=config,
        create_admin_app=_create_admin_app_factory(config),
        run_admin=_create_admin_runner(config),
        run_bot=_create_bot_runner(config),
    )


def _create_admin_app_factory(config: Config) -> AdminAppFactory:
    from dvorik.admin.server import create_app

    def factory() -> "Flask":
        return create_app(config=config)

    return factory


def _create_admin_runner(config: Config) -> AdminRunner:
    factory = _create_admin_app_factory(config)

    def runner(*, host: str = "0.0.0.0", port: int | None = None, debug: bool = True) -> None:
        app = factory()
        final_port = port if port is not None else config.admin_port
        logger.info("Starting admin server on %s:%s", host, final_port)
        app.run(host=host, port=final_port, debug=debug)

    return runner


def _create_bot_runner(config: Config) -> BotRunner:
    from dvorik.bot.main import run_bot

    async def runner() -> None:
        await run_bot(config=config)

    return runner


def _register_admin_components() -> None:
    try:
        from dvorik.admin.widgets import register_builtin_widgets
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to import admin widgets for registration")
        raise

    try:
        register_builtin_widgets()
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to register built-in admin widgets")
        raise


def _register_bot_components() -> None:
    try:
        from dvorik.bot.routers import register_builtin_routers
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to import bot routers for registration")
        raise

    try:
        register_builtin_routers()
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to register built-in bot routers")
        raise


def _register_scheduler_jobs() -> None:
    async def _emit_daily_tick() -> None:
        await events.publish(_DAILY_TICK_EVENT)

    job = register_daily(_DAILY_TICK_JOB_KEY, _emit_daily_tick, at=_DAILY_TICK_TIME)
    JobRegistry.register(_DAILY_TICK_JOB_KEY, job, replace=True)
    logger.debug(
        "Registered daily scheduler job '%s' at %s",
        _DAILY_TICK_JOB_KEY,
        _DAILY_TICK_TIME.isoformat(timespec="minutes"),
    )


def _register_notifications(config: Config) -> None:
    from dvorik.repo.stock_repo import SQLiteStockRepo
    from dvorik.services.notify import (
        notify_instant_thresholds,
        notify_instant_to_skl,
        send_daily_digests,
    )

    existing = _NOTIFICATION_STATE.pop("unsubscribers", None)
    if isinstance(existing, (list, tuple)):
        for unsubscribe in existing:
            try:
                if callable(unsubscribe):
                    unsubscribe()
            except Exception:  # pragma: no cover - logging for observability
                logger.exception("Failed to unregister previous notification subscriber")

    conn = _NOTIFICATION_STATE.get("conn")
    if isinstance(conn, sqlite3.Connection):
        try:
            conn.execute("PRAGMA user_version")
        except sqlite3.ProgrammingError:
            conn = db()
    else:
        conn = db()
    repo = SQLiteStockRepo(conn)

    async def _dispatch(payload: Mapping[str, object]) -> None:
        await events.publish(_NOTIFICATION_EVENT, payload=payload)

    threshold = 2.0
    stock_limit = max(config.stock_page_size, 50)
    unsubscribers = [
        notify_instant_thresholds(repo, _dispatch, threshold=threshold, limit=stock_limit),
        notify_instant_to_skl(_dispatch),
        send_daily_digests(repo, _dispatch, threshold=threshold, limit=stock_limit * 4),
    ]

    _NOTIFICATION_STATE["conn"] = conn
    _NOTIFICATION_STATE["unsubscribers"] = unsubscribers
    logger.debug("Notification subscribers registered (%d handlers)", len(unsubscribers))


def _run_plugin_migrations(plugins: Sequence[PluginDescriptor]) -> None:
    migratable = [plugin for plugin in plugins if callable(plugin.migrate)]
    if not migratable:
        logger.debug("No plugin migrations detected")
        return

    conn = db()
    try:
        for plugin in migratable:
            migrate = plugin.migrate
            if migrate is None:
                continue

            logger.info("Running migration for plugin %s", plugin.name)
            try:
                migrate(conn)
                conn.commit()
            except Exception:  # pragma: no cover - surfaced via logs
                conn.rollback()
                logger.exception("Plugin %s migration failed", plugin.name)
    finally:
        conn.close()


__all__ = ["create_system", "DvorikSystem"]

