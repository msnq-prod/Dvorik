from __future__ import annotations

"""Composition root wiring shared infrastructure for the Dvorik project."""

import datetime as dt
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from dvorik.core import events
from dvorik.core.config import Config, get_config
from dvorik.core.plugins import load_plugins
from dvorik.core.registry import JobRegistry
from dvorik.core.scheduler import register_daily
from dvorik.db import init_db
from dvorik.services.menu_catalog import sync_menu_catalog

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from flask import Flask

logger = logging.getLogger(__name__)

AdminAppFactory = Callable[[], "Flask"]
AdminRunner = Callable[..., None]
BotRunner = Callable[[], Awaitable[None]]

_DAILY_TICK_JOB_KEY = "builtin.scheduler.daily_tick"
_DAILY_TICK_EVENT = "scheduler.daily"
_DAILY_TICK_TIME = dt.time(hour=9, minute=0)


@dataclass(slots=True)
class DvorikSystem:
    """Container exposing factories for the admin UI and Telegram bot."""

    config: Config
    create_admin_app: AdminAppFactory
    run_admin: AdminRunner
    run_bot: BotRunner


def create_system(*, config: Config | None = None) -> DvorikSystem:
    """Initialise shared services and return runnable application factories."""

    config = config or get_config()

    logger.debug("Initialising Dvorik system")
    init_db()

    plugins = load_plugins()
    if plugins:
        logger.info("Loaded %d plugin(s)", len(plugins))
    else:
        logger.info("No plugins discovered")

    _register_admin_components()
    _register_bot_components()
    _register_scheduler_jobs()
    _sync_menu_entries()

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


def _sync_menu_entries() -> None:
    try:
        synced = sync_menu_catalog()
    except Exception:  # pragma: no cover - defensive guard
        logger.exception("Failed to synchronise menu catalogue")
        raise

    logger.debug("Synchronised %d menu entries into ui_menu", len(synced))


__all__ = ["create_system", "DvorikSystem"]

