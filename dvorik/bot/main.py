from __future__ import annotations

"""Bot entrypoint wiring core services together."""

import asyncio
import importlib
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from dvorik.core.config import Config, get_config
from dvorik.core.plugins import load_plugins
from dvorik.core.registry import BotRouterRegistry
from dvorik.core.scheduler import run_forever
from dvorik.db import init_db
from dvorik.bot import notifications as bot_notifications

logger = logging.getLogger(__name__)


async def run_bot(*, config: Config | None = None) -> None:
    """Initialise services and start polling Telegram updates."""

    config = config or get_config()
    if not config.bot_token:
        raise RuntimeError("BOT_TOKEN must be configured before starting the bot")

    init_db()
    load_plugins()
    _register_builtin_components()

    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher()
    _attach_registered_routers(dispatcher)
    notification_unsubscribe = bot_notifications.setup_notification_bridge(bot, config)

    loop = asyncio.get_running_loop()
    scheduler_task = loop.create_task(run_forever(loop))
    logger.info("Starting Telegram polling")

    try:
        await dispatcher.start_polling(bot)
    finally:
        scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        notification_unsubscribe()
        await bot.session.close()
        logger.info("Bot shutdown complete")


def _register_builtin_components() -> None:
    """Register routers provided by the core package."""

    module_name = f"{__package__}.routers"

    try:
        routers = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            logger.info("No built-in bot routers found; skipping registration")
            return
        raise
    except Exception:  # pragma: no cover - logged for visibility
        logger.exception("Failed to import bot routers module")
        raise

    register_callable = getattr(routers, "register_builtin_routers", None)
    if callable(register_callable):
        register_callable()
    else:  # pragma: no cover - defensive path
        logger.debug("routers.register_builtin_routers is not defined; nothing to do")


def _attach_registered_routers(dispatcher: Dispatcher) -> None:
    """Attach routers from the registry to the dispatcher."""

    for key, router in BotRouterRegistry.items():
        try:
            dispatcher.include_router(router)
            logger.debug("Attached bot router: %s", key)
        except Exception:  # pragma: no cover - logged for debugging
            logger.exception("Failed to attach router %s", key)
            raise


def main() -> None:
    """Convenience wrapper running :func:`run_bot` via ``asyncio``."""

    asyncio.run(run_bot())


__all__ = ["run_bot", "main"]
