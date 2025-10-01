"""Simple event bus supporting sync and async subscribers."""
from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List

logger = logging.getLogger(__name__)

Subscriber = Callable[..., Any]


class _EventRegistry:
    """Registry storing subscribers grouped by event name."""

    def __init__(self) -> None:
        self._callbacks: Dict[str, List[Subscriber]] = defaultdict(list)

    def subscribe(self, event: str, callback: Subscriber) -> None:
        if callback in self._callbacks[event]:
            return
        self._callbacks[event].append(callback)

    def unsubscribe(self, event: str, callback: Subscriber) -> None:
        callbacks = self._callbacks.get(event)
        if not callbacks:
            return
        try:
            callbacks.remove(callback)
        except ValueError:
            return
        if not callbacks:
            self._callbacks.pop(event, None)

    def get_callbacks(self, event: str) -> List[Subscriber]:
        return list(self._callbacks.get(event, ()))

    def clear(self) -> None:
        self._callbacks.clear()


_registry = _EventRegistry()


def subscribe(event: str, callback: Subscriber) -> None:
    """Subscribe ``callback`` to ``event`` notifications."""

    if not callable(callback):
        raise TypeError("callback must be callable")
    _registry.subscribe(event, callback)


def unsubscribe(event: str, callback: Subscriber) -> None:
    """Remove ``callback`` subscription for ``event`` (no-op if missing)."""

    _registry.unsubscribe(event, callback)


async def publish(event: str, *args: Any, **kwargs: Any) -> None:
    """Publish ``event`` data to all registered subscribers."""

    callbacks = _registry.get_callbacks(event)
    if not callbacks:
        return

    await asyncio.gather(
        *(_invoke_callback(event, callback, *args, **kwargs) for callback in callbacks),
        return_exceptions=False,
    )


async def _invoke_callback(
    event: str, callback: Subscriber, *args: Any, **kwargs: Any
) -> None:
    try:
        result = callback(*args, **kwargs)
        if inspect.isawaitable(result):
            await result  # type: ignore[func-returns-value]
    except Exception:  # pragma: no cover - logged for observability
        logger.exception("Error in event subscriber", extra={"event": event, "callback": repr(callback)})


def _clear_subscribers() -> None:
    """Testing helper to reset the registry."""

    _registry.clear()


__all__ = ["publish", "subscribe", "unsubscribe"]
