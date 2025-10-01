"""Built-in routers bundled with the core Dvorik bot package."""

from __future__ import annotations

from collections.abc import Iterable
from importlib import import_module
from types import ModuleType

from dvorik.core.registry import BotRouterRegistry

__all__ = ["register_builtin_routers"]


def _iter_router_modules() -> Iterable[ModuleType]:
    """Yield router modules that are part of the built-in bundle."""

    module_names = (
        ".core",
        ".admin",
        ".stock",
        ".supply",
    )

    for suffix in module_names:
        yield import_module(suffix, package=__name__)


def _call_if_callable(obj: object, name: str) -> None:
    """Invoke ``name`` attribute on ``obj`` when it is callable."""

    attribute = getattr(obj, name, None)
    if callable(attribute):
        attribute()


def register_builtin_routers() -> None:
    """Register core routers in the :class:`BotRouterRegistry`."""

    for module in _iter_router_modules():
        _call_if_callable(module, "register")

    # Eagerly touch the registry to surface missing routers early.
    for key in ("builtin.core", "builtin.admin", "builtin.stock", "builtin.supply"):
        if key not in BotRouterRegistry:
            raise RuntimeError(f"Router '{key}' was not registered")
