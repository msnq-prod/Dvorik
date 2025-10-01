"""Plugin loading utilities and registry helpers."""

from __future__ import annotations

import importlib
import inspect
import logging
import pkgutil
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, Iterable, List, Sequence

from .registry import BotRouterRegistry, JobRegistry, MenuRegistry, WidgetRegistry

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PluginDescriptor:
    """Runtime metadata describing a loaded plugin."""

    name: str
    module: ModuleType
    version: str | None = None
    description: str | None = None

    @property
    def module_name(self) -> str:
        return self.module.__name__


_PLUGINS: Dict[str, PluginDescriptor] = {}
_MODULE_TO_PLUGIN: Dict[str, str] = {}


def register_plugin(
    name: str,
    *,
    module: ModuleType | None = None,
    version: str | None = None,
    description: str | None = None,
    replace: bool = True,
) -> None:
    """Register plugin metadata in the in-memory catalogue."""

    resolved_module = module or _resolve_caller_module()
    if resolved_module is None:
        raise RuntimeError("register_plugin must be called from within a module")

    descriptor = PluginDescriptor(
        name=name,
        module=resolved_module,
        version=version,
        description=description,
    )

    if not replace and name in _PLUGINS:
        raise KeyError(f"Plugin '{name}' is already registered")

    _PLUGINS[name] = descriptor
    _MODULE_TO_PLUGIN[resolved_module.__name__] = name


def get_plugins() -> Sequence[PluginDescriptor]:
    """Return a snapshot of all registered plugins."""

    return tuple(_PLUGINS.values())


def get_plugin(name: str) -> PluginDescriptor:
    """Retrieve metadata for a specific plugin."""

    try:
        return _PLUGINS[name]
    except KeyError as exc:  # pragma: no cover - defensive path
        raise KeyError(f"Plugin '{name}' is not registered") from exc


def iter_plugins() -> Iterable[PluginDescriptor]:
    """Iterate over registered plugins."""

    return _PLUGINS.values()


def load_plugins(dir: str = "dvorik/plugins") -> Sequence[PluginDescriptor]:
    """Discover and import plugins from ``dir`` (defaults to ``dvorik/plugins``)."""

    package_name = _normalise_package(dir)
    try:
        package = importlib.import_module(package_name)
    except ModuleNotFoundError:
        logger.info("Plugin package %s not found; skipping", package_name)
        return tuple()

    package_path = getattr(package, "__path__", None)
    if package_path is None:
        logger.info("Plugin package %s has no __path__; skipping", package_name)
        return tuple()

    loaded_modules: List[ModuleType] = []

    for module_info in pkgutil.iter_modules(package_path, f"{package_name}."):
        if module_info.name.rsplit(".", 1)[-1].startswith("_"):
            continue
        try:
            module = importlib.import_module(module_info.name)
        except Exception:  # pragma: no cover - logged for observability
            logger.exception("Failed to load plugin %s", module_info.name)
            continue

        loaded_modules.append(module)
        if module.__name__ not in _MODULE_TO_PLUGIN:
            register_plugin(module.__name__.rsplit(".", 1)[-1], module=module)

    logger.info("Loaded %d plugins from %s", len(loaded_modules), package_name)
    return get_plugins()


def register_menu(key: str, value: object, *, replace: bool = False) -> None:
    """Helper delegating to :class:`MenuRegistry`."""

    MenuRegistry.register(key, value, replace=replace)


def register_widget(key: str, widget: object, *, replace: bool = False) -> None:
    """Helper delegating to :class:`WidgetRegistry`."""

    WidgetRegistry.register(key, widget, replace=replace)


def register_bot_router(key: str, router: object, *, replace: bool = False) -> None:
    """Helper delegating to :class:`BotRouterRegistry`."""

    BotRouterRegistry.register(key, router, replace=replace)


def register_job(key: str, job: object, *, replace: bool = False) -> None:
    """Helper delegating to :class:`JobRegistry`."""

    JobRegistry.register(key, job, replace=replace)


def _normalise_package(value: str) -> str:
    candidate = value.replace("\\", "/").strip("/ ")
    if "/" in candidate:
        parts = [part for part in candidate.split("/") if part]
        return ".".join(parts)
    return candidate.strip(".") or "dvorik.plugins"


def _resolve_caller_module() -> ModuleType | None:
    frame = inspect.currentframe()
    if frame is None:
        return None
    caller_frame = frame.f_back.f_back if frame.f_back else None
    if caller_frame is None:
        return None
    module_name = caller_frame.f_globals.get("__name__")
    if not module_name:
        return None
    module = importlib.import_module(module_name)
    return module


__all__ = [
    "PluginDescriptor",
    "get_plugin",
    "get_plugins",
    "iter_plugins",
    "load_plugins",
    "register_plugin",
    "register_menu",
    "register_widget",
    "register_bot_router",
    "register_job",
]
