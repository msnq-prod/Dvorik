"""Plugin loading utilities and registry helpers."""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import pkgutil
from dataclasses import dataclass, field
from types import ModuleType
from typing import Dict, Iterable, List, Mapping, Sequence, Set, Tuple

from .registry import BotRouterRegistry, JobRegistry, MenuRegistry, WidgetRegistry
from .version import API_VERSION as CORE_API_VERSION

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PluginDescriptor:
    """Runtime metadata describing a loaded plugin."""

    name: str
    module: ModuleType
    version: str | None = None
    api_versions: tuple[str, ...] = field(default_factory=tuple)
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
    api_version: str | Sequence[str] | None = None,
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
        api_versions=_normalise_declared_versions(api_version),
        description=description,
    )

    if not replace and name in _PLUGINS:
        raise KeyError(f"Plugin '{name}' is already registered")

    _PLUGINS[name] = descriptor
    _MODULE_TO_PLUGIN[resolved_module.__name__] = name
    logger.info(
        "Registered plugin %s (version=%s, api=%s)",
        descriptor.name,
        descriptor.version or "n/a",
        ", ".join(descriptor.api_versions) if descriptor.api_versions else "unspecified",
    )


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


def load_plugins(
    *directories: str | os.PathLike[str],
    dir: str | os.PathLike[str] | None = None,
    allow: Iterable[str] | None = None,
    skip: Iterable[str] | None = None,
) -> Sequence[PluginDescriptor]:
    """Discover and import plugins from one or more packages.

    Parameters
    ----------
    *directories:
        One or more module paths or dotted package names containing plugins.
    dir:
        Backwards compatible alias for a single directory.
    allow:
        Optional iterable of plugin names/modules that should be loaded.
    skip:
        Optional iterable of plugin names/modules that must be ignored.
    """

    search_roots: Tuple[str, ...]
    if dir is not None:
        search_roots = (str(dir), *[str(entry) for entry in directories])
    elif directories:
        search_roots = tuple(str(entry) for entry in directories)
    else:
        search_roots = ("dvorik/plugins",)

    allow_lookup = _normalise_name_set(allow)
    skip_lookup = _normalise_name_set(skip)

    loaded_plugins: List[str] = []
    skipped_plugins: List[tuple[str, str]] = []

    for root in search_roots:
        package_name = _normalise_package(root)
        try:
            package = importlib.import_module(package_name)
        except ModuleNotFoundError:
            logger.info("Plugin package %s not found; skipping", package_name)
            continue

        package_path = getattr(package, "__path__", None)
        if package_path is None:
            logger.info("Plugin package %s has no __path__; skipping", package_name)
            continue

        for module_info in pkgutil.iter_modules(package_path, f"{package_name}."):
            if module_info.name.rsplit(".", 1)[-1].startswith("_"):
                continue
            try:
                module = importlib.import_module(module_info.name)
            except Exception:  # pragma: no cover - logged for observability
                logger.exception("Failed to load plugin %s", module_info.name)
                skipped_plugins.append((module_info.name, "import error"))
                continue

            metadata = _introspect_plugin_module(module)
            canonical_names = _canonical_identifiers(metadata.name, module.__name__)

            if allow_lookup and allow_lookup.isdisjoint(canonical_names):
                skipped_plugins.append((module.__name__, "not in allowlist"))
                _purge_plugin_metadata(module)
                continue

            if skip_lookup and not skip_lookup.isdisjoint(canonical_names):
                skipped_plugins.append((module.__name__, "disabled"))
                _purge_plugin_metadata(module)
                continue

            if not metadata.api_versions:
                logger.warning(
                    "Plugin %s does not declare API_VERSION; skipping", module.__name__
                )
                skipped_plugins.append((module.__name__, "missing API_VERSION"))
                _purge_plugin_metadata(module)
                continue

            if CORE_API_VERSION not in metadata.api_versions:
                logger.warning(
                    "Plugin %s targets incompatible API %s (expected %s); skipping",
                    module.__name__,
                    ", ".join(metadata.api_versions),
                    CORE_API_VERSION,
                )
                skipped_plugins.append((module.__name__, "incompatible API"))
                _purge_plugin_metadata(module)
                continue

            existing_name = _MODULE_TO_PLUGIN.get(module.__name__)
            if existing_name and metadata.name and metadata.name != existing_name:
                logger.warning(
                    "Plugin %s attempted to change its registered name from %s to %s; keeping original.",
                    module.__name__,
                    existing_name,
                    metadata.name,
                )

            plugin_name = existing_name or metadata.name or module.__name__.rsplit(".", 1)[-1]
            existing_descriptor = _PLUGINS.get(plugin_name)
            register_plugin(
                plugin_name,
                module=module,
                version=metadata.version
                or (existing_descriptor.version if existing_descriptor else None),
                api_version=metadata.api_versions
                or (existing_descriptor.api_versions if existing_descriptor else None),
                description=metadata.description
                or (existing_descriptor.description if existing_descriptor else None),
            )
            loaded_plugins.append(plugin_name)

    if loaded_plugins:
        logger.info("Loaded plugins: %s", ", ".join(sorted(set(loaded_plugins))))
    else:
        logger.info("No plugins loaded")

    if skipped_plugins:
        details = ", ".join(f"{name} ({reason})" for name, reason in skipped_plugins)
        logger.info("Skipped plugins: %s", details)

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
    candidate = str(value).replace("\\", "/").strip("/ ")
    if "/" in candidate:
        parts = [part for part in candidate.split("/") if part]
        return ".".join(parts)
    return candidate.strip(".") or "dvorik.plugins"


def _normalise_name_set(values: Iterable[str] | None) -> Set[str]:
    if not values:
        return set()
    normalised: Set[str] = set()
    for entry in values:
        if entry is None:
            continue
        text = str(entry).strip().lower()
        if text:
            normalised.add(text)
    return normalised


def _canonical_identifiers(declared_name: str | None, module_name: str) -> Set[str]:
    names = {module_name.lower()}
    short_name = module_name.rsplit(".", 1)[-1]
    names.add(short_name.lower())
    if declared_name:
        names.add(str(declared_name).strip().lower())
    return names


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


@dataclass(slots=True)
class _PluginMetadata:
    name: str | None
    version: str | None
    api_versions: tuple[str, ...]
    description: str | None


def _introspect_plugin_module(module: ModuleType) -> _PluginMetadata:
    api_versions = _normalise_declared_versions(getattr(module, "API_VERSION", None))
    version = getattr(module, "PLUGIN_VERSION", None) or getattr(module, "__version__", None)
    name = getattr(module, "PLUGIN_NAME", None)
    description = getattr(module, "__doc__", None)

    info_callable = getattr(module, "plugin_info", None)
    if callable(info_callable):
        try:
            info = info_callable()
        except Exception:  # pragma: no cover - surfaced via logs
            logger.exception("plugin_info() raised an exception for %s", module.__name__)
        else:
            if not isinstance(info, Mapping):
                logger.warning(
                    "plugin_info() for %s returned %r instead of a mapping", module.__name__, info
                )
            else:
                api_versions = _normalise_declared_versions(info.get("api_version")) or api_versions
                version = info.get("version") or version
                name = info.get("name") or name
                description = info.get("description") or description

    description = description.strip() if isinstance(description, str) else description
    return _PluginMetadata(name=name, version=version, api_versions=api_versions, description=description)


def _normalise_declared_versions(
    value: str | Sequence[str] | tuple[str, ...] | None,
) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        cleaned = value.strip()
        return (cleaned,) if cleaned else tuple()

    versions: list[str] = []
    for candidate in value:
        if candidate is None:
            continue
        text = str(candidate).strip()
        if text:
            versions.append(text)
    return tuple(dict.fromkeys(versions))


def _purge_plugin_metadata(module: ModuleType) -> None:
    module_name = module.__name__
    plugin_name = _MODULE_TO_PLUGIN.pop(module_name, None)
    if plugin_name:
        _PLUGINS.pop(plugin_name, None)


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
