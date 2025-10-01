"""Central registries used across the new Dvorik architecture."""

from __future__ import annotations

from typing import Any, Dict, Generic, Iterable, Iterator, MutableMapping, Tuple, TypeVar


T = TypeVar("T")


class _Registry(Generic[T]):
    """Generic in-memory registry keyed by string identifiers."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._items: Dict[str, T] = {}

    def register(self, key: str, value: T, *, replace: bool = False) -> None:
        """Store ``value`` under ``key``.

        Parameters
        ----------
        key:
            Unique string identifier for the entry.
        value:
            Entry payload.
        replace:
            When ``False`` (default) attempting to register an existing key raises
            ``KeyError``. When ``True`` the previous value is silently replaced.
        """

        if not replace and key in self._items:
            raise KeyError(f"{self._name} '{key}' is already registered")
        self._items[key] = value

    def unregister(self, key: str) -> None:
        """Remove ``key`` from the registry (no-op if absent)."""

        self._items.pop(key, None)

    def get(self, key: str) -> T:
        """Return the value registered under ``key``."""

        try:
            return self._items[key]
        except KeyError as exc:  # pragma: no cover - defensive message enrichment
            raise KeyError(f"{self._name} '{key}' is not registered") from exc

    def ensure(self, key: str, default: T) -> T:
        """Return an entry, storing ``default`` if the key was missing."""

        return self._items.setdefault(key, default)

    def keys(self) -> Iterable[str]:
        return tuple(self._items.keys())

    def values(self) -> Iterable[T]:
        return tuple(self._items.values())

    def items(self) -> Iterable[Tuple[str, T]]:
        return tuple(self._items.items())

    def __contains__(self, key: object) -> bool:
        return key in self._items

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def clear(self) -> None:
        self._items.clear()


class _QueryRegistry(_Registry[str]):
    """Registry for SQL snippets allowing defaults to be supplied."""

    def get(self, key: str, default: str | None = None) -> str:
        if default is not None:
            return self._items.get(key, default)
        return super().get(key)

    def as_mapping(self) -> MutableMapping[str, str]:
        """Expose registry as a mutable mapping for DB layer integration."""

        return self._items


MenuRegistry = _Registry[Any]("menu registry")
WidgetRegistry = _Registry[Any]("widget registry")
BotRouterRegistry = _Registry[Any]("bot router registry")
JobRegistry = _Registry[Any]("job registry")
QueryRegistry = _QueryRegistry("query registry")


__all__ = [
    "MenuRegistry",
    "WidgetRegistry",
    "BotRouterRegistry",
    "JobRegistry",
    "QueryRegistry",
]
