"""Central registries used across the new Dvorik architecture."""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Iterator,
    MutableMapping,
    Tuple,
    TypeVar,
    overload,
)


T = TypeVar("T")


class _Registry(Generic[T]):
    """Generic in-memory registry keyed by string identifiers."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._store: Dict[str, T] = {}

    def _storage(self) -> MutableMapping[str, T]:
        """Return the backing storage used by the registry."""

        return self._store

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

        storage = self._storage()

        if not replace and key in storage:
            raise KeyError(f"{self._name} '{key}' is already registered")
        storage[key] = value

    def unregister(self, key: str) -> None:
        """Remove ``key`` from the registry (no-op if absent)."""

        self._storage().pop(key, None)

    def get(self, key: str) -> T:
        """Return the value registered under ``key``."""

        try:
            return self._storage()[key]
        except KeyError as exc:  # pragma: no cover - defensive message enrichment
            raise KeyError(f"{self._name} '{key}' is not registered") from exc

    @overload
    def ensure(self, key: str, default: T) -> T:  # pragma: no cover - typing helper
        ...

    @overload
    def ensure(self, key: str, default: Callable[[], T]) -> T:  # pragma: no cover
        ...

    def ensure(self, key: str, default: T | Callable[[], T]) -> T:
        """Return an entry, storing ``default`` if the key was missing."""

        storage = self._storage()

        if key in storage:
            return storage[key]

        value = default() if callable(default) else default
        storage[key] = value
        return value

    def keys(self) -> Iterable[str]:
        return tuple(self._storage().keys())

    def values(self) -> Iterable[T]:
        return tuple(self._storage().values())

    def items(self) -> Iterable[Tuple[str, T]]:
        return tuple(self._storage().items())

    def __contains__(self, key: object) -> bool:
        return key in self._storage()

    def __iter__(self) -> Iterator[str]:
        return iter(self._storage())

    def __len__(self) -> int:
        return len(self._storage())

    def __getitem__(self, key: str) -> T:
        return self.get(key)

    def clear(self) -> None:
        self._storage().clear()

    def snapshot(self) -> Dict[str, T]:
        """Return a shallow copy of the registry contents."""

        return dict(self._storage())

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<{self.__class__.__name__} name={self._name!r} entries={len(self)}>"


class _QueryRegistry(_Registry[str]):
    """Registry for SQL snippets allowing defaults to be supplied."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._bound: MutableMapping[str, str] | None = None

    def bind_to(self, storage: MutableMapping[str, str] | None) -> None:
        """Bind the registry to external storage (e.g. DB-backed mapping)."""

        self._bound = storage

    def _storage(self) -> MutableMapping[str, str]:
        if self._bound is not None:
            return self._bound
        return super()._storage()

    def get(self, key: str, default: str | None = None) -> str:
        storage = self._storage()
        if default is not None:
            return storage.get(key, default)
        return super().get(key)

    def as_mapping(self) -> MutableMapping[str, str]:
        """Expose registry as a mutable mapping for DB layer integration."""

        return self._storage()


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
