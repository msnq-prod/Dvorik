"""Core abstractions for admin dashboard widgets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Mapping, MutableMapping


@dataclass(slots=True)
class WidgetContext:
    """Context information passed to widgets during rendering."""

    config: Mapping[str, Any] = field(default_factory=dict)
    extra: MutableMapping[str, Any] = field(default_factory=dict)


class Widget:
    """Base class for dashboard widgets rendered on admin pages."""

    slug: ClassVar[str] = "widget"
    title: ClassVar[str] = "Widget"
    description: ClassVar[str] | None = None

    def __init__(self, *, config: Mapping[str, Any] | None = None) -> None:
        self.config: Mapping[str, Any] = dict(config) if config is not None else {}

    def render(self, context: WidgetContext | None = None) -> str:
        raise NotImplementedError

    @classmethod
    def entrypoint(cls) -> str:
        return f"{cls.__module__}:{cls.__name__}"


__all__ = ["Widget", "WidgetContext"]
