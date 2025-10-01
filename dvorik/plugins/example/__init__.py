"""Example plugin showcasing integration with the plugin loader."""

from __future__ import annotations

from dvorik.core.plugins import register_plugin

register_plugin(
    "example",
    description="Example plugin bundled with the core distribution",
)

__all__ = ["__doc__"]
