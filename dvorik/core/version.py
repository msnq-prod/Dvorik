"""Plugin API version definitions for Dvorik.

Plugins must explicitly declare which revision of the host API they target to
avoid accidental breakage when the core evolves. Each plugin module is expected
to expose one of the following interfaces during import time:

* An ``API_VERSION`` attribute with the supported version string (for example
  ``API_VERSION = "1.0"``).
* A :func:`plugin_info` callable returning a mapping that includes the key
  ``"api_version"`` alongside optional metadata such as ``"name"``,
  ``"version"`` or ``"description"``.

When neither contract is implemented the plugin will be ignored during the
automatic discovery phase.
"""

from __future__ import annotations

# The current plugin API revision shipped with the core distribution.
API_VERSION = "1.0"

__all__ = ["API_VERSION"]
