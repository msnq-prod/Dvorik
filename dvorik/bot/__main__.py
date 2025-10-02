from __future__ import annotations

"""Module entry point exposing ``python -m dvorik.bot``."""

import asyncio

from dvorik.app import create_system
from dvorik.core.logging import bootstrap_logging


def main() -> None:
    bootstrap_logging()
    system = create_system()
    asyncio.run(system.run_bot())


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    main()
