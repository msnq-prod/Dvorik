from __future__ import annotations

import sys
from pathlib import Path

import asyncio
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dvorik.core import events


@pytest.fixture(autouse=True)
def clear_event_bus():
    events._clear_subscribers()
    yield
    events._clear_subscribers()


def test_publish_notifies_sync_and_async_subscribers():
    results = []

    def sync_handler(value):
        results.append(("sync", value))

    async def async_handler(value):
        results.append(("async", value))

    events.subscribe("demo", sync_handler)
    events.subscribe("demo", async_handler)

    asyncio.run(events.publish("demo", 7))

    assert ("sync", 7) in results
    assert ("async", 7) in results
    assert len(results) == 2


def test_unsubscribe_removes_handler():
    called = False

    def handler():
        nonlocal called
        called = True

    events.subscribe("demo", handler)
    events.unsubscribe("demo", handler)

    asyncio.run(events.publish("demo"))

    assert called is False
