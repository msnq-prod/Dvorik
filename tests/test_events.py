import asyncio

import pytest

from dvorik.core import events


@pytest.fixture(autouse=True)
def _reset_event_registry():
    events._clear_subscribers()
    yield
    events._clear_subscribers()


def test_publish_delivers_to_sync_and_async_subscribers():
    async def main():
        received = []

        def sync_handler(**payload):
            received.append(("sync", payload))

        async def async_handler(**payload):
            received.append(("async", payload))

        events.subscribe("example", sync_handler)
        events.subscribe("example", async_handler)

        await events.publish("example", value=42, status="ok")

        assert len(received) == 2
        assert ("sync", {"value": 42, "status": "ok"}) in received
        assert ("async", {"value": 42, "status": "ok"}) in received

    asyncio.run(main())


def test_unsubscribe_prevents_future_notifications():
    async def main():
        called = False

        async def handler(**_payload):
            nonlocal called
            called = True

        events.subscribe("demo", handler)
        events.unsubscribe("demo", handler)

        await events.publish("demo", foo="bar")

        assert called is False

    asyncio.run(main())


def test_subscribe_requires_callable():
    with pytest.raises(TypeError):
        events.subscribe("broken", object())
