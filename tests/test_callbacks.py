import pytest

from dvorik.bot import callbacks


def test_build_and_parse_roundtrip():
    payload = callbacks.build(
        "inventory",
        "view",
        params={"product_id": 123, "show_stock": True},
        user="alice",
    )

    parsed = callbacks.parse(payload, expected_namespace="inventory")

    assert parsed.namespace == "inventory"
    assert parsed.action == "view"
    assert parsed.params == {
        "product_id": "123",
        "show_stock": "True",
        "user": "alice",
    }


def test_build_rejects_duplicate_parameters():
    with pytest.raises(ValueError):
        callbacks.build("ns", "act", params={"id": 1}, id=2)


def test_parse_validates_namespace():
    payload = callbacks.build("stock", "adjust")

    with pytest.raises(ValueError):
        callbacks.parse(payload, expected_namespace="supply")


def test_parse_handles_blank_values():
    raw = "demo:action:key="
    parsed = callbacks.parse(raw)

    assert parsed.namespace == "demo"
    assert parsed.action == "action"
    assert parsed.params == {"key": ""}


def test_validate_segment_rejects_reserved_characters():
    with pytest.raises(ValueError):
        callbacks.build("core", "move", params={"id": "a:1"})
