"""Utilities for building and parsing namespaced callback payloads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping

__all__ = ["CallbackPayload", "build", "parse"]

_SEGMENT_SEPARATOR = ":"
_FIELD_SEPARATOR = "&"
_ASSIGNMENT_SEPARATOR = "="


@dataclass(frozen=True, slots=True)
class CallbackPayload:
    """Structured representation of callback data.

    Attributes
    ----------
    namespace:
        Unique namespace used to avoid collisions between different routers or
        plugins.
    action:
        Action identifier within the namespace.
    params:
        Optional parameters encoded in the payload.
    """

    namespace: str
    action: str
    params: Dict[str, str]


def build(
    namespace: str,
    action: str,
    /,
    *,
    params: Mapping[str, Any] | None = None,
    **extra: Any,
) -> str:
    """Return callback data string with namespacing applied.

    Parameters
    ----------
    namespace:
        High-level grouping for callbacks. Typically matches router or plugin
        identifier.
    action:
        Specific action to be performed when the callback is received.
    params / extra:
        Additional key/value pairs encoded into the payload. ``None`` values are
        omitted.
    """

    _validate_segment(namespace, "namespace")
    _validate_segment(action, "action")

    combined: Dict[str, str] = {}
    if params:
        combined.update(_normalise_params(params))
    if extra:
        extra_params = _normalise_params(extra)
        overlap = combined.keys() & extra_params.keys()
        if overlap:
            joined = ", ".join(sorted(overlap))
            raise ValueError(f"duplicate callback params for keys: {joined}")
        combined.update(extra_params)

    pieces = [namespace, action]
    if combined:
        encoded = _FIELD_SEPARATOR.join(
            f"{key}{_ASSIGNMENT_SEPARATOR}{value}" for key, value in sorted(combined.items())
        )
        pieces.append(encoded)

    return _SEGMENT_SEPARATOR.join(pieces)


def parse(data: str, /, *, expected_namespace: str | None = None) -> CallbackPayload:
    """Parse callback data into a structured payload.

    Parameters
    ----------
    data:
        Raw callback payload received from Telegram.
    expected_namespace:
        When provided, ``ValueError`` is raised if the payload belongs to a
        different namespace.
    """

    if not data:
        raise ValueError("callback data is empty")

    parts = data.split(_SEGMENT_SEPARATOR, 2)
    if len(parts) < 2:
        raise ValueError("callback data must include namespace and action")

    namespace, action = parts[0], parts[1]
    if expected_namespace is not None and namespace != expected_namespace:
        raise ValueError("callback namespace mismatch")

    params_segment = parts[2] if len(parts) == 3 else ""
    params: Dict[str, str] = {}
    if params_segment:
        for chunk in params_segment.split(_FIELD_SEPARATOR):
            if not chunk:
                continue
            if _ASSIGNMENT_SEPARATOR in chunk:
                key, value = chunk.split(_ASSIGNMENT_SEPARATOR, 1)
            else:
                key, value = chunk, ""
            if not key:
                raise ValueError("callback data contains empty parameter name")
            params[key] = value

    return CallbackPayload(namespace=namespace, action=action, params=params)


def _validate_segment(value: str, label: str) -> None:
    if not value:
        raise ValueError(f"callback {label} must not be empty")
    if any(separator in value for separator in (_SEGMENT_SEPARATOR, _FIELD_SEPARATOR, _ASSIGNMENT_SEPARATOR)):
        raise ValueError(
            f"callback {label} must not contain '{_SEGMENT_SEPARATOR}', '{_FIELD_SEPARATOR}' or '{_ASSIGNMENT_SEPARATOR}'"
        )


def _normalise_params(params: Mapping[str, Any]) -> Dict[str, str]:
    normalised: Dict[str, str] = {}
    for key, raw_value in params.items():
        if raw_value is None:
            continue
        key_str = str(key)
        _validate_segment(key_str, "parameter name")
        value_str = str(raw_value)
        _ensure_value_safe(value_str)
        normalised[key_str] = value_str
    return normalised


def _ensure_value_safe(value: str) -> None:
    if any(separator in value for separator in (_SEGMENT_SEPARATOR, _FIELD_SEPARATOR, _ASSIGNMENT_SEPARATOR)):
        raise ValueError(
            "callback parameter values must not contain reserved separators ':' '&' '='"
        )
