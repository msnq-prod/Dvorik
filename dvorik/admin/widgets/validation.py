"""Utilities for validating widget configuration JSON against schemas."""
from __future__ import annotations

import json
from typing import Any, Mapping

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError, ValidationError

class WidgetConfigError(ValueError):
    """Raised when a widget configuration payload fails validation."""


def validate_widget_config(config_json: str | None, schema_json: str | None) -> dict[str, Any]:
    """Validate ``config_json`` against ``schema_json`` and return the parsed mapping."""

    payload = _load_config_payload(config_json)

    schema = _load_schema(schema_json)
    if schema is not None:
        try:
            Draft7Validator(schema).validate(payload)
        except ValidationError as exc:  # pragma: no cover - relies on third party lib
            message = _format_validation_error(exc)
            raise WidgetConfigError(message) from exc

    return payload


def _load_config_payload(config_json: str | None) -> dict[str, Any]:
    if config_json is None:
        return {}

    try:
        payload = json.loads(config_json)
    except json.JSONDecodeError as exc:
        raise WidgetConfigError(f"Config JSON is not valid: {exc.msg}.") from exc

    if not isinstance(payload, dict):
        raise WidgetConfigError("Config JSON must be an object.")

    return payload


def _load_schema(schema_json: str | None) -> Mapping[str, Any] | None:
    if not schema_json:
        return None

    try:
        schema = json.loads(schema_json)
    except json.JSONDecodeError as exc:
        raise WidgetConfigError(f"Config schema is not valid JSON: {exc.msg}.") from exc

    if not isinstance(schema, dict):
        raise WidgetConfigError("Config schema must be a JSON object.")

    try:
        Draft7Validator.check_schema(schema)
    except SchemaError as exc:  # pragma: no cover - defensive guard
        message = exc.message or "Schema is invalid."
        raise WidgetConfigError(f"Config schema is invalid: {message}") from exc

    return schema


def _format_validation_error(exc: ValidationError) -> str:
    location = " > ".join(str(element) for element in exc.path)
    if location:
        return f"{exc.message} (at {location})"
    return exc.message


__all__ = ["WidgetConfigError", "validate_widget_config"]
