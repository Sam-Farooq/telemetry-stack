"""OTLP log records into span events, or into Elasticsearch documents.

A log line emitted inside a span is the same fact as the span, stored twice. If
it carries a trace and span id it becomes an event on that span and travels
with the trace; if it does not, it becomes a document of its own. Deciding this
in the gateway rather than in each service means the services keep logging
normally.

The input is the OTLP JSON encoding, the one the collector's own exporters
write, including its quirks: every value is wrapped in a type tag, 64 bit
integers arrive as strings, and an absent trace id is 32 zeros rather than a
missing field.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

TRACE_ID_HEX_LEN = 32
SPAN_ID_HEX_LEN = 16
_ZERO_TRACE = "0" * TRACE_ID_HEX_LEN
_ZERO_SPAN = "0" * SPAN_ID_HEX_LEN
_HEX_DIGITS = frozenset("0123456789abcdef")

# OTLP severity numbers. The text is advisory, the number is what is compared.
SEVERITY_NUMBERS: dict[str, int] = {
    "TRACE": 1,
    "DEBUG": 5,
    "INFO": 9,
    "WARN": 13,
    "WARNING": 13,
    "ERROR": 17,
    "FATAL": 21,
    "CRITICAL": 21,
}
DEFAULT_SEVERITY_NUMBER = SEVERITY_NUMBERS["INFO"]

# Long strings are cut here, in both the body and the attributes. A stack
# trace arrives as `exception.stacktrace`, which is an attribute, so a limit
# that only reads the body does nothing about the one field that gets large.
MAX_BODY_CHARS = 2048
TRUNCATION_MARKER = "[truncated]"


class Route(Enum):
    DROP = "drop"
    SPAN_EVENT = "span_event"
    DOCUMENT = "document"


@dataclass(frozen=True)
class SpanEvent:
    trace_id: str
    span_id: str
    name: str
    time_unix_nano: int
    attributes: dict[str, Any]


def any_value(wrapped: Mapping[str, Any] | None) -> Any:
    """Unwrap one OTLP AnyValue. Unknown tags come back as None, not a crash."""
    if not wrapped:
        return None
    if "stringValue" in wrapped:
        return wrapped["stringValue"]
    if "boolValue" in wrapped:
        return bool(wrapped["boolValue"])
    if "intValue" in wrapped:
        # JSON carries 64 bit integers as strings to survive a float64 parser.
        return int(wrapped["intValue"])
    if "doubleValue" in wrapped:
        return float(wrapped["doubleValue"])
    if "bytesValue" in wrapped:
        return str(wrapped["bytesValue"])
    if "arrayValue" in wrapped:
        return [any_value(v) for v in wrapped["arrayValue"].get("values", [])]
    if "kvlistValue" in wrapped:
        return flatten_attributes(wrapped["kvlistValue"].get("values", []))
    return None


def flatten_attributes(attributes: Iterable[Mapping[str, Any]] | None) -> dict[str, Any]:
    """OTLP KeyValue list into a dict, nested kvlists joined with dots."""
    flat: dict[str, Any] = {}
    for item in attributes or []:
        key = item.get("key")
        if not key:
            continue
        value = any_value(item.get("value"))
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                flat[f"{key}.{inner_key}"] = inner_value
        else:
            flat[key] = value
    return flat


def _is_hex_id(value: Any, length: int, zero: str) -> bool:
    if not isinstance(value, str) or len(value) != length:
        return False
    lowered = value.lower()
    if lowered == zero:
        return False
    return set(lowered) <= _HEX_DIGITS


def has_trace_context(record: Mapping[str, Any]) -> bool:
    """True only for a usable pair. All zeros means the SDK had no active span."""
    return _is_hex_id(record.get("traceId"), TRACE_ID_HEX_LEN, _ZERO_TRACE) and _is_hex_id(
        record.get("spanId"), SPAN_ID_HEX_LEN, _ZERO_SPAN
    )


def severity_number(record: Mapping[str, Any]) -> int:
    """The number if present, else the text, else INFO."""
    number = record.get("severityNumber")
    if isinstance(number, int) and number > 0:
        return number
    text = str(record.get("severityText", "")).strip().upper()
    return SEVERITY_NUMBERS.get(text, DEFAULT_SEVERITY_NUMBER)


def route_log(
    record: Mapping[str, Any], min_severity_number: int = SEVERITY_NUMBERS["INFO"]
) -> Route:
    """Where one record goes. Severity is checked first because it is free."""
    if severity_number(record) < min_severity_number:
        return Route.DROP
    return Route.SPAN_EVENT if has_trace_context(record) else Route.DOCUMENT


def body_text(record: Mapping[str, Any]) -> str:
    value = any_value(record.get("body"))
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return str(value)
    return str(value)


def truncate(text: str, limit: int = MAX_BODY_CHARS) -> tuple[str, bool]:
    if limit < len(TRUNCATION_MARKER):
        raise ValueError("limit has to leave room for the marker")
    if len(text) <= limit:
        return text, False
    return text[: limit - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER, True


def truncate_attributes(
    attributes: Mapping[str, Any], limit: int = MAX_BODY_CHARS
) -> tuple[dict[str, Any], list[str]]:
    """Cut every oversized string value, and name the keys that were cut."""
    out: dict[str, Any] = {}
    cut_keys: list[str] = []
    for key, value in attributes.items():
        if isinstance(value, str) and len(value) > limit:
            out[key], _ = truncate(value, limit)
            cut_keys.append(key)
        else:
            out[key] = value
    return out, sorted(cut_keys)


def iso_timestamp(time_unix_nano: int | str) -> str:
    """Nanoseconds into the millisecond ISO form date_optional_time accepts.

    Integer arithmetic rather than a float division: 1.7e18 nanoseconds does
    not fit in a float64 without losing the low digits, and the low digits are
    the ordering between two log lines in the same millisecond.
    """
    nanos = int(time_unix_nano)
    seconds, remainder = divmod(nanos, 1_000_000_000)
    moment = datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=remainder // 1000)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_name(attributes: Mapping[str, Any]) -> str:
    """`exception` when the semantic convention keys are there, else `log`."""
    return "exception" if "exception.type" in attributes else "log"


def to_span_event(
    record: Mapping[str, Any], max_body_chars: int = MAX_BODY_CHARS
) -> SpanEvent | None:
    if not has_trace_context(record):
        return None
    attributes = flatten_attributes(record.get("attributes"))
    attributes, cut_keys = truncate_attributes(attributes, max_body_chars)
    body, was_cut = truncate(body_text(record), max_body_chars)
    payload: dict[str, Any] = dict(attributes)
    payload["log.severity"] = record.get("severityText") or severity_number(record)
    payload["log.message"] = body
    if was_cut:
        payload["log.body.truncated"] = True
    if cut_keys:
        payload["log.truncated_fields"] = cut_keys
    return SpanEvent(
        trace_id=str(record["traceId"]).lower(),
        span_id=str(record["spanId"]).lower(),
        name=event_name(attributes),
        time_unix_nano=int(record.get("timeUnixNano", 0)),
        attributes=payload,
    )


def to_es_document(
    record: Mapping[str, Any],
    max_body_chars: int = MAX_BODY_CHARS,
) -> dict[str, Any]:
    """One flat document. Flat because the index template has dynamic mapping off."""
    attributes, cut_keys = truncate_attributes(
        flatten_attributes(record.get("attributes")), max_body_chars
    )
    resource = flatten_attributes((record.get("resource") or {}).get("attributes"))
    body, was_cut = truncate(body_text(record), max_body_chars)
    document: dict[str, Any] = {
        "@timestamp": iso_timestamp(record.get("timeUnixNano", 0)),
        "severity_text": str(record.get("severityText") or "").upper() or "INFO",
        "severity_number": severity_number(record),
        "body": body,
        "service.name": resource.get("service.name", "unknown"),
        "service.version": resource.get("service.version"),
        "deployment.environment": resource.get("deployment.environment"),
    }
    if was_cut:
        document["body.truncated"] = True
    if cut_keys:
        document["truncated_fields"] = cut_keys
    if has_trace_context(record):
        document["trace.id"] = str(record["traceId"]).lower()
        document["span.id"] = str(record["spanId"]).lower()
    for key, value in attributes.items():
        document[f"attributes.{key}"] = value
    return {k: v for k, v in document.items() if v is not None}
