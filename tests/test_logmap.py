from __future__ import annotations

from typing import Any

import pytest

from telemetry.logmap import (
    MAX_BODY_CHARS,
    SEVERITY_NUMBERS,
    TRUNCATION_MARKER,
    Route,
    any_value,
    event_name,
    flatten_attributes,
    has_trace_context,
    iso_timestamp,
    route_log,
    severity_number,
    to_es_document,
    to_span_event,
    truncate,
    truncate_attributes,
)


def test_the_fixture_is_the_shape_the_collector_writes(log_records: list[dict]) -> None:
    assert len(log_records) == 10
    assert all("timeUnixNano" in record for record in log_records)
    assert all(isinstance(record.get("body", {}), dict) for record in log_records)


def test_anyvalue_unwrapping_covers_the_tags_otlp_actually_sends() -> None:
    assert any_value({"stringValue": "card"}) == "card"
    assert any_value({"intValue": "9007199254740993"}) == 9007199254740993
    assert any_value({"doubleValue": 240.5}) == 240.5
    assert any_value({"boolValue": False}) is False
    assert any_value({"arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}}) == [1, 2]
    assert any_value(None) is None
    assert any_value({"somethingNew": 1}) is None


def test_a_sixty_four_bit_integer_survives_the_json_round_trip() -> None:
    # The whole reason OTLP sends integers as strings. A float64 would round
    # this to ...992.
    assert any_value({"intValue": "9007199254740993"}) != 9007199254740992


def test_nested_attributes_are_flattened_with_dots(log_records: list[dict]) -> None:
    attributes = flatten_attributes(log_records[7]["attributes"])
    assert attributes == {"payment.method": "card", "payment.amount": 240.5, "retry.count": 2}


def test_an_all_zero_trace_id_is_not_a_trace(log_records: list[dict]) -> None:
    assert has_trace_context(log_records[0]) is True
    assert has_trace_context(log_records[3]) is False
    assert has_trace_context(log_records[4]) is False
    # Right length, wrong alphabet.
    assert has_trace_context(log_records[9]) is False


def test_severity_falls_back_to_the_text_then_to_info(log_records: list[dict]) -> None:
    assert severity_number(log_records[5]) == SEVERITY_NUMBERS["FATAL"]
    assert severity_number(log_records[6]) == SEVERITY_NUMBERS["INFO"]
    assert severity_number({"severityNumber": 0, "severityText": "ERROR"}) == 17


def test_routing_drops_debug_and_splits_the_rest(log_records: list[dict]) -> None:
    routes = [route_log(record) for record in log_records]
    assert routes[1] is Route.DROP
    assert routes[0] is Route.SPAN_EVENT
    assert routes[3] is Route.DOCUMENT
    assert routes[9] is Route.DOCUMENT
    assert routes.count(Route.DROP) == 1
    assert routes.count(Route.SPAN_EVENT) == 4
    assert routes.count(Route.DOCUMENT) == 5


def test_lowering_the_threshold_brings_debug_back(log_records: list[dict]) -> None:
    debug = log_records[1]
    assert route_log(debug, min_severity_number=SEVERITY_NUMBERS["DEBUG"]) is Route.SPAN_EVENT


def test_raising_the_threshold_keeps_errors_only(log_records: list[dict]) -> None:
    kept = [r for r in log_records if route_log(r, SEVERITY_NUMBERS["ERROR"]) is not Route.DROP]
    assert len(kept) == 3
    assert {severity_number(r) for r in kept} == {17, 21}


def test_an_exception_log_becomes_an_exception_event(log_records: list[dict]) -> None:
    event = to_span_event(log_records[2])
    assert event is not None
    assert event.name == "exception"
    assert event.trace_id == "139258a727d98a37ab422d8af5478411"
    assert event.span_id == "32bdc7d0a89e5fa1"
    assert event.attributes["exception.type"] == "CardDeclined"
    assert event.attributes["exception.message"] == "issuer declined: 51"
    assert event.attributes["log.severity"] == "ERROR"
    # The body here is six words. The stack trace is the large field, and it
    # is an attribute, which is the part the first version of this missed.
    assert len(event.attributes["exception.stacktrace"]) == MAX_BODY_CHARS
    assert event.attributes["exception.stacktrace"].endswith(TRUNCATION_MARKER)
    assert event.attributes["log.truncated_fields"] == ["exception.stacktrace"]
    assert "log.body.truncated" not in event.attributes


def test_the_document_form_cuts_the_same_attribute(log_records: list[dict]) -> None:
    document = to_es_document(log_records[2])
    assert len(document["attributes.exception.stacktrace"]) == MAX_BODY_CHARS
    assert document["truncated_fields"] == ["exception.stacktrace"]
    assert document["body"] == "settlement failed"
    assert "body_truncated" not in document


def test_short_attributes_are_untouched_and_non_strings_are_left_alone(
    log_records: list[dict],
) -> None:
    kept, cut = truncate_attributes({"a": "short", "n": 2, "f": 1.5, "b": True})
    assert cut == []
    assert kept == {"a": "short", "n": 2, "f": 1.5, "b": True}
    event = to_span_event(log_records[0])
    assert event is not None
    assert "log.truncated_fields" not in event.attributes


def test_an_ordinary_log_becomes_a_log_event(log_records: list[dict]) -> None:
    event = to_span_event(log_records[0])
    assert event is not None
    assert event.name == "log"
    assert event.attributes["log.message"] == "order accepted"
    assert event.attributes["http.response.status_code"] == 201
    assert event.time_unix_nano == 1791000000123456789


def test_a_record_with_no_span_makes_no_event(log_records: list[dict]) -> None:
    assert to_span_event(log_records[4]) is None


def test_event_name_reads_the_semantic_convention() -> None:
    assert event_name({"exception.type": "ValueError"}) == "exception"
    assert event_name({"exception.message": "no type key"}) == "log"
    assert event_name({}) == "log"


def test_a_long_body_is_cut_and_the_document_says_so() -> None:
    record = {
        "timeUnixNano": "1791000000123456789",
        "severityText": "ERROR",
        "body": {"stringValue": "x" * 5000},
        "resource": {"attributes": []},
    }
    document = to_es_document(record)
    assert len(document["body"]) == MAX_BODY_CHARS
    assert document["body"].endswith(TRUNCATION_MARKER)
    assert document["body_truncated"] is True


def test_truncation_keeps_the_limit_and_marks_the_cut() -> None:
    text = "x" * 5000
    cut, was_cut = truncate(text, 100)
    assert was_cut is True
    assert len(cut) == 100
    assert cut.endswith(TRUNCATION_MARKER)
    assert truncate("short", 100) == ("short", False)
    with pytest.raises(ValueError):
        truncate("anything", 3)


def test_timestamps_keep_milliseconds_and_the_elasticsearch_suffix() -> None:
    assert iso_timestamp(1791000000123456789) == "2026-10-03T04:00:00.123Z"
    assert iso_timestamp("1791000000999000000") == "2026-10-03T04:00:00.999Z"
    assert iso_timestamp(0) == "1970-01-01T00:00:00.000Z"
    # Two lines in the same millisecond keep their order in the nanos, which is
    # why this does not go through a float.
    assert iso_timestamp(1791000000123456789) == iso_timestamp(1791000000123999999)


def test_a_document_is_flat_and_carries_the_resource(log_records: list[dict]) -> None:
    document = to_es_document(log_records[3])
    assert document["@timestamp"] == "2026-10-03T04:00:00.132Z"
    assert document["severity_text"] == "WARN"
    assert document["severity_number"] == 13
    assert document["service.name"] == "settlement-worker"
    assert document["service.version"] == "0.9.7"
    assert document["attributes.messaging.system"] == "kafka"
    assert "trace.id" not in document
    assert all(not isinstance(value, dict) for value in document.values())


def test_a_document_from_a_traced_record_carries_the_correlation_keys(
    log_records: list[dict],
) -> None:
    document = to_es_document(log_records[0])
    assert document["trace.id"] == "139258a727d98a37ab422d8af5478411"
    assert document["span.id"] == "3145f562a345f051"
    assert document["attributes.order.expedited"] is True


def test_a_non_string_body_still_produces_a_string(log_records: list[dict]) -> None:
    document = to_es_document(log_records[8])
    assert document["body"] == "409"
    assert document["severity_text"] == "INFO"


def test_no_document_carries_a_null_field(log_records: list[dict]) -> None:
    # Elasticsearch indexes a null as a missing field anyway, and a null in a
    # bulk body costs bytes for nothing.
    for record in log_records:
        document: dict[str, Any] = to_es_document(record)
        assert None not in document.values()
