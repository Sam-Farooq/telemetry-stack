from __future__ import annotations

import pytest

from telemetry.backpressure import (
    EXIT_CODES,
    QueueState,
    Status,
    classify,
    explain,
    read_queue_states,
    resident_items,
    seconds_to_full,
    worst,
)
from telemetry.promtext import Sample


def q(**kw: float | str) -> QueueState:
    base: dict[str, float | str] = {
        "exporter": "elasticsearch",
        "data_type": "traces",
        "size": 0.0,
        "capacity": 256.0,
    }
    base.update(kw)
    return QueueState(**base)  # type: ignore[arg-type]


def test_the_fixture_shows_the_gateway_dropping(
    collector_samples: list[Sample],
) -> None:
    states = {(s.exporter, s.data_type): s for s in read_queue_states(collector_samples)}
    assert set(states) == {
        ("elasticsearch", "traces"),
        ("elasticsearch/logs", "logs"),
        ("prometheus", "metrics"),
    }

    traces = states[("elasticsearch", "traces")]
    assert (traces.size, traces.capacity) == (196.0, 256.0)
    assert traces.saturation == pytest.approx(0.765625)
    assert traces.enqueue_failed == 61440.0
    assert traces.send_failed == 20480.0
    assert classify(traces) is Status.DROPPING

    logs = states[("elasticsearch/logs", "logs")]
    assert logs.enqueue_failed == 0.0
    assert classify(logs) is Status.HEALTHY
    assert classify(states[("prometheus", "metrics")]) is Status.HEALTHY

    assert worst(classify(s) for s in states.values()) is Status.DROPPING


def test_counters_are_summed_across_the_error_label(collector_samples: list[Sample]) -> None:
    # send_failed carries a different `error` label per rejection reason, so a
    # single-sample lookup reads one reason and under-reports the rest.
    traces = next(
        s for s in read_queue_states(collector_samples) if s.data_type == "traces"
    )
    assert traces.sent == 1284922.0


def test_saturation_thresholds() -> None:
    assert classify(q(size=0)) is Status.HEALTHY
    assert classify(q(size=127)) is Status.HEALTHY
    assert classify(q(size=128)) is Status.FILLING
    assert classify(q(size=230)) is Status.FILLING
    assert classify(q(size=231)) is Status.SATURATED
    assert classify(q(size=256)) is Status.SATURATED


def test_a_queue_with_no_capacity_is_unknown_not_healthy() -> None:
    assert classify(q(capacity=0)) is Status.UNKNOWN
    assert q(capacity=0).saturation == 0.0


def test_one_scrape_cannot_tell_now_from_ever() -> None:
    before = q(size=10, enqueue_failed=61440)
    after = q(size=10, enqueue_failed=61440)
    # Absolute reading: data was lost at some point since start.
    assert classify(after) is Status.DROPPING
    # Delta reading: nothing is being lost between these two scrapes.
    assert classify(after, previous=before) is Status.HEALTHY
    assert classify(q(size=10, enqueue_failed=61441), previous=before) is Status.DROPPING


def test_retried_send_failures_are_not_loss() -> None:
    state = q(size=12, send_failed=20480)
    assert classify(state) is Status.HEALTHY
    assert "being retried" in explain(state, Status.HEALTHY)


def test_worst_wins_and_an_empty_set_is_unknown() -> None:
    assert worst([Status.HEALTHY, Status.FILLING, Status.SATURATED]) is Status.SATURATED
    assert worst([Status.DROPPING, Status.HEALTHY]) is Status.DROPPING
    assert worst([]) is Status.UNKNOWN
    assert worst([Status.UNKNOWN, Status.HEALTHY]) is Status.HEALTHY


def test_exit_codes_only_fail_for_the_states_worth_failing_for() -> None:
    assert EXIT_CODES[Status.HEALTHY] == 0
    assert EXIT_CODES[Status.FILLING] == 0
    assert EXIT_CODES[Status.UNKNOWN] == 0
    assert EXIT_CODES[Status.SATURATED] == 1
    assert EXIT_CODES[Status.DROPPING] == 2
    assert set(EXIT_CODES) == set(Status)


def test_time_to_full_is_headroom_over_the_net_rate() -> None:
    state = q(size=196)
    assert seconds_to_full(state, enqueue_rate=20, drain_rate=10) == pytest.approx(6.0)
    assert seconds_to_full(state, enqueue_rate=10, drain_rate=10) is None
    assert seconds_to_full(state, enqueue_rate=5, drain_rate=10) is None
    assert seconds_to_full(q(size=256), enqueue_rate=20, drain_rate=0) == 0.0
    with pytest.raises(ValueError):
        seconds_to_full(state, enqueue_rate=-1, drain_rate=0)


def test_resident_items_turns_batches_back_into_spans() -> None:
    assert resident_items(256, 2048) == 524288
    assert resident_items(10000, 2048) == 20480000
    assert resident_items(0, 2048) == 0
    with pytest.raises(ValueError):
        resident_items(-1, 2048)


def test_explain_names_the_queue_and_the_numbers() -> None:
    line = explain(q(size=196, enqueue_failed=61440), Status.DROPPING)
    assert line.startswith("elasticsearch/traces: 196 of 256 batches (77%)")
    assert "61440 items refused" in line
