from __future__ import annotations

import pytest

from telemetry.sampling import (
    DEFAULT_POLICY,
    Policy,
    Trace,
    decide,
    errors_lost_to_head_sampling,
    head_sample,
    keep_rate,
    policy_counts,
    resident_spans,
    trace_id_fraction,
)
from tests.ids import tid, tid_at


def mk(trace_id: str, **kw: object) -> Trace:
    base: dict[str, object] = {
        "service": "checkout-api",
        "route": "/orders/:id",
        "duration_ms": 40.0,
        "span_count": 7,
    }
    base.update(kw)
    return Trace(trace_id=trace_id, **base)  # type: ignore[arg-type]


def test_fraction_reads_the_low_eight_bytes() -> None:
    assert trace_id_fraction("a" * 16 + "0" * 16) == 0.0
    assert trace_id_fraction("0" * 16 + "8" + "0" * 15) == 0.5
    assert trace_id_fraction("f" * 32) == pytest.approx(1.0, abs=1e-15)
    # The high bytes are ignored, so two ids that differ only there collide.
    assert trace_id_fraction("0" * 32) == trace_id_fraction("f" * 16 + "0" * 16)


@pytest.mark.parametrize("bad", ["", "abc", "a" * 31, "a" * 33, "z" * 32])
def test_fraction_rejects_anything_that_is_not_a_trace_id(bad: str) -> None:
    with pytest.raises(ValueError):
        trace_id_fraction(bad)


def test_a_failing_health_probe_is_kept() -> None:
    # The classic mistake is dropping /healthz wholesale, which hides exactly
    # the readiness failure that took the pod out of rotation.
    decision = decide(mk(tid(0.99), route="/healthz", status_code=503))
    assert decision == decide(mk(tid(0.99), route="/healthz", status_code=503))
    assert decision.keep is True
    assert decision.policy == "errors"


def test_a_passing_health_probe_is_dropped_even_when_the_hash_says_keep() -> None:
    lucky = tid(0.001)
    assert head_sample(lucky, DEFAULT_POLICY.probabilistic_percentage) is True
    decision = decide(mk(lucky, route="/readyz", status_code=200))
    assert decision.keep is False
    assert decision.policy == "health-routes"


def test_latency_threshold_is_inclusive_and_beats_an_unlucky_hash() -> None:
    unlucky = tid(0.97)
    assert decide(mk(unlucky, duration_ms=500.0)).policy == "slow"
    assert decide(mk(unlucky, duration_ms=500.0)).keep is True
    assert decide(mk(unlucky, duration_ms=499.9)).keep is False


def test_error_flag_without_an_http_status_still_keeps() -> None:
    # A Kafka consumer span has no status code. It still has a status.
    kept = decide(mk(tid(0.8), route="orders.settle", status_code=0, error=True))
    assert (kept.keep, kept.policy) == (True, "errors")


def test_probabilistic_branch_lands_on_the_configured_percentage() -> None:
    traces = [mk(tid_at(i, 2000), duration_ms=12.0) for i in range(2000)]
    rate = keep_rate(traces)
    assert 0.095 <= rate <= 0.105
    # Nothing else may fire on this set: no errors, nothing slow.
    assert set(policy_counts(traces)) == {"sample-the-rest"}


def test_tail_keeps_every_error_where_head_sampling_at_the_same_rate_does_not() -> None:
    traces = [
        mk(tid_at(i, 400), status_code=500 if i % 4 == 0 else 200, duration_ms=30.0)
        for i in range(400)
    ]
    errors = sum(1 for t in traces if t.status_code == 500)
    assert errors == 100

    counts = policy_counts(traces)
    assert counts["errors"] == errors

    lost = errors_lost_to_head_sampling(traces, DEFAULT_POLICY.probabilistic_percentage)
    assert lost == 90
    assert lost / errors > 0.5


def test_a_wider_percentage_keeps_a_superset() -> None:
    traces = [mk(tid_at(i, 500), duration_ms=10.0) for i in range(500)]
    narrow = {t.trace_id for t in traces if decide(t, Policy(probabilistic_percentage=5)).keep}
    wide = {t.trace_id for t in traces if decide(t, Policy(probabilistic_percentage=25)).keep}
    assert narrow < wide
    assert len(narrow) < len(wide)


def test_resident_spans_is_the_rate_times_the_window() -> None:
    assert resident_spans(2000, 10) == 20000
    assert resident_spans(0, 10) == 0
    with pytest.raises(ValueError):
        resident_spans(-1, 10)
