"""Tail sampling, as the gateway collector decides it.

The gateway sees every span of a trace, waits `decision_wait`, then keeps or
drops the whole trace. The policies below are evaluated in order and the first
match wins, which is how the collector's `tail_sampling` processor behaves
once its OR-ed policy list is read top to bottom.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

# Routes whose successful traces are not worth storing. A failing probe still
# is, so this set is consulted only on the probabilistic branch.
HEALTH_ROUTES = frozenset({"/healthz", "/readyz", "/metrics"})

TRACE_ID_HEX_LEN = 32
_LOW_BYTES_START = 16
_UINT64 = 1 << 64


@dataclass(frozen=True)
class Trace:
    """What the gateway knows about a trace when the decision window closes."""

    trace_id: str
    service: str
    route: str
    duration_ms: float
    span_count: int
    status_code: int = 200
    error: bool = False


@dataclass(frozen=True)
class Policy:
    latency_ms: float = 500.0
    probabilistic_percentage: float = 10.0
    error_status_floor: int = 500
    drop_routes: frozenset[str] = HEALTH_ROUTES


@dataclass(frozen=True)
class Decision:
    keep: bool
    policy: str


DEFAULT_POLICY = Policy()


def trace_id_fraction(trace_id: str) -> float:
    """Map a 32-hex trace id onto [0, 1) using its low 8 bytes.

    The probabilistic branch has to agree across gateway replicas, so it reads
    the trace id rather than calling a random number generator. The low bytes
    are the ones the W3C format leaves to the generator; the high bytes carry
    a timestamp in some SDKs and are not uniform.
    """
    cleaned = trace_id.strip().lower()
    if len(cleaned) != TRACE_ID_HEX_LEN:
        raise ValueError(f"trace id must be {TRACE_ID_HEX_LEN} hex chars, got {len(cleaned)}")
    try:
        low = int(cleaned[_LOW_BYTES_START:], 16)
    except ValueError as exc:
        raise ValueError(f"trace id is not hex: {trace_id!r}") from exc
    return low / _UINT64


def decide(trace: Trace, policy: Policy = DEFAULT_POLICY) -> Decision:
    """Keep or drop one trace, naming the policy that settled it."""
    if trace.error or trace.status_code >= policy.error_status_floor:
        return Decision(True, "errors")
    if trace.duration_ms >= policy.latency_ms:
        return Decision(True, "slow")
    if trace.route in policy.drop_routes:
        return Decision(False, "health-routes")
    if trace_id_fraction(trace.trace_id) * 100.0 < policy.probabilistic_percentage:
        return Decision(True, "sample-the-rest")
    return Decision(False, "sample-the-rest")


def head_sample(trace_id: str, percentage: float) -> bool:
    """The decision an SDK can make at the root span, before anything happened.

    Same hash, so the keep rate is the same. It runs before the error and the
    duration are known, which is the whole of the difference.
    """
    return trace_id_fraction(trace_id) * 100.0 < percentage


def keep_rate(traces: Iterable[Trace], policy: Policy = DEFAULT_POLICY) -> float:
    kept = 0
    total = 0
    for trace in traces:
        total += 1
        kept += int(decide(trace, policy).keep)
    return kept / total if total else 0.0


def policy_counts(traces: Iterable[Trace], policy: Policy = DEFAULT_POLICY) -> dict[str, int]:
    """Traces kept per policy name, so a report can show which policy pays."""
    counts: dict[str, int] = {}
    for trace in traces:
        decision = decide(trace, policy)
        if decision.keep:
            counts[decision.policy] = counts.get(decision.policy, 0) + 1
    return counts


def errors_lost_to_head_sampling(traces: Iterable[Trace], percentage: float) -> int:
    """How many failed traces a head sampler at this percentage would throw away."""
    lost = 0
    for trace in traces:
        failed = trace.error or trace.status_code >= DEFAULT_POLICY.error_status_floor
        if failed and not head_sample(trace.trace_id, percentage):
            lost += 1
    return lost


def resident_spans(spans_per_second: float, decision_wait_seconds: float) -> float:
    """Spans the gateway holds while it waits. This is the cost of tail sampling."""
    if spans_per_second < 0 or decision_wait_seconds < 0:
        raise ValueError("rates and windows are not negative")
    return spans_per_second * decision_wait_seconds
