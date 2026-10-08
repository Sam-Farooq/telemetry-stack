"""What happens to the collector when Elasticsearch is slow, read from metrics.

The exporter holds a sending queue of batches. While Elasticsearch keeps up the
queue sits near zero. When it does not, the queue fills, and when the queue is
full the exporter refuses new batches and the data is gone. Three counters tell
three different stories and they are easy to confuse:

  otelcol_exporter_sent_spans            left the process
  otelcol_exporter_send_failed_spans     an attempt failed, retry may still win
  otelcol_exporter_enqueue_failed_spans  refused by a full queue, data lost

Only the third is loss. A dashboard that alerts on the second pages somebody
every time Elasticsearch returns a 429 that the retry then absorbs.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum

from telemetry.promtext import Sample

FILLING_AT = 0.5
SATURATED_AT = 0.9


class Status(Enum):
    HEALTHY = "healthy"
    FILLING = "filling"
    SATURATED = "saturated"
    DROPPING = "dropping"
    UNKNOWN = "unknown"


# What scripts/collector-health.py returns to the shell. Saturated is worth a
# shout in CI or a cron; dropping is worth waking somebody.
EXIT_CODES: dict[Status, int] = {
    Status.HEALTHY: 0,
    Status.FILLING: 0,
    Status.UNKNOWN: 0,
    Status.SATURATED: 1,
    Status.DROPPING: 2,
}

_SEVERITY = [Status.UNKNOWN, Status.HEALTHY, Status.FILLING, Status.SATURATED, Status.DROPPING]


@dataclass(frozen=True)
class QueueState:
    exporter: str
    data_type: str
    size: float
    capacity: float
    enqueue_failed: float = 0.0
    send_failed: float = 0.0
    sent: float = 0.0

    @property
    def saturation(self) -> float:
        if self.capacity <= 0:
            return 0.0
        return self.size / self.capacity

    @property
    def headroom(self) -> float:
        return max(self.capacity - self.size, 0.0)


def _counter(samples: Iterable[Sample], name: str, exporter: str) -> float:
    """Sum a counter across its label sets. The error label splits it up."""
    return sum(s.value for s in samples if s.name == name and s.labels.get("exporter") == exporter)


def read_queue_states(samples: Sequence[Sample]) -> list[QueueState]:
    """Pull one state per exporter and data type out of a scrape."""
    sizes = [s for s in samples if s.name == "otelcol_exporter_queue_size"]
    capacities = {
        (s.labels.get("exporter"), s.labels.get("data_type")): s.value
        for s in samples
        if s.name == "otelcol_exporter_queue_capacity"
    }
    states: list[QueueState] = []
    for size in sizes:
        exporter = size.labels.get("exporter", "")
        data_type = size.labels.get("data_type", "")
        failed_suffix = {"traces": "spans", "logs": "log_records", "metrics": "metric_points"}.get(
            data_type, "spans"
        )
        states.append(
            QueueState(
                exporter=exporter,
                data_type=data_type,
                size=size.value,
                capacity=capacities.get((exporter, data_type), 0.0),
                enqueue_failed=_counter(
                    samples, f"otelcol_exporter_enqueue_failed_{failed_suffix}", exporter
                ),
                send_failed=_counter(
                    samples, f"otelcol_exporter_send_failed_{failed_suffix}", exporter
                ),
                sent=_counter(samples, f"otelcol_exporter_sent_{failed_suffix}", exporter),
            )
        )
    return sorted(states, key=lambda s: (s.exporter, s.data_type))


def classify(state: QueueState, previous: QueueState | None = None) -> Status:
    """One queue's status.

    With two scrapes the enqueue counter is read as a delta, which is the
    honest reading: the counter is cumulative since process start, so a single
    scrape can only say that data was lost at some point, not that it is being
    lost now.
    """
    if state.capacity <= 0:
        return Status.UNKNOWN
    lost = state.enqueue_failed - previous.enqueue_failed if previous else state.enqueue_failed
    if lost > 0:
        return Status.DROPPING
    if state.saturation >= SATURATED_AT:
        return Status.SATURATED
    if state.saturation >= FILLING_AT:
        return Status.FILLING
    return Status.HEALTHY


def worst(statuses: Iterable[Status]) -> Status:
    collected = list(statuses)
    if not collected:
        return Status.UNKNOWN
    return max(collected, key=_SEVERITY.index)


def seconds_to_full(state: QueueState, enqueue_rate: float, drain_rate: float) -> float | None:
    """None when the queue is draining at least as fast as it fills."""
    if enqueue_rate < 0 or drain_rate < 0:
        raise ValueError("rates are not negative")
    net = enqueue_rate - drain_rate
    if net <= 0:
        return None
    return state.headroom / net


def resident_items(capacity_batches: float, max_items_per_batch: int) -> float:
    """Worst case items held in memory by a full queue.

    This is the number that has to be compared against memory_limiter. A queue
    sized in batches hides it: 256 batches sounds small and 2,048 items a batch
    makes it half a million spans.
    """
    if capacity_batches < 0 or max_items_per_batch < 0:
        raise ValueError("sizes are not negative")
    return capacity_batches * max_items_per_batch


def explain(state: QueueState, status: Status) -> str:
    """One line per queue, for the script's output."""
    percent = state.saturation * 100
    base = (
        f"{state.exporter}/{state.data_type}: {state.size:.0f} of {state.capacity:.0f} "
        f"batches ({percent:.0f}%), status {status.value}"
    )
    if status is Status.DROPPING:
        return f"{base}, {state.enqueue_failed:.0f} items refused by a full queue"
    if state.send_failed:
        return f"{base}, {state.send_failed:.0f} failed send attempts being retried"
    return base
