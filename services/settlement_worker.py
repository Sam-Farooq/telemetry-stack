#!/usr/bin/env python3
"""Consumes orders, settles them, and continues the trace that produced them.

The interesting part is the first ten lines of `handle`: the span here is a
child of the producer's span because the context travelled in the Kafka
headers. Without the extract call this process generates a second trace and
the gateway's tail sampler decides on each half separately, which is how a
trace ends up half kept.
"""

from __future__ import annotations

import json
import signal
import sys
import time
from types import FrameType

from telemetry.otel import LAG_INSTRUMENT, SETTLED_INSTRUMENT, settings_from_env, setup

POLL_SECONDS = 1.0
STATUSES = ("settled", "declined", "pending", "chargeback")
METHODS = ("card", "bank_transfer", "wallet", "voucher", "credit")

_running = True


def _stop(_signum: int, _frame: FrameType | None) -> None:
    global _running
    _running = False


def settle(payload: dict[str, object]) -> tuple[str, str]:
    """Deterministic from the order id, so a replayed message settles the same."""
    order_id = int(payload.get("order_id", 0))
    return STATUSES[order_id % len(STATUSES)], METHODS[order_id % len(METHODS)]


def main() -> int:
    from confluent_kafka import Consumer, TopicPartition
    from opentelemetry.propagate import extract
    from opentelemetry.trace import SpanKind

    settings = settings_from_env()
    telemetry = setup(settings)
    settled = telemetry.meter.create_counter(
        SETTLED_INSTRUMENT, description="Orders that reached a terminal settlement state"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.orders_topic])

    def lag_callback(options: object) -> list[object]:
        """Observable gauge: lag is read when the SDK collects, not on a timer
        of its own. Partition is a bounded label, which is why it is allowed."""
        from opentelemetry.metrics import Observation

        observations = []
        for assigned in consumer.assignment():
            position = consumer.position([TopicPartition(assigned.topic, assigned.partition)])
            _, high = consumer.get_watermark_offsets(assigned, timeout=1.0, cached=True)
            offset = position[0].offset if position else -1
            if offset is None or offset < 0 or high is None:
                continue
            observations.append(
                Observation(
                    max(high - offset, 0),
                    {
                        "service": settings.service_name,
                        "topic": assigned.topic,
                        "partition": str(assigned.partition),
                    },
                )
            )
        return observations

    telemetry.meter.create_observable_gauge(
        LAG_INSTRUMENT,
        callbacks=[lag_callback],
        description="Records behind the end of the partition",
    )

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    print(f"settlement-worker consuming {settings.orders_topic}", flush=True)

    try:
        while _running:
            message = consumer.poll(POLL_SECONDS)
            if message is None:
                continue
            if message.error():
                print(f"consumer error: {message.error()}", file=sys.stderr, flush=True)
                continue

            headers = {key: value.decode() for key, value in (message.headers() or [])}
            parent = extract(headers)
            with telemetry.tracer.start_as_current_span(
                f"{settings.orders_topic} process", context=parent, kind=SpanKind.CONSUMER
            ) as span:
                payload = json.loads(message.value())
                status, method = settle(payload)
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination.name", message.topic())
                span.set_attribute("messaging.kafka.partition", message.partition())
                span.set_attribute("settlement.status", status)
                observation = telemetry.guard.observe(
                    "orders_settled_total",
                    {
                        "service": settings.service_name,
                        "settlement_status": status,
                        "payment_method": method,
                    },
                )
                settled.add(1, attributes=observation.labels)
                time.sleep(0.01)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        telemetry.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
