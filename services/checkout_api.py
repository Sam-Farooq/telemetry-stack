#!/usr/bin/env python3
"""A small HTTP service that places orders onto Kafka.

Instrumented by hand rather than by an auto-instrumentation agent, because the
point of this repo is the attributes and the labels, and the agent's defaults
are exactly what the cardinality guard exists to argue with.

stdlib http.server, so the HTTP side adds no dependency and no framework
opinion. It is a producer with a socket, not a web application.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import FrameType

from telemetry.cardinality import normalise_route, status_class
from telemetry.otel import LATENCY_INSTRUMENT, settings_from_env, setup

PORT = int(os.environ.get("PORT", "8080"))
HEALTH_ROUTES = ("/healthz", "/readyz")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "checkout-api"

    def log_message(self, fmt: str, *args: object) -> None:
        """Silence the default stderr access log. The spans are the access log."""

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def _handle(self, method: str) -> None:
        state = self.server.state  # type: ignore[attr-defined]
        raw_path = self.path
        route = normalise_route(raw_path)

        if raw_path in HEALTH_ROUTES:
            self._respond(200, {"status": "ok"})
            state.record(method, route, 200, 0.0)
            return

        started = time.perf_counter()
        with state.telemetry.tracer.start_as_current_span(f"{method} {route}") as span:
            span.set_attribute("http.request.method", method)
            span.set_attribute("http.route", route)
            # The raw path is on the span, where one value per request is fine.
            # It is never a metric label, which is the whole distinction.
            span.set_attribute("url.path", raw_path)

            order_id = random.randrange(10_000, 99_999)
            fails = random.random() < state.settings.error_rate
            slow = random.random() < state.settings.slow_rate
            if slow:
                time.sleep(0.6)

            if fails:
                span.set_attribute("http.response.status_code", 503)
                span.set_status(_error_status("issuer unreachable"))
                state.log("ERROR", "order rejected", {"order.id": order_id})
                self._respond(503, {"error": "issuer unreachable"})
                code = 503
            else:
                state.produce(order_id, span)
                span.set_attribute("http.response.status_code", 201)
                state.log("INFO", "order accepted", {"order.id": order_id})
                self._respond(201, {"order_id": order_id})
                code = 201

        state.record(method, route, code, time.perf_counter() - started)

    def _respond(self, code: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _error_status(message: str):  # noqa: ANN202 - the SDK type is imported lazily
    from opentelemetry.trace import Status, StatusCode

    return Status(StatusCode.ERROR, message)


class State:
    def __init__(self) -> None:
        from confluent_kafka import Producer
        from opentelemetry.propagate import inject

        self.settings = settings_from_env()
        self.telemetry = setup(self.settings)
        self._inject = inject
        self.producer = Producer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap,
                "linger.ms": 20,
                "enable.idempotence": True,
            }
        )
        self.latency = self.telemetry.meter.create_histogram(
            LATENCY_INSTRUMENT, unit="s", description="Duration of inbound HTTP requests"
        )

    def produce(self, order_id: int, span: object) -> None:
        # The trace continues across the topic. Without these headers the
        # consumer's span is a new trace and the two halves never meet.
        carrier: dict[str, str] = {}
        self._inject(carrier)
        self.producer.produce(
            self.settings.orders_topic,
            key=str(order_id),
            value=json.dumps({"order_id": order_id, "amount": round(random.uniform(5, 400), 2)}),
            headers=[(key, value.encode()) for key, value in carrier.items()],
        )
        self.producer.poll(0)

    def record(self, method: str, route: str, code: int, seconds: float) -> None:
        observation = self.telemetry.guard.observe(
            "http_server_request_duration_seconds",
            {
                "service": self.settings.service_name,
                "http_route": route,
                "http_request_method": method,
                "status_class": status_class(code),
            },
        )
        self.latency.record(seconds, attributes=observation.labels)

    def log(self, severity: str, message: str, attributes: dict[str, object]) -> None:
        """Emit a log record inside the current span.

        The severity number is set, not only the text. The gateway filters on
        `severity_number < SEVERITY_NUMBER_INFO`, and a record that carries no
        number is compared as zero, so a text-only record is dropped on arrival
        however urgent its text says it is.

        No trace id is passed. The record reads the active context itself, and
        passing one by hand is how a log ends up attached to the wrong span.
        """
        from opentelemetry._logs import SeverityNumber

        self.telemetry.logger.emit(
            timestamp=time.time_ns(),
            severity_text=severity,
            severity_number=SeverityNumber[severity],
            body=message,
            attributes=attributes,
        )


def main() -> int:
    state = State()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.state = state  # type: ignore[attr-defined]

    def stop(_signum: int, _frame: FrameType | None) -> None:
        server.shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"checkout-api listening on :{PORT}", flush=True)
    try:
        server.serve_forever()
    finally:
        state.producer.flush(5)
        state.telemetry.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
