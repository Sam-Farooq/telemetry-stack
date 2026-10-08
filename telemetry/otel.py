"""SDK wiring for the two services, and the settings they read.

Head sampling is left at always_on here. The gateway does the sampling, and an
SDK that drops a span first takes the choice away from the only process that
can see the whole trace.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from telemetry.cardinality import CardinalityGuard, bucket_boundaries, load_budgets

LATENCY_INSTRUMENT = "http.server.request.duration"
SETTLED_INSTRUMENT = "orders.settled"
LAG_INSTRUMENT = "kafka.consumer.lag.records"


@dataclass(frozen=True)
class Settings:
    service_name: str
    otlp_endpoint: str
    kafka_bootstrap: str
    orders_topic: str
    consumer_group: str
    requests_per_second: float
    error_rate: float
    slow_rate: float


def _fraction(env: Mapping[str, str], key: str, default: str) -> float:
    value = float(env.get(key, default))
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{key} is a fraction between 0 and 1, got {value}")
    return value


def settings_from_env(env: Mapping[str, str] | None = None) -> Settings:
    """Read the environment once, loudly. A service with no endpoint should not
    start and quietly emit into nothing."""
    env = os.environ if env is None else env
    endpoint = env.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        raise ValueError("OTEL_EXPORTER_OTLP_ENDPOINT is required")
    bootstrap = env.get("KAFKA_BOOTSTRAP", "").strip()
    if not bootstrap:
        raise ValueError("KAFKA_BOOTSTRAP is required")
    rate = float(env.get("REQUESTS_PER_SECOND", "40"))
    if rate <= 0:
        raise ValueError(f"REQUESTS_PER_SECOND has to be positive, got {rate}")
    return Settings(
        service_name=env.get("OTEL_SERVICE_NAME", "unnamed-service"),
        otlp_endpoint=endpoint,
        kafka_bootstrap=bootstrap,
        orders_topic=env.get("ORDERS_TOPIC", "orders.placed"),
        consumer_group=env.get("CONSUMER_GROUP", "settlement"),
        requests_per_second=rate,
        error_rate=_fraction(env, "ERROR_RATE", "0.02"),
        slow_rate=_fraction(env, "SLOW_RATE", "0.03"),
    )


@dataclass
class Telemetry:
    tracer: Any
    meter: Any
    logger: Any
    guard: CardinalityGuard
    shutdown: Any


def setup(settings: Settings) -> Telemetry:
    """Build the providers. Imported lazily so the pure helpers stay importable
    without the SDK, which is what the offline tests rely on."""
    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON

    resource = Resource.create({"service.name": settings.service_name})

    tracer_provider = TracerProvider(resource=resource, sampler=ALWAYS_ON)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otlp_endpoint, insecure=True))
    )
    trace.set_tracer_provider(tracer_provider)

    # The bucket boundaries come from label-budgets.json, which is also where
    # the histogram's series arithmetic is written down.
    latency_view = View(
        instrument_name=LATENCY_INSTRUMENT,
        aggregation=ExplicitBucketHistogramAggregation(tuple(bucket_boundaries())),
    )
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=settings.otlp_endpoint, insecure=True),
        export_interval_millis=15_000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader], views=[latency_view])
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=settings.otlp_endpoint, insecure=True))
    )

    def shutdown() -> None:
        tracer_provider.shutdown()
        meter_provider.shutdown()
        logger_provider.shutdown()

    return Telemetry(
        tracer=trace.get_tracer(settings.service_name),
        meter=metrics.get_meter(settings.service_name),
        logger=logger_provider.get_logger(settings.service_name),
        guard=CardinalityGuard(load_budgets()),
        shutdown=shutdown,
    )
