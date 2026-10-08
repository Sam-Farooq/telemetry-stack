"""The services, checked as far as they can be without Kafka or a collector.

Two things are worth proving offline. One, the modules import, which catches an
SDK whose API moved underneath them. Two, a log emitted inside a span arrives
with the trace ids and with a severity number the gateway's filter will let
through. The second one is not obvious: a record carrying only `severity_text`
has a severity number of zero, and `severity_number < SEVERITY_NUMBER_INFO`
drops it on arrival however urgent the text is.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

SERVICES = ("checkout_api", "settlement_worker")


def load_service(repo_root: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, repo_root / "services" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("name", SERVICES)
def test_the_service_module_imports(repo_root: Path, name: str) -> None:
    module = load_service(repo_root, name)
    assert module.__name__ == name


def test_the_consumer_settles_the_same_order_the_same_way(repo_root: Path) -> None:
    worker = load_service(repo_root, "settlement_worker")
    first = worker.settle({"order_id": 4812})
    assert first == worker.settle({"order_id": 4812})
    assert first[0] in worker.STATUSES
    assert first[1] in worker.METHODS
    # Bounded, because both are metric labels and the budget says so.
    statuses = {worker.settle({"order_id": i})[0] for i in range(200)}
    methods = {worker.settle({"order_id": i})[1] for i in range(200)}
    assert statuses == set(worker.STATUSES)
    assert methods == set(worker.METHODS)


def test_the_producer_keeps_the_raw_path_off_the_metric(repo_root: Path) -> None:
    api = load_service(repo_root, "checkout_api")
    assert api.HEALTH_ROUTES == ("/healthz", "/readyz")
    # The handler normalises before the label, and puts the raw path on the
    # span instead. Both halves are asserted here by reading the source,
    # because the socket path needs a running server.
    source = (repo_root / "services" / "checkout_api.py").read_text(encoding="utf-8")
    assert 'span.set_attribute("url.path", raw_path)' in source
    assert "route = normalise_route(raw_path)" in source
    assert '"http_route": route' in source


def test_a_log_in_a_span_carries_the_trace_and_a_severity_number(
    repo_root: Path,
) -> None:
    from opentelemetry._logs import SeverityNumber
    from opentelemetry.sdk._logs import LoggerProvider, LogRecordProcessor
    from opentelemetry.sdk.trace import TracerProvider

    captured: list[Any] = []

    class Capture(LogRecordProcessor):
        def on_emit(self, record: Any) -> None:
            captured.append(record)

        def emit(self, record: Any) -> None:
            captured.append(record)

        def shutdown(self) -> None:
            return None

        def force_flush(self, timeout_millis: int = 0) -> bool:
            return True

    logger_provider = LoggerProvider()
    logger_provider.add_log_record_processor(Capture())
    logger = logger_provider.get_logger("probe")
    tracer = TracerProvider().get_tracer("probe")

    with tracer.start_as_current_span("POST /orders"):
        logger.emit(
            severity_text="ERROR",
            severity_number=SeverityNumber["ERROR"],
            body="order rejected",
            attributes={"order.id": 4812},
        )

    assert len(captured) == 1
    record = captured[0].log_record
    assert record.trace_id != 0
    assert record.span_id != 0
    assert record.body == "order rejected"

    gateway = yaml.safe_load((repo_root / "collector" / "gateway.yaml").read_text())
    condition = gateway["processors"]["filter/logs"]["logs"]["log_record"][0]
    threshold = SeverityNumber[condition.rsplit("_", 1)[1]]
    assert record.severity_number.value >= threshold.value


def test_the_severity_names_the_producer_uses_are_all_known() -> None:
    from opentelemetry._logs import SeverityNumber

    for name in ("INFO", "ERROR"):
        assert SeverityNumber[name].value > 0
    with pytest.raises(KeyError):
        SeverityNumber["LOUD"]
