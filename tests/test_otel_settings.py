from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from telemetry import promtext
from telemetry.cardinality import bucket_boundaries
from telemetry.otel import LATENCY_INSTRUMENT, Settings, settings_from_env

MINIMAL = {
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-agent:4317",
    "KAFKA_BOOTSTRAP": "kafka:9092",
}


def test_the_defaults_are_the_ones_compose_sets() -> None:
    settings = settings_from_env(MINIMAL)
    assert isinstance(settings, Settings)
    assert settings.orders_topic == "orders.placed"
    assert settings.consumer_group == "settlement"
    assert settings.requests_per_second == 40.0
    assert settings.error_rate == 0.02
    assert settings.slow_rate == 0.03


def test_the_env_file_covers_every_setting(repo_root: Path) -> None:
    declared = {
        line.split("=", 1)[0]
        for line in (repo_root / ".env.example").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    for key in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "KAFKA_BOOTSTRAP",
        "ORDERS_TOPIC",
        "CONSUMER_GROUP",
        "REQUESTS_PER_SECOND",
        "ERROR_RATE",
        "SLOW_RATE",
    ):
        assert key in declared, key


@pytest.mark.parametrize("missing", ["OTEL_EXPORTER_OTLP_ENDPOINT", "KAFKA_BOOTSTRAP"])
def test_a_service_with_nowhere_to_send_refuses_to_start(missing: str) -> None:
    env = dict(MINIMAL)
    env[missing] = "   "
    with pytest.raises(ValueError, match=missing):
        settings_from_env(env)


@pytest.mark.parametrize(("key", "value"), [("ERROR_RATE", "1.4"), ("SLOW_RATE", "-0.1")])
def test_a_fraction_outside_zero_to_one_is_refused(key: str, value: str) -> None:
    with pytest.raises(ValueError, match="fraction"):
        settings_from_env({**MINIMAL, key: value})


def test_a_request_rate_of_zero_is_refused() -> None:
    with pytest.raises(ValueError, match="positive"):
        settings_from_env({**MINIMAL, "REQUESTS_PER_SECOND": "0"})


def test_the_histogram_boundaries_come_from_the_budget_file() -> None:
    boundaries = bucket_boundaries()
    assert boundaries == sorted(boundaries)
    assert len(set(boundaries)) == len(boundaries) == 12
    assert boundaries[0] == 0.005
    assert boundaries[-1] == 10.0
    assert LATENCY_INSTRUMENT == "http.server.request.duration"


def test_reading_a_scrape_from_a_file(repo_root: Path) -> None:
    text = promtext.read_exposition(str(repo_root / "fixtures" / "app-metrics.txt"))
    assert "http_server_request_duration_seconds_bucket" in text
    assert len(promtext.parse(text)) == 25


def test_reading_a_scrape_from_a_url_without_a_server(monkeypatch: Any) -> None:
    class FakeResponse:
        def read(self) -> bytes:
            return b"a_metric 1\n"

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    calls: list[tuple[str, float]] = []

    def fake_urlopen(url: str, timeout: float = 0.0) -> FakeResponse:
        calls.append((url, timeout))
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    text = promtext.read_exposition("http://localhost:8888/metrics", timeout=2.0)
    assert text == "a_metric 1\n"
    assert calls == [("http://localhost:8888/metrics", 2.0)]
    assert promtext.parse(text)[0].value == 1.0
