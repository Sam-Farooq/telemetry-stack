"""The configs are the product here, so they are asserted like code.

Two kinds of check: invariants the collector itself will not complain about
until it is running under load, and agreements between a config and the Python
that mirrors it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from telemetry.backpressure import resident_items
from telemetry.logmap import SEVERITY_NUMBERS
from telemetry.sampling import DEFAULT_POLICY

CONFIGS = ("agent.yaml", "gateway.yaml")


@pytest.fixture(scope="module")
def configs(repo_root: Path) -> dict[str, dict[str, Any]]:
    return {
        name: yaml.safe_load((repo_root / "collector" / name).read_text(encoding="utf-8"))
        for name in CONFIGS
    }


@pytest.mark.parametrize("name", CONFIGS)
def test_every_component_a_pipeline_names_is_defined(
    configs: dict[str, dict[str, Any]], name: str
) -> None:
    # A typo here starts the collector with a silently shorter pipeline, or
    # fails at boot with a message about an unknown id. Either way it is
    # cheaper to catch in CI.
    config = configs[name]
    for pipeline, spec in config["service"]["pipelines"].items():
        for section in ("receivers", "processors", "exporters"):
            declared = set(config.get(section, {}) or {})
            used = set(spec.get(section, []) or [])
            assert used <= declared, f"{name}:{pipeline}:{section} {used - declared}"
    assert set(config["service"]["extensions"]) <= set(config["extensions"])


@pytest.mark.parametrize("name", CONFIGS)
def test_the_memory_limiter_runs_first_and_the_batcher_last(
    configs: dict[str, dict[str, Any]], name: str
) -> None:
    for pipeline, spec in configs[name]["service"]["pipelines"].items():
        processors = spec["processors"]
        assert processors[0] == "memory_limiter", f"{name}:{pipeline}"
        assert processors[-1] == "batch", f"{name}:{pipeline}"


@pytest.mark.parametrize("name", CONFIGS)
def test_the_limiter_leaves_headroom_for_a_spike(
    configs: dict[str, dict[str, Any]], name: str
) -> None:
    limiter = configs[name]["processors"]["memory_limiter"]
    assert limiter["spike_limit_mib"] < limiter["limit_mib"]
    assert limiter["spike_limit_mib"] >= limiter["limit_mib"] * 0.2


def test_the_agent_takes_no_sampling_decision(configs: dict[str, dict[str, Any]]) -> None:
    # It sees one node's spans, which is part of a trace.
    assert "tail_sampling" not in configs["agent.yaml"]["processors"]
    assert "tail_sampling" in configs["gateway.yaml"]["processors"]


def test_traces_are_routed_by_trace_id_and_metrics_are_not(
    configs: dict[str, dict[str, Any]],
) -> None:
    agent = configs["agent.yaml"]
    balancer = agent["exporters"]["loadbalancing"]
    assert balancer["routing_key"] == "traceID"
    pipelines = agent["service"]["pipelines"]
    assert pipelines["traces"]["exporters"] == ["loadbalancing"]
    assert pipelines["logs"]["exporters"] == ["loadbalancing"]
    # Routing a metric by trace id would mean hashing a field it does not have.
    assert pipelines["metrics"]["exporters"] == ["otlp/gateway"]


def test_the_agent_points_at_the_port_the_gateway_listens_on(
    configs: dict[str, dict[str, Any]],
) -> None:
    agent = configs["agent.yaml"]
    resolver = agent["exporters"]["loadbalancing"]["resolver"]["dns"]
    gateway_grpc = configs["gateway.yaml"]["receivers"]["otlp"]["protocols"]["grpc"]["endpoint"]
    # resolver.dns.port is declared as a string by the loadbalancing exporter and
    # an int is refused outright, not coerced. This assertion used to read
    # int(...) on both sides, which made it agree with a `port: 4317` that
    # otelcol validate rejects with
    # "'resolver.dns.port' expected type 'string', got unconvertible type 'int'".
    # A test that coerces cannot see a type error, so it checks the type first.
    assert isinstance(resolver["port"], str), "resolver.dns.port must be quoted"
    assert resolver["port"] == gateway_grpc.split(":")[-1]
    # The host comes from the environment so compose and the chart can give it
    # different names without the file being copied.
    assert resolver["hostname"] == "${env:GATEWAY_DNS_NAME}"
    assert agent["exporters"]["otlp/gateway"]["endpoint"] == "${env:GATEWAY_DNS_NAME}:4317"


def test_the_sampling_policies_are_the_ones_the_python_implements(
    configs: dict[str, dict[str, Any]],
) -> None:
    sampling = configs["gateway.yaml"]["processors"]["tail_sampling"]
    policies = {policy["name"]: policy for policy in sampling["policies"]}
    assert set(policies) == {"errors", "server-errors", "slow", "sample-the-rest"}

    assert policies["slow"]["latency"]["threshold_ms"] == DEFAULT_POLICY.latency_ms
    numeric = policies["server-errors"]["numeric_attribute"]
    assert numeric["min_value"] == DEFAULT_POLICY.error_status_floor
    assert numeric["max_value"] == 599

    sub = {p["name"]: p for p in policies["sample-the-rest"]["and"]["and_sub_policy"]}
    assert sub["one-in-ten"]["probabilistic"]["sampling_percentage"] == (
        DEFAULT_POLICY.probabilistic_percentage
    )
    health = sub["not-a-health-route"]["string_attribute"]
    assert health["invert_match"] is True
    assert set(health["values"]) == set(DEFAULT_POLICY.drop_routes)


def test_the_decision_buffer_covers_the_decision_window(
    configs: dict[str, dict[str, Any]],
) -> None:
    sampling = configs["gateway.yaml"]["processors"]["tail_sampling"]
    wait_seconds = int(str(sampling["decision_wait"]).removesuffix("s"))
    arriving = sampling["expected_new_traces_per_sec"] * wait_seconds
    # 2,000 a second for 10 seconds is 20,000 traces open at once. A num_traces
    # below that evicts traces before their window closes, which shows up as
    # sampling_trace_dropped_too_early and looks like missing data, not like a
    # misconfiguration.
    assert arriving == 20000
    assert sampling["num_traces"] >= arriving


def test_the_logs_pipeline_drops_below_the_same_threshold_the_mapper_uses(
    configs: dict[str, dict[str, Any]],
) -> None:
    conditions = configs["gateway.yaml"]["processors"]["filter/logs"]["logs"]["log_record"]
    assert conditions == ["severity_number < SEVERITY_NUMBER_INFO"]
    assert SEVERITY_NUMBERS["INFO"] == 9


def test_the_route_transform_covers_the_id_shapes(configs: dict[str, dict[str, Any]]) -> None:
    statements = configs["gateway.yaml"]["processors"]["transform/route"]["trace_statements"]
    flat = " ".join(s for group in statements for s in group["statements"])
    assert "/[0-9]+" in flat
    assert "[0-9a-fA-F]{8}-" in flat
    assert 'delete_key(attributes, "customer.id")' in flat


@pytest.mark.parametrize("exporter", ["elasticsearch", "elasticsearch/logs"])
def test_the_elasticsearch_queue_is_bounded_durable_and_gives_up(
    configs: dict[str, dict[str, Any]], exporter: str
) -> None:
    gateway = configs["gateway.yaml"]
    spec = gateway["exporters"][exporter]
    queue = spec["sending_queue"]
    # The Elasticsearch exporter declares `retry`, not the standard
    # `retry_on_failure`, and otelcol refuses the latter as an invalid key.
    assert "retry_on_failure" not in spec, "the Elasticsearch exporter rejects this key"
    retry = spec["retry"]

    assert queue["enabled"] is True
    assert queue["storage"] == "file_storage"
    assert "file_storage" in gateway["service"]["extensions"]
    assert retry["enabled"] is True
    # A retry that never gives up is a queue that never drains. This exporter
    # counts retries instead of taking a time budget, so the bound is a count:
    # 8 retries at 1s doubling to a 30s ceiling is about 121 seconds.
    assert 0 < retry["max_retries"] <= 10

    batch_max = gateway["processors"]["batch"]["send_batch_max_size"]
    held = resident_items(queue["queue_size"], batch_max)
    # The limiter stops the process at 512 MiB. A queue sized in batches hides
    # how many spans that is, and a queue that can hold more than the limiter
    # will ever allow is a number that does nothing.
    assert held <= 600_000, f"{exporter} can hold {held:.0f} items"


def test_the_prometheus_exporter_does_not_promote_resource_attributes(
    configs: dict[str, dict[str, Any]],
) -> None:
    # resource_to_telemetry_conversion turns every resource attribute into a
    # label, which is how service.instance.id ends up multiplying every series
    # by the number of pods that have ever run.
    exporter = configs["gateway.yaml"]["exporters"]["prometheus"]
    assert exporter["resource_to_telemetry_conversion"]["enabled"] is False
    assert exporter["endpoint"].endswith(":8889")


@pytest.mark.parametrize("name", CONFIGS)
def test_each_collector_exposes_its_own_telemetry(
    configs: dict[str, dict[str, Any]], name: str
) -> None:
    telemetry = configs[name]["service"]["telemetry"]
    assert telemetry["metrics"]["address"].endswith(":8888")
    assert telemetry["metrics"]["level"] == "normal"
