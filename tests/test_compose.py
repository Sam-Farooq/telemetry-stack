"""compose.yaml is the thing most people will run, so it is checked too.

Every failure here is one that only shows up as a container that will not
start: a mount path that does not exist, a config that asks for an environment
variable nobody sets, a collector told to read a file nobody mounted.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.119.0"


@pytest.fixture(scope="module")
def compose(repo_root: Path) -> dict[str, Any]:
    return yaml.safe_load((repo_root / "compose.yaml").read_text(encoding="utf-8"))


def binds(service: dict[str, Any]) -> list[tuple[str, str]]:
    out = []
    for volume in service.get("volumes", []) or []:
        source, target = str(volume).split(":")[:2]
        if source.startswith("./") or source.startswith("/"):
            out.append((source, target))
    return out


def test_the_stack_is_the_eight_containers_the_readme_lists(compose: dict[str, Any]) -> None:
    assert set(compose["services"]) == {
        "kafka",
        "elasticsearch",
        "otel-gateway",
        "otel-agent",
        "prometheus",
        "grafana",
        "checkout-api",
        "settlement-worker",
    }
    assert compose["name"] == "telemetry-stack"


def test_every_bind_mount_exists_in_the_repo(compose: dict[str, Any], repo_root: Path) -> None:
    checked = 0
    for name, service in compose["services"].items():
        for source, _ in binds(service):
            assert (repo_root / source.removeprefix("./")).exists(), f"{name}: {source}"
            checked += 1
    assert checked >= 5


def test_nothing_runs_on_a_floating_tag(compose: dict[str, Any]) -> None:
    for name, service in compose["services"].items():
        image = service.get("image")
        if image is None:
            assert service.get("build") == ".", name
            continue
        assert ":" in image, f"{name} has no tag"
        assert not image.endswith(":latest"), f"{name} floats"


def test_both_collectors_read_the_config_that_is_mounted(compose: dict[str, Any]) -> None:
    for name, expected in (("otel-agent", "agent.yaml"), ("otel-gateway", "gateway.yaml")):
        service = compose["services"][name]
        assert service["image"] == COLLECTOR_IMAGE
        flag = next(arg for arg in service["command"] if arg.startswith("--config="))
        path = flag.split("=", 1)[1]
        mounted = {target for _, target in binds(service)}
        assert path in mounted, f"{name} reads {path}, which is not mounted"
        assert path.endswith(expected)


def test_every_environment_variable_the_configs_expand_is_set(
    compose: dict[str, Any], repo_root: Path
) -> None:
    pattern = re.compile(r"\$\{env:([A-Z0-9_]+)\}")
    for service_name, config_name in (
        ("otel-agent", "agent.yaml"),
        ("otel-gateway", "gateway.yaml"),
    ):
        text = (repo_root / "collector" / config_name).read_text(encoding="utf-8")
        needed = set(pattern.findall(text))
        provided = set(compose["services"][service_name].get("environment", {}) or {})
        assert needed, f"{config_name} expands nothing, check the pattern"
        assert needed <= provided, f"{service_name} is missing {needed - provided}"


def test_the_queue_directory_is_a_volume_not_a_bind(
    compose: dict[str, Any], repo_root: Path
) -> None:
    gateway = compose["services"]["otel-gateway"]
    queue_mounts = [v for v in gateway["volumes"] if "/var/lib/otelcol/queue" in str(v)]
    assert len(queue_mounts) == 1
    name = str(queue_mounts[0]).split(":")[0]
    assert not name.startswith("./")
    assert name in compose["volumes"]
    # The gateway's file_storage extension writes here, so it has to survive a
    # restart, and it must not be a directory in the checkout.
    text = (repo_root / "collector" / "gateway.yaml").read_text(encoding="utf-8")
    assert "directory: /var/lib/otelcol/queue" in text


def test_the_agent_waits_for_the_gateway_and_the_gateway_for_elasticsearch(
    compose: dict[str, Any],
) -> None:
    services = compose["services"]
    assert services["otel-gateway"]["depends_on"]["elasticsearch"]["condition"] == (
        "service_healthy"
    )
    assert "otel-gateway" in services["otel-agent"]["depends_on"]
    for app in ("checkout-api", "settlement-worker"):
        assert services[app]["depends_on"]["kafka"]["condition"] == "service_healthy"
        assert "otel-agent" in services[app]["depends_on"]


def test_the_services_send_otlp_to_the_agent_and_not_to_the_gateway(
    compose: dict[str, Any],
) -> None:
    for app in ("checkout-api", "settlement-worker"):
        endpoint = compose["services"][app]["environment"]["OTEL_EXPORTER_OTLP_ENDPOINT"]
        assert endpoint == "http://otel-agent:4317"


def test_the_agent_is_told_where_the_gateway_is(compose: dict[str, Any]) -> None:
    assert compose["services"]["otel-agent"]["environment"]["GATEWAY_DNS_NAME"] == "otel-gateway"


def test_the_stateful_containers_have_health_checks(compose: dict[str, Any]) -> None:
    for name in ("kafka", "elasticsearch"):
        check = compose["services"][name]["healthcheck"]
        assert check["test"]
        assert check["retries"] >= 10


def test_only_the_ports_a_person_opens_are_published(compose: dict[str, Any]) -> None:
    published = {
        name: service.get("ports", [])
        for name, service in compose["services"].items()
        if service.get("ports")
    }
    # checkout-api is in here because scripts/loadgen.py posts to it from the
    # host, which is what the README tells a person to do.
    assert set(published) == {
        "elasticsearch",
        "otel-gateway",
        "prometheus",
        "grafana",
        "checkout-api",
    }
    assert "8080:8080" in compose["services"]["checkout-api"]["ports"]
    # 4317 is not published: nothing outside the compose network sends OTLP.
    assert all("4317" not in str(ports) for ports in published.values())
