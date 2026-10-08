"""The chart is rendered by CI with `helm template`. These are the parts a
render cannot check: that the config is passed in rather than copied, and that
the container limits leave the memory limiter room to work.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture(scope="module")
def values(repo_root: Path) -> dict[str, Any]:
    return yaml.safe_load((repo_root / "helm" / "values.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def chart(repo_root: Path) -> dict[str, Any]:
    return yaml.safe_load((repo_root / "helm" / "Chart.yaml").read_text(encoding="utf-8"))


def mib(value: str) -> int:
    return int(str(value).removesuffix("Mi"))


def test_the_chart_holds_no_copy_of_the_collector_config(
    values: dict[str, Any], repo_root: Path
) -> None:
    assert values["agentConfig"] == ""
    assert values["gatewayConfig"] == ""
    templates = (repo_root / "helm" / "templates" / "configmaps.yaml").read_text(encoding="utf-8")
    assert "--set-file agentConfig=collector/agent.yaml" in templates
    assert "required" in templates
    # Nothing under helm/ may contain a receiver block, which would mean a
    # second copy of the pipeline drifting from collector/.
    for path in (repo_root / "helm").rglob("*.yaml"):
        assert "tail_sampling:" not in path.read_text(encoding="utf-8"), path


def test_the_container_limit_is_above_the_memory_limiter(
    values: dict[str, Any], repo_root: Path
) -> None:
    configs = {
        tier: yaml.safe_load((repo_root / "collector" / f"{tier}.yaml").read_text())
        for tier in ("agent", "gateway")
    }
    for tier, key in (("agent", "agent"), ("gateway", "gateway")):
        limiter = configs[tier]["processors"]["memory_limiter"]
        container = mib(values[key]["resources"]["limits"]["memory"])
        assert container >= limiter["limit_mib"] + limiter["spike_limit_mib"], tier


def test_the_gateway_keeps_enough_replicas_to_hash_across(values: dict[str, Any]) -> None:
    assert values["gateway"]["replicas"] >= 2
    assert values["gateway"]["maxUnavailable"] == 1
    assert values["gateway"]["queue"]["size"].endswith("Gi")


def test_the_image_tag_matches_the_one_compose_runs(
    values: dict[str, Any], chart: dict[str, Any], repo_root: Path
) -> None:
    compose = yaml.safe_load((repo_root / "compose.yaml").read_text(encoding="utf-8"))
    compose_tag = compose["services"]["otel-gateway"]["image"].split(":")[-1]
    assert str(values["image"]["tag"]) == compose_tag
    assert str(chart["appVersion"]) == compose_tag
