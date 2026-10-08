"""Retention and mapping, checked without an Elasticsearch running.

The two failures worth catching here are an index that grows forever because
nothing deletes it, and a document the mapping cannot accept.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from telemetry.logmap import to_es_document

FILES = ("ilm-logs.json", "ilm-traces.json", "template-logs.json", "template-traces.json")


def load(repo_root: Path, name: str) -> dict[str, Any]:
    return json.loads((repo_root / "elasticsearch" / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def es(repo_root: Path) -> dict[str, dict[str, Any]]:
    return {name: load(repo_root, name) for name in FILES}


def days(value: str) -> int:
    return int(str(value).removesuffix("d"))


@pytest.mark.parametrize("name", ["ilm-logs.json", "ilm-traces.json"])
def test_every_policy_rolls_over_and_eventually_deletes(
    es: dict[str, dict[str, Any]], name: str
) -> None:
    phases = es[name]["policy"]["phases"]
    assert set(phases) == {"hot", "warm", "delete"}
    rollover = phases["hot"]["actions"]["rollover"]
    assert rollover["max_age"] == "1d"
    assert rollover["max_primary_shard_size"] == "30gb"
    # The phase that makes this bounded. Without it the index is forever.
    assert phases["delete"]["actions"]["delete"] == {}
    assert days(phases["delete"]["min_age"]) >= 1
    assert days(phases["warm"]["min_age"]) < days(phases["delete"]["min_age"])


def test_traces_are_kept_for_less_time_than_logs(es: dict[str, dict[str, Any]]) -> None:
    traces = days(es["ilm-traces.json"]["policy"]["phases"]["delete"]["min_age"])
    logs = days(es["ilm-logs.json"]["policy"]["phases"]["delete"]["min_age"])
    assert (traces, logs) == (7, 14)
    assert traces < logs


def test_the_retention_ceiling_is_the_number_the_readme_states(
    es: dict[str, dict[str, Any]],
) -> None:
    # One shard rolled at 30gb, once a day, deleted after N days, so the index
    # set cannot exceed 30gb times N. This is the whole of the capacity plan.
    for name, ceiling_gb in (("ilm-logs.json", 420), ("ilm-traces.json", 210)):
        phases = es[name]["policy"]["phases"]
        size_gb = int(phases["hot"]["actions"]["rollover"]["max_primary_shard_size"].rstrip("gb"))
        assert size_gb * days(phases["delete"]["min_age"]) == ceiling_gb


@pytest.mark.parametrize("name", ["template-logs.json", "template-traces.json"])
def test_each_template_points_at_a_policy_that_exists(
    es: dict[str, dict[str, Any]], name: str
) -> None:
    lifecycle = es[name]["template"]["settings"]["index"]["lifecycle"]
    declared = {
        es[policy]["policy"]["_meta"]["policy_name"]: es[policy]["policy"]["_meta"]["write_alias"]
        for policy in ("ilm-logs.json", "ilm-traces.json")
    }
    assert lifecycle["name"] in declared
    # Rollover through an alias only works when the template says which alias.
    assert lifecycle["rollover_alias"] == declared[lifecycle["name"]]


def test_the_write_aliases_are_the_ones_the_gateway_exports_to(
    es: dict[str, dict[str, Any]], repo_root: Path
) -> None:
    import yaml

    gateway = yaml.safe_load((repo_root / "collector" / "gateway.yaml").read_text())
    assert (
        gateway["exporters"]["elasticsearch"]["traces_index"]
        == es["ilm-traces.json"]["policy"]["_meta"]["write_alias"]
    )
    assert (
        gateway["exporters"]["elasticsearch/logs"]["logs_index"]
        == es["ilm-logs.json"]["policy"]["_meta"]["write_alias"]
    )


@pytest.mark.parametrize("name", ["template-logs.json", "template-traces.json"])
def test_mappings_are_closed_and_the_field_count_is_capped(
    es: dict[str, dict[str, Any]], name: str
) -> None:
    template = es[name]["template"]
    assert template["mappings"]["dynamic"] is False
    assert template["mappings"]["properties"]["attributes"]["dynamic"] is False
    assert template["settings"]["index"]["mapping"]["total_fields"]["limit"] == 1000


@pytest.mark.parametrize("name", ["template-logs.json", "template-traces.json"])
def test_refresh_is_relaxed_and_the_index_is_compressed(
    es: dict[str, dict[str, Any]], name: str
) -> None:
    index = es[name]["template"]["settings"]["index"]
    # The default 1s refresh makes a segment a second. Nothing here is read
    # within a second of being written.
    assert index["refresh_interval"] == "5s"
    assert index["codec"] == "best_compression"
    assert index["sort"]["field"] == "@timestamp"


def test_the_two_templates_do_not_fight_over_an_index(es: dict[str, dict[str, Any]]) -> None:
    logs = set(es["template-logs.json"]["index_patterns"])
    traces = set(es["template-traces.json"]["index_patterns"])
    assert logs == {"logs-otel-*"}
    assert traces == {"traces-otel-*"}
    assert not logs & traces
    assert es["template-logs.json"]["priority"] == es["template-traces.json"]["priority"]


def _is_mapped(properties: dict[str, Any], dotted: str) -> bool:
    """Walk a dotted field name through a mapping's properties."""
    node: Any = {"properties": properties}
    for part in dotted.split("."):
        children = node.get("properties") or {}
        if part not in children:
            return False
        node = children[part]
    return "type" in node or "properties" in node


def test_every_field_outside_attributes_has_a_mapping(
    es: dict[str, dict[str, Any]], log_records: list[dict]
) -> None:
    properties = es["template-logs.json"]["template"]["mappings"]["properties"]
    checked = 0
    for record in log_records:
        for field in to_es_document(record):
            if field.startswith("attributes."):
                continue
            assert _is_mapped(properties, field), f"{field} is not in template-logs.json"
            checked += 1
    assert checked > 30


def test_an_undeclared_attribute_is_stored_and_not_searchable(
    es: dict[str, dict[str, Any]], log_records: list[dict]
) -> None:
    # This is the cost of dynamic: false, and it is the trade the repo takes.
    # `attributes.order.expedited` is in _source, so it shows in the document,
    # and a query on it matches nothing until the field is added here.
    properties = es["template-logs.json"]["template"]["mappings"]["properties"]
    emitted = set()
    for record in log_records:
        emitted.update(k for k in to_es_document(record) if k.startswith("attributes."))

    assert "attributes.order.expedited" in emitted
    assert _is_mapped(properties, "attributes.order.expedited") is False
    # The ones a dashboard filters on are declared.
    for field in (
        "attributes.http.route",
        "attributes.exception.type",
        "attributes.messaging.system",
    ):
        assert _is_mapped(properties, field) is True


def test_the_walker_itself_is_honest(es: dict[str, dict[str, Any]]) -> None:
    properties = es["template-logs.json"]["template"]["mappings"]["properties"]
    assert _is_mapped(properties, "service.name") is True
    assert _is_mapped(properties, "attributes.http.route") is True
    assert _is_mapped(properties, "service.nickname") is False
    assert _is_mapped(properties, "nothing") is False


def test_a_dotted_key_under_a_text_field_would_not_map(
    es: dict[str, dict[str, Any]],
) -> None:
    # Why the mapper emits body_truncated and not body.truncated: `body` is a
    # text field, so there is nowhere for `body.truncated` to go.
    properties = es["template-logs.json"]["template"]["mappings"]["properties"]
    assert properties["body"]["type"] == "match_only_text"
    assert _is_mapped(properties, "body_truncated") is True
    assert _is_mapped(properties, "body.truncated") is False
