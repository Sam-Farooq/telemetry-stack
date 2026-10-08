"""Scrape config, alert rules and dashboards.

The check worth having here is the boring one: every metric a panel or an
alert names has to be a metric something in this stack actually emits. The
fixtures in fixtures/ are recorded scrapes, so they answer that question
without Prometheus running. A panel that queries a metric nobody produces
renders an empty graph, and an empty graph is read as "nothing is wrong".
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from telemetry.backpressure import FILLING_AT, SATURATED_AT
from telemetry.cardinality import load_budgets
from telemetry.promtext import base_name, metric_names, parse_file

# PromQL functions, aggregators and keywords. Anything else that looks like an
# identifier is treated as a metric name and has to exist in a fixture.
PROMQL_WORDS = frozenset(
    {
        "rate",
        "irate",
        "increase",
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "count_values",
        "topk",
        "bottomk",
        "quantile",
        "histogram_quantile",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "offset",
        "and",
        "or",
        "unless",
        "bool",
        "absent",
        "clamp_max",
        "clamp_min",
        "delta",
        "deriv",
        "label_replace",
        "predict_linear",
        "stddev",
        "sum_over_time",
        "avg_over_time",
        "max_over_time",
        "min_over_time",
        "time",
        "vector",
        "scalar",
    }
)
IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
STRING = re.compile(r"""("[^"]*"|'[^']*')""")
DURATION = re.compile(r"\[\d+[smhdwy]\]")
SELECTOR = re.compile(r"\{[^{}]*\}")
GROUPING = re.compile(r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^()]*\)")


@pytest.fixture(scope="module")
def known_metrics(repo_root: Path) -> set[str]:
    names: set[str] = set()
    for fixture in sorted((repo_root / "fixtures").glob("*.txt")):
        samples = parse_file(fixture)
        names |= metric_names(samples)
        names |= metric_names(samples, families=True)
    return names


@pytest.fixture(scope="module")
def prometheus_config(repo_root: Path) -> dict[str, Any]:
    return yaml.safe_load((repo_root / "prometheus" / "prometheus.yml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def rules(repo_root: Path) -> dict[str, Any]:
    path = repo_root / "prometheus" / "rules" / "collector.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def dashboards(repo_root: Path) -> dict[str, dict[str, Any]]:
    folder = repo_root / "grafana" / "dashboards"
    return {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(folder.glob("*.json"))
    }


def metrics_in(expr: str) -> set[str]:
    """Metric names only: label selectors, grouping lists and ranges removed."""
    text = STRING.sub(" ", expr)
    text = SELECTOR.sub(" ", text)
    text = GROUPING.sub(" ", text)
    text = DURATION.sub(" ", text)
    return {token for token in IDENTIFIER.findall(text) if token not in PROMQL_WORDS}


def exprs_in(dashboard: dict[str, Any]) -> list[tuple[str, str]]:
    out = []
    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            out.append((panel["title"], target["expr"]))
    return out


# --- the scrape config ------------------------------------------------------


def test_every_job_has_a_sample_limit(prometheus_config: dict[str, Any]) -> None:
    jobs = prometheus_config["scrape_configs"]
    assert {job["job_name"] for job in jobs} == {
        "otel-gateway",
        "otel-agent",
        "app-metrics",
        "prometheus",
    }
    for job in jobs:
        assert job["sample_limit"] > 0, job["job_name"]


def test_the_scrape_targets_are_the_ports_the_collectors_open(
    prometheus_config: dict[str, Any], repo_root: Path
) -> None:
    targets = {
        job["job_name"]: job["static_configs"][0]["targets"][0]
        for job in prometheus_config["scrape_configs"]
    }
    configs = {
        tier: yaml.safe_load((repo_root / "collector" / f"{tier}.yaml").read_text())
        for tier in ("agent", "gateway")
    }
    for tier, job in (("agent", "otel-agent"), ("gateway", "otel-gateway")):
        internal = configs[tier]["service"]["telemetry"]["metrics"]["address"]
        assert targets[job].endswith(internal.split(":")[-1])
    exporter = configs["gateway"]["exporters"]["prometheus"]["endpoint"]
    assert targets["app-metrics"].endswith(exporter.split(":")[-1])


def test_the_unbounded_labels_are_dropped_at_the_scrape_too(
    prometheus_config: dict[str, Any],
) -> None:
    job = next(j for j in prometheus_config["scrape_configs"] if j["job_name"] == "app-metrics")
    relabel = job["metric_relabel_configs"][0]
    assert relabel["action"] == "labeldrop"
    assert "customer_id" in relabel["regex"]


def test_the_rule_file_the_config_names_is_the_one_in_the_repo(
    prometheus_config: dict[str, Any], repo_root: Path
) -> None:
    declared = prometheus_config["rule_files"]
    assert declared == ["/etc/prometheus/rules/collector.yml"]
    compose = yaml.safe_load((repo_root / "compose.yaml").read_text(encoding="utf-8"))
    mounts = compose["services"]["prometheus"]["volumes"]
    assert any(str(m).startswith("./prometheus/rules:/etc/prometheus/rules") for m in mounts)
    assert (repo_root / "prometheus" / "rules" / "collector.yml").exists()


# --- the alert rules -------------------------------------------------------


def test_every_alert_waits_and_says_what_to_do(rules: dict[str, Any]) -> None:
    alerts = [rule for group in rules["groups"] for rule in group["rules"]]
    assert len(alerts) == 6
    for alert in alerts:
        assert alert["for"], alert["alert"]
        assert alert["labels"]["severity"] in {"warning", "critical"}
        assert alert["annotations"]["summary"]
        assert len(alert["annotations"]["description"]) > 80, alert["alert"]


def test_nothing_pages_on_a_retried_send(rules: dict[str, Any]) -> None:
    exprs = " ".join(rule["expr"] for group in rules["groups"] for rule in group["rules"])
    assert "enqueue_failed" in exprs
    assert "send_failed" not in exprs


def test_the_saturation_alert_uses_the_same_threshold_as_the_code(
    rules: dict[str, Any],
) -> None:
    alert = next(
        rule
        for group in rules["groups"]
        for rule in group["rules"]
        if rule["alert"] == "CollectorQueueSaturated"
    )
    threshold = float(alert["expr"].rsplit(">", 1)[1])
    assert threshold == SATURATED_AT


def test_the_series_alert_is_above_what_the_budgets_add_up_to(rules: dict[str, Any]) -> None:
    alert = next(
        rule
        for group in rules["groups"]
        for rule in group["rules"]
        if rule["alert"] == "SeriesBudgetExceeded"
    )
    limit = float(alert["expr"].rsplit(">", 1)[1])
    budgeted = sum(budget.projected_series() for budget in load_budgets().values())
    # 17,000-odd budgeted series against a 100,000 alarm: the gap is the
    # collector's own telemetry and Prometheus itself, and it is deliberate.
    assert budgeted < limit
    assert 10_000 < budgeted < 20_000


def test_the_metric_reader_ignores_labels_and_grouping() -> None:
    assert metrics_in('sum(rate(a_total{code="500"}[5m])) by (job)') == {"a_total"}
    assert metrics_in("count(count by (service, route) (b_count))") == {"b_count"}
    assert metrics_in("c / d") == {"c", "d"}


def test_every_metric_an_alert_names_is_one_something_emits(
    rules: dict[str, Any], known_metrics: set[str]
) -> None:
    for group in rules["groups"]:
        for rule in group["rules"]:
            for metric in metrics_in(rule["expr"]):
                assert metric in known_metrics, f"{rule['alert']} queries {metric}"


# --- the dashboards --------------------------------------------------------


def test_both_dashboards_are_present_and_identified(dashboards: dict[str, dict]) -> None:
    assert set(dashboards) == {"pipeline-health.json", "cardinality.json"}
    uids = {doc["uid"] for doc in dashboards.values()}
    assert uids == {"telemetry-pipeline", "telemetry-cardinality"}
    for name, doc in dashboards.items():
        assert doc["title"], name
        assert doc["panels"], name
        assert doc["timezone"] == "utc"


def test_every_panel_names_a_provisioned_datasource(
    dashboards: dict[str, dict], repo_root: Path
) -> None:
    provisioned = {
        source["uid"]
        for source in yaml.safe_load(
            (repo_root / "grafana" / "provisioning" / "datasources.yaml").read_text()
        )["datasources"]
    }
    assert provisioned == {"prometheus", "es-traces", "es-logs"}
    for name, doc in dashboards.items():
        for panel in doc["panels"]:
            assert panel["datasource"]["uid"] in provisioned, f"{name}: {panel['title']}"
            for target in panel["targets"]:
                assert target["datasource"]["uid"] in provisioned
                assert target["expr"].strip()


def test_every_metric_a_panel_queries_is_one_something_emits(
    dashboards: dict[str, dict], known_metrics: set[str]
) -> None:
    checked = 0
    for name, doc in dashboards.items():
        for title, expr in exprs_in(doc):
            for metric in metrics_in(expr):
                assert metric in known_metrics, f"{name}: {title} queries {metric}"
                checked += 1
    assert checked == 18


def test_the_queue_gauge_bands_match_the_classifier(dashboards: dict[str, dict]) -> None:
    panel = next(
        p
        for p in dashboards["pipeline-health.json"]["panels"]
        if p["title"] == "Exporter queue fill"
    )
    steps = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
    values = [step["value"] for step in steps]
    assert values == [None, FILLING_AT, SATURATED_AT]
    assert panel["fieldConfig"]["defaults"]["unit"] == "percentunit"


def test_the_panel_that_would_mislead_is_absent(dashboards: dict[str, dict]) -> None:
    # send_failed next to enqueue_failed on the same graph reads as if both
    # were loss. Only one of them is.
    all_exprs = " ".join(expr for doc in dashboards.values() for _, expr in exprs_in(doc))
    assert "enqueue_failed_spans" in all_exprs
    assert "send_failed_spans" not in all_exprs


def test_the_histogram_panel_counts_label_sets_the_budget_declares(
    dashboards: dict[str, dict],
) -> None:
    panel = next(
        p
        for p in dashboards["cardinality.json"]["panels"]
        if p["title"] == "Label sets on the request histogram"
    )
    expr = panel["targets"][0]["expr"]
    budget = load_budgets()["http_server_request_duration_seconds"]
    for label in budget.labels:
        assert label in expr, label
    assert base_name("http_server_request_duration_seconds_count") in expr


def test_the_dashboards_are_not_editable_in_the_ui(repo_root: Path) -> None:
    provider = yaml.safe_load(
        (repo_root / "grafana" / "provisioning" / "dashboards.yaml").read_text()
    )["providers"][0]
    assert provider["allowUiUpdates"] is False
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"
    compose = yaml.safe_load((repo_root / "compose.yaml").read_text(encoding="utf-8"))
    mounts = compose["services"]["grafana"]["volumes"]
    assert any(provider["options"]["path"] in str(m) for m in mounts)
