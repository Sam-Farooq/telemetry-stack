from __future__ import annotations

import pytest

from telemetry.cardinality import (
    OVERFLOW_VALUE,
    UNKNOWN_VALUE,
    Budget,
    CardinalityGuard,
    UndeclaredMetric,
    audit,
    bucket_boundaries,
    load_budgets,
    normalise_route,
    status_class,
)

LATENCY = "http_server_request_duration_seconds"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/orders/4812/items", "/orders/:id/items"),
        ("/orders/3f2a9c1b-0d44-4e7a-9f31-0a1b2c3d4e5f", "/orders/:id"),
        ("/users/d41d8cd98f00b204e9800998ecf8427e/sessions", "/users/:id/sessions"),
        ("/customers/01HQ3K8ZC9VT6XJ2M4N5P7R8TB", "/customers/:id"),
        ("/accounts/finance@example.com", "/accounts/:id"),
        ("/orders/4812?include=lines&cursor=abc", "/orders/:id"),
        ("/Orders/Items#frag", "/orders/items"),
        ("//orders//4812//", "/orders/:id"),
        ("/", "/"),
        ("", "/"),
    ],
)
def test_route_normalisation_removes_the_variable_parts(raw: str, expected: str) -> None:
    assert normalise_route(raw) == expected


def test_normalisation_is_idempotent() -> None:
    once = normalise_route("/orders/4812/items/9")
    assert once == "/orders/:id/items/:id"
    assert normalise_route(once) == once


def test_short_hex_segments_are_left_alone() -> None:
    # `abc123` is a product slug, not an id. Eleven hex chars stays a word.
    assert normalise_route("/catalog/abc123") == "/catalog/abc123"
    assert normalise_route("/catalog/abcdef123456") == "/catalog/:id"


@pytest.mark.parametrize(
    ("code", "expected"), [(200, "2xx"), (404, "4xx"), (503, "5xx"), (0, UNKNOWN_VALUE)]
)
def test_status_class_collapses_sixty_codes_into_five(code: int, expected: str) -> None:
    assert status_class(code) == expected


def test_shipped_budgets_can_be_met_by_their_own_declared_labels() -> None:
    budgets = load_budgets()
    assert set(budgets) == {
        LATENCY,
        "kafka_consumer_lag_records",
        "orders_settled_total",
        "otelcol_exporter_send_failed_spans",
    }
    for budget in budgets.values():
        assert budget.projected_label_sets() <= budget.max_label_sets, budget.metric


def test_the_latency_histogram_arithmetic_is_what_the_readme_claims() -> None:
    budgets = load_budgets()
    latency = budgets[LATENCY]
    assert len(bucket_boundaries()) == 12
    assert latency.histogram_buckets == 12
    assert latency.projected_label_sets() == 1120
    # 12 boundaries plus _sum, _count and +Inf.
    assert latency.projected_series() == 16800
    assert budgets["orders_settled_total"].projected_series() == 80


def test_an_undeclared_metric_is_refused_rather_than_guessed() -> None:
    guard = CardinalityGuard(load_budgets())
    with pytest.raises(UndeclaredMetric):
        guard.observe("orders_total_v2", {"service": "checkout-api"})


def test_undeclared_label_keys_are_dropped_and_missing_ones_filled() -> None:
    guard = CardinalityGuard(load_budgets())
    observation = guard.observe(
        LATENCY,
        {
            "service": "checkout-api",
            "http_route": "/orders/4812",
            "customer_id": "cus_8812",  # the unbounded one
            "http_request_method": "GET",
        },
    )
    assert "customer_id" not in observation.labels
    assert observation.labels["http_route"] == "/orders/:id"
    assert observation.labels["status_class"] == UNKNOWN_VALUE
    assert observation.over_budget is False


def test_distinct_customer_ids_collapse_onto_one_route_series() -> None:
    guard = CardinalityGuard(load_budgets())
    for customer in range(500):
        guard.observe(
            LATENCY,
            {
                "service": "checkout-api",
                "http_route": f"/customers/{customer}/orders",
                "http_request_method": "GET",
                "status_class": "2xx",
            },
        )
    assert guard.series_count(LATENCY) == 1
    assert guard.overflow_count(LATENCY) == 0


def test_the_guard_folds_instead_of_dropping_once_the_budget_is_full() -> None:
    budgets = {
        "orders_settled_total": Budget(
            metric="orders_settled_total",
            cardinalities={"settlement_status": 4, "payment_method": 5},
            max_label_sets=20,
        )
    }
    guard = CardinalityGuard(budgets)
    folded = 0
    total = 2000
    for i in range(total):
        observation = guard.observe(
            "orders_settled_total",
            {"settlement_status": f"status-{i}", "payment_method": "card"},
        )
        if observation.over_budget:
            folded += 1
            assert observation.labels == {
                "payment_method": OVERFLOW_VALUE,
                "settlement_status": OVERFLOW_VALUE,
            }

    assert guard.series_count("orders_settled_total") == 20
    assert folded == total - 20
    # Every label set here is distinct, so the two counters have to account for
    # all 2,000 observations between them. Nothing was dropped.
    overflowed = guard.overflow_count("orders_settled_total")
    assert overflowed + guard.series_count("orders_settled_total") == total


def test_a_repeated_label_set_does_not_consume_more_budget() -> None:
    guard = CardinalityGuard(load_budgets())
    for _ in range(50):
        guard.observe(
            "kafka_consumer_lag_records",
            {"service": "settlement-worker", "topic": "orders.placed", "partition": "3"},
        )
    assert guard.series_count("kafka_consumer_lag_records") == 1


def test_audit_names_the_label_that_broke_the_budget() -> None:
    budgets = load_budgets()
    observations = [
        (
            LATENCY,
            {
                "service": "checkout-api",
                "http_route": f"/orders/{i}",
                "http_request_method": "GET",
                "status_class": "2xx",
            },
        )
        for i in range(1500)
    ]
    breaches = audit(observations, budgets)
    assert len(breaches) == 1
    breach = breaches[0]
    assert breach.metric == LATENCY
    assert breach.declared == 1200
    assert breach.observed == 1500
    assert breach.worst_label == "http_route"
    assert breach.worst_label_values == 1500


def test_audit_is_quiet_when_everything_fits_and_ignores_unbudgeted_metrics() -> None:
    budgets = load_budgets()
    fits = {
        "service": "checkout-api",
        "http_route": "/orders/:id",
        "http_request_method": "GET",
        "status_class": "2xx",
    }
    observations: list[tuple[str, dict[str, str]]] = [
        (LATENCY, fits),
        ("something_else_total", {"anything": "goes"}),
    ]
    assert audit(observations, budgets) == []
