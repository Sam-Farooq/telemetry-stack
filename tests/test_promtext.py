from __future__ import annotations

import math

import pytest

from telemetry.cardinality import audit, load_budgets
from telemetry.promtext import (
    ExpositionError,
    Sample,
    base_name,
    label_values,
    metric_names,
    observations,
    parse,
    parse_line,
    series_by_metric,
    value_of,
)


def test_a_bare_sample_and_a_labelled_sample() -> None:
    assert parse_line("otelcol_process_uptime 1864.52") == Sample(
        name="otelcol_process_uptime", value=1864.52
    )
    sample = parse_line('otelcol_exporter_queue_size{exporter="elasticsearch"} 196')
    assert sample is not None
    assert sample.name == "otelcol_exporter_queue_size"
    assert sample.labels == {"exporter": "elasticsearch"}
    assert sample.value == 196.0


def test_comments_type_lines_and_blanks_are_not_samples() -> None:
    assert parse_line("# HELP x helpful") is None
    assert parse_line("# TYPE x counter") is None
    assert parse_line("   ") is None
    assert parse_line("") is None


def test_scientific_notation_infinity_and_nan() -> None:
    assert parse_line("otelcol_process_memory_rss 4.0255488e+08") == Sample(
        name="otelcol_process_memory_rss", value=402554880.0
    )
    inf = parse_line('x_bucket{le="+Inf"} 3611')
    assert inf is not None and inf.labels["le"] == "+Inf"
    assert parse_line('x{le="+Inf"} +Inf').value == math.inf  # type: ignore[union-attr]
    assert math.isnan(parse_line("x NaN").value)  # type: ignore[union-attr]


def test_a_label_value_containing_a_quoted_json_blob_survives() -> None:
    line = (
        'otelcol_exporter_send_failed_spans{error="bulk indexer flush: 429 '
        'es_rejected_execution_exception, queue capacity {\\"limit\\":1000}",'
        'exporter="elasticsearch"} 20480'
    )
    sample = parse_line(line)
    assert sample is not None
    assert sample.labels["exporter"] == "elasticsearch"
    assert sample.labels["error"].endswith('queue capacity {"limit":1000}')
    assert "," in sample.labels["error"]
    assert sample.value == 20480.0


def test_an_escaped_newline_in_a_label_becomes_a_newline() -> None:
    sample = parse_line('x{msg="first\\nsecond"} 1')
    assert sample is not None
    assert sample.labels["msg"] == "first\nsecond"


@pytest.mark.parametrize(
    "line",
    [
        "no_value_here",
        'x{bad} 1',
        'x{k=unquoted} 1',
        'x{k="unterminated} 1',
        '{name="not_supported"} 1',
        'x{k="v"}',
    ],
)
def test_a_malformed_line_raises_rather_than_being_skipped(line: str) -> None:
    with pytest.raises(ExpositionError):
        parse_line(line)


def test_the_collector_fixture_parses_whole(collector_samples: list[Sample]) -> None:
    assert len(collector_samples) == 31
    names = metric_names(collector_samples)
    assert "otelcol_exporter_queue_size" in names
    assert "otelcol_processor_tail_sampling_count_traces_sampled" in names
    # Comments and HELP lines outnumber nothing: every non-comment line is a sample.
    text = (
        "# HELP a b\n"
        "a 1\n"
        "\n"
        "# TYPE c counter\n"
        'c{d="e"} 2\n'
    )
    assert len(parse(text)) == 2


def test_base_name_folds_a_histogram_family_but_not_a_counter() -> None:
    assert base_name("http_server_request_duration_seconds_bucket") == (
        "http_server_request_duration_seconds"
    )
    assert base_name("http_server_request_duration_seconds_count") == (
        "http_server_request_duration_seconds"
    )
    # `_total` is part of the name, not a suffixed view.
    assert base_name("orders_settled_total") == "orders_settled_total"
    assert base_name("_count") == "_count"


def test_value_of_insists_on_exactly_one_match(collector_samples: list[Sample]) -> None:
    assert (
        value_of(
            collector_samples,
            "otelcol_exporter_queue_size",
            exporter="elasticsearch",
            data_type="traces",
        )
        == 196.0
    )
    with pytest.raises(KeyError):
        value_of(collector_samples, "otelcol_exporter_queue_size")
    with pytest.raises(KeyError):
        value_of(collector_samples, "otelcol_exporter_queue_size", exporter="nothing")


def test_series_counts_distinct_label_sets(collector_samples: list[Sample]) -> None:
    counts = series_by_metric(collector_samples)
    assert counts["otelcol_exporter_queue_size"] == 3
    assert counts["otelcol_receiver_accepted_spans"] == 2
    assert counts["otelcol_processor_tail_sampling_count_traces_sampled"] == 4
    assert counts["otelcol_process_uptime"] == 1


def test_label_values_reads_across_a_family(app_samples: list[Sample]) -> None:
    routes = label_values(app_samples, "http_server_request_duration_seconds", "http_route")
    assert routes == {"/orders", "/orders/:id", "/healthz"}
    assert label_values(app_samples, "orders_settled_total", "payment_method") >= {
        "card",
        "bank_transfer",
        "wallet",
    }


def test_the_le_label_does_not_count_towards_a_budget(app_samples: list[Sample]) -> None:
    pairs = list(observations(app_samples))
    assert all("le" not in labels for _, labels in pairs)
    latency = {
        tuple(sorted(labels.items()))
        for metric, labels in pairs
        if metric == "http_server_request_duration_seconds"
    }
    # Four label sets across twelve buckets plus sum and count.
    assert len(latency) == 4


def test_the_shipped_app_scrape_is_inside_its_budgets(app_samples: list[Sample]) -> None:
    assert audit(observations(app_samples), load_budgets()) == []
