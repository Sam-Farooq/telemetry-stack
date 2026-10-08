# fixtures

All five files are synthetic and hand-authored. Nothing in this repo has been
run against a collector, a Prometheus or an Elasticsearch, so none of these is
a recording of anything. They exist so the functions in `telemetry/` and the
scripts in `scripts/` can be tested and run with nothing up.

| file | what it stands in for | how it was written |
|---|---|---|
| `collector-metrics.txt` | the gateway's own telemetry on `:8888/metrics` | names, types and label keys copied from the collector's exposition; values picked so `elasticsearch/traces` reads as dropping at 196 of 256 batches and the other two exporters read as healthy |
| `app-metrics.txt` | the gateway's prometheus exporter on `:8889` | a histogram, a gauge and a counter, with label sets kept inside `prometheus/label-budgets.json` so the passing case is the one CI asserts |
| `prometheus-metrics.txt` | Prometheus' own `:9090/metrics` | four families: the three the cardinality dashboard and the alert rules query, plus the duplicate-timestamp counter |
| `traces.jsonl` | 600 finished traces, one JSON object per line | random trace ids, 6187 spans across six routes of `checkout-api`, 26 failures (4.33 percent, split across 500, 503 and 504) and 18 traces at or over 300ms (3.00 percent) |
| `logs.jsonl` | 10 OTLP log records | one per branch `telemetry/logmap.py` has to take: traced and untraced, a severity it does not know and a lowercase one, a record with no severity text at all, an 11KB stack trace, a non-string body, nested and array attributes, an all-zero trace id and a malformed one |

The numbers the README quotes out of `fixtures/` are a script's output over
these files. They are what the shipped policy and the shipped budgets do to
this input, which is a real property of the code, and they are not a
measurement of a running system.

`traces.jsonl` is the one file whose contents change an answer: the
probabilistic branch of the tail policy hashes the low 8 bytes of the trace
id, so editing an id moves the keep count that the README and
`tests/test_scripts.py` both read.
