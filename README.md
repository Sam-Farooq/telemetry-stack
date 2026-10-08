# telemetry-stack

Two collector tiers, OTLP in, traces and logs into Elasticsearch, metrics into
Prometheus, Grafana on top. The code here is small on purpose: the decisions
live in `collector/agent.yaml` and `collector/gateway.yaml`, and the Python is
the three places those configs make a choice, written as functions so they can
be tested without anything running.

```
collector/agent.yaml      per host. receives, tags, forwards. decides nothing
collector/gateway.yaml    holds whole traces, samples, writes, exposes metrics
telemetry/sampling.py     the tail policy, as a function
telemetry/cardinality.py  the label guard and the route normaliser
telemetry/logmap.py       OTLP log record to span event, or to an ES document
telemetry/backpressure.py what the exporter queue metrics mean
telemetry/promtext.py     a reader for the exposition format
services/                 a producer with an HTTP socket, and a consumer
scripts/                  budgets, queue health, a sampling report, bootstrap
prometheus/               scrape limits, six alert rules, the label budgets
elasticsearch/            two ILM policies, two index templates
grafana/                  provisioned datasources and two dashboards
helm/                     the same two configs, on Kubernetes
```

## Why there are two collector tiers

A tail sampling decision needs every span of a trace in one process. An agent
on a node sees the spans produced on that node, which is a fragment, so the
agent forwards and the gateway decides.

The agent's `loadbalancing` exporter hashes the trace id and sends all spans of
a trace to the same gateway replica. In Kubernetes the gateway service is
headless (`clusterIP: None`) so that exporter can resolve pod addresses: a
normal cluster IP would balance per connection and scatter a trace across
replicas. Metrics skip the load balancer entirely, because routing a metric by
trace id means hashing a field it does not have.

What this costs: a second hop, a second process to run, and a gateway that is a
StatefulSet rather than a Deployment because its sending queue is on disk.

## Tail sampling against head sampling

Head sampling decides at the root span, before the request has failed or run
long. Tail sampling decides when the trace is complete, and pays for it in
memory: the gateway holds every open trace for `decision_wait`.

`fixtures/traces.jsonl` is 600 synthetic traces, written rather than captured;
`fixtures/README.md` says how. `scripts/tail-sample-report.py` runs the
shipped policy over them:

```
traces      600
kept        89 (14.83%)
  sample-the-rest  46
  errors           26
  slow             17
spans       1184 of 6187 stored (19.14%)
errors      26, all kept by the error policy
head at 10% would lose 22 of those 26
```

Same keep rate, four of the 26 failures kept instead of all 26. That is the
argument, and it is the only reason the gateway exists.

The cost is arithmetic, not opinion. `decision_wait: 10s` at the configured
`expected_new_traces_per_sec: 2000` is 20,000 traces open at once, which is why
`num_traces` is 50,000 and not the default. Set `num_traces` below the arrival
rate times the window and traces are evicted before their window closes:
`otelcol_processor_tail_sampling_sampling_trace_dropped_too_early` counts it,
and in a UI it looks like missing spans rather than a misconfiguration. A test
does that multiplication.

A successful `/healthz` trace is dropped. A failing one is kept, because the
error policy runs first and the health route check only applies to the
probabilistic branch. Dropping probes wholesale hides the readiness failure
that took the pod out of rotation.

## Cardinality, which is how Prometheus dies

Series count is the product of label value counts. One label carrying a
customer id multiplies every other label by the number of customers.

`prometheus/label-budgets.json` declares, per metric, which labels exist and
how many values each is allowed. The request histogram's four labels multiply
out to 1,120 label sets, budgeted at 1,200; at twelve bucket boundaries plus
`_sum`, `_count` and `+Inf` that is 16,800 series. A test asserts the product
is inside the budget, because a budget its own labels cannot meet is a lie.

Three layers, deliberately overlapping:

1. `CardinalityGuard` at emit time. Undeclared label keys are dropped,
   route-shaped values are normalised (`/orders/4812/items` to
   `/orders/:id/items`), and once a metric reaches its budget, new label
   combinations are folded into one `__over_budget__` series.
2. `transform/route` in the gateway, for spans that arrive with a raw path
   anyway, plus `delete_key` on `customer.id`.
3. `labeldrop` in `prometheus.yml`, and a `sample_limit` on every scrape job.

Folding rather than dropping is the trade: a dropped measurement makes a rate
quietly wrong, a folded one keeps the total right and loses the breakdown. The
overflow series is the signal that a label needs attention, and there is a
panel for it.

The sample limit is blunt on purpose. A scrape that exceeds it is rejected
whole, so every series from that target is missing for those scrapes, not just
the new ones. That is worse for one target and better for the server.

## What happens when Elasticsearch is slow

Three counters, three different stories, and the dashboard shows one of them:

```
otelcol_exporter_sent_spans            left the process
otelcol_exporter_send_failed_spans     an attempt failed, retry may still win
otelcol_exporter_enqueue_failed_spans  refused by a full queue, data is gone
```

Only the third is loss. Nothing alerts on the second, because paging on a 429
that the retry absorbs teaches people to ignore the alert.

`fixtures/collector-metrics.txt` is a hand-authored exposition of that state,
not a capture of one. `scripts/collector-health.py` reads it:

```
elasticsearch/traces: 196 of 256 batches (77%), status dropping, 61440 items refused by a full queue
elasticsearch/logs/logs: 12 of 256 batches (5%), status healthy
prometheus/metrics: 0 of 256 batches (0%), status healthy
receiver has refused 41280 spans back to the SDKs
overall: dropping
```

With `--previous`, the enqueue counter is read as a delta instead, and the same
scrape reports `filling` rather than `dropping`: the counter is cumulative, so
one scrape can only say data was lost at some point, not that it is being lost
now. The exit codes differ accordingly, 0 and 2.

The queue is sized in batches, which hides what it holds.
`queue_size: 256` at `send_batch_max_size: 2048` is 524,288 spans in the worst
case. It started at 10,000 batches, which is 20.5 million spans, far past the
512 MiB `memory_limiter`: the limiter would trip first and the queue number was
decoration. A test now does the multiplication.

`memory_limiter` is the first processor in every pipeline and `batch` is the
last, and a test asserts both. Order matters here: the limiter refuses data at
the receiver, which makes the SDK retry and keeps the loss visible on the
producer side instead of inside a queue, and batching before the limiter runs
means deciding about memory that has already been allocated.

The sending queue is on disk through the `file_storage` extension, so a gateway
restart does not throw away what Elasticsearch has not accepted. That costs a
write per batch and a volume to attach.

## Retention, so neither index grows forever

| | rollover | delete | ceiling it implies |
|---|---|---|---|
| `logs-otel-*` | 30gb per primary shard or 1 day | 14 days | 420gb |
| `traces-otel-*` | the same | 7 days | 210gb |

Spans outnumber log records here and a week-old span has not answered a
question yet, so traces go first.

Both templates set `dynamic: false` rather than `strict`. Strict rejects a
document carrying an unmapped field, which loses the log line you most wanted;
`false` keeps it in `_source` and leaves it unsearchable. So
`attributes.order.expedited` is readable in the document and matches nothing in
a query until somebody adds it to the template. A test proves both halves of
that.

`refresh_interval` is 5s, not the 1s default: a segment a second buys
visibility nothing here needs. The field limit is 1,000 and the codec is
`best_compression`.

## Running it

```bash
docker compose up -d
./scripts/bootstrap-elasticsearch.sh
python scripts/loadgen.py --rate 40 --seconds 120
```

Grafana is on `localhost:3000` (anonymous, admin), Prometheus on `9090`,
Elasticsearch on `9200`, and the gateway's re-exported app metrics on `8889`.
`compose.yaml` publishes nothing else: the OTLP ports are internal, because
nothing outside the network sends OTLP.

On Kubernetes the chart takes the same two config files rather than keeping a
copy:

```bash
helm lint ./helm \
  --set-file agentConfig=collector/agent.yaml \
  --set-file gatewayConfig=collector/gateway.yaml
```

A copy is how the thing Kubernetes runs stops being the thing compose runs, and
that drift is invisible until the two behave differently.

## Tests

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
ruff check telemetry services scripts tests
```

187 tests, none of which start a service. The processing logic is pure, so it
is tested directly; the configuration is read with PyYAML and asserted the same
way code is. The checks worth knowing about:

- Every component a pipeline names has to be defined. A typo there starts a
  collector with a quietly shorter pipeline.
- The sampling numbers in `gateway.yaml` have to equal the numbers in
  `telemetry/sampling.py`. Two copies of a policy drift; a test makes them one.
- Every metric a dashboard panel or an alert rule queries has to appear in one
  of the exposition fixtures under `fixtures/`. A panel querying a metric
  nobody emits renders an empty graph, and an empty graph reads as good news.
- Every bind mount in `compose.yaml` has to exist, and every `${env:...}` the
  collector configs expand has to be set on that service.
- Every field `to_es_document` can emit has to be mapped in the logs template,
  apart from the attributes that are deliberately unmapped.

`scripts/check-budgets.py`, `scripts/collector-health.py` and
`scripts/tail-sample-report.py` each take `--source`, so CI runs them against
the fixtures and the exit codes are part of the suite. The second CI job runs
`otelcol validate` and `promtool check config` in containers, plus
`docker compose config` on the runner. Those containers are the only place
anything knows what a processor is actually called.

## What this does not do

- No span metrics. RED numbers come from the services' own histogram, not from
  a `spanmetrics` connector, so a sampled-out trace still counts in the metric.
  Deriving metrics from spans after tail sampling would count the 15 percent
  that was kept and call it traffic.
- No auto-instrumentation agent. The spans and labels here are written by hand,
  which is the point, and the cost is that a library this repo does not call is
  not instrumented.
- No trace lookup UI beyond Grafana's Elasticsearch datasource. There is no
  service graph and no dependency map.
- No alertmanager, so the rules in `prometheus/rules/` evaluate and route
  nowhere. They are checked by `promtool` and read by a person.
- Nothing here has been run against a running stack. The Python is tested
  locally; the collector, Prometheus, Elasticsearch and Kubernetes paths go
  no further than the two CI jobs, and `otelcol validate`, `promtool`,
  `docker compose config`, `helm lint` and `helm template` all read a config
  without starting a pipeline. Every number in this README is either
  arithmetic from the configuration or a script's output over `fixtures/`,
  and the fixtures are hand-authored rather than captured, which
  `fixtures/README.md` sets out file by file. There is no production here to
  measure.
