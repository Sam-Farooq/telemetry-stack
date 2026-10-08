"""Label guard. An unbounded label value is the ordinary way to kill Prometheus.

Series count is the product of label value counts, so one label carrying a
customer id multiplies every other label by the number of customers. The guard
here refuses to let that happen at emit time: label keys that were never
declared are dropped, route-shaped values are normalised, and once a metric has
reached its declared number of label sets, further combinations are folded into
a single overflow series instead of being invented.

Folding rather than dropping is deliberate. A dropped measurement makes a rate
quietly wrong; a folded one keeps the total right and loses only the breakdown,
and the overflow series itself is the signal that a label needs attention.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import reduce
from pathlib import Path

OVERFLOW_VALUE = "__over_budget__"
UNKNOWN_VALUE = "unknown"

# Histograms carry one series per bucket plus _sum, _count and the +Inf bucket.
HISTOGRAM_SERIES_OVERHEAD = 3

# Resolved from the checkout, which is how this runs: the repo is mounted into
# the service containers and installed with `pip install -e`.
DEFAULT_BUDGETS_PATH = Path(__file__).resolve().parent.parent / "prometheus" / "label-budgets.json"

_UUID = re.compile(r"\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z", re.I)
_LONG_HEX = re.compile(r"\A[0-9a-f]{12,}\Z", re.I)
_DIGITS = re.compile(r"\A\d+\Z")
_ULID = re.compile(r"\A[0-9A-HJKMNP-TV-Z]{26}\Z")


class UndeclaredMetric(KeyError):
    """Raised when a metric is emitted without a budget. There is no default."""


def _is_identifier(segment: str) -> bool:
    if _DIGITS.match(segment) or _UUID.match(segment) or _LONG_HEX.match(segment):
        return True
    if _ULID.match(segment):
        return True
    return "@" in segment


def normalise_route(path: str) -> str:
    """Collapse the variable parts of a URL path into `:id`.

    `http.route` is supposed to arrive as a template. It does not always, and a
    raw path is the single most common unbounded label in an HTTP service.
    """
    without_query = path.split("?", 1)[0].split("#", 1)[0]
    segments = [s for s in without_query.split("/") if s]
    if not segments:
        return "/"
    kept = [":id" if _is_identifier(s) else s.lower() for s in segments]
    return "/" + "/".join(kept)


def status_class(status_code: int) -> str:
    """5xx, 4xx and so on. The raw code is 60-odd values for nothing."""
    if status_code <= 0:
        return UNKNOWN_VALUE
    return f"{status_code // 100}xx"


@dataclass(frozen=True)
class Budget:
    metric: str
    cardinalities: Mapping[str, int]
    max_label_sets: int
    kind: str = "counter"
    histogram_buckets: int = 0

    @property
    def labels(self) -> tuple[str, ...]:
        return tuple(sorted(self.cardinalities))

    def projected_label_sets(self) -> int:
        return reduce(lambda a, b: a * b, self.cardinalities.values(), 1)

    def projected_series(self) -> int:
        sets = self.projected_label_sets()
        if self.kind == "histogram":
            return sets * (self.histogram_buckets + HISTOGRAM_SERIES_OVERHEAD)
        return sets


def load_budgets(path: Path | str = DEFAULT_BUDGETS_PATH) -> dict[str, Budget]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    buckets = len(raw.get("latency_bucket_boundaries_seconds", []))
    budgets: dict[str, Budget] = {}
    for metric, spec in raw["metrics"].items():
        kind = spec.get("kind", "counter")
        budgets[metric] = Budget(
            metric=metric,
            cardinalities=dict(spec["labels"]),
            max_label_sets=int(spec["max_label_sets"]),
            kind=kind,
            histogram_buckets=buckets if kind == "histogram" else 0,
        )
    return budgets


def bucket_boundaries(path: Path | str = DEFAULT_BUDGETS_PATH) -> list[float]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return [float(b) for b in raw["latency_bucket_boundaries_seconds"]]


@dataclass(frozen=True)
class Observation:
    labels: dict[str, str]
    over_budget: bool


class CardinalityGuard:
    """Stateful per process. Each replica tracks its own label sets."""

    def __init__(self, budgets: Mapping[str, Budget]) -> None:
        self._budgets = dict(budgets)
        self._seen: dict[str, set[tuple[str, ...]]] = {m: set() for m in self._budgets}
        self._overflowed: dict[str, int] = dict.fromkeys(self._budgets, 0)

    def budget(self, metric: str) -> Budget:
        try:
            return self._budgets[metric]
        except KeyError as exc:
            raise UndeclaredMetric(f"{metric} has no entry in label-budgets.json") from exc

    def sanitize(self, metric: str, labels: Mapping[str, object]) -> dict[str, str]:
        """Declared keys only, route values normalised, missing keys filled in."""
        budget = self.budget(metric)
        clean: dict[str, str] = {}
        for key in budget.labels:
            if key not in labels or labels[key] is None:
                clean[key] = UNKNOWN_VALUE
                continue
            value = str(labels[key])
            clean[key] = normalise_route(value) if key.endswith("route") else value
        return clean

    def observe(self, metric: str, labels: Mapping[str, object]) -> Observation:
        budget = self.budget(metric)
        clean = self.sanitize(metric, labels)
        key = tuple(clean[label] for label in budget.labels)
        seen = self._seen[metric]
        if key in seen:
            return Observation(clean, over_budget=False)
        if len(seen) >= budget.max_label_sets:
            self._overflowed[metric] += 1
            return Observation(dict.fromkeys(budget.labels, OVERFLOW_VALUE), over_budget=True)
        seen.add(key)
        return Observation(clean, over_budget=False)

    def series_count(self, metric: str) -> int:
        return len(self._seen[self.budget(metric).metric])

    def overflow_count(self, metric: str) -> int:
        return self._overflowed[self.budget(metric).metric]


@dataclass(frozen=True)
class Breach:
    metric: str
    declared: int
    observed: int
    worst_label: str
    worst_label_values: int


def audit(
    observations: Iterable[tuple[str, Mapping[str, str]]],
    budgets: Mapping[str, Budget],
) -> list[Breach]:
    """Compare label sets seen in a scrape against the declared budgets.

    Takes plain pairs rather than parsed samples so it can be fed from a live
    scrape, a saved exposition file, or a test.
    """
    sets: dict[str, set[tuple[str, ...]]] = {}
    values: dict[str, dict[str, set[str]]] = {}
    for metric, labels in observations:
        budget = budgets.get(metric)
        if budget is None:
            continue
        key = tuple(str(labels.get(label, UNKNOWN_VALUE)) for label in budget.labels)
        sets.setdefault(metric, set()).add(key)
        for label in budget.labels:
            values.setdefault(metric, {}).setdefault(label, set()).add(
                str(labels.get(label, UNKNOWN_VALUE))
            )

    breaches: list[Breach] = []
    for metric, observed in sorted(sets.items()):
        budget = budgets[metric]
        if len(observed) <= budget.max_label_sets:
            continue
        worst_label, worst_values = max(
            values[metric].items(), key=lambda item: (len(item[1]), item[0])
        )
        breaches.append(
            Breach(
                metric=metric,
                declared=budget.max_label_sets,
                observed=len(observed),
                worst_label=worst_label,
                worst_label_values=len(worst_values),
            )
        )
    return breaches
