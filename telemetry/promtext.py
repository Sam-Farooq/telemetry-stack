"""A reader for the Prometheus exposition format, for the operational scripts.

The scripts in scripts/ answer questions about a scrape: how full is the
exporter queue, which label broke its budget. They read the text endpoint
rather than the query API, because the text endpoint is what is available when
Prometheus itself is the thing that is unhappy, and because a saved scrape is
something a test can hold.

This is a reader, not a client. It parses what the collector and Prometheus
emit: escaped label values, +Inf and NaN, optional trailing timestamps.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path

# Suffixes a histogram or summary family adds to its base metric name. `_total`
# is not here: the OTLP to Prometheus translation makes it part of a counter's
# name, so `orders_settled_total` is the metric, not a suffixed view of one.
_FAMILY_SUFFIXES = ("_bucket", "_sum", "_count", "_created")
_ESCAPES = {"n": "\n", "\\": "\\", '"': '"'}


class ExpositionError(ValueError):
    """A line that is not in the format. Worth failing on, not skipping."""


@dataclass(frozen=True)
class Sample:
    name: str
    value: float
    labels: Mapping[str, str] = field(default_factory=dict)

    def label(self, key: str, default: str = "") -> str:
        return self.labels.get(key, default)


def _parse_value(token: str) -> float:
    cleaned = token.strip()
    if cleaned in {"+Inf", "Inf"}:
        return float("inf")
    if cleaned == "-Inf":
        return float("-inf")
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ExpositionError(f"not a value: {token!r}") from exc


def _parse_labels(block: str) -> dict[str, str]:
    """Read `a="1",b="x\\"y"` without splitting on commas inside the quotes."""
    labels: dict[str, str] = {}
    index = 0
    length = len(block)
    while index < length:
        while index < length and block[index] in ", ":
            index += 1
        if index >= length:
            break
        equals = block.find("=", index)
        if equals == -1:
            raise ExpositionError(f"label without a value: {block!r}")
        key = block[index:equals].strip()
        if block[equals + 1] != '"':
            raise ExpositionError(f"unquoted label value for {key!r}")
        index = equals + 2
        chars: list[str] = []
        while index < length:
            char = block[index]
            if char == "\\":
                index += 1
                if index >= length:
                    raise ExpositionError("escape at end of label block")
                chars.append(_ESCAPES.get(block[index], block[index]))
            elif char == '"':
                break
            else:
                chars.append(char)
            index += 1
        if index >= length or block[index] != '"':
            raise ExpositionError(f"unterminated label value for {key!r}")
        labels[key] = "".join(chars)
        index += 1
    return labels


def parse_line(line: str) -> Sample | None:
    """One sample, or None for a blank line, a comment, HELP or TYPE."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("{"):
        raise ExpositionError("name-in-labels form is not supported")

    brace = stripped.find("{")
    if brace == -1:
        parts = stripped.split()
        if len(parts) < 2:
            raise ExpositionError(f"no value on line: {line!r}")
        return Sample(name=parts[0], value=_parse_value(parts[1]))

    closing = stripped.rfind("}")
    if closing < brace:
        raise ExpositionError(f"unbalanced braces: {line!r}")
    name = stripped[:brace].strip()
    labels = _parse_labels(stripped[brace + 1 : closing])
    rest = stripped[closing + 1 :].split()
    if not rest:
        raise ExpositionError(f"no value on line: {line!r}")
    return Sample(name=name, value=_parse_value(rest[0]), labels=labels)


def parse(text: str) -> list[Sample]:
    return [sample for line in text.splitlines() if (sample := parse_line(line)) is not None]


def parse_file(path: Path | str) -> list[Sample]:
    return parse(Path(path).read_text(encoding="utf-8"))


def read_exposition(source: str, timeout: float = 5.0) -> str:
    """Text from a file, a URL, or `-` for stdin.

    The scripts take the same argument so a saved scrape and a live endpoint
    are interchangeable: the file is how CI runs them, the URL is how a person
    does, and the output is identical.
    """
    if source == "-":
        return sys.stdin.read()
    if source.startswith(("http://", "https://")):
        from urllib.request import urlopen

        with urlopen(source, timeout=timeout) as response:  # noqa: S310 - fixed schemes
            return str(response.read().decode("utf-8"))
    return Path(source).read_text(encoding="utf-8")


def base_name(name: str) -> str:
    """`http_..._bucket` and `http_..._count` both belong to one family."""
    for suffix in _FAMILY_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def metric_names(samples: Iterable[Sample], families: bool = False) -> set[str]:
    return {base_name(s.name) if families else s.name for s in samples}


def series_by_metric(samples: Iterable[Sample]) -> dict[str, int]:
    """Distinct label sets per metric name. This is what a budget is about."""
    seen: dict[str, set[tuple[tuple[str, str], ...]]] = {}
    for sample in samples:
        key = tuple(sorted(sample.labels.items()))
        seen.setdefault(sample.name, set()).add(key)
    return {name: len(sets) for name, sets in sorted(seen.items())}


def label_values(samples: Iterable[Sample], metric: str, label: str) -> set[str]:
    return {
        sample.labels[label]
        for sample in samples
        if base_name(sample.name) == metric and label in sample.labels
    }


def value_of(samples: Iterable[Sample], metric: str, **labels: str) -> float:
    """The one sample matching these labels exactly. Raises if there is not one."""
    matches = [
        sample
        for sample in samples
        if sample.name == metric and all(sample.labels.get(k) == v for k, v in labels.items())
    ]
    if len(matches) != 1:
        raise KeyError(f"{metric}{labels} matched {len(matches)} samples, wanted 1")
    return matches[0].value


def observations(
    samples: Iterable[Sample], drop_labels: Iterable[str] = ("le", "quantile")
) -> Iterator[tuple[str, dict[str, str]]]:
    """Metric family and labels, ready for cardinality.audit.

    The `le` label is dropped because a bucket is not a label set: twelve
    buckets of one label set are one series in a budget, not twelve.
    """
    dropped = set(drop_labels)
    for sample in samples:
        yield (
            base_name(sample.name),
            {key: value for key, value in sample.labels.items() if key not in dropped},
        )
