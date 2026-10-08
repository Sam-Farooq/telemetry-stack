#!/usr/bin/env python3
"""Compare a scrape against prometheus/label-budgets.json.

    scripts/check-budgets.py                                  # the live endpoint
    scripts/check-budgets.py --source fixtures/app-metrics.txt
    curl -s localhost:8889/metrics | scripts/check-budgets.py --source -

Exit 1 on a breach, so it works as a CI step or a cron. The report names the
label with the most values, which is the one to go and look at.
"""

from __future__ import annotations

import argparse
import sys

from telemetry.cardinality import audit, load_budgets
from telemetry.promtext import observations, parse, read_exposition, series_by_metric

DEFAULT_SOURCE = "http://localhost:8889/metrics"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="URL, path, or - for stdin")
    parser.add_argument("--budgets", default=None, help="an alternative label-budgets.json")
    parser.add_argument("--quiet", action="store_true", help="print only breaches")
    args = parser.parse_args(argv)

    budgets = load_budgets(args.budgets) if args.budgets else load_budgets()
    samples = parse(read_exposition(args.source))
    if not samples:
        print(f"no samples at {args.source}", file=sys.stderr)
        return 1

    pairs = list(observations(samples))
    breaches = audit(pairs, budgets)
    series = series_by_metric(samples)

    if not args.quiet:
        print(f"{len(samples)} samples, {sum(series.values())} series, {len(budgets)} budgets")
        for metric, budget in sorted(budgets.items()):
            observed = len({
                tuple(sorted(labels.items())) for name, labels in pairs if name == metric
            })
            print(
                f"  {metric}: {observed} label sets, budget {budget.max_label_sets}, "
                f"declared product {budget.projected_label_sets()}"
            )

    for breach in breaches:
        print(
            f"OVER BUDGET {breach.metric}: {breach.observed} label sets against "
            f"{breach.declared}. Worst label is {breach.worst_label} with "
            f"{breach.worst_label_values} values.",
            file=sys.stderr,
        )
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
