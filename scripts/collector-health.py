#!/usr/bin/env python3
"""Read a collector's own metrics and say whether it is losing data.

    scripts/collector-health.py                                      # live
    scripts/collector-health.py --source fixtures/collector-metrics.txt
    scripts/collector-health.py --source now.txt --previous a-minute-ago.txt

Exit codes: 0 healthy or filling, 1 a queue over 90 percent full, 2 data being
refused. With --previous the enqueue counter is read as a delta, which is the
difference between "lost something once" and "losing it now".
"""

from __future__ import annotations

import argparse
import sys

from telemetry.backpressure import EXIT_CODES, classify, explain, read_queue_states, worst
from telemetry.promtext import parse, read_exposition, value_of

DEFAULT_SOURCE = "http://localhost:8888/metrics"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="URL, path, or - for stdin")
    parser.add_argument("--previous", default=None, help="an earlier scrape, for the delta")
    parser.add_argument("--exit-zero", action="store_true", help="report without failing")
    args = parser.parse_args(argv)

    samples = parse(read_exposition(args.source))
    if not samples:
        print(f"no samples at {args.source}", file=sys.stderr)
        return 1

    states = read_queue_states(samples)
    if not states:
        print("no exporter queue metrics in this scrape", file=sys.stderr)
        return 1

    earlier = {}
    if args.previous:
        earlier = {
            (state.exporter, state.data_type): state
            for state in read_queue_states(parse(read_exposition(args.previous)))
        }

    statuses = []
    for state in states:
        status = classify(state, earlier.get((state.exporter, state.data_type)))
        statuses.append(status)
        print(explain(state, status))

    try:
        refused = value_of(samples, "otelcol_receiver_refused_spans", transport="grpc")
        if refused:
            print(f"receiver has refused {refused:.0f} spans back to the SDKs")
    except KeyError:
        pass

    overall = worst(statuses)
    print(f"overall: {overall.value}")
    return 0 if args.exit_zero else EXIT_CODES[overall]


if __name__ == "__main__":
    sys.exit(main())
