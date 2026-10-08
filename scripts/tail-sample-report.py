#!/usr/bin/env python3
"""Run the sampling policy over recorded traces and print what it keeps.

    scripts/tail-sample-report.py
    scripts/tail-sample-report.py --percentage 5 --latency-ms 250

The last two lines are the argument for tail sampling: the same keep rate at
the head loses most of the failures, because the root span is decided before
anything has gone wrong.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from telemetry.sampling import (
    DEFAULT_POLICY,
    Policy,
    Trace,
    decide,
    errors_lost_to_head_sampling,
    policy_counts,
)

DEFAULT_TRACES = Path(__file__).resolve().parent.parent / "fixtures" / "traces.jsonl"


def load(path: Path) -> list[Trace]:
    traces = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        traces.append(
            Trace(
                trace_id=row["trace_id"],
                service=row["service"],
                route=row["route"],
                duration_ms=float(row["duration_ms"]),
                span_count=int(row["span_count"]),
                status_code=int(row["status_code"]),
                error=bool(row["error"]),
            )
        )
    return traces


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--traces", default=str(DEFAULT_TRACES))
    parser.add_argument("--percentage", type=float, default=DEFAULT_POLICY.probabilistic_percentage)
    parser.add_argument("--latency-ms", type=float, default=DEFAULT_POLICY.latency_ms)
    args = parser.parse_args(argv)

    policy = Policy(latency_ms=args.latency_ms, probabilistic_percentage=args.percentage)
    traces = load(Path(args.traces))
    if not traces:
        print(f"no traces in {args.traces}", file=sys.stderr)
        return 1

    kept = [t for t in traces if decide(t, policy).keep]
    counts = policy_counts(traces, policy)
    spans = sum(t.span_count for t in traces)
    kept_spans = sum(t.span_count for t in kept)
    errors = sum(1 for t in traces if t.error or t.status_code >= policy.error_status_floor)
    lost = errors_lost_to_head_sampling(traces, args.percentage)

    print(f"traces      {len(traces)}")
    print(f"kept        {len(kept)} ({len(kept) / len(traces):.2%})")
    for name, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {name:<16} {count}")
    print(f"spans       {kept_spans} of {spans} stored ({kept_spans / spans:.2%})")
    print(f"errors      {errors}, all kept by the error policy")
    print(f"head at {args.percentage:g}% would lose {lost} of those {errors}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
