#!/usr/bin/env python3
"""Point traffic at checkout-api so the dashboards have something to draw.

    scripts/loadgen.py --rate 40 --seconds 120

Needs the stack up. It opens plain HTTP connections and ignores the responses
except to count them: the interesting output is in Grafana, not here.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from collections import Counter


def request(base: str, path: str, body: dict[str, object] | None) -> int:
    url = f"{base.rstrip('/')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:  # noqa: S310 - local only
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except OSError:
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://localhost:8080")
    parser.add_argument("--rate", type=float, default=40.0, help="requests per second")
    parser.add_argument("--seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    if args.rate <= 0:
        parser.error("--rate has to be positive")

    rng = random.Random()
    interval = 1.0 / args.rate
    deadline = time.monotonic() + args.seconds
    codes: Counter[int] = Counter()

    # Weighted the way the fixture is: mostly reads, a steady drip of probes.
    while time.monotonic() < deadline:
        roll = rng.random()
        if roll < 0.2:
            codes[request(args.base, "/orders", {"sku": "A-1", "qty": rng.randint(1, 4)})] += 1
        elif roll < 0.85:
            codes[request(args.base, f"/orders/{rng.randrange(1000, 9999)}", None)] += 1
        else:
            codes[request(args.base, "/healthz", None)] += 1
        time.sleep(interval)

    total = sum(codes.values())
    print(f"{total} requests in {args.seconds:g}s")
    for code, count in sorted(codes.items()):
        label = "connection refused" if code == 0 else str(code)
        print(f"  {label:<20} {count}")
    return 0 if total and codes[0] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
