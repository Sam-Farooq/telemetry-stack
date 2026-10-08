"""Trace ids with a known position in the probabilistic range.

The sampler reads the low 8 bytes of the id, so a test that wants a trace on
one side of the 10 percent line builds the id rather than retrying randoms.
"""

from __future__ import annotations

UINT64 = 1 << 64
_HIGH = "a1b2c3d4e5f60718"


def tid(fraction: float, high: str = _HIGH) -> str:
    """A 32-hex trace id whose `trace_id_fraction` is `fraction`, near enough."""
    if not 0.0 <= fraction < 1.0:
        raise ValueError("fraction belongs to [0, 1)")
    return high + f"{int(fraction * UINT64):016x}"


def tid_at(index: int, buckets: int, high: str = _HIGH) -> str:
    """Id number `index` of `buckets` ids spread evenly across the range."""
    return high + f"{index * (UINT64 // buckets):016x}"
