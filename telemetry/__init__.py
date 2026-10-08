"""Processing logic that the collector configs mirror, kept as plain functions.

Nothing here opens a socket. The sampling decision, the label guard and the
log router are the three places where this pipeline makes a choice, so they are
written as functions and tested as functions. The YAML in collector/ is the
same decision expressed for the collector, and tests/test_collector_config.py
fails when the two disagree.
"""

__all__ = ["backpressure", "cardinality", "logmap", "promtext", "sampling"]
