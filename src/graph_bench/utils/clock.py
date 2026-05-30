# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Monotonic and wall-clock helpers used by the harness.

Latency intervals must be measured using `perf_counter_ns` to avoid wall-clock
adjustments (NTP slews) contaminating the measurement. Event timestamps that are
compared across machines use `time.time_ns` (UTC wall clock).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


def monotonic_ns() -> int:
    """Return a high-resolution monotonic timestamp in nanoseconds.

    Use this around `Driver.execute` calls; never use wall clock for latency.
    """
    return time.perf_counter_ns()


def wall_ns() -> int:
    """Return UTC wall-clock time in nanoseconds since the epoch.

    Use this to timestamp events that need to be correlated across machines.
    """
    return time.time_ns()


def now_utc() -> datetime:
    """Return a timezone-aware UTC datetime for human-readable logs."""
    return datetime.now(tz=timezone.utc)
