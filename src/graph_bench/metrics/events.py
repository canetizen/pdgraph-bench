# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Append-only event log for scenario phase changes and lifecycle events.

Written as JSON Lines. Each event carries a UTC wall-clock timestamp so that
events from the benchmark driver can be correlated with events emitted by SUT
nodes (e.g., scale-out completion observed by cluster_status polling).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from graph_bench.utils.clock import wall_ns


class EventLog:
    """Structured JSONL event writer.

    Events are flushed immediately to disk so that a crashed run still yields a
    useful post-mortem trace. The underlying file is opened once at construction
    and held until `close()`; the writer is not thread-safe and must be owned by
    a single task.
    """

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._fp = path.open("a", encoding="utf-8")

    def emit(self, event_type: str, **payload: Any) -> None:
        """Append one event with a UTC nanosecond timestamp."""
        record: dict[str, Any] = {
            "ts_ns": wall_ns(),
            "type": event_type,
        }
        record.update(payload)
        self._fp.write(json.dumps(record, default=str) + "\n")
        self._fp.flush()

    def close(self) -> None:
        if not self._fp.closed:
            self._fp.close()

    def __enter__(self) -> "EventLog":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
