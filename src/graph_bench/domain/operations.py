# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Query-level value types exchanged between the harness and drivers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class QueryStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class QueryRef:
    """Logical query identifier within a workload.

    A `QueryRef` is the stable, system-agnostic name of a query (e.g., IS1 in LDBC
    SNB Interactive v2). Drivers translate it into their native query language.
    """

    workload: str
    id: str

    def __str__(self) -> str:
        return f"{self.workload}:{self.id}"


@dataclass(frozen=True, slots=True)
class QueryRequest:
    """A parameterised query issued by a worker."""

    ref: QueryRef
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Outcome of a single query execution as reported by a driver.

    Latency is not part of the result; it is measured by the harness around the
    `Driver.execute` call to avoid systematic bias from driver-internal bookkeeping.
    """

    ref: QueryRef
    status: QueryStatus
    row_count: int = 0
    result_hash: str | None = None
    error_message: str | None = None
