# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""The `Driver` protocol — contract between the harness and each System Under Test."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from graph_bench.domain.operations import QueryRequest, QueryResult


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """Specification of a cluster node to be added during scale-out."""

    hostname: str
    role: str


@dataclass(frozen=True, slots=True)
class ClusterStatus:
    """Observable state of the SUT cluster at a point in time."""

    node_count: int
    healthy_nodes: int
    version: str | None = None
    extra: dict[str, str] | None = None


@runtime_checkable
class Driver(Protocol):
    """Contract for one System Under Test.

    Concrete drivers wrap a vendor client library and translate abstract
    `QueryRequest`s into system-specific queries. The driver is the only part of
    the codebase that knows about a given SUT's query language or wire protocol.
    """

    name: str
    """Stable SUT identifier, e.g., 'nebulagraph', 'arangodb', 'dgraph', 'mock'."""

    async def connect(self) -> None:
        """Establish client connection(s). Raise on unrecoverable failure."""
        ...

    async def disconnect(self) -> None:
        """Release client connection(s)."""
        ...

    async def execute(self, request: QueryRequest) -> QueryResult:
        """Execute one logical query.

        The harness measures wall-clock latency around this call. Implementations
        must not retry silently: either succeed, or return a `QueryResult` whose
        status is `ERROR` or `TIMEOUT`.
        """
        ...

    async def cluster_status(self) -> ClusterStatus:
        """Report current cluster state (for lifecycle + scale-out monitoring)."""
        ...

    async def add_node(self, spec: NodeSpec) -> None:
        """Register a new node with the running cluster (S5 scale-out trigger).

        Drivers that do not participate in scale-out scenarios may raise
        `NotImplementedError`.
        """
        ...
