# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""In-process mock SUT used for harness validation and unit tests.

The mock driver does not talk to any real system; it simulates per-class
latencies drawn from an exponential distribution with configurable means. It
supports `add_node` so that S5 scale-out scenarios can be exercised in tests
without any real cluster.

This is the only driver that is always available. The real-SUT drivers are
optional extras (`pip install graph-bench[drivers]`).
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from typing import Any

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


class MockDriver:
    """Simulated SUT with configurable per-class latency distributions."""

    name = "mock"

    def __init__(
        self,
        *,
        default_mean_us: float = 5_000.0,
        per_class_mean_us: dict[str, float] | None = None,
        error_rate: float = 0.0,
        seed: int = 0,
        initial_nodes: int = 3,
    ) -> None:
        self._default_mean_us = default_mean_us
        self._per_class_mean_us = per_class_mean_us or {}
        self._error_rate = error_rate
        self._rng = random.Random(seed)
        self._node_count = initial_nodes
        self._healthy_nodes = initial_nodes
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def execute(self, request: QueryRequest) -> QueryResult:
        if not self._connected:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message="driver not connected",
            )

        # Fail a configured fraction of requests so tests can exercise failure paths.
        if self._error_rate > 0.0 and self._rng.random() < self._error_rate:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message="synthetic error",
            )

        mean_us = self._per_class_mean_us.get(str(request.ref), self._default_mean_us)
        # Exponential distribution captures the long-tail shape of database latencies
        # well enough for a synthetic SUT without needing a full parameterisation.
        latency_us = self._rng.expovariate(1.0 / mean_us)
        await asyncio.sleep(latency_us / 1_000_000.0)

        # Deterministic "result" that a validator could check without us having
        # to maintain a real graph — the hash is stable for the same (ref, params).
        payload = f"{request.ref}|{sorted(request.params.items())}"
        result_hash = hashlib.blake2b(payload.encode(), digest_size=8).hexdigest()
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=1,
            result_hash=result_hash,
        )

    async def cluster_status(self) -> ClusterStatus:
        return ClusterStatus(
            node_count=self._node_count,
            healthy_nodes=self._healthy_nodes,
            version="mock-0.1",
        )

    async def add_node(self, spec: NodeSpec) -> None:
        # Simulate a small provisioning delay.
        await asyncio.sleep(0.05)
        self._node_count += 1
        self._healthy_nodes += 1


def _factory(config: dict[str, Any]) -> MockDriver:
    return MockDriver(
        default_mean_us=float(config.get("default_mean_us", 5_000.0)),
        per_class_mean_us=config.get("per_class_mean_us"),
        error_rate=float(config.get("error_rate", 0.0)),
        seed=int(config.get("seed", 0)),
        initial_nodes=int(config.get("initial_nodes", 3)),
    )


DriverRegistry.register("mock", _factory)
