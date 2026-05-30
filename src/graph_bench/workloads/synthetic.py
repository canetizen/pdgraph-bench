# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Synthetic workload used by unit + integration tests.

Not a real LDBC workload; just three synthetic query classes weighted 7:2:1 so
that the harness's mixed-workload paths are exercised by the test suite without
depending on LDBC data generation.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.registry import WorkloadRegistry


_READ_LIGHT = QueryRef(workload="synthetic", id="read_light")
_READ_HEAVY = QueryRef(workload="synthetic", id="read_heavy")
_WRITE = QueryRef(workload="synthetic", id="write")


class SyntheticWorkload:
    """Minimal three-class mixed workload; purely synthetic."""

    name = "synthetic"

    def __init__(self) -> None:
        self.mix = WorkloadMix(
            weights={
                _READ_LIGHT: 7.0,
                _READ_HEAVY: 2.0,
                _WRITE: 1.0,
            }
        )
        self._refs = list(self.mix.weights.keys())
        self._weights = list(self.mix.weights.values())

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        rng = random.Random(seed)
        counter = 0
        while True:
            ref = rng.choices(self._refs, weights=self._weights, k=1)[0]
            counter += 1
            yield QueryRequest(
                ref=ref,
                params={
                    "id": rng.randint(1, 1_000_000),
                    "seq": counter,
                },
            )


def _factory(_config: dict[str, Any]) -> SyntheticWorkload:
    return SyntheticWorkload()


WorkloadRegistry.register("synthetic", _factory, overwrite=True)
