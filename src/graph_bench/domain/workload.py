# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""The `Workload` protocol — contract between the harness and each benchmark workload."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol

from graph_bench.domain.operations import QueryRef, QueryRequest


@dataclass(frozen=True, slots=True)
class WorkloadMix:
    """Weighted mix of logical queries within a workload.

    Weights are relative; they do not need to sum to 1. The scheduler normalises
    them when sampling.
    """

    weights: dict[QueryRef, float]

    def __post_init__(self) -> None:
        if not self.weights:
            raise ValueError("WorkloadMix must contain at least one query")
        total = sum(self.weights.values())
        if total <= 0:
            raise ValueError("WorkloadMix weights must sum to a positive value")
        for ref, w in self.weights.items():
            if w < 0:
                raise ValueError(f"WorkloadMix weight for {ref} is negative ({w})")


class Workload(Protocol):
    """Contract for a workload: produces query requests for the harness to execute."""

    name: str
    """Stable workload identifier, e.g., 'snb_iv2', 'snb_bi', 'finbench'."""

    mix: WorkloadMix

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        """Yield an infinite, reproducible sequence of `QueryRequest`s.

        The same seed must produce the same sequence. Parameters are drawn from
        the workload's parameter-curation source using the seed; the scheduler
        samples the next `QueryRef` from `mix` using a seeded RNG derived from
        the same seed.
        """
        ...
