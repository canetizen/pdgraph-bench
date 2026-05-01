# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""`WeightedScheduler` — samples the next logical query class under a seeded RNG.

The scheduler is a pure sampling strategy; it does not know about workers,
parameters, or drivers. Workers consume `QueryRef`s from the scheduler and
retrieve parameters from the workload's parameter iterator.

Determinism: the scheduler is constructed with a seed and produces the same
sequence of `QueryRef`s for that seed. Workers derive their seeds from the
scenario config (see `Workload.iter_requests`).
"""

from __future__ import annotations

import random

from graph_bench.domain import QueryRef, WorkloadMix


class WeightedScheduler:
    """Seeded weighted-random sampler over a `WorkloadMix`."""

    def __init__(self, mix: WorkloadMix, seed: int) -> None:
        self._rng = random.Random(seed)
        self._refs: list[QueryRef] = list(mix.weights.keys())
        self._weights: list[float] = [mix.weights[r] for r in self._refs]

    def next_ref(self) -> QueryRef:
        """Return the next `QueryRef` drawn from the mix."""
        return self._rng.choices(self._refs, weights=self._weights, k=1)[0]
