# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench workload."""

from __future__ import annotations

import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.ldbc_params import SeededParamCycler, find_param_file, read_param_file
from graph_bench.workloads.registry import WorkloadRegistry


_SR_REFS = [QueryRef("finbench", f"SR{i}") for i in range(1, 7)]
_W_REFS = [QueryRef("finbench", f"W{i}") for i in range(1, 4)]


class FinBenchWorkload:
    name = "finbench"

    def __init__(self, *, parameter_dir: Path | None = None) -> None:
        weights: dict[QueryRef, float] = {}
        for ref in _SR_REFS:
            weights[ref] = 80.0 / len(_SR_REFS)
        for ref in _W_REFS:
            weights[ref] = 20.0 / len(_W_REFS)
        self.mix = WorkloadMix(weights=weights)
        self._parameter_dir = parameter_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if self._parameter_dir is None or not self._parameter_dir.exists():
            raise RuntimeError(
                "finbench workload was not given a parameter_dir or the directory does not exist"
            )
        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(self.mix.weights):
            f = find_param_file(self._parameter_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        if not cyclers:
            raise RuntimeError(f"no FinBench parameter files under {self._parameter_dir}")

        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))


def _factory(config: dict[str, Any]) -> FinBenchWorkload:
    pd = config.get("parameter_dir")
    return FinBenchWorkload(parameter_dir=Path(pd) if pd else None)


WorkloadRegistry.register("finbench", _factory, overwrite=True)
