# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Business Intelligence workload."""

from __future__ import annotations

import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.ldbc_params import SeededParamCycler, find_param_file, read_param_file
from graph_bench.workloads.registry import WorkloadRegistry


_BI_REFS = [QueryRef("snb_bi", f"Q{i}") for i in (1, 2, 6, 11)]


class SnbBiWorkload:
    name = "snb_bi"

    def __init__(self, *, parameter_dir: Path | None = None) -> None:
        self.mix = WorkloadMix(weights={ref: 1.0 for ref in _BI_REFS})
        self._parameter_dir = parameter_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if self._parameter_dir is None or not self._parameter_dir.exists():
            raise RuntimeError(
                "snb_bi workload was not given a parameter_dir or the directory does not exist"
            )
        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(_BI_REFS):
            f = find_param_file(self._parameter_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        if not cyclers:
            raise RuntimeError(f"no BI parameter files under {self._parameter_dir}")

        rng = random.Random(seed)
        refs = list(cyclers.keys())
        while True:
            ref = rng.choice(refs)
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))


def _factory(config: dict[str, Any]) -> SnbBiWorkload:
    pd = config.get("parameter_dir")
    return SnbBiWorkload(parameter_dir=Path(pd) if pd else None)


WorkloadRegistry.register("snb_bi", _factory, overwrite=True)
