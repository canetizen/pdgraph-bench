# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Composite analytical workload for scenario S2.

Per the Progress Report Section 5 (Workload Subset and Query Translation), the
analytical-only scenario draws complex reads IC1/IC3/IC5/IC6 from LDBC SNB
Interactive v2 *and* power-class reads Q1/Q2/Q6/Q11 from LDBC SNB BI. These
live in two distinct parameter-curation file trees produced by separate
generators. The composite workload wraps both: each `QueryRequest` carries the
originating workload tag in its `QueryRef`, so the per-driver template lookup
routes to `snb_iv2.queries` or `snb_bi.queries` accordingly.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.ldbc_params import SeededParamCycler, find_param_file, read_param_file
from graph_bench.workloads.registry import WorkloadRegistry


_IC_REFS = [QueryRef("snb_iv2", f"IC{i}") for i in (1, 3, 5, 6)]
_BI_REFS = [QueryRef("snb_bi", f"Q{i}") for i in (1, 2, 6, 11)]


class SnbAnalyticalWorkload:
    name = "snb_analytical"

    def __init__(
        self,
        *,
        iv2_parameter_dir: Path | None = None,
        bi_parameter_dir: Path | None = None,
    ) -> None:
        # Equal weighting across the eight refs (per Progress Report).
        weights: dict[QueryRef, float] = {ref: 1.0 for ref in _IC_REFS + _BI_REFS}
        self.mix = WorkloadMix(weights=weights)
        self._iv2_dir = iv2_parameter_dir
        self._bi_dir = bi_parameter_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if self._iv2_dir is None or not self._iv2_dir.exists():
            raise RuntimeError(
                "snb_analytical: iv2_parameter_dir missing; set workload.options.iv2_parameter_dir"
            )
        if self._bi_dir is None or not self._bi_dir.exists():
            raise RuntimeError(
                "snb_analytical: bi_parameter_dir missing; set workload.options.bi_parameter_dir"
            )

        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(_IC_REFS):
            f = find_param_file(self._iv2_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        for offset, ref in enumerate(_BI_REFS, start=len(_IC_REFS)):
            f = find_param_file(self._bi_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)

        if not cyclers:
            raise RuntimeError(
                f"snb_analytical: no parameter files matched any ref under "
                f"{self._iv2_dir} or {self._bi_dir}"
            )

        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))


def _factory(config: dict[str, Any]) -> SnbAnalyticalWorkload:
    iv2 = config.get("iv2_parameter_dir")
    bi = config.get("bi_parameter_dir")
    return SnbAnalyticalWorkload(
        iv2_parameter_dir=Path(iv2) if iv2 else None,
        bi_parameter_dir=Path(bi) if bi else None,
    )


WorkloadRegistry.register("snb_analytical", _factory, overwrite=True)
