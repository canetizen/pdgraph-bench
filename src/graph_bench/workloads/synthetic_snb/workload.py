# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Synthetic SNB-shaped workload."""

from __future__ import annotations

import json
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.registry import WorkloadRegistry


_IS1 = QueryRef("synthetic_snb", "IS1")
_IS2 = QueryRef("synthetic_snb", "IS2")
_IS3 = QueryRef("synthetic_snb", "IS3")
_IU1 = QueryRef("synthetic_snb", "IU1")


_DEFAULT_MIX: dict[QueryRef, float] = {
    _IS1: 50.0,
    _IS2: 25.0,
    _IS3: 15.0,
    _IU1: 10.0,
}


class SyntheticSnbWorkload:
    name = "synthetic_snb"

    def __init__(
        self,
        *,
        person_ids: list[int] | None = None,
        param_source: Path | None = None,
        mix: dict[str, float] | None = None,
    ) -> None:
        if person_ids is not None and param_source is not None:
            raise ValueError("provide either person_ids or param_source, not both")
        if param_source is not None:
            with Path(param_source).open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            person_ids = list(payload["person_ids"])
        if not person_ids:
            # Allow construction without a parameter file so unit tests do not
            # need an on-disk dataset; iter_requests will raise if invoked.
            person_ids = []
        self._person_ids = person_ids

        if mix is not None:
            weights = {ref: mix.get(ref.id, _DEFAULT_MIX[ref]) for ref in _DEFAULT_MIX}
        else:
            weights = dict(_DEFAULT_MIX)
        self.mix = WorkloadMix(weights=weights)

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if not self._person_ids:
            raise RuntimeError(
                "synthetic_snb workload constructed without person ids; "
                "load the parameter file produced by data/generators/synthetic_snb.py"
            )
        rng = random.Random(seed)
        n = len(self._person_ids)
        while True:
            choice = rng.random()
            cum = 0.0
            picked: QueryRef = _IS1
            for ref, w in self.mix.weights.items():
                cum += w
                if choice * sum(self.mix.weights.values()) <= cum:
                    picked = ref
                    break

            vid = self._person_ids[rng.randrange(n)]
            if picked is _IU1:
                src = vid
                dst = self._person_ids[rng.randrange(n)]
                yield QueryRequest(
                    ref=picked,
                    params={"src": src, "dst": dst, "ts": rng.randint(1_700_000_000, 1_800_000_000)},
                )
            else:
                yield QueryRequest(ref=picked, params={"vid": vid})


def _factory(config: dict[str, Any]) -> SyntheticSnbWorkload:
    param_source = config.get("parameter_source")
    return SyntheticSnbWorkload(
        person_ids=config.get("person_ids"),
        param_source=Path(param_source) if param_source else None,
        mix=config.get("mix"),
    )


WorkloadRegistry.register("synthetic_snb", _factory, overwrite=True)
