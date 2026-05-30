# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Domain invariants."""

from __future__ import annotations

import pytest

from graph_bench.domain import QueryRef, WorkloadMix


def test_query_ref_str() -> None:
    assert str(QueryRef("snb_iv2", "IS1")) == "snb_iv2:IS1"


def test_workload_mix_rejects_empty() -> None:
    with pytest.raises(ValueError):
        WorkloadMix(weights={})


def test_workload_mix_rejects_negative() -> None:
    with pytest.raises(ValueError):
        WorkloadMix(weights={QueryRef("x", "a"): -1.0})


def test_workload_mix_rejects_zero_total() -> None:
    with pytest.raises(ValueError):
        WorkloadMix(weights={QueryRef("x", "a"): 0.0})
