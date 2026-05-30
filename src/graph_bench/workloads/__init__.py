# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Workload implementations.

A workload produces an infinite, reproducible sequence of `QueryRequest`s when
given a seed. Per-system query translation is handled inside the corresponding
driver; workloads emit only logical `QueryRef`s and parameter dictionaries.
"""

from graph_bench.workloads import (  # noqa: F401 — side-effect registration
    finbench,
    snb_analytical,
    snb_bi,
    snb_iv2,
    synthetic,
    synthetic_snb,
)
from graph_bench.workloads.registry import WorkloadRegistry, get_workload
from graph_bench.workloads.synthetic import SyntheticWorkload

__all__ = ["SyntheticWorkload", "WorkloadRegistry", "get_workload"]
