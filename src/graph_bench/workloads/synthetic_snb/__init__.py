# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Synthetic SNB-shaped workload used for the demo cluster.

NOT a substitute for LDBC SNB Interactive v2. The schema (Person + KNOWS) and
the four query classes (IS1, IS2, IS3, IU1) imitate the *shape* of LDBC short
reads + an update so that the harness can be exercised end-to-end against a
real distributed graph database before LDBC datagen integration is wired in.

Parameter source: a JSON file produced by `data/generators/synthetic_snb.py`,
listing the person ids materialised in the loaded dataset.
"""

from graph_bench.workloads.synthetic_snb.workload import SyntheticSnbWorkload

__all__ = ["SyntheticSnbWorkload"]
