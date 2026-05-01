# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 workload — scaffold.

Query subset (per the Progress Report):
- Short reads:   IS1, IS2, IS3, IS4, IS5, IS6, IS7
- Complex reads: IC1, IC3, IC5, IC6
- Updates:       IU1, IU2, IU6

Parameters are drawn from the official parameter-curation files of the LDBC
SNB Datagen output. The concrete iterator implementation wires into the LDBC
parameter-file format and is filled in when dataset generation is integrated.
"""

from graph_bench.workloads.snb_iv2.workload import SnbInteractiveV2Workload

__all__ = ["SnbInteractiveV2Workload"]
