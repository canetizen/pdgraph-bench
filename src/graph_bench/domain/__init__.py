# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Domain model: abstract benchmark concepts. Has no infrastructure dependencies."""

from graph_bench.domain.driver import ClusterStatus, Driver, NodeSpec
from graph_bench.domain.operations import QueryRef, QueryRequest, QueryResult, QueryStatus
from graph_bench.domain.scenario import ScaleOutTrigger, ScenarioPhase, ScenarioSpec
from graph_bench.domain.workload import Workload, WorkloadMix

__all__ = [
    "ClusterStatus",
    "Driver",
    "NodeSpec",
    "QueryRef",
    "QueryRequest",
    "QueryResult",
    "QueryStatus",
    "ScaleOutTrigger",
    "ScenarioPhase",
    "ScenarioSpec",
    "Workload",
    "WorkloadMix",
]
