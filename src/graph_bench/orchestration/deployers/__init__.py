# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-SUT deployers — topology + scale-out logic.

Each module in this package defines exactly one `Deployer`. The registry
exposes them by SUT name so that the Fabric tasks can resolve a deployer from
configuration.
"""

from __future__ import annotations

from graph_bench.orchestration.deployers.arangodb import ArangoDBDeployer
from graph_bench.orchestration.deployers.dgraph import DgraphDeployer
from graph_bench.orchestration.deployers.hugegraph import HugeGraphDeployer
from graph_bench.orchestration.deployers.janusgraph import JanusGraphDeployer
from graph_bench.orchestration.deployers.nebulagraph import NebulaGraphDeployer
from graph_bench.orchestration.plan import Deployer


_DEPLOYERS: dict[str, Deployer] = {
    NebulaGraphDeployer.sut_name: NebulaGraphDeployer(),
    ArangoDBDeployer.sut_name: ArangoDBDeployer(),
    DgraphDeployer.sut_name: DgraphDeployer(),
    JanusGraphDeployer.sut_name: JanusGraphDeployer(),
    HugeGraphDeployer.sut_name: HugeGraphDeployer(),
}


def get_deployer(sut: str) -> Deployer:
    if sut not in _DEPLOYERS:
        known = ", ".join(sorted(_DEPLOYERS))
        raise KeyError(f"unknown SUT {sut!r}; known: {known}")
    return _DEPLOYERS[sut]


def known_systems() -> list[str]:
    return sorted(_DEPLOYERS)


__all__ = [
    "ArangoDBDeployer",
    "DgraphDeployer",
    "HugeGraphDeployer",
    "JanusGraphDeployer",
    "NebulaGraphDeployer",
    "get_deployer",
    "known_systems",
]
