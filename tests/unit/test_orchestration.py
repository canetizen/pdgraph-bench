# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Smoke-test the orchestration package: planning produces sensible plans."""

from __future__ import annotations

from graph_bench.orchestration import Inventory, NodeInfo, render_compose
from graph_bench.orchestration.deployers import (
    ArangoDBDeployer,
    DgraphDeployer,
    HugeGraphDeployer,
    JanusGraphDeployer,
    NebulaGraphDeployer,
)


def _five_node_inv() -> Inventory:
    return Inventory(
        cluster_name="test",
        nodes=(
            NodeInfo(name="node1", hostname="n1", ip=None, ssh_user=None, roles=("client",)),
            NodeInfo(name="node2", hostname="n2", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node3", hostname="n3", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node4", hostname="n4", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node5", hostname="n5", ip=None, ssh_user=None, roles=("reserve",)),
        ),
    )


def test_nebulagraph_plan_covers_three_sut_nodes() -> None:
    inv = _five_node_inv()
    plan = NebulaGraphDeployer().plan_initial(inv)
    assert plan.sut == "nebulagraph"
    assert set(plan.services_per_node) == {"n2", "n3", "n4"}
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert names == {"nebula-metad", "nebula-storaged", "nebula-graphd"}


def test_nebulagraph_scaleout_targets_reserve() -> None:
    inv = _five_node_inv()
    host, svc = NebulaGraphDeployer().plan_scaleout(inv, "node5")
    assert host == "n5"
    assert svc.name == "nebula-storaged"


def test_arangodb_plan() -> None:
    plan = ArangoDBDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"n2", "n3", "n4"}
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert {"arangodb-agent", "arangodb-dbserver", "arangodb-coordinator"} == names


def test_dgraph_plan_seed_is_first_sut_node() -> None:
    plan = DgraphDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"n2", "n3", "n4"}
    # Each node has zero + alpha; the alpha command references the seed (node2's zero).
    for host in ("n2", "n3", "n4"):
        services = plan.services_per_node[host]
        alpha = next(s for s in services if s.name == "dgraph-alpha")
        assert any("--zero=n2:5080" in c for c in alpha.command)


def test_janusgraph_plan_includes_cassandra() -> None:
    plan = JanusGraphDeployer().plan_initial(_five_node_inv())
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert names == {"cassandra", "janusgraph"}


def test_hugegraph_plan_includes_cassandra() -> None:
    plan = HugeGraphDeployer().plan_initial(_five_node_inv())
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert names == {"cassandra", "hugegraph"}


def test_render_compose_emits_yaml() -> None:
    plan = NebulaGraphDeployer().plan_initial(_five_node_inv())
    yml = render_compose(plan.sut, plan.services_per_node["n2"])
    assert "services:" in yml
    assert "nebula-metad" in yml
    assert "nebula-storaged" in yml
    assert "nebula-graphd" in yml
