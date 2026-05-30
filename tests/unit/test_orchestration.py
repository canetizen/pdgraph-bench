# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Smoke-test the orchestration package: planning produces sensible plans."""

from __future__ import annotations

from graph_bench.orchestration import Inventory, NodeInfo, render_compose
from graph_bench.orchestration.deployers import (
    ArangoDBDeployer,
    DgraphDeployer,
    MemgraphDeployer,
    NebulaGraphDeployer,
    OrientDBDeployer,
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


def _single_host_inv() -> Inventory:
    """Playground-shape inventory: every node co-located on `localhost`."""
    return Inventory(
        cluster_name="playground",
        nodes=(
            NodeInfo(name="node1", hostname="localhost", ip=None, ssh_user=None, roles=("client",)),
            NodeInfo(name="node2", hostname="localhost", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node3", hostname="localhost", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node4", hostname="localhost", ip=None, ssh_user=None, roles=("sut",)),
            NodeInfo(name="node5", hostname="localhost", ip=None, ssh_user=None, roles=("reserve",)),
        ),
    )


def test_nebulagraph_plan_covers_three_sut_nodes() -> None:
    inv = _five_node_inv()
    plan = NebulaGraphDeployer().plan_initial(inv)
    assert plan.sut == "nebulagraph"
    assert set(plan.services_per_node) == {"node2", "node3", "node4"}
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert names == {"nebula-metad", "nebula-storaged", "nebula-graphd"}


def test_nebulagraph_plan_applies_port_offset_when_co_located() -> None:
    plan = NebulaGraphDeployer().plan_initial(_single_host_inv())
    # Three sut nodes share localhost → ports 9559/9569/9579 for metad.
    metad_ports = []
    for node_name in ("node2", "node3", "node4"):
        metad = next(s for s in plan.services_per_node[node_name] if s.name == "nebula-metad")
        metad_ports.append([c for c in metad.command if c.startswith("--port=")][0])
    assert metad_ports == ["--port=9559", "--port=9569", "--port=9579"]


def test_nebulagraph_scaleout_targets_reserve() -> None:
    inv = _five_node_inv()
    target_node, svc = NebulaGraphDeployer().plan_scaleout(inv, "node5")
    assert target_node == "node5"
    assert svc.name == "nebula-storaged"


def test_arangodb_plan() -> None:
    plan = ArangoDBDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"node2", "node3", "node4"}
    for services in plan.services_per_node.values():
        names = {s.name for s in services}
        assert {"arangodb-agent", "arangodb-dbserver", "arangodb-coordinator"} == names


def test_dgraph_plan_seed_is_first_sut_node() -> None:
    plan = DgraphDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"node2", "node3", "node4"}
    # Each node has zero + alpha; the alpha command references the seed (node2's zero on n2:5080).
    for node_name in ("node2", "node3", "node4"):
        services = plan.services_per_node[node_name]
        alpha = next(s for s in services if s.name == "dgraph-alpha")
        assert any("--zero=n2:5080" in c for c in alpha.command)


def test_dgraph_port_offset_when_co_located() -> None:
    plan = DgraphDeployer().plan_initial(_single_host_inv())
    expected_offsets = {"node2": 0, "node3": 10, "node4": 20}
    for node_name, offset in expected_offsets.items():
        zero = next(s for s in plan.services_per_node[node_name] if s.name == "dgraph-zero")
        assert f"--port_offset={offset}" in zero.command


def test_memgraph_plan_one_per_sut() -> None:
    plan = MemgraphDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"node2", "node3", "node4"}
    for services in plan.services_per_node.values():
        assert {s.name for s in services} == {"memgraph"}


def test_memgraph_port_offset_when_co_located() -> None:
    plan = MemgraphDeployer().plan_initial(_single_host_inv())
    bolt_ports: list[str] = []
    for node_name in ("node2", "node3", "node4"):
        svc = plan.services_per_node[node_name][0]
        bolt_ports.append([c for c in svc.command if c.startswith("--bolt-port=")][0])
    assert bolt_ports == ["--bolt-port=7687", "--bolt-port=7697", "--bolt-port=7707"]


def test_orientdb_plan_one_per_sut() -> None:
    plan = OrientDBDeployer().plan_initial(_five_node_inv())
    assert set(plan.services_per_node) == {"node2", "node3", "node4"}
    for services in plan.services_per_node.values():
        assert {s.name for s in services} == {"orientdb"}


def test_orientdb_distributed_entrypoint() -> None:
    """OrientDB must launch via `dserver.sh` (not `server.sh`) for Hazelcast
    cluster mode; otherwise the three nodes never gossip + form a quorum."""
    plan = OrientDBDeployer().plan_initial(_single_host_inv())
    for services in plan.services_per_node.values():
        svc = services[0]
        assert svc.entrypoint is not None
        assert "dserver.sh" in svc.entrypoint[0]
        assert svc.env.get("ORIENTDB_DISTRIBUTED") == "true"


def test_render_compose_emits_yaml() -> None:
    plan = NebulaGraphDeployer().plan_initial(_five_node_inv())
    yml = render_compose(plan.sut, "node2", plan.services_per_node["node2"])
    assert "services:" in yml
    assert "nebula-metad" in yml
    assert "nebula-storaged" in yml
    assert "nebula-graphd" in yml
    assert "name: gb-nebulagraph-node2" in yml
