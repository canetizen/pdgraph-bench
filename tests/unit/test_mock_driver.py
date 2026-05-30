# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""MockDriver basic behaviour."""

from __future__ import annotations

import pytest

from graph_bench.domain import NodeSpec, QueryRef, QueryRequest, QueryStatus
from graph_bench.drivers import MockDriver, get_driver


pytestmark = pytest.mark.asyncio


async def test_connect_execute_disconnect() -> None:
    driver = MockDriver(default_mean_us=100.0, seed=42)
    await driver.connect()
    req = QueryRequest(ref=QueryRef("test", "q1"), params={"id": 1})
    result = await driver.execute(req)
    assert result.status is QueryStatus.OK
    assert result.ref == req.ref
    assert result.result_hash is not None
    await driver.disconnect()


async def test_execute_before_connect_returns_error() -> None:
    driver = MockDriver()
    req = QueryRequest(ref=QueryRef("test", "q1"), params={})
    result = await driver.execute(req)
    assert result.status is QueryStatus.ERROR


async def test_add_node_increments_cluster() -> None:
    driver = MockDriver(initial_nodes=3)
    await driver.connect()
    status = await driver.cluster_status()
    assert status.node_count == 3
    await driver.add_node(NodeSpec(hostname="node5", role="storaged"))
    status = await driver.cluster_status()
    assert status.node_count == 4
    assert status.healthy_nodes == 4


async def test_registry_factory() -> None:
    driver = get_driver("mock", {"default_mean_us": 10.0, "seed": 1})
    await driver.connect()
    result = await driver.execute(QueryRequest(ref=QueryRef("t", "q"), params={}))
    assert result.status is QueryStatus.OK
    await driver.disconnect()


async def test_synthetic_error_rate() -> None:
    # With error_rate=1.0 every request should fail.
    driver = MockDriver(error_rate=1.0, seed=0)
    await driver.connect()
    result = await driver.execute(QueryRequest(ref=QueryRef("t", "q"), params={}))
    assert result.status is QueryStatus.ERROR
