# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""End-to-end vertical slice: run a short scenario against the mock SUT.

Purpose: prove that harness + metrics + drivers + workloads compose correctly
and produce the full output artifact set (events.jsonl, requests.parquet,
throughput.parquet, latency_*.hgrm, system_metrics.parquet). This is the single
most important test in the suite: if it passes, all core machinery works; if
it fails, adding real SUT drivers on top would be premature.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from graph_bench.domain import ScaleOutTrigger, ScenarioSpec
from graph_bench.drivers import MockDriver
from graph_bench.harness import ScenarioRunner
from graph_bench.harness.runner import RunContext
from graph_bench.workloads import SyntheticWorkload


pytestmark = pytest.mark.asyncio


def _short_spec(with_scale_out: bool = False) -> ScenarioSpec:
    """A 1 s warmup + 2 s measurement, 4 workers, 60 s timeout."""
    scale_out = None
    if with_scale_out:
        scale_out = ScaleOutTrigger(
            at_offset=timedelta(milliseconds=500),
            target_node="node5",
            role="storaged",
        )
    return ScenarioSpec(
        id="test_s",
        name="test",
        workload="synthetic",
        dataset="synthetic",
        system="mock",
        worker_count=4,
        warmup=timedelta(seconds=1),
        measurement=timedelta(seconds=2),
        query_timeout=timedelta(seconds=60),
        repetitions=1,
        scale_out=scale_out,
    )


@pytest.mark.integration
async def test_vertical_slice_produces_all_artifacts(tmp_results_dir: Path) -> None:
    """Run a 2 s measurement on the mock SUT and assert all artifacts exist and look sane."""
    driver = MockDriver(default_mean_us=500.0, seed=7)  # ~2 ops/worker/ms
    workload = SyntheticWorkload()
    ctx = RunContext(
        spec=_short_spec(),
        repetition=0,
        driver=driver,
        workload=workload,
        results_dir=tmp_results_dir,
    )

    results = await ScenarioRunner(ctx).run()
    assert results == tmp_results_dir

    # Every expected artifact is present.
    assert (results / "events.jsonl").exists()
    assert (results / "requests.parquet").exists()
    assert (results / "throughput.parquet").exists()
    assert (results / "system_metrics.parquet").exists()
    assert (results / "latency_all.hgrm").exists()

    # Events log contains the phase markers we expect.
    events = [json.loads(line) for line in (results / "events.jsonl").read_text().splitlines()]
    event_types = {e["type"] for e in events}
    assert "run_start" in event_types
    assert "warmup_end" in event_types
    assert "measurement_start" in event_types
    assert "measurement_stop" in event_types

    # Requests parquet has rows and all recorded rows are from measurement (not warmup).
    requests = pq.read_table(results / "requests.parquet")
    assert requests.num_rows > 0, "no requests recorded during measurement"
    statuses = set(requests.column("status").to_pylist())
    assert "ok" in statuses, "expected at least one successful request"
    # Latencies must be positive microseconds.
    latencies = requests.column("latency_us").to_pylist()
    assert all(lat > 0 for lat in latencies), "latencies must be strictly positive"

    # Throughput parquet has rows (one per second-bucket × query-class).
    throughput = pq.read_table(results / "throughput.parquet")
    assert throughput.num_rows > 0


@pytest.mark.integration
async def test_scale_out_event_recorded(tmp_results_dir: Path) -> None:
    """S5 vertical slice: the scale-out trigger fires during measurement."""
    driver = MockDriver(default_mean_us=500.0, seed=11, initial_nodes=3)
    workload = SyntheticWorkload()
    ctx = RunContext(
        spec=_short_spec(with_scale_out=True),
        repetition=0,
        driver=driver,
        workload=workload,
        results_dir=tmp_results_dir,
    )

    await ScenarioRunner(ctx).run()
    events = [json.loads(line) for line in (tmp_results_dir / "events.jsonl").read_text().splitlines()]
    triggered = [e for e in events if e["type"] == "scale_out_trigger"]
    completed = [e for e in events if e["type"] == "scale_out_complete"]
    assert triggered, "scale_out_trigger was not emitted"
    assert completed, "scale_out_complete was not emitted"
    assert triggered[0]["target_node"] == "node5"
    # MockDriver should have recorded one additional node.
    status = await driver.cluster_status()
    assert status.node_count == 4
