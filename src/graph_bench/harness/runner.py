# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""`ScenarioRunner` — Template Method for the invariant scenario lifecycle.

The runner orchestrates a single (system, scenario, repetition) execution:

    bring-up → load → validate → warmup → measurement → (scale-out) → teardown

Load and validate are no-ops when the caller asserts the SUT already has data
(e.g., for the mock SUT used in tests). Bring-up and teardown are delegated to
the deployment layer (Fabric) in production and stubbed out in tests.

Workload parameter iteration is seeded deterministically from
`(system, scenario, repetition)`; the same triple produces the same request
sequence so that repetitions are reproducible.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable

from graph_bench.domain import (
    Driver,
    QueryRef,
    ScenarioSpec,
    Workload,
)
from graph_bench.harness.scaleout import Provisioner, ScaleOutController
from graph_bench.harness.scheduler import WeightedScheduler
from graph_bench.harness.worker import Worker
from graph_bench.metrics import MetricsCollector
from graph_bench.utils.clock import monotonic_ns
from graph_bench.utils.logging import get_logger


_log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RunContext:
    """Inputs required to execute one repetition."""

    spec: ScenarioSpec
    repetition: int
    driver: Driver
    workload: Workload
    results_dir: Path
    scale_out_provisioner: Provisioner | None = None
    """Optional callable that starts the new SUT container on the reserve node
    and returns its cluster-visible hostname. Required for production S5; left
    `None` in tests where `MockDriver.add_node` is the only side-effect needed."""


def _derive_seed(system: str, scenario_id: str, repetition: int) -> int:
    """Deterministic 63-bit seed from `(system, scenario, repetition)`."""
    h = hashlib.blake2b(
        f"{system}|{scenario_id}|{repetition}".encode("utf-8"), digest_size=8
    ).digest()
    return int.from_bytes(h, "big", signed=False) >> 1


class ScenarioRunner:
    """Run one scenario repetition end-to-end, producing a `results/<run_id>/` directory."""

    def __init__(
        self,
        ctx: RunContext,
        *,
        bring_up: Callable[[], Awaitable[None]] | None = None,
        load: Callable[[], Awaitable[None]] | None = None,
        validate: Callable[[], Awaitable[None]] | None = None,
        teardown: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._ctx = ctx
        self._bring_up = bring_up
        self._load = load
        self._validate = validate
        self._teardown = teardown

    async def run(self) -> Path:
        """Execute the full lifecycle; return the results directory."""
        ctx = self._ctx
        results = ctx.results_dir
        results.mkdir(parents=True, exist_ok=True)

        collector = MetricsCollector(results)
        collector.events.emit(
            "run_start",
            system=ctx.driver.name,
            scenario=ctx.spec.id,
            repetition=ctx.repetition,
            workload=ctx.workload.name,
            worker_count=ctx.spec.worker_count,
        )

        try:
            if self._bring_up is not None:
                collector.events.emit("phase", name="bring_up")
                await self._bring_up()

            await ctx.driver.connect()

            if self._load is not None:
                collector.events.emit("phase", name="load")
                await self._load()

            if self._validate is not None:
                collector.events.emit("phase", name="validate")
                await self._validate()

            seed = _derive_seed(ctx.driver.name, ctx.spec.id, ctx.repetition)

            collector.events.emit("phase", name="warmup", duration_s=ctx.spec.warmup.total_seconds())
            await self._run_queries(
                ctx, collector, seed, ctx.spec.warmup.total_seconds(), record=False
            )
            collector.events.emit("warmup_end")

            collector.events.emit(
                "phase",
                name="measurement",
                duration_s=ctx.spec.measurement.total_seconds(),
            )
            await collector.start_measurement()
            try:
                await self._run_queries(
                    ctx, collector, seed, ctx.spec.measurement.total_seconds(), record=True
                )
            finally:
                await collector.stop_measurement()

            if self._teardown is not None:
                collector.events.emit("phase", name="teardown")
                await self._teardown()

        finally:
            with contextlib.suppress(Exception):
                await ctx.driver.disconnect()
            # Ensure event log is closed even if measurement never started.
            with contextlib.suppress(Exception):
                collector.events.close()

        return results

    async def _run_queries(
        self,
        ctx: RunContext,
        collector: MetricsCollector,
        base_seed: int,
        duration_s: float,
        *,
        record: bool,
    ) -> None:
        """Spin up the worker pool for `duration_s` seconds.

        `record=False` during warmup: the collector is constructed but not yet
        in the measuring state, so `record_request` drops all samples.
        """
        scheduler = WeightedScheduler(ctx.workload.mix, seed=base_seed)

        # Each worker consumes parameters from the workload; provide a per-worker
        # parameter iterator derived from the same seed so that any stochastic
        # choices inside `iter_requests` stay deterministic per worker.
        param_iters: dict[QueryRef, list] = {}
        # The scheduler picks the ref; the workload's iter_requests provides
        # parameters. A concrete workload may choose to ignore the ref (drawing
        # its own) — in which case the scheduler is effectively a hint.
        per_worker_iters: list[dict[QueryRef, object]] = []
        for worker_id in range(ctx.spec.worker_count):
            per_worker_iters.append(
                _build_param_iterators(ctx.workload, base_seed + worker_id)
            )

        scaleout_ctrl: ScaleOutController | None = None
        if record and ctx.spec.scale_out is not None:
            scaleout_ctrl = ScaleOutController(
                ctx.driver,
                collector,
                ctx.spec.scale_out,
                provisioner=ctx.scale_out_provisioner,
            )

        workers = [
            Worker(
                worker_id=i,
                driver=ctx.driver,
                scheduler=scheduler,
                param_iters=per_worker_iters[i],  # type: ignore[arg-type]
                collector=collector,
                query_timeout_s=ctx.spec.query_timeout.total_seconds(),
            )
            for i in range(ctx.spec.worker_count)
        ]

        start_ns = monotonic_ns()
        tasks = [asyncio.create_task(w.run(), name=f"worker-{i}") for i, w in enumerate(workers)]
        if scaleout_ctrl is not None:
            scaleout_ctrl.start()

        try:
            await asyncio.sleep(duration_s)
        finally:
            for w in workers:
                w.stop()
            # Drain workers so any in-flight request completes or times out.
            await asyncio.gather(*tasks, return_exceptions=True)
            if scaleout_ctrl is not None:
                scaleout_ctrl.cancel()

        elapsed_s = (monotonic_ns() - start_ns) / 1e9
        _log.info(
            "phase_complete",
            recorded=record,
            elapsed_s=round(elapsed_s, 2),
            workers=ctx.spec.worker_count,
        )


def _build_param_iterators(
    workload: Workload, seed: int
) -> dict[QueryRef, "ParamIterator"]:
    """Build per-`QueryRef` parameter iterators from the workload.

    The default implementation drains `workload.iter_requests(seed)` lazily,
    partitioning requests by their `ref`. Concrete workloads are free to
    optimise this — e.g., by returning distinct iterators per ref.
    """
    from collections import defaultdict

    iterator = workload.iter_requests(seed)
    buckets: dict[QueryRef, list[dict[str, object]]] = defaultdict(list)

    def refill(ref: QueryRef) -> dict[str, object]:
        # Pull from the workload until a request for the requested ref arrives.
        while True:
            try:
                req = next(iterator)
            except StopIteration as exc:
                raise RuntimeError(
                    f"workload {workload.name!r} exhausted before producing {ref}"
                ) from exc
            if req.ref == ref:
                return req.params
            buckets[req.ref].append(req.params)

    class ParamIterator:
        def __init__(self, ref: QueryRef) -> None:
            self._ref = ref

        def __next__(self) -> dict[str, object]:
            queue = buckets[self._ref]
            if queue:
                return queue.pop(0)
            return refill(self._ref)

        def __iter__(self) -> "ParamIterator":
            return self

    return {ref: ParamIterator(ref) for ref in workload.mix.weights}
