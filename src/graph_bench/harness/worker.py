# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Closed-loop benchmark worker.

Each worker runs its own asyncio task. Within the measurement window it repeatedly:
1. samples the next query class from a shared scheduler,
2. draws the next parameter set for that class from a per-worker parameter iterator,
3. issues the query through the driver,
4. records the outcome via the metrics collector.

The worker respects the per-query timeout; on timeout it records a
`QueryStatus.TIMEOUT` and proceeds to the next request without retrying.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING

from graph_bench.domain import Driver, QueryRef, QueryRequest, QueryResult, QueryStatus
from graph_bench.utils.clock import monotonic_ns, wall_ns
from graph_bench.utils.logging import get_logger

if TYPE_CHECKING:
    from graph_bench.harness.scheduler import WeightedScheduler
    from graph_bench.metrics import MetricsCollector


_log = get_logger(__name__)


class Worker:
    """One closed-loop query issuer bound to a driver and a scheduler."""

    def __init__(
        self,
        worker_id: int,
        driver: Driver,
        scheduler: WeightedScheduler,
        param_iters: dict[QueryRef, Iterator[dict[str, object]]],
        collector: MetricsCollector,
        query_timeout_s: float,
    ) -> None:
        self._id = worker_id
        self._driver = driver
        self._scheduler = scheduler
        self._param_iters = param_iters
        self._collector = collector
        self._timeout_s = query_timeout_s
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Issue queries in a closed loop until `stop()` is called."""
        while not self._stop.is_set():
            ref = self._scheduler.next_ref()
            params_iter = self._param_iters.get(ref)
            params: dict[str, object] = next(params_iter) if params_iter is not None else {}
            req = QueryRequest(ref=ref, params=params)

            issued_wall = wall_ns()
            start = monotonic_ns()
            status: QueryStatus
            row_count = 0
            result_hash: str | None = None
            error_message: str | None = None

            try:
                result: QueryResult = await asyncio.wait_for(
                    self._driver.execute(req), timeout=self._timeout_s
                )
                status = result.status
                row_count = result.row_count
                result_hash = result.result_hash
                error_message = result.error_message
            except asyncio.TimeoutError:
                status = QueryStatus.TIMEOUT
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — benchmark loop must not die on driver errors
                status = QueryStatus.ERROR
                error_message = f"{type(exc).__name__}: {exc}"
                _log.warning("worker_execute_exception", worker_id=self._id, ref=str(ref), error=error_message)

            completed = monotonic_ns()
            completed_wall = wall_ns()
            latency_us = max(0, (completed - start) // 1_000)

            self._collector.record_request(
                ref=ref,
                status=status,
                worker_id=self._id,
                issued_ns=issued_wall,
                completed_ns=completed_wall,
                latency_us=latency_us,
                row_count=row_count,
                result_hash=result_hash,
                error_message=error_message,
            )

            # Yield to the event loop between iterations so that the stop event
            # has a chance to be observed promptly.
            await asyncio.sleep(0)

        with contextlib.suppress(Exception):
            _log.debug("worker_stopped", worker_id=self._id)
