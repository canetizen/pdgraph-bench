# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Scale-out controller — fires the S5 add-node event at a scheduled offset.

The controller has two responsibilities, executed in order during the
measurement window of scenario S5:

1. **Provision** — start the new SUT process on the reserve node. This is the
   deploy-side action (Fabric / docker compose) that brings a previously-idle
   storaged / dbserver / alpha container up. The harness consumes this work
   through an injected `Provisioner` callable so the controller stays free of
   any orchestration imports.
2. **Register** — make the running SUT cluster aware of the new instance via
   a system-specific administrative call (`driver.add_node`).

Both phases emit `events.jsonl` entries so that scale-out stabilization time
and throughput dip can be computed relative to the trigger timestamp.

If no provisioner is supplied (e.g., in tests with a `MockDriver`), the
controller falls back to calling `driver.add_node` directly using the trigger's
`target_node` as the hostname.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable

from graph_bench.domain import NodeSpec, ScaleOutTrigger
from graph_bench.utils.logging import get_logger

if TYPE_CHECKING:
    from graph_bench.domain import Driver
    from graph_bench.metrics import MetricsCollector


_log = get_logger(__name__)


# A provisioner starts the new container on the reserve node and returns the
# hostname at which the SUT cluster will see it. The returned hostname is what
# `driver.add_node` polls for membership.
Provisioner = Callable[[], Awaitable[str]]


class ScaleOutController:
    """Schedule a single provision + register cycle at `trigger.at_offset` after start."""

    def __init__(
        self,
        driver: "Driver",
        collector: "MetricsCollector",
        trigger: ScaleOutTrigger,
        *,
        provisioner: Provisioner | None = None,
    ) -> None:
        self._driver = driver
        self._collector = collector
        self._trigger = trigger
        self._provisioner = provisioner
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        await asyncio.sleep(self._trigger.at_offset.total_seconds())

        self._collector.events.emit(
            "scale_out_trigger",
            target_node=self._trigger.target_node,
            role=self._trigger.role,
        )

        # Phase A — provision the new container on the reserve node.
        if self._provisioner is not None:
            try:
                hostname = await self._provisioner()
                self._collector.events.emit(
                    "scale_out_provisioned",
                    target_node=self._trigger.target_node,
                    hostname=hostname,
                )
            except Exception as exc:  # noqa: BLE001
                _log.error(
                    "scale_out_provision_failed",
                    error=f"{type(exc).__name__}: {exc}",
                    target_node=self._trigger.target_node,
                )
                self._collector.events.emit(
                    "scale_out_failed",
                    phase="provision",
                    target_node=self._trigger.target_node,
                    error=f"{type(exc).__name__}: {exc}",
                )
                return
        else:
            # Tests / single-host smoke runs: trigger.target_node is also the
            # cluster-visible hostname.
            hostname = self._trigger.target_node

        # Phase B — register the new instance with the running SUT cluster.
        spec = NodeSpec(hostname=hostname, role=self._trigger.role)
        try:
            await self._driver.add_node(spec)
            self._collector.events.emit(
                "scale_out_complete",
                target_node=self._trigger.target_node,
                hostname=hostname,
            )
        except Exception as exc:  # noqa: BLE001
            _log.error(
                "scale_out_register_failed",
                error=f"{type(exc).__name__}: {exc}",
                target_node=self._trigger.target_node,
            )
            self._collector.events.emit(
                "scale_out_failed",
                phase="register",
                target_node=self._trigger.target_node,
                hostname=hostname,
                error=f"{type(exc).__name__}: {exc}",
            )

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="scale-out-controller")

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    def cancel(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
