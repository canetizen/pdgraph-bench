# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Scenario specification: immutable value type constructed from validated YAML."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum


class ScenarioPhase(str, Enum):
    """Named phases in the scenario lifecycle; emitted as events."""

    BRING_UP = "bring_up"
    LOAD = "load"
    VALIDATE = "validate"
    WARMUP = "warmup"
    MEASUREMENT = "measurement"
    SCALE_OUT = "scale_out"
    TEARDOWN = "teardown"


@dataclass(frozen=True, slots=True)
class ScaleOutTrigger:
    """Scheduling of the S5 scale-out event."""

    at_offset: timedelta
    """Offset from the start of the measurement phase."""

    target_node: str
    """Hostname of the reserve node being activated."""

    role: str
    """System-specific role hint for the new node (e.g., 'storaged', 'alpha')."""


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    """Immutable scenario specification.

    Constructed from a validated YAML config via `graph_bench.utils.config`. The
    runner consumes it directly; neither the runner nor the workers should parse
    YAML themselves.
    """

    id: str
    """Stable scenario identifier (e.g., 's1', 's3', 's5')."""

    name: str

    workload: str
    """Name of the `Workload` to run (resolved through the workload registry)."""

    dataset: str
    """Identifier of the dataset variant (e.g., 'snb_iv2_sf1')."""

    system: str
    """Name of the target SUT (resolved through the driver registry)."""

    worker_count: int

    warmup: timedelta

    measurement: timedelta

    query_timeout: timedelta

    repetitions: int

    scale_out: ScaleOutTrigger | None = None
