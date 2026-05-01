# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Pydantic models for all YAML configs and the loaders that materialise them.

Config layer responsibilities:
- Parse and validate YAML into typed models (`InventoryConfig`, `SystemConfig`,
  `WorkloadConfig`, `ScenarioConfig`).
- Convert validated config into immutable domain value types (`ScenarioSpec`).

Neither the runner nor the workers parse YAML themselves; they consume the
results of this module.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from graph_bench.domain import ScaleOutTrigger, ScenarioSpec


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class NodeConfig(_FrozenModel):
    """One node in the cluster inventory."""

    hostname: str
    ip: str | None = None
    ssh_user: str | None = None
    roles: list[str] = Field(default_factory=list)


class InventoryConfig(_FrozenModel):
    """Full cluster inventory: named nodes and their static roles."""

    cluster_name: str
    nodes: dict[str, NodeConfig]


class SystemEndpointConfig(_FrozenModel):
    """Endpoint information for a single SUT client connection."""

    host: str
    port: int
    extra: dict[str, Any] = Field(default_factory=dict)


class SystemConfig(_FrozenModel):
    """Per-SUT configuration (deployment-agnostic client settings)."""

    name: str
    image: str | None = None
    endpoints: list[SystemEndpointConfig] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)


class WorkloadConfig(_FrozenModel):
    """Per-workload configuration — query set and parameter source."""

    name: str
    queries: list[str]
    parameter_source: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class ScaleOutConfig(_FrozenModel):
    """YAML form of the S5 scale-out trigger."""

    at_offset_seconds: int
    target_node: str
    role: str


class ScenarioConfig(_FrozenModel):
    """YAML form of a scenario. Use `to_spec()` to obtain the domain `ScenarioSpec`."""

    id: str
    name: str
    workload: str
    dataset: str
    system: str
    worker_count: int = 16
    warmup_seconds: int = 300
    measurement_seconds: int = 900
    query_timeout_seconds: int = 60
    repetitions: int = 3
    mix: dict[str, float] = Field(default_factory=dict)
    scale_out: ScaleOutConfig | None = None

    @field_validator("worker_count", "repetitions")
    @classmethod
    def _positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("must be positive")
        return v

    @field_validator("warmup_seconds", "measurement_seconds", "query_timeout_seconds")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be non-negative")
        return v

    def to_spec(self) -> ScenarioSpec:
        """Project the YAML form onto the immutable domain `ScenarioSpec`."""
        trigger: ScaleOutTrigger | None = None
        if self.scale_out is not None:
            trigger = ScaleOutTrigger(
                at_offset=timedelta(seconds=self.scale_out.at_offset_seconds),
                target_node=self.scale_out.target_node,
                role=self.scale_out.role,
            )
        return ScenarioSpec(
            id=self.id,
            name=self.name,
            workload=self.workload,
            dataset=self.dataset,
            system=self.system,
            worker_count=self.worker_count,
            warmup=timedelta(seconds=self.warmup_seconds),
            measurement=timedelta(seconds=self.measurement_seconds),
            query_timeout=timedelta(seconds=self.query_timeout_seconds),
            repetitions=self.repetitions,
            scale_out=trigger,
        )


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML document into a plain dict."""
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a top-level mapping, got {type(data).__name__}")
    return data


def load_scenario(path: Path) -> ScenarioConfig:
    return ScenarioConfig.model_validate(load_yaml(path))


def load_system(path: Path) -> SystemConfig:
    return SystemConfig.model_validate(load_yaml(path))


def load_workload(path: Path) -> WorkloadConfig:
    return WorkloadConfig.model_validate(load_yaml(path))


def load_inventory(path: Path) -> InventoryConfig:
    return InventoryConfig.model_validate(load_yaml(path))
