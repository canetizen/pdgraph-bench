# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Workload registry — name → factory.

Mirrors the shape of the driver registry: factories are callables
`(config: dict) -> Workload` and are registered at module-import time.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from graph_bench.domain import Workload


WorkloadFactory = Callable[[dict[str, Any]], Workload]


class WorkloadRegistry:
    _factories: dict[str, WorkloadFactory] = {}

    @classmethod
    def register(cls, name: str, factory: WorkloadFactory, *, overwrite: bool = False) -> None:
        if name in cls._factories and not overwrite:
            raise ValueError(
                f"workload {name!r} is already registered; pass overwrite=True to replace"
            )
        cls._factories[name] = factory

    @classmethod
    def create(cls, name: str, config: dict[str, Any]) -> Workload:
        if name not in cls._factories:
            known = ", ".join(sorted(cls._factories))
            raise KeyError(f"unknown workload {name!r} (known: {known})")
        return cls._factories[name](config)

    @classmethod
    def known(cls) -> list[str]:
        return sorted(cls._factories)


def get_workload(name: str, config: dict[str, Any] | None = None) -> Workload:
    return WorkloadRegistry.create(name, config or {})
