# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Driver registry — name → factory.

Factories are callables `(config: dict) -> Driver`. The registry is populated
at import time by each driver module (side-effect registration) so that
`get_driver(name)` always returns the right implementation without the CLI
having to know which classes exist.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from graph_bench.domain import Driver


DriverFactory = Callable[[dict[str, Any]], Driver]


class DriverRegistry:
    _factories: dict[str, DriverFactory] = {}

    @classmethod
    def register(cls, name: str, factory: DriverFactory, *, overwrite: bool = False) -> None:
        if name in cls._factories and not overwrite:
            raise ValueError(
                f"driver {name!r} is already registered; pass overwrite=True to replace"
            )
        cls._factories[name] = factory

    @classmethod
    def create(cls, name: str, config: dict[str, Any]) -> Driver:
        if name not in cls._factories:
            known = ", ".join(sorted(cls._factories))
            raise KeyError(f"unknown driver {name!r} (known: {known})")
        return cls._factories[name](config)

    @classmethod
    def known(cls) -> list[str]:
        return sorted(cls._factories)


def get_driver(name: str, config: dict[str, Any] | None = None) -> Driver:
    return DriverRegistry.create(name, config or {})
