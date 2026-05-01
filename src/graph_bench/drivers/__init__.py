# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-SUT driver implementations.

New SUTs are added by (a) implementing `Driver` in this package, (b) registering
the concrete class in `registry.py`, (c) providing a compose bundle under
`deploy/compose/<system>/`. No other module needs to change.

Importing this package triggers side-effect registration of every driver, so
`get_driver('nebulagraph')` works without the caller having to import the
concrete module first.
"""

from graph_bench.drivers import (  # noqa: F401 — side-effect registration
    arangodb,
    dgraph,
    hugegraph,
    janusgraph,
    mock,
    nebulagraph,
)
from graph_bench.drivers.mock import MockDriver
from graph_bench.drivers.registry import DriverRegistry, get_driver

__all__ = ["DriverRegistry", "MockDriver", "get_driver"]
