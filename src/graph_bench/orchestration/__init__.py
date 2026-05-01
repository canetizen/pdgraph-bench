# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Multi-host orchestration: inventory → deployment plan → SSH-driven execution.

This package generates per-node Docker Compose projects from an `Inventory`
(`configs/inventory/cluster.yaml`) and a `Deployer` (one per SUT). The Fabric
tasks under `deploy/fabric/` invoke this package; the harness itself does not
import it.
"""

from graph_bench.orchestration.inventory import Inventory, NodeInfo, load_inventory
from graph_bench.orchestration.plan import DeploymentPlan, ServiceSpec
from graph_bench.orchestration.runner import SSHRunner, render_compose

__all__ = [
    "DeploymentPlan",
    "Inventory",
    "NodeInfo",
    "ServiceSpec",
    "SSHRunner",
    "load_inventory",
    "render_compose",
]
