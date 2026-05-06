# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Typer-based command-line interface for `pdgraph-bench`.

Entry points:
- `pdgraph-bench list-systems`     — print registered drivers.
- `pdgraph-bench list-workloads`   — print registered workloads.
- `pdgraph-bench run <scenario>`   — execute a scenario config against a chosen driver.

The CLI is a thin wiring layer: it resolves driver/workload names via the
registries, loads YAML into Pydantic config models, projects those into
`ScenarioSpec`s, builds the optional scale-out provisioner from the inventory
and per-SUT deployer, and delegates execution to `ScenarioRunner`.
"""

from __future__ import annotations

import asyncio
import importlib
import uuid
from pathlib import Path

import typer

from graph_bench.drivers import DriverRegistry
from graph_bench.harness import ScenarioRunner
from graph_bench.harness.runner import RunContext
from graph_bench.utils.config import load_scenario, load_system, load_workload
from graph_bench.utils.logging import configure, get_logger
from graph_bench.workloads import WorkloadRegistry

# Side-effect imports: trigger registration of all drivers and workloads even
# if the caller did not import them directly.
importlib.import_module("graph_bench.drivers")
importlib.import_module("graph_bench.workloads")


app = typer.Typer(add_completion=False, help="Portable distributed graph-DB benchmark.")
_log = get_logger(__name__)


@app.command("list-systems")
def list_systems() -> None:
    """Print every registered driver name, one per line."""
    for name in DriverRegistry.known():
        typer.echo(name)


@app.command("list-workloads")
def list_workloads() -> None:
    """Print every registered workload name, one per line."""
    for name in WorkloadRegistry.known():
        typer.echo(name)


def _first_sut_hostname(inventory_path: Path) -> str | None:
    """Return the first sut node's hostname from `inventory_path`, or `None` if
    the inventory has no sut nodes. Used to resolve `host: auto` in system YAML.
    """
    from graph_bench.orchestration import load_inventory
    inv = load_inventory(inventory_path)
    return inv.sut_nodes[0].hostname if inv.sut_nodes else None


def _build_scale_out_provisioner(
    system_name: str,
    target_node_logical: str,
    inventory_path: Path,
):
    """Scale-out provisioner — bring up the new SUT instance on the reserve
    node and return its cluster-visible hostname.

    Single code path for production and playground: `SSHRunner` falls back to
    a local subprocess when the reserve node's hostname resolves to localhost,
    so the same call works whether the reserve "node" is a remote host or
    co-located with the orchestrator.

    `graph_bench.orchestration` is imported lazily so scenarios without a
    scale-out trigger don't pull in the deploy extra.
    """
    from graph_bench.orchestration import SSHRunner, load_inventory
    from graph_bench.orchestration.deployers import get_deployer

    inventory = load_inventory(inventory_path)
    deployer = get_deployer(system_name)
    runner = SSHRunner(inventory)

    async def _provisioner() -> str:
        target_node, service = deployer.plan_scaleout(inventory, target_node_logical)
        await asyncio.to_thread(runner.add_service, target_node, system_name, service)
        # Return the cluster-visible `host:port` so `driver.add_node` registers
        # the right endpoint (NebulaGraph picks the storaged port the deployer
        # actually used; Dgraph picks the alpha grpc-int port; etc.).
        return deployer.scale_out_endpoint(inventory, target_node_logical)

    return _provisioner


@app.command("run")
def run(
    scenario: Path = typer.Argument(..., exists=True, readable=True, help="Scenario YAML."),
    system: Path = typer.Option(..., "--system", exists=True, readable=True, help="System YAML."),
    workload: Path = typer.Option(..., "--workload", exists=True, readable=True, help="Workload YAML."),
    inventory: Path = typer.Option(
        Path("configs/inventory/cluster.yaml"),
        "--inventory",
        help="Cluster inventory YAML; required when the scenario includes a scale-out trigger.",
    ),
    system_name: str | None = typer.Option(
        None, "--system-name",
        help="Override the scenario YAML's `system` field (driver registry name "
             "like `nebulagraph`/`arangodb`). Lets one scenario YAML drive every SUT.",
    ),
    warmup_seconds: int | None = typer.Option(
        None, "--warmup-seconds", min=0,
        help="Override the scenario YAML's `warmup_seconds`. Useful for dry-runs.",
    ),
    measurement_seconds: int | None = typer.Option(
        None, "--measurement-seconds", min=1,
        help="Override the scenario YAML's `measurement_seconds`. Useful for dry-runs.",
    ),
    worker_count: int | None = typer.Option(
        None, "--worker-count", min=1,
        help="Override the scenario YAML's `worker_count`.",
    ),
    results_root: Path = typer.Option(
        Path("results"), "--results", help="Parent directory for per-run output."
    ),
    repetition: int = typer.Option(0, "--repetition", min=0, help="Repetition index."),
    run_id: str | None = typer.Option(None, "--run-id", help="Override the auto-generated run id."),
) -> None:
    """Execute one repetition of a scenario and write results."""
    configure()

    scenario_cfg = load_scenario(scenario)
    overrides: dict[str, object] = {}
    if system_name:
        overrides["system"] = system_name
    if warmup_seconds is not None:
        overrides["warmup_seconds"] = warmup_seconds
    if measurement_seconds is not None:
        overrides["measurement_seconds"] = measurement_seconds
    if worker_count is not None:
        overrides["worker_count"] = worker_count
    if overrides:
        scenario_cfg = scenario_cfg.model_copy(update=overrides)
    sut_hostname = _first_sut_hostname(inventory) if inventory.exists() else None
    system_cfg = load_system(system, sut_hostname=sut_hostname)
    workload_cfg = load_workload(workload)

    driver = DriverRegistry.create(scenario_cfg.system, system_cfg.options)
    workload_impl = WorkloadRegistry.create(scenario_cfg.workload, workload_cfg.options)

    rid = run_id or f"{scenario_cfg.system}-{scenario_cfg.id}-{repetition}-{uuid.uuid4().hex[:8]}"
    results_dir = results_root / rid

    spec = scenario_cfg.to_spec()

    provisioner = None
    if spec.scale_out is not None:
        if not inventory.exists():
            raise typer.BadParameter(
                f"scenario {spec.id!r} declares a scale-out trigger but inventory "
                f"{inventory} does not exist; copy "
                f"configs/inventory/cluster.yaml.playground (or cluster.yaml.example) "
                f"to that path and adjust as needed."
            )
        provisioner = _build_scale_out_provisioner(
            scenario_cfg.system,
            spec.scale_out.target_node,
            inventory,
        )

    ctx = RunContext(
        spec=spec,
        repetition=repetition,
        driver=driver,
        workload=workload_impl,
        results_dir=results_dir,
        scale_out_provisioner=provisioner,
    )
    runner = ScenarioRunner(ctx)

    asyncio.run(runner.run())
    typer.echo(str(results_dir))
