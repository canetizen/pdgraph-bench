# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Typer-based command-line interface for `graph-bench`.

Entry points:
- `graph-bench list-systems`     — print registered drivers.
- `graph-bench list-workloads`   — print registered workloads.
- `graph-bench run <scenario>`   — execute a scenario config against a chosen driver.

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


def _build_scale_out_provisioner(
    system_name: str,
    target_node_logical: str,
    inventory_path: Path,
):
    """Production scale-out provisioner: SSH into the reserve node and `docker
    compose up` the new SUT instance.

    Imports `graph_bench.orchestration` lazily so that running scenarios that
    do not include a scale-out trigger does not require the `deploy` extra to
    be installed.
    """
    from graph_bench.orchestration import SSHRunner, load_inventory
    from graph_bench.orchestration.deployers import get_deployer

    inventory = load_inventory(inventory_path)
    deployer = get_deployer(system_name)
    runner = SSHRunner(inventory)

    async def _provisioner() -> str:
        target_host, service = deployer.plan_scaleout(inventory, target_node_logical)
        await asyncio.to_thread(runner.add_service, target_host, system_name, service)
        return target_host

    return _provisioner


def _build_local_compose_provisioner(compose_path: Path, target_hostname: str):
    """Single-host scale-out provisioner: `docker compose up -d` against a
    pre-baked compose file on the local docker daemon.

    Used when the new SUT instance lives in the same docker daemon as the
    benchmark client (e.g., the playground compose), where SSHing into the
    "node" makes no sense because the node IS the local host.
    """
    import subprocess

    async def _provisioner() -> str:
        await asyncio.to_thread(
            subprocess.run,
            ["docker", "compose", "-f", str(compose_path), "up", "-d"],
            check=True,
        )
        return target_hostname

    return _provisioner


@app.command("run")
def run(
    scenario: Path = typer.Argument(..., exists=True, readable=True, help="Scenario YAML."),
    system: Path = typer.Option(..., "--system", exists=True, readable=True, help="System YAML."),
    workload: Path = typer.Option(..., "--workload", exists=True, readable=True, help="Workload YAML."),
    inventory: Path = typer.Option(
        Path("configs/inventory/cluster.yaml"),
        "--inventory",
        help="Cluster inventory YAML; required when the scenario includes a scale-out "
             "trigger and no --scale-out-compose is given (production path).",
    ),
    scale_out_compose: Path | None = typer.Option(
        None, "--scale-out-compose",
        help="Path to a docker-compose file that brings up the reserve SUT instance "
             "on the local docker daemon. When set, the scale-out provisioner runs "
             "`docker compose up -d` against this file instead of going through "
             "Fabric/SSH. Used by the single-host playground.",
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
    system_cfg = load_system(system)
    workload_cfg = load_workload(workload)

    driver = DriverRegistry.create(scenario_cfg.system, system_cfg.options)
    workload_impl = WorkloadRegistry.create(scenario_cfg.workload, workload_cfg.options)

    rid = run_id or f"{scenario_cfg.system}-{scenario_cfg.id}-{repetition}-{uuid.uuid4().hex[:8]}"
    results_dir = results_root / rid

    spec = scenario_cfg.to_spec()

    provisioner = None
    if spec.scale_out is not None:
        if scale_out_compose is not None:
            if not scale_out_compose.exists():
                raise typer.BadParameter(
                    f"--scale-out-compose {scale_out_compose} does not exist"
                )
            provisioner = _build_local_compose_provisioner(
                scale_out_compose,
                target_hostname=spec.scale_out.target_node,
            )
        else:
            if not inventory.exists():
                raise typer.BadParameter(
                    f"scenario {spec.id!r} declares a scale-out trigger; provide either "
                    f"--inventory pointing at a populated cluster inventory (production) "
                    f"or --scale-out-compose pointing at a local docker compose file "
                    f"(playground)."
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
