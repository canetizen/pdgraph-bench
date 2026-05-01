# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Fabric orchestration entry point.

Tasks (invoked from any node that has SSH reach into the cluster):

    fab bring-up      --system=<name>
    fab tear-down     --system=<name>
    fab generate-data --workload=<name> --sf=<n>
    fab load-data     --system=<name> --workload=<name>
    fab validate      --system=<name>
    fab run-scenario  --system=<name> --scenario=<id> --workload=<name>
    fab scale-out     --system=<name> --target=<node-name>
    fab collect-results --run-id=<id>

Behind the scenes the tasks consult `configs/inventory/cluster.yaml` and the
SUT-specific `Deployer` (under `graph_bench.orchestration.deployers`). When an
inventory hostname resolves to localhost the runner falls back to local
`docker compose`; otherwise commands are executed via SSH (Fabric `Connection`).

The single docker-compose.cluster.yaml under repo root remains as the *local
simulation* of the production topology — useful for smoke testing without
touching real machines. Production runs use the orchestration package.
"""

from __future__ import annotations

import shlex
from pathlib import Path

from invoke import task


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INVENTORY = REPO_ROOT / "configs" / "inventory" / "cluster.yaml"


def _load_inventory(inventory_path: Path):
    from graph_bench.orchestration import load_inventory
    return load_inventory(inventory_path)


def _client_node(inventory):
    clients = inventory.client_nodes
    if not clients:
        raise RuntimeError("inventory has no node with role 'client'")
    return clients[0]


# -------------------------------------------------------------- bring-up / down
@task(help={"system": "SUT identifier", "inventory": "Inventory YAML path"})
def bring_up(ctx, system: str, inventory: str = str(DEFAULT_INVENTORY)) -> None:
    from graph_bench.orchestration import SSHRunner
    from graph_bench.orchestration.deployers import get_deployer
    inv = _load_inventory(Path(inventory))
    plan = get_deployer(system).plan_initial(inv)
    SSHRunner(inv).bring_up(plan)


@task
def tear_down(ctx, system: str, inventory: str = str(DEFAULT_INVENTORY)) -> None:
    from graph_bench.orchestration import SSHRunner
    from graph_bench.orchestration.deployers import get_deployer
    inv = _load_inventory(Path(inventory))
    plan = get_deployer(system).plan_initial(inv)
    SSHRunner(inv).tear_down(plan)


# ------------------------------------------------------------------ data
@task(help={
    "workload": "Workload identifier: snb_iv2 | snb_bi | finbench | synthetic_snb",
    "sf": "Scale factor (e.g., 0.1, 1, 3)",
    "out": "Output directory under data/generated/ on the client node",
})
def generate_data(ctx, workload: str, sf: float = 1, out: str = "") -> None:
    target = out or f"data/generated/{workload}_sf{sf}"
    if workload == "synthetic_snb":
        # Synthetic generator runs in-process; SF is ignored, persons/avg-degree control size.
        ctx.run(f"python -m data.generators.synthetic_snb --out {target}", pty=False)
        return
    if workload in {"snb_iv2", "snb_bi"}:
        profile = "interactive" if workload == "snb_iv2" else "bi"
        ctx.run(f"bash data/generators/ldbc_snb_datagen.sh {profile} {sf} {target}", pty=False)
        return
    if workload == "finbench":
        ctx.run(f"bash data/generators/ldbc_finbench_datagen.sh {sf} {target}", pty=False)
        return
    raise ValueError(f"unknown workload {workload!r}")


@task(help={
    "system": "SUT identifier",
    "workload": "snb_iv2 | snb_bi | finbench | synthetic_snb",
    "dataset-path": "Path to generated dataset (defaults under data/generated/)",
    "system-config": "Override path to the system YAML; default configs/systems/<system>.yaml",
})
def load_data(ctx, system: str, workload: str, dataset_path: str = "",
              system_config: str = "") -> None:
    """Run the per-(system, workload) loader.

    The system YAML supplies host/port/user/password/space/database/graph to the
    loader as command-line flags so the loader's container-default values
    (e.g., `graphd0`) never leak into a real-cluster run.
    """
    from graph_bench.utils.config import load_system
    from graph_bench.orchestration import SSHRunner
    inv = _load_inventory(Path(DEFAULT_INVENTORY))
    client = _client_node(inv)

    sys_yaml = Path(system_config or f"configs/systems/{system}.yaml")
    sys_cfg = load_system(sys_yaml)
    loader_args = _loader_cli_args(system, sys_cfg.options)

    dataset = dataset_path or f"data/generated/{workload}"
    module = _loader_module(system, workload)
    cmd = (
        f"python -m {module} --dataset {shlex.quote(dataset)} "
        + " ".join(loader_args)
    )

    runner = SSHRunner(inv)
    if runner._is_local(client) if hasattr(runner, "_is_local") else (client.hostname in {"localhost", "127.0.0.1"}):
        ctx.run(cmd, pty=False)
    else:
        from fabric import Connection
        with Connection(host=client.hostname, user=client.ssh_user) as conn:
            conn.run(f"cd {REPO_ROOT} && {cmd}")


def _loader_cli_args(system: str, options: dict) -> list[str]:
    """Translate per-SUT system YAML options into loader CLI flags.

    Loader argparse contracts accept the same names across SUTs where they make
    sense (host, port, user, password) and SUT-specific flags otherwise (space,
    database, graph, zero_host, zero_port).
    """
    args: list[str] = []
    if "host" in options:
        args.append(f"--host {shlex.quote(str(options['host']))}")
    if "port" in options:
        args.append(f"--port {int(options['port'])}")

    if system == "nebulagraph":
        if "user" in options:
            args.append(f"--user {shlex.quote(str(options['user']))}")
        if "password" in options:
            args.append(f"--password {shlex.quote(str(options['password']))}")
        if "space" in options:
            args.append(f"--space {shlex.quote(str(options['space']))}")
    elif system == "arangodb":
        if "user" in options:
            args.append(f"--user {shlex.quote(str(options['user']))}")
        if "password" in options:
            args.append(f"--password {shlex.quote(str(options['password']))}")
        if "database" in options:
            args.append(f"--database {shlex.quote(str(options['database']))}")
    elif system == "dgraph":
        # Dgraph loader uses --host/--port for alpha. Zero is contacted by
        # `add_node`/`cluster_status`; the SNB loader does not need it directly.
        pass
    elif system == "janusgraph":
        # No additional flags beyond host/port.
        pass
    elif system == "hugegraph":
        if "graph" in options:
            args.append(f"--graph {shlex.quote(str(options['graph']))}")
    return args


def _loader_module(system: str, workload: str) -> str:
    # SNB BI shares the SNB Interactive v2 schema and CSV layout; the same
    # per-SUT loader handles both datasets. The dataset path discriminates them.
    iv2_or_bi = {"snb_iv2", "snb_bi"}
    table = {
        # SNB IV2 / BI — Tier 1
        **{("nebulagraph", w): "data.loaders.ldbc_snb_nebulagraph" for w in iv2_or_bi},
        **{("arangodb", w): "data.loaders.ldbc_snb_arangodb" for w in iv2_or_bi},
        **{("dgraph", w): "data.loaders.ldbc_snb_dgraph" for w in iv2_or_bi},
        # SNB IV2 — Tier 2 (Tier 2 only runs S1 = SNB Iv2)
        ("janusgraph", "snb_iv2"): "data.loaders.ldbc_snb_janusgraph",
        ("hugegraph", "snb_iv2"): "data.loaders.ldbc_snb_hugegraph",
        # FinBench — Tier 1 only
        ("nebulagraph", "finbench"): "data.loaders.ldbc_finbench_nebulagraph",
        ("arangodb", "finbench"): "data.loaders.ldbc_finbench_arangodb",
        ("dgraph", "finbench"): "data.loaders.ldbc_finbench_dgraph",
        # Synthetic playground — used by integration smoke runs
        ("nebulagraph", "synthetic_snb"): "data.loaders.nebulagraph",
        ("arangodb", "synthetic_snb"): "data.loaders.arangodb",
        ("dgraph", "synthetic_snb"): "data.loaders.dgraph",
    }
    key = (system, workload)
    if key not in table:
        raise NotImplementedError(f"no loader registered for {key}; add one under data/loaders/")
    return table[key]


# ------------------------------------------------------------------ validate
@task(help={
    "system": "SUT identifier; resolved to configs/systems/<system>.yaml unless overridden",
    "dataset-path": "Generated dataset directory",
    "workload": "Workload identifier — picks the per-workload known-answer query set",
    "system-config": "Override path to the system YAML; default configs/systems/<system>.yaml",
})
def validate(ctx, system: str, dataset_path: str, workload: str = "synthetic_snb",
             system_config: str = "") -> None:
    sys_yaml = system_config or f"configs/systems/{system}.yaml"
    cmd = (
        f"python -m data.validators.runner --system {system} "
        f"--system-config {shlex.quote(sys_yaml)} "
        f"--workload {workload} "
        f"--dataset {shlex.quote(dataset_path)}"
    )
    ctx.run(cmd, pty=False)


# ------------------------------------------------------------------ scenario
@task(help={
    "system": "SUT identifier",
    "scenario": "Scenario id (s1..s5 or 'demo')",
    "workload": "Workload identifier",
    "system-config": "Override path to the system YAML; default configs/systems/<system>.yaml",
    "inventory": "Inventory YAML (only required for scenarios with a scale-out trigger)",
})
def run_scenario(ctx, system: str, scenario: str = "demo", workload: str = "synthetic_snb",
                 system_config: str = "",
                 inventory: str = str(DEFAULT_INVENTORY)) -> None:
    scenario_yaml = f"configs/scenarios/{scenario}-cluster.yaml" if scenario == "demo" \
        else f"configs/scenarios/{scenario}.yaml"
    sys_yaml = system_config or f"configs/systems/{system}.yaml"
    cmd = (
        f"pdgraph-bench run {shlex.quote(scenario_yaml)} "
        f"--system {shlex.quote(sys_yaml)} "
        f"--workload configs/workloads/{workload}.yaml "
        f"--inventory {shlex.quote(inventory)}"
    )
    ctx.run(cmd, pty=False)


# ------------------------------------------------------------------ scale-out
@task
def scale_out(ctx, system: str, target: str = "node5",
              inventory: str = str(DEFAULT_INVENTORY)) -> None:
    """Bring up the reserve service on `target` and trigger SUT-side registration."""
    from graph_bench.orchestration import SSHRunner
    from graph_bench.orchestration.deployers import get_deployer
    inv = _load_inventory(Path(inventory))
    deployer = get_deployer(system)
    target_host, service = deployer.plan_scaleout(inv, target)
    SSHRunner(inv).add_service(target_host, system, service)
    print(f"[scale-out] started {service.container_name} on {target_host}; "
          "SUT-side registration is performed by the benchmark driver during the run.")


# ------------------------------------------------------------------ collect
@task
def collect_results(ctx, run_id: str) -> None:
    inv = _load_inventory(Path(DEFAULT_INVENTORY))
    client = _client_node(inv)
    target = REPO_ROOT / "results" / run_id
    target.mkdir(parents=True, exist_ok=True)
    if client.hostname in {"localhost", "127.0.0.1"}:
        ctx.run(f"cp -r {REPO_ROOT}/results/{run_id}/. {target}/", pty=False)
        return
    ctx.run(
        f"rsync -avz {client.ssh_user + '@' if client.ssh_user else ''}"
        f"{client.hostname}:{REPO_ROOT}/results/{run_id}/ {target}/",
        pty=False,
    )
