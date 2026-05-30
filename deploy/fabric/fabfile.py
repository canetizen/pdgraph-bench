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

There is one code path for both production and the single-host playground —
the only thing that differs is the inventory. Use
`configs/inventory/cluster.yaml.playground` for a fully local run (3 sut +
1 reserve nodes all co-located on localhost, deployer auto-applies port
offsets) or `configs/inventory/cluster.yaml.example` as a template for a
real multi-host cluster.
"""

from __future__ import annotations

import shlex
from pathlib import Path

from invoke import task


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INVENTORY = REPO_ROOT / "configs" / "inventory" / "cluster.yaml"

# Loader/validator/datagen invocations go through `uv run` so the project's
# pinned dep set (drivers, neo4j, httpx, ...) is on the import path. Use the
# absolute path to uv so it works under non-interactive SSH shells where
# `~/.local/bin` is not on PATH.
_PY = "$HOME/.local/bin/uv run --no-sync python3"


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
def generate_data(ctx, workload: str, sf: str = "1", out: str = "") -> None:
    # `sf` is a free-form string (e.g. "0.003", "1", "3") so invoke does not
    # try to coerce it to int based on the default literal type.
    target = out or f"data/generated/{workload}_sf{sf}"
    if workload == "synthetic_snb":
        # Synthetic generator runs in-process; SF is ignored, persons/avg-degree control size.
        ctx.run(f"{_PY} -m data.generators.synthetic_snb --out {target}", pty=False)
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
              system_config: str = "",
              inventory: str = str(DEFAULT_INVENTORY)) -> None:
    """Run the per-(system, workload) loader.

    The system YAML supplies host/port/user/password/space/database/graph to the
    loader as command-line flags so the loader's container-default values
    (e.g., `graphd0`) never leak into a real-cluster run.
    """
    from graph_bench.utils.config import load_system
    from graph_bench.orchestration import SSHRunner
    inv = _load_inventory(Path(inventory))
    client = _client_node(inv)
    sut_hostname = inv.sut_nodes[0].hostname if inv.sut_nodes else None

    sys_yaml = Path(system_config or f"configs/systems/{system}.yaml")
    sys_cfg = load_system(sys_yaml, sut_hostname=sut_hostname)
    loader_args = _loader_cli_args(system, sys_cfg.options, inv, workload)

    dataset = dataset_path or f"data/generated/{workload}"
    module = _loader_module(system, workload)
    cmd = (
        f"{_PY} -m {module} --dataset {shlex.quote(dataset)} "
        + " ".join(loader_args)
    )

    runner = SSHRunner(inv)
    if runner._is_local(client) if hasattr(runner, "_is_local") else (client.hostname in {"localhost", "127.0.0.1"}):
        ctx.run(cmd, pty=False)
    else:
        from fabric import Connection
        with Connection(host=client.hostname, user=client.ssh_user) as conn:
            conn.run(f"cd {REPO_ROOT} && {cmd}")


def _loader_cli_args(system: str, options: dict, inv, workload: str = "") -> list[str]:
    """Translate per-SUT system YAML options into loader CLI flags.

    Loader argparse contracts accept the same names across SUTs where they make
    sense (host, port, user, password) and SUT-specific flags otherwise (space,
    database, graph, zero_host, zero_port).

    `inv` lets us derive cluster-shape facts (storage hosts for NebulaGraph,
    seed/agency endpoints for others) from the inventory + per-SUT deployer
    rather than hard-coding them in the system YAML.
    """
    args: list[str] = []
    if "host" in options:
        args.append(f"--host {shlex.quote(str(options['host']))}")
    if "port" in options:
        args.append(f"--port {int(options['port'])}")
    # Every SNB loader (Iv2 + BI) shares an argparse `--mode` flag; default
    # is `interactive` so we only emit it for the BI variant.
    if workload == "snb_bi":
        args.append("--mode bi")

    if system == "nebulagraph":
        if "user" in options:
            args.append(f"--user {shlex.quote(str(options['user']))}")
        if "password" in options:
            args.append(f"--password {shlex.quote(str(options['password']))}")
        if "space" in options:
            args.append(f"--space {shlex.quote(str(options['space']))}")
        # Derive storage hosts (host:port pairs) from the deployer's port plan
        # so the loader can ADD HOSTS without the operator hand-editing YAML.
        from graph_bench.orchestration.deployers.nebulagraph import (
            _port_offsets, _STORAGED_BASE,
        )
        offsets = _port_offsets(inv)
        storage_hosts = ",".join(
            f"{n.hostname}:{_STORAGED_BASE + offsets[n.name]}"
            for n in inv.sut_nodes
        )
        if storage_hosts:
            args.append(f"--storage-hosts {shlex.quote(storage_hosts)}")
            args.append(f"--expected-storage-hosts {len(inv.sut_nodes)}")
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
    elif system == "memgraph":
        if "user" in options:
            args.append(f"--user {shlex.quote(str(options['user']))}")
        if "password" in options:
            args.append(f"--password {shlex.quote(str(options['password']))}")
    elif system == "orientdb":
        if "user" in options:
            args.append(f"--user {shlex.quote(str(options['user']))}")
        if "password" in options:
            args.append(f"--password {shlex.quote(str(options['password']))}")
        if "database" in options:
            args.append(f"--database {shlex.quote(str(options['database']))}")
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
        # SNB IV2 / BI — Tier 2 (Memgraph + OrientDB; S1 only in the
        # Tier-2 scenario set but loaders accept either dataset variant).
        **{("memgraph", w): "data.loaders.ldbc_snb_memgraph" for w in iv2_or_bi},
        **{("orientdb", w): "data.loaders.ldbc_snb_orientdb" for w in iv2_or_bi},
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
             system_config: str = "",
             inventory: str = str(DEFAULT_INVENTORY)) -> None:
    sys_yaml = system_config or f"configs/systems/{system}.yaml"
    cmd = (
        f"{_PY} -m data.validators.runner --system {system} "
        f"--system-config {shlex.quote(sys_yaml)} "
        f"--workload {workload} "
        f"--dataset {shlex.quote(dataset_path)} "
        f"--inventory {shlex.quote(inventory)}"
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
