# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Workload-aware post-load validator.

Per Progress Report Section 5, after bulk-load each scenario runs a
post-load validator that issues five known-answer queries to confirm the
dataset has been loaded correctly. The implementation here dispatches on the
workload identifier; each per-workload validator owns:

  - extracting "facts" from the on-disk dataset (e.g., Person ids, KNOWS
    edge counts)
  - selecting a small set of probe queries against the SUT
  - judging the SUT response (row count > 0, status == OK)

Adding a new workload means dropping a new module under `data/validators/` and
registering its `validate` function in `_REGISTRY`.

CLI:
    python -m data.validators.runner \\
        --system nebulagraph \\
        --workload snb_iv2 \\
        --dataset data/generated/snb_iv2_sf1 \\
        --system-config configs/systems/nebulagraph.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from graph_bench.drivers import get_driver
from graph_bench.utils.config import load_system


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class ValidationResult:
    system: str
    workload: str
    dataset: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    def summary(self) -> str:
        lines = [
            f"validator: system={self.system} workload={self.workload} dataset={self.dataset}"
        ]
        for c in self.checks:
            lines.append(
                f"  [{'OK' if c.ok else 'FAIL'}] {c.name}"
                + (f" — {c.detail}" if c.detail else "")
            )
        return "\n".join(lines)


# Each validator coroutine: (driver, dataset_dir) -> list[CheckResult].
WorkloadValidator = Callable[[Any, Path], Awaitable[list[CheckResult]]]


def _registry() -> dict[str, WorkloadValidator]:
    # Lazy import so optional drivers aren't a hard requirement.
    from data.validators import synthetic_snb_validator, ldbc_snb_validator, ldbc_finbench_validator

    return {
        "synthetic_snb": synthetic_snb_validator.validate,
        "snb_iv2": ldbc_snb_validator.validate,
        "snb_bi": ldbc_snb_validator.validate,  # shared schema with Iv2
        "snb_analytical": ldbc_snb_validator.validate,
        "finbench": ldbc_finbench_validator.validate,
    }


async def run_validation(
    system_name: str,
    system_options: dict[str, Any],
    workload: str,
    dataset_dir: Path,
) -> ValidationResult:
    result = ValidationResult(system=system_name, workload=workload, dataset=str(dataset_dir))

    registry = _registry()
    if workload not in registry:
        result.checks.append(
            CheckResult(name="workload_known", ok=False,
                        detail=f"no validator registered for workload {workload!r}")
        )
        return result

    driver = get_driver(system_name, system_options)
    try:
        await driver.connect()
    except Exception as exc:  # noqa: BLE001
        result.checks.append(CheckResult(name="connect", ok=False, detail=str(exc)))
        return result

    try:
        try:
            status = await driver.cluster_status()
            ok = status.healthy_nodes >= 1
            result.checks.append(
                CheckResult(
                    name="cluster_status",
                    ok=ok,
                    detail=f"node_count={status.node_count} healthy={status.healthy_nodes}",
                )
            )
        except Exception as exc:  # noqa: BLE001
            result.checks.append(CheckResult(name="cluster_status", ok=False, detail=str(exc)))

        per_workload = await registry[workload](driver, dataset_dir)
        result.checks.extend(per_workload)
    finally:
        try:
            await driver.disconnect()
        except Exception:  # noqa: BLE001
            pass

    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run post-load validation against a SUT.")
    parser.add_argument("--system", required=True, help="Driver registry name (e.g., nebulagraph).")
    parser.add_argument(
        "--workload", required=True,
        help="Workload identifier — selects which per-workload validator to run.",
    )
    parser.add_argument("--system-config", type=Path, help="Path to system YAML; options are read from it.")
    parser.add_argument("--dataset", type=Path, required=True, help="Generated dataset directory.")
    args = parser.parse_args(argv)

    options = load_system(args.system_config).options if args.system_config else {}
    result = asyncio.run(run_validation(args.system, options, args.workload, args.dataset))
    print(result.summary())
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
