# Post-load validators

After a bulk load completes, each SUT must pass a small set of known-answer
queries against the loaded dataset. A validator failure aborts the benchmark
run before warmup, so that measurement is never taken on a partially-loaded
graph.

## Implementation

`data/validators/runner.py` is a system-agnostic runner. It reads the dataset
from `data/generated/<dataset>/` to derive expected counts and at least one
known person id with KNOWS edges, then exercises the connected SUT through
the driver protocol.

Checks performed:

| Check            | What it verifies |
| ---              | ---              |
| `connect`        | Driver can establish its connection pool. |
| `cluster_status` | At least one healthy storage node is online. |
| `IS1` ×3         | Person fetch by id returns ≥ 1 row for sampled ids. |
| `IS2` ×3         | 1-hop KNOWS traversal returns ≥ 1 row for sampled ids. |

Exit code is `0` if every check passes, non-zero otherwise.

## CLI

```bash
python -m data.validators.runner \
  --system nebulagraph \
  --system-config configs/systems/nebulagraph-demo.yaml \
  --dataset data/generated/synthetic_snb
```

When integrated into the benchmark lifecycle, the Fabric `validate` task
invokes this runner before the `run-scenario` task is allowed to start.
