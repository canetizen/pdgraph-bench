# pdgraph-bench

A portable benchmark for open-source distributed property-graph databases under
mixed transactional and analytical workloads, including a controlled elastic
scale-out event executed during a live mixed workload.

## What this is

`pdgraph-bench` is a portable evaluation benchmark — and the codebase that
implements it — for cross-system measurement of five open-source distributed
property-graph database systems:

- **NebulaGraph** (nGQL)
- **ArangoDB 3.11** (AQL)
- **Dgraph** (DQL)
- **Memgraph** (Cypher over Bolt)
- **OrientDB** (OrientSQL over REST, Hazelcast-clustered)

The benchmark draws its workload from publicly available LDBC components:

- **LDBC SNB Interactive v2** — primary transactional dataset
- **LDBC SNB Business Intelligence** — analytical baseline
- **LDBC FinBench** — financial-domain transactional baseline (built from
  upstream source via a multi-stage Dockerfile, since LDBC does not publish
  a pre-built FinBench Datagen image)

Five scenarios are defined: `S1` transactional-only (cross-tier baseline),
`S2` analytical-only, `S3` 70/20/10 mixed, `S4` financial transactional,
`S5` mixed plus one scale-out event at *t* = 300 s into the measurement
window. Three Tier-1 systems (NebulaGraph, ArangoDB, Dgraph) run all five
scenarios; the two Tier-2 systems (Memgraph, OrientDB) run `S1` only,
giving a transactional baseline against the single-node-per-process
class without expanding the campaign cost beyond the time budget.

The protocol is closed-loop, deterministically seeded per
`(system, scenario, repetition)`, and produces per-request Parquet logs,
HDR latency histograms, one-second throughput buckets, per-node resource
samples, and a phase-boundary `events.jsonl` per run.

## What is in this repository

```
configs/    scenario / workload / system / inventory YAMLs
deploy/     Fabric orchestration + docker-compose bundles
data/       LDBC datagen wrappers, per-SUT bulk loaders, validators
src/        benchmark harness (Python package graph_bench)
analysis/   stats + plots + report generation
tests/      pytest unit + integration + E2E smoke
results/    per-run output directories (gitignored)
```

The harness uses `asyncio` + `anyio` for the closed-loop worker pool,
Pydantic v2 for YAML configuration, Apache Parquet for per-request logs,
HDR histograms for latency aggregation, and Fabric for multi-host
orchestration. The analysis layer uses NumPy / SciPy / Pandas for
statistics and Matplotlib for figures.

## Quick start

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
make dev-install
make test
```

The test suite runs an integration test that exercises a short benchmark
scenario against an in-memory mock SUT and emits the full output artifact
set (Parquet, HDR histogram, events JSONL). This validates the harness
end-to-end without needing a real database.

## Single-host playground

The same orchestration code path that drives a real cluster also drives a
single-host simulation — the only thing that differs is the inventory.
`configs/inventory/cluster.yaml.playground` declares all five logical nodes
on `localhost`; the deployers detect co-location and apply per-node port
offsets (10-spaced) so each instance keeps a clear port window.

```bash
cp configs/inventory/cluster.yaml.playground configs/inventory/cluster.yaml
bash scripts/setup-cluster.sh nebulagraph snb_iv2 0.003

uv run pdgraph-bench run \
    configs/scenarios/playground-s1.yaml \
    --system configs/systems/nebulagraph.yaml \
    --workload configs/workloads/snb_iv2.yaml \
    --inventory configs/inventory/cluster.yaml
```

S5 (with scale-out) is invoked the same way against
`configs/scenarios/playground-s5.yaml` — the harness's scale-out
provisioner brings up the reserve storaged on `node5` (which resolves to
`localhost` in the playground) using the same deployer + runner that
production uses.

## Deploying on a real cluster

1. Copy
   [configs/inventory/cluster.yaml.example](configs/inventory/cluster.yaml.example)
   to `configs/inventory/cluster.yaml` and fill in hostname / IP / SSH user
   for each cluster node. Each node needs:
   - Linux + Docker engine + passwordless sudo for the SSH user
   - Every other node's hostname resolvable (the deployers use
     `network_mode: host`)
2. Bring up a SUT, generate + load a dataset, run the post-load validator
   — all in one command:
   ```bash
   bash scripts/setup-cluster.sh nebulagraph snb_iv2 1
   #    setup-cluster.sh <system> <workload> [<scale-factor>]
   ```
   `<system>` ∈ `nebulagraph` · `arangodb` · `dgraph` · `memgraph` · `orientdb`
   `<workload>` ∈ `snb_iv2` · `snb_bi` · `finbench` · `synthetic_snb`
3. Run a scenario:
   ```bash
   uv run fab --search-root deploy/fabric run-scenario \
       --system=nebulagraph --scenario=s5 --workload=snb_iv2
   ```
4. Generate the report:
   ```bash
   uv run python -m analysis.reports.generate_report --runs 'results/*'
   ```

The orchestration layer in
[src/graph_bench/orchestration/](src/graph_bench/orchestration/) computes
a per-node Docker Compose project for each SUT from the inventory and
pushes it via SSH.

## Adding a new SUT

Three touches, no framework changes:

1. `src/graph_bench/drivers/<system>.py` — implement the `Driver` protocol.
2. `src/graph_bench/orchestration/deployers/<system>.py` — produce the
   per-node `ServiceSpec`s for cluster bring-up and scale-out.
3. `configs/systems/<system>.yaml` — endpoint + deployment config
   referenced by the inventory.

## License

MIT.
