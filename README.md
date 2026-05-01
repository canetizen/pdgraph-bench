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
- **JanusGraph** (Gremlin, Cassandra-backed)
- **Apache HugeGraph** (Gremlin/REST, Cassandra-backed)

The benchmark draws its workload from publicly available LDBC components:

- **LDBC SNB Interactive v2** — primary transactional dataset
- **LDBC SNB Business Intelligence** — analytical baseline
- **LDBC FinBench** — financial-domain transactional baseline (built from
  upstream source via a multi-stage Dockerfile, since LDBC does not publish
  a pre-built FinBench Datagen image)

Five scenarios are defined: `S1` transactional-only (cross-tier baseline),
`S2` analytical-only, `S3` 70/20/10 mixed, `S4` financial transactional,
`S5` mixed plus one scale-out event at *t* = 300 s into the measurement
window. Three Tier-1 systems run all five scenarios; the two Tier-2 systems
run `S1` only because of their backend deployment complexity.

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

For local validation against a real NebulaGraph cluster simulated on one
host:

```bash
docker compose -f docker-compose.cluster.yaml up -d
docker exec gb-node1 python -m data.loaders.ldbc_snb_nebulagraph \
    --host 127.0.0.1 --port 9669 --user root --password nebula \
    --space snb \
    --storage-hosts "127.0.0.1:9779,127.0.0.1:9789,127.0.0.1:9799" \
    --dataset /app/data/generated/snb_iv2_sf0.003
docker exec gb-node1 pdgraph-bench run \
    configs/scenarios/playground-s1.yaml \
    --system configs/systems/nebulagraph-playground.yaml \
    --workload configs/workloads/snb_iv2-playground.yaml \
    --results results --run-id playground-s1
```

S5 (with scale-out) is invoked the same way, with the reserve container's
compose file passed via `--scale-out-compose docker-compose.scaleout.yaml`.

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
   `<system>` ∈ `nebulagraph` · `arangodb` · `dgraph` · `janusgraph` · `hugegraph`
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
