# Bulk loaders

One module per SUT. Each loader receives the path to a generated dataset
directory (local to the SUT node, staged there by Fabric-driven rsync) and
invokes the SUT's native bulk loader:

- NebulaGraph → `nebula-importer`
- ArangoDB    → `arangoimport`
- Dgraph      → `dgraph live` / `dgraph bulk`
- JanusGraph  → Gremlin `io().read()` against the attached Cassandra backend
- HugeGraph   → `hugegraph-loader`

Loaders are invoked by the Fabric task `load-data`; they are not part of the
benchmark harness itself and never run during measurement.
