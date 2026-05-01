# JanusGraph compose bundle

Placeholder. Populate with `docker-compose.yaml` files running a Cassandra
cluster (3 nodes across node2–node4) plus one JanusGraph server per node
attached to the shared Cassandra keyspace.

Tier-2 in this study: scenario S1 only; does not participate in scale-out.
The Cassandra cluster is shared with the HugeGraph bundle through a distinct
keyspace; Tier-2 runs are scheduled in non-overlapping windows.
