# NebulaGraph compose bundle

Placeholder. Populate with per-node `docker-compose.yaml` files (`metad.yaml`,
`storaged.yaml`, `graphd.yaml`) pinning the image digest, and wire Fabric tasks
in `deploy/fabric/fabfile.py` to invoke them.

Initial cluster (node2–node4): one metad, one storaged, one graphd per node.
Scale-out target (node5): one additional storaged, activated by `ADD HOSTS`.
