# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-SUT DDL generators for the LDBC SNB Interactive v2 schema.

Each generator returns the schema operations needed to materialise:

- one tag/collection/predicate set per `VertexType`
- one edge label/collection/predicate per standalone `EdgeType`
- one edge label/collection/predicate per FK-derived edge (e.g., HAS_CREATOR,
  CONTAINER_OF, REPLY_OF_POST, IS_LOCATED_IN_*)

FK-derived edges carry no properties; standalone edges carry the properties
declared on `EdgeType.properties`.
"""

from __future__ import annotations

from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES, ForeignKey, Property


# --------------------------------------------------------------------- nebulagraph

def _ngql_type(p: Property) -> str:
    return {
        "id": "int",
        "int": "int",
        "long": "int",
        "bigint": "int",
        "string": "string",
        "datetime": "int",
        "date": "int",
        "float": "double",
    }[p.dtype]


def nebulagraph_ddl(space: str, partition_num: int = 30, replica_factor: int = 1) -> list[str]:
    """nGQL statements that materialise the SNB Iv2 schema in a fresh space."""
    stmts: list[str] = [
        f"DROP SPACE IF EXISTS {space}",
        (
            f"CREATE SPACE IF NOT EXISTS {space} ("
            f"partition_num={partition_num}, replica_factor={replica_factor}, "
            f"vid_type=INT64)"
        ),
    ]
    for v in VERTICES:
        body = ", ".join(f"`{p.name}` {_ngql_type(p)}" for p in v.properties)
        if not body:
            body = "gb_p int"
        # Backtick the label: SNB names like `Tag`, `Comment`, `Type` collide
        # with nGQL reserved keywords.
        stmts.append(f"USE {space}; CREATE TAG IF NOT EXISTS `{v.name}`({body})")

    edge_labels: dict[str, tuple[Property, ...]] = {}
    for e in EDGES:
        edge_labels[e.name] = e.properties
    # FK-derived edges have no properties.
    for v in VERTICES:
        for fk in v.foreign_keys:
            edge_labels.setdefault(fk.edge_label, ())

    for label, props in edge_labels.items():
        body = ", ".join(f"`{p.name}` {_ngql_type(p)}" for p in props)
        if not body:
            body = "gb_p int"  # NebulaGraph requires at least one property.
        stmts.append(f"USE {space}; CREATE EDGE IF NOT EXISTS `{label}`({body})")
    return stmts


# --------------------------------------------------------------------- arangodb

def arangodb_collections() -> tuple[list[str], list[tuple[str, str, str]]]:
    """Return ``(vertex_collections, edge_definitions)``.

    Edge definitions are tuples ``(edge_collection, from_vertex, to_vertex)``.
    Includes both standalone and FK-derived edges.
    """
    vertices = [v.name for v in VERTICES]
    edges: list[tuple[str, str, str]] = [
        (e.name, e.src_label, e.dst_label) for e in EDGES
    ]
    for v in VERTICES:
        for fk in v.foreign_keys:
            if fk.direction == "in":
                src, dst = fk.target_label, v.name
            else:
                src, dst = v.name, fk.target_label
            edges.append((fk.edge_label, src, dst))
    return vertices, edges


# --------------------------------------------------------------------- dgraph

_DGRAPH_TYPE = {
    "id": "int",
    "int": "int",
    "long": "int",
    "bigint": "int",
    "string": "string",
    "datetime": "int",
    "date": "int",
    "float": "float",
}


def dgraph_schema() -> str:
    """Dgraph predicates + types covering vertices, standalone edges, FK edges."""
    lines: list[str] = []
    seen: set[str] = set()

    # Scalar predicates from vertex + edge properties.
    for v in VERTICES:
        for p in v.properties:
            if p.name in seen:
                continue
            seen.add(p.name)
            t = _DGRAPH_TYPE[p.dtype]
            if p.name == "id":
                lines.append(f"{p.name}: {t} @index({t}) .")
            elif t == "string" and p.name in {"firstName", "lastName", "name"}:
                lines.append(f"{p.name}: {t} @index(exact, term) .")
            else:
                lines.append(f"{p.name}: {t} .")
    if "id" not in seen:
        lines.append("id: int @index(int) .")
        seen.add("id")

    # Edge predicates: each label becomes `[uid] @count @reverse`.
    for e in EDGES:
        lines.append(f"{e.name}: [uid] @count @reverse .")
        for p in e.properties:
            if p.name in seen:
                continue
            seen.add(p.name)
            t = _DGRAPH_TYPE[p.dtype]
            lines.append(f"{p.name}: {t} .")
    fk_labels: set[str] = set()
    for v in VERTICES:
        for fk in v.foreign_keys:
            fk_labels.add(fk.edge_label)
    for label in sorted(fk_labels):
        lines.append(f"{label}: [uid] @count @reverse .")

    # Type declarations: one per vertex, listing scalar props + outgoing edge labels.
    out_edge_labels: dict[str, list[str]] = {v.name: [] for v in VERTICES}
    for e in EDGES:
        out_edge_labels[e.src_label].append(e.name)
    for v in VERTICES:
        for fk in v.foreign_keys:
            if fk.direction == "in":
                out_edge_labels.setdefault(fk.target_label, []).append(fk.edge_label)
            else:
                out_edge_labels[v.name].append(fk.edge_label)

    for v in VERTICES:
        prop_names = [p.name for p in v.properties]
        edge_names = out_edge_labels.get(v.name, [])
        body = " ".join(prop_names + edge_names).strip()
        lines.append(f"type {v.name} {{ {body} }}")

    return "\n".join(lines) + "\n"


__all__ = ["nebulagraph_ddl", "arangodb_collections", "dgraph_schema"]
