# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-SUT DDL generators for the LDBC FinBench schema."""

from __future__ import annotations

from graph_bench.workloads.finbench.schema import EDGES, VERTICES, Property


def _ngql_type(p: Property) -> str:
    return {
        "id": "int",
        "int": "int",
        "long": "int",
        "bigint": "int",
        "string": "string",
        "datetime": "int",
        "float": "double",
    }[p.dtype]


def nebulagraph_ddl(space: str, partition_num: int = 30, replica_factor: int = 1) -> list[str]:
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
        stmts.append(f"USE {space}; CREATE TAG IF NOT EXISTS `{v.name}`({body})")
    for e in EDGES:
        body = ", ".join(f"`{p.name}` {_ngql_type(p)}" for p in e.properties)
        if not body:
            body = "gb_p int"
        stmts.append(f"USE {space}; CREATE EDGE IF NOT EXISTS `{e.name}`({body})")
    return stmts


def arangodb_collections() -> tuple[list[str], list[tuple[str, str, str]]]:
    return (
        [v.name for v in VERTICES],
        [(e.name, e.src_label, e.dst_label) for e in EDGES],
    )


_DTYPE = {
    "id": "int", "int": "int", "long": "int", "bigint": "int",
    "string": "string", "datetime": "int", "float": "float",
}


def dgraph_schema() -> str:
    seen: set[str] = {"id"}
    lines: list[str] = ["id: int @index(int) ."]
    for v in VERTICES:
        for p in v.properties:
            if p.name in seen:
                continue
            seen.add(p.name)
            t = _DTYPE[p.dtype]
            if t == "string" and p.name in {"name", "country", "city", "type"}:
                lines.append(f"{p.name}: {t} @index(exact) .")
            else:
                lines.append(f"{p.name}: {t} .")
    for e in EDGES:
        lines.append(f"{e.name}: [uid] @count @reverse .")
        for p in e.properties:
            if p.name in seen:
                continue
            seen.add(p.name)
            t = _DTYPE[p.dtype]
            lines.append(f"{p.name}: {t} .")

    out_edges: dict[str, list[str]] = {v.name: [] for v in VERTICES}
    for e in EDGES:
        out_edges[e.src_label].append(e.name)
    for v in VERTICES:
        prop_names = [p.name for p in v.properties]
        body = " ".join(prop_names + out_edges.get(v.name, [])).strip()
        lines.append(f"type {v.name} {{ {body} }}")
    return "\n".join(lines) + "\n"


__all__ = ["nebulagraph_ddl", "arangodb_collections", "dgraph_schema"]
