# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Business Intelligence query catalog (subset Q1, Q2, Q6, Q11).

Translated to nGQL, AQL, DQL. The translations target the analytical-only
S2 scenario; they intentionally simplify the official BI specification where
the simplification keeps the queries portable across all three Tier-1 engines.
Where a portability compromise is unavoidable (e.g., percentile aggregates
that DQL does not natively express) the simplified form is documented inline.
"""

from __future__ import annotations


# --------------------------------------------------------------------- nGQL
NEBULAGRAPH: dict[str, str] = {
    # Q1 — Posting summary: count Posts in a length window.
    # Parameters: minLength
    "Q1": (
        "USE {space}; "
        "LOOKUP ON `Post` WHERE `Post`.length >= {minLength} "
        "YIELD properties(vertex).length AS len | "
        "GROUP BY $-.len YIELD $-.len AS len, count(*) AS cnt "
        "| ORDER BY $-.cnt DESC | LIMIT 50"
    ),
    # Q2 — `Tag` evolution: posts tagged with a given tag, ordered by creationDate.
    # Parameters: tagId
    "Q2": (
        "USE {space}; "
        "GO FROM {tagId} OVER `HAS_TAG_POST` REVERSELY YIELD dst(edge) AS post | "
        "FETCH PROP ON `Post` $-.post YIELD properties(vertex).creationDate AS ts | "
        "ORDER BY $-.ts DESC | LIMIT 100"
    ),
    # Q6 — Most active forum members: count of Posts within a Forum's authors.
    # Parameters: forumId
    "Q6": (
        "USE {space}; "
        "GO FROM {forumId} OVER `CONTAINER_OF` YIELD dst(edge) AS post | "
        "GO FROM $-.post OVER `HAS_CREATOR` YIELD dst(edge) AS author | "
        "GROUP BY $-.author YIELD $-.author AS author, count(*) AS posts "
        "| ORDER BY $-.posts DESC | LIMIT 100"
    ),
    # Q11 — Friend triangle proxy: 2-hop `KNOWS` distinct count from a `Person`.
    # Parameters: personId
    "Q11": (
        "USE {space}; "
        "GO 2 STEPS FROM {personId} OVER `KNOWS` YIELD DISTINCT dst(edge) AS fof | "
        "LIMIT 200"
    ),
}


# --------------------------------------------------------------------- AQL
ARANGODB: dict[str, str] = {
    "Q1": (
        "FOR p IN Post FILTER p.length >= @minLength "
        "  COLLECT len = p.length WITH COUNT INTO cnt "
        "  SORT cnt DESC LIMIT 50 RETURN {len: len, cnt: cnt}"
    ),
    "Q2": (
        "FOR t IN Tag FILTER t._key == @tagId "
        "  FOR post IN 1..1 INBOUND t HAS_TAG_POST "
        "  SORT post.creationDate DESC LIMIT 100 "
        "  RETURN {post: post._key, ts: post.creationDate}"
    ),
    "Q6": (
        "FOR f IN Forum FILTER f._key == @forumId "
        "  FOR post IN 1..1 OUTBOUND f CONTAINER_OF "
        "    FOR author IN 1..1 OUTBOUND post HAS_CREATOR "
        "    COLLECT a = author._key WITH COUNT INTO posts "
        "    SORT posts DESC LIMIT 100 "
        "    RETURN {author: a, posts: posts}"
    ),
    "Q11": (
        "FOR p IN Person FILTER p._key == @personId "
        "  FOR fof IN 2..2 OUTBOUND p KNOWS LIMIT 200 RETURN DISTINCT fof._key"
    ),
}


# --------------------------------------------------------------------- DQL
# Dgraph aggregations on COLLECT-style queries are limited; the translations
# below fetch raw rows and let the benchmark client compute per-class metrics.
# This is the documented portability compromise for Q1 / Q6.
DGRAPH: dict[str, str] = {
    "Q1": (
        "query q1($minLength:int) { "
        "  posts(func: ge(length, $minLength), first: 5000) { id length } "
        "}"
    ),
    "Q2": (
        "query q2($tagId:int) { "
        "  t(func: eq(id, $tagId)) @filter(type(Tag)) { "
        "    ~HAS_TAG_POST (orderdesc: creationDate, first: 100) { id creationDate } "
        "  } "
        "}"
    ),
    "Q6": (
        "query q6($forumId:int) { "
        "  f(func: eq(id, $forumId)) @filter(type(Forum)) { "
        "    CONTAINER_OF (first: 5000) { "
        "      HAS_CREATOR { id firstName } "
        "    } "
        "  } "
        "}"
    ),
    "Q11": (
        "query q11($personId:int) { "
        "  p(func: eq(id, $personId)) @filter(type(Person)) { "
        "    KNOWS { KNOWS (first: 200) { id } } "
        "  } "
        "}"
    ),
}


_BY_SYSTEM: dict[str, dict[str, str]] = {
    "nebulagraph": NEBULAGRAPH,
    "arangodb": ARANGODB,
    "dgraph": DGRAPH,
}


def query_template_for(system: str, ref_id: str) -> str | None:
    return _BY_SYSTEM.get(system, {}).get(ref_id)
