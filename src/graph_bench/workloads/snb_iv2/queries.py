# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 query catalog, parameterised per target system.

The benchmark uses a portable subset of LDBC IS1..IS7 short reads, IC1/IC3/IC5/IC6
complex reads, and IU1/IU2/IU6 update queries. Each query is expressed in three
target languages (nGQL, AQL, DQL); the corresponding driver picks the right
text via `query_template_for(system, ref_id)`.

The drivers' module-level `_QUERY_CATALOG` dicts are now thin wrappers around
this catalog so that the per-system templates have a single canonical home.
"""

from __future__ import annotations


# --------------------------------------------------------------------- nGQL
NEBULAGRAPH: dict[str, str] = {
    # IS1: profile of one Person.
    "IS1": (
        "USE {space}; FETCH PROP ON `Person` {personId} "
        "YIELD properties(vertex).firstName AS firstName, "
        "properties(vertex).lastName AS lastName, "
        "properties(vertex).birthday AS birthday, "
        "properties(vertex).locationIP AS locationIP, "
        "properties(vertex).browserUsed AS browserUsed, "
        "properties(vertex).gender AS gender, "
        "properties(vertex).creationDate AS creationDate"
    ),
    # IS2: 10 most recent Messages by Person (Posts + Comments). HAS_CREATOR
    # carries Post→Person; HAS_CREATOR_C carries Comment→Person — traverse both
    # in REVERSE so we end up at messages the Person authored. Note: nGQL
    # cannot ORDER BY through a `properties(vertex)` map, so we YIELD
    # individual fields explicitly.
    "IS2": (
        "USE {space}; "
        "GO FROM {personId} OVER `HAS_CREATOR`,`HAS_CREATOR_C` REVERSELY YIELD dst(edge) AS msg | "
        "FETCH PROP ON `Post`,`Comment` $-.msg "
        "YIELD properties(vertex).creationDate AS createdAt, "
        "properties(vertex).content AS content | "
        "ORDER BY $-.createdAt DESC | LIMIT 10"
    ),
    # IS3: friends of a Person, ordered by friendship creationDate desc.
    "IS3": (
        "USE {space}; "
        "GO FROM {personId} OVER `KNOWS` YIELD dst(edge) AS friend, properties(edge).creationDate AS since | "
        "ORDER BY $-.since DESC"
    ),
    # IS4: content + creationDate of one Message (Post or Comment).
    "IS4": (
        "USE {space}; FETCH PROP ON `Post`,`Comment` {messageId} "
        "YIELD properties(vertex).content AS content, "
        "properties(vertex).creationDate AS creationDate"
    ),
    # IS5: creator of a Message.
    "IS5": (
        "USE {space}; "
        "GO FROM {messageId} OVER `HAS_CREATOR`,`HAS_CREATOR_C` YIELD dst(edge) AS person"
    ),
    # IS6: forum + moderator of a given Message's containing Forum.
    "IS6": (
        "USE {space}; "
        "GO FROM {messageId} OVER `REPLY_OF_POST`,`REPLY_OF_COMMENT` YIELD dst(edge) AS post | "
        "GO FROM $-.post OVER `CONTAINER_OF` REVERSELY YIELD dst(edge) AS forum"
    ),
    # IS7: replies (Comments) to one Message.
    "IS7": (
        "USE {space}; "
        "GO FROM {messageId} OVER `REPLY_OF_POST`,`REPLY_OF_COMMENT` REVERSELY "
        "YIELD dst(edge) AS reply"
    ),
    # IC1 — abridged: Persons within 3 hops of one Person filtered by first name.
    "IC1": (
        "USE {space}; "
        "GO 1 TO 3 STEPS FROM {personId} OVER `KNOWS` "
        "YIELD DISTINCT dst(edge) AS friend "
        "| FETCH PROP ON `Person` $-.friend YIELD properties(vertex).firstName AS fn "
        "| YIELD $-.fn AS fn WHERE $-.fn == '{firstName}' | LIMIT 20"
    ),
    "IC3": (
        "USE {space}; "
        "GO 1 TO 2 STEPS FROM {personId} OVER `KNOWS` YIELD DISTINCT dst(edge) AS friend "
        "| GO FROM $-.friend OVER `HAS_CREATOR` REVERSELY YIELD dst(edge) AS msg "
        "| LIMIT 20"
    ),
    "IC5": (
        "USE {space}; "
        "GO 1 TO 2 STEPS FROM {personId} OVER `KNOWS` YIELD DISTINCT dst(edge) AS friend "
        "| GO FROM $-.friend OVER `HAS_MEMBER` REVERSELY YIELD dst(edge) AS forum "
        "| LIMIT 20"
    ),
    "IC6": (
        "USE {space}; "
        "GO 1 TO 2 STEPS FROM {personId} OVER `KNOWS` YIELD DISTINCT dst(edge) AS friend "
        "| GO FROM $-.friend OVER `HAS_CREATOR` REVERSELY YIELD dst(edge) AS msg "
        "| GO FROM $-.msg OVER `HAS_TAG_POST`,`HAS_TAG_COMMENT` YIELD dst(edge) AS taggedBy | LIMIT 20"
    ),
    # IU1: add a Person.
    "IU1": (
        "USE {space}; "
        "INSERT VERTEX `Person`(`firstName`, `lastName`, `gender`, `birthday`, `creationDate`, `locationIP`, `browserUsed`) "
        "VALUES {personId}:('{firstName}', '{lastName}', '{gender}', {birthday}, {creationDate}, '{locationIP}', '{browserUsed}')"
    ),
    # IU2: add a likes-Post edge.
    "IU2": (
        "USE {space}; INSERT EDGE `LIKES_POST`(`creationDate`) VALUES {personId}->{postId}:({creationDate})"
    ),
    # IU6: add a Comment.
    "IU6": (
        "USE {space}; "
        "INSERT VERTEX `Comment`(`creationDate`, `locationIP`, `browserUsed`, `content`, `length`) "
        "VALUES {commentId}:({creationDate}, '{locationIP}', '{browserUsed}', '{content}', {length})"
    ),
}


# --------------------------------------------------------------------- AQL
ARANGODB: dict[str, str] = {
    "IS1": (
        "FOR p IN Person FILTER p._key == @personId LIMIT 1 RETURN p"
    ),
    "IS2": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR msg IN 1..1 INBOUND person HAS_CREATOR, HAS_CREATOR_C "
        "  SORT msg.creationDate DESC LIMIT 10 RETURN msg"
    ),
    "IS3": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR friend, e IN 1..1 OUTBOUND person KNOWS "
        "  SORT e.creationDate DESC RETURN {friend: friend._key, since: e.creationDate}"
    ),
    "IS4": (
        "FOR m IN Post FILTER m._key == @messageId LIMIT 1 "
        "  RETURN {content: m.content, creationDate: m.creationDate}"
    ),
    "IS5": (
        "FOR m IN Post FILTER m._key == @messageId "
        "  FOR creator IN 1..1 OUTBOUND m HAS_CREATOR RETURN creator._key"
    ),
    "IS6": (
        "FOR m IN Post FILTER m._key == @messageId "
        "  FOR post IN 1..2 OUTBOUND m REPLY_OF_POST, REPLY_OF_COMMENT "
        "  FOR forum IN 1..1 INBOUND post CONTAINER_OF RETURN forum._key"
    ),
    "IS7": (
        "FOR m IN Post FILTER m._key == @messageId "
        "  FOR reply IN 1..1 INBOUND m REPLY_OF_POST, REPLY_OF_COMMENT "
        "  RETURN reply._key"
    ),
    "IC1": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR friend IN 1..3 OUTBOUND person KNOWS "
        "  FILTER friend.firstName == @firstName LIMIT 20 RETURN friend._key"
    ),
    "IC3": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR friend IN 1..2 OUTBOUND person KNOWS "
        "  FOR msg IN 1..1 INBOUND friend HAS_CREATOR "
        "  LIMIT 20 RETURN msg._key"
    ),
    "IC5": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR friend IN 1..2 OUTBOUND person KNOWS "
        "  FOR forum IN 1..1 INBOUND friend HAS_MEMBER "
        "  LIMIT 20 RETURN forum._key"
    ),
    "IC6": (
        "FOR person IN Person FILTER person._key == @personId "
        "  FOR friend IN 1..2 OUTBOUND person KNOWS "
        "  FOR msg IN 1..1 INBOUND friend HAS_CREATOR "
        "  FOR tag IN 1..1 OUTBOUND msg HAS_TAG_POST, HAS_TAG_COMMENT "
        "  LIMIT 20 RETURN tag._key"
    ),
    "IU1": (
        "INSERT { _key: TO_STRING(@personId), firstName: @firstName, lastName: @lastName, "
        "         gender: @gender, birthday: @birthday, creationDate: @creationDate, "
        "         locationIP: @locationIP, browserUsed: @browserUsed } INTO Person"
    ),
    "IU2": (
        "INSERT { _from: CONCAT('Person/', TO_STRING(@personId)), "
        "         _to: CONCAT('Post/', TO_STRING(@postId)), "
        "         creationDate: @creationDate } INTO LIKES_POST"
    ),
    "IU6": (
        "INSERT { _key: TO_STRING(@commentId), creationDate: @creationDate, "
        "         locationIP: @locationIP, browserUsed: @browserUsed, "
        "         content: @content, length: @length } INTO Comment"
    ),
}


# --------------------------------------------------------------------- DQL
DGRAPH: dict[str, str] = {
    "IS1": (
        "query is1($personId:int) { "
        "  p(func: eq(id, $personId)) @filter(type(Person)) { "
        "    firstName lastName birthday gender locationIP browserUsed creationDate "
        "  } "
        "}"
    ),
    "IS2": (
        "query is2($personId:int) { "
        "  p(func: eq(id, $personId)) @filter(type(Person)) { "
        "    msgs: ~HAS_CREATOR (orderdesc: creationDate, first: 10) { id content creationDate } "
        "  } "
        "}"
    ),
    "IS3": (
        "query is3($personId:int) { "
        "  p(func: eq(id, $personId)) @filter(type(Person)) { "
        "    KNOWS @facets(orderdesc: creationDate) { id firstName lastName } "
        "  } "
        "}"
    ),
    "IS4": (
        "query is4($messageId:int) { "
        "  m(func: eq(id, $messageId)) { content creationDate } "
        "}"
    ),
    "IS5": (
        "query is5($messageId:int) { "
        "  m(func: eq(id, $messageId)) { HAS_CREATOR { id firstName lastName } } "
        "}"
    ),
    "IS6": (
        "query is6($messageId:int) { "
        "  m(func: eq(id, $messageId)) { "
        "    REPLY_OF_POST { ~CONTAINER_OF { id title } } "
        "  } "
        "}"
    ),
    "IS7": (
        "query is7($messageId:int) { "
        "  m(func: eq(id, $messageId)) { "
        "    ~REPLY_OF_POST { id content creationDate } "
        "  } "
        "}"
    ),
    # Dgraph rejects two KNOWS expansions at the same query level
    # ("not allowed multiple times in same sub-query"); use a single
    # 1-hop expansion plus the @filter on firstName. This widens
    # the result set vs. the spec's friends+fof, but keeps the query
    # syntactically valid for a portable benchmark.
    "IC1": (
        "query ic1($personId:int, $firstName:string) { "
        "  p(func: eq(id, $personId)) { "
        "    friends as KNOWS { } "
        "  } "
        "  result(func: uid(friends), first: 20) "
        "    @filter(eq(firstName, $firstName)) { id firstName lastName } "
        "}"
    ),
    "IC3": (
        "query ic3($personId:int) { "
        "  p(func: eq(id, $personId)) { "
        "    KNOWS { "
        "      ~HAS_CREATOR (first: 20) { id content } "
        "    } "
        "  } "
        "}"
    ),
    "IC5": (
        "query ic5($personId:int) { "
        "  p(func: eq(id, $personId)) { "
        "    KNOWS { "
        "      ~HAS_MEMBER (first: 20) { id title } "
        "    } "
        "  } "
        "}"
    ),
    "IC6": (
        "query ic6($personId:int) { "
        "  p(func: eq(id, $personId)) { "
        "    KNOWS { "
        "      ~HAS_CREATOR (first: 20) { "
        "        HAS_TAG_POST { id name } "
        "      } "
        "    } "
        "  } "
        "}"
    ),
    # IU1/IU6 are inserts of NEW vertices — use blank-node syntax (`_:p`)
    # so Dgraph allocates a fresh uid per call. The earlier `uid(p)` form
    # was invalid: `uid()` resolves a variable from a query block but the
    # mutation had no surrounding query. N-quads must be newline-terminated
    # — Dgraph's lexer rejects multiple statements collapsed onto one line
    # ("Expected newline or # after .").
    "IU1": (
        "{ set {\n"
        "  _:p <id> \"$personId\" .\n"
        "  _:p <firstName> \"$firstName\" .\n"
        "  _:p <lastName> \"$lastName\" .\n"
        "  _:p <gender> \"$gender\" .\n"
        "  _:p <birthday> \"$birthday\" .\n"
        "  _:p <creationDate> \"$creationDate\" .\n"
        "  _:p <locationIP> \"$locationIP\" .\n"
        "  _:p <browserUsed> \"$browserUsed\" .\n"
        "  _:p <dgraph.type> \"Person\" .\n"
        "}}\n"
    ),
    # IU2 inserts a LIKES_POST edge between two EXISTING vertices —
    # upsert query resolves their uids, mutation adds the edge.
    "IU2": (
        "upsert {\n"
        "  query {\n"
        "    src(func: eq(id, \"$personId\")) { sp as uid }\n"
        "    dst(func: eq(id, \"$postId\")) { dp as uid }\n"
        "  }\n"
        "  mutation { set { uid(sp) <LIKES_POST> uid(dp) . } }\n"
        "}\n"
    ),
    "IU6": (
        "{ set {\n"
        "  _:c <id> \"$commentId\" .\n"
        "  _:c <creationDate> \"$creationDate\" .\n"
        "  _:c <locationIP> \"$locationIP\" .\n"
        "  _:c <browserUsed> \"$browserUsed\" .\n"
        "  _:c <content> \"$content\" .\n"
        "  _:c <length> \"$length\" .\n"
        "  _:c <dgraph.type> \"Comment\" .\n"
        "}}\n"
    ),
}


# --------------------------------------------------------------------- Cypher (Memgraph)
MEMGRAPH: dict[str, str] = {
    "IS1": (
        "MATCH (p:Person {id: $personId}) "
        "RETURN p.firstName, p.lastName, p.birthday, p.gender, "
        "p.locationIP, p.browserUsed, p.creationDate"
    ),
    "IS2": (
        "MATCH (p:Person {id: $personId})<-[:HAS_CREATOR]-(m) "
        "RETURN m.id AS id ORDER BY m.creationDate DESC LIMIT 10"
    ),
    "IS3": (
        "MATCH (p:Person {id: $personId})-[r:KNOWS]->(f:Person) "
        "RETURN f.id, f.firstName, f.lastName ORDER BY f.creationDate DESC LIMIT 20"
    ),
    "IS4": "MATCH (m {id: $messageId}) RETURN m.content, m.creationDate",
    "IS5": (
        "MATCH (m {id: $messageId})-[:HAS_CREATOR]->(p:Person) "
        "RETURN p.id, p.firstName, p.lastName"
    ),
    "IS6": (
        "MATCH (m {id: $messageId})-[:REPLY_OF_POST*0..3]->(p:Post)"
        "<-[:CONTAINER_OF]-(f:Forum) "
        "RETURN f.id, f.title LIMIT 1"
    ),
    "IS7": (
        "MATCH (m {id: $messageId})<-[:REPLY_OF_POST]-(reply) "
        "RETURN reply.id, reply.content, reply.creationDate"
    ),
    "IC1": (
        "MATCH (p:Person {id: $personId})-[:KNOWS]->(f:Person) "
        "WHERE f.firstName = $firstName "
        "RETURN f.id, f.firstName, f.lastName LIMIT 20"
    ),
    "IC3": (
        "MATCH (p:Person {id: $personId})-[:KNOWS]->(:Person)"
        "<-[:HAS_CREATOR]-(m) "
        "RETURN m.id, m.content LIMIT 20"
    ),
    "IC5": (
        "MATCH (p:Person {id: $personId})-[:KNOWS]->(f:Person)"
        "<-[:HAS_MEMBER]-(forum:Forum) "
        "RETURN forum.id, forum.title LIMIT 20"
    ),
    "IC6": (
        "MATCH (p:Person {id: $personId})-[:KNOWS]->(:Person)"
        "<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG_POST]->(tag:Tag) "
        "RETURN tag.id, tag.name LIMIT 20"
    ),
    "IU1": (
        "CREATE (p:Person {id: $personId, firstName: $firstName, "
        "lastName: $lastName, gender: $gender, birthday: $birthday, "
        "creationDate: $creationDate, locationIP: $locationIP, "
        "browserUsed: $browserUsed})"
    ),
    "IU2": (
        "MATCH (s:Person {id: $personId}), (d:Post {id: $postId}) "
        "CREATE (s)-[:LIKES_POST {creationDate: $creationDate}]->(d)"
    ),
    "IU6": (
        "CREATE (c:Comment {id: $commentId, creationDate: $creationDate, "
        "locationIP: $locationIP, browserUsed: $browserUsed, "
        "content: $content, length: $length})"
    ),
}


# --------------------------------------------------------------------- OrientSQL
ORIENTDB: dict[str, str] = {
    "IS1": (
        "SELECT firstName, lastName, birthday, gender, locationIP, browserUsed, "
        "creationDate FROM Person WHERE id = :personId"
    ),
    "IS2": (
        "SELECT FROM (SELECT EXPAND(in('HAS_CREATOR')) FROM Person "
        "WHERE id = :personId) ORDER BY creationDate DESC LIMIT 10"
    ),
    "IS3": (
        "SELECT id, firstName, lastName FROM (SELECT EXPAND(out('KNOWS')) "
        "FROM Person WHERE id = :personId) LIMIT 20"
    ),
    "IS4": "SELECT content, creationDate FROM V WHERE id = :messageId",
    "IS5": "SELECT EXPAND(out('HAS_CREATOR')) FROM V WHERE id = :messageId",
    "IS6": (
        "SELECT EXPAND(in('CONTAINER_OF')) FROM (SELECT EXPAND("
        "out('REPLY_OF_POST')) FROM V WHERE id = :messageId) LIMIT 1"
    ),
    "IS7": "SELECT EXPAND(in('REPLY_OF_POST')) FROM V WHERE id = :messageId",
    "IC1": (
        "SELECT id, firstName, lastName FROM (SELECT EXPAND(out('KNOWS')) "
        "FROM Person WHERE id = :personId) WHERE firstName = :firstName LIMIT 20"
    ),
    "IC3": (
        "SELECT id, content FROM (SELECT EXPAND(in('HAS_CREATOR')) FROM "
        "(SELECT EXPAND(out('KNOWS')) FROM Person WHERE id = :personId)) LIMIT 20"
    ),
    "IC5": (
        "SELECT id, title FROM (SELECT EXPAND(in('HAS_MEMBER')) FROM "
        "(SELECT EXPAND(out('KNOWS')) FROM Person WHERE id = :personId)) LIMIT 20"
    ),
    "IC6": (
        "SELECT id, name FROM (SELECT EXPAND(out('HAS_TAG_POST')) FROM "
        "(SELECT EXPAND(in('HAS_CREATOR')) FROM (SELECT EXPAND(out('KNOWS')) "
        "FROM Person WHERE id = :personId))) LIMIT 20"
    ),
    "IU1": (
        "INSERT INTO Person SET id = :personId, firstName = :firstName, "
        "lastName = :lastName, gender = :gender, birthday = :birthday, "
        "creationDate = :creationDate, locationIP = :locationIP, "
        "browserUsed = :browserUsed"
    ),
    "IU2": (
        "CREATE EDGE LIKES_POST FROM (SELECT FROM Person WHERE id = :personId) "
        "TO (SELECT FROM Post WHERE id = :postId) "
        "SET creationDate = :creationDate"
    ),
    "IU6": (
        "INSERT INTO Comment SET id = :commentId, creationDate = :creationDate, "
        "locationIP = :locationIP, browserUsed = :browserUsed, "
        "content = :content, length = :length"
    ),
}


_BY_SYSTEM: dict[str, dict[str, str]] = {
    "nebulagraph": NEBULAGRAPH,
    "arangodb": ARANGODB,
    "dgraph": DGRAPH,
    "memgraph": MEMGRAPH,
    "orientdb": ORIENTDB,
}


def query_template_for(system: str, ref_id: str) -> str | None:
    return _BY_SYSTEM.get(system, {}).get(ref_id)
