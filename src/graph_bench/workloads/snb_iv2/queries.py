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
    "IC1": (
        "query ic1($personId:int, $firstName:string) { "
        "  p(func: eq(id, $personId)) { "
        "    friends as KNOWS { } "
        "    fof as KNOWS { KNOWS { } } "
        "  } "
        "  result(func: uid(friends, fof)) @filter(eq(firstName, $firstName)) "
        "    (first: 20) { id firstName lastName } "
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
    "IU1": (
        "{ set { "
        "  uid(p) <id> \"$personId\" . "
        "  uid(p) <firstName> \"$firstName\" . "
        "  uid(p) <lastName> \"$lastName\" . "
        "  uid(p) <gender> \"$gender\" . "
        "  uid(p) <birthday> \"$birthday\" . "
        "  uid(p) <creationDate> \"$creationDate\" . "
        "  uid(p) <locationIP> \"$locationIP\" . "
        "  uid(p) <browserUsed> \"$browserUsed\" . "
        "  uid(p) <dgraph.type> \"Person\" . "
        "}}"
    ),
    "IU2": (
        "{ set { uid(src) <LIKES_POST> uid(dst) (creationDate=$creationDate) . }}"
    ),
    "IU6": (
        "{ set { "
        "  uid(c) <id> \"$commentId\" . "
        "  uid(c) <creationDate> \"$creationDate\" . "
        "  uid(c) <locationIP> \"$locationIP\" . "
        "  uid(c) <browserUsed> \"$browserUsed\" . "
        "  uid(c) <content> \"$content\" . "
        "  uid(c) <length> \"$length\" . "
        "  uid(c) <dgraph.type> \"Comment\" . "
        "}}"
    ),
}


_BY_SYSTEM: dict[str, dict[str, str]] = {
    "nebulagraph": NEBULAGRAPH,
    "arangodb": ARANGODB,
    "dgraph": DGRAPH,
}


def query_template_for(system: str, ref_id: str) -> str | None:
    return _BY_SYSTEM.get(system, {}).get(ref_id)
