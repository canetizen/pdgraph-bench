# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 schema, aligned with the official Datagen output.

Authoritative source: `ldbc/datagen-standalone:0.5.1-2.12_spark3.2`, run with
`--mode interactive --format csv`. The generator emits the
``composite-merged-fk`` layout, which carries each entity in a per-table
Spark-partitioned subdirectory under
``graphs/csv/<mode>/composite-merged-fk/{dynamic,static}/<Entity>/part-*.csv``.

Two notable shape choices in that layout that the schema below reflects:

1. **Foreign keys are embedded in vertex CSVs** rather than being separate edge
   files. For example, ``Person.LocationCityId`` carries the IS_LOCATED_IN edge
   from each Person to a Place; ``Comment.ParentPostId`` carries REPLY_OF_POST.
   The schema declares these as ``ForeignKey`` items on the owning vertex; the
   loader synthesises the corresponding edges as a second pass over the same
   CSVs.

2. **Some "vertex types" in the conceptual SNB schema are merged.** The
   datagen produces a single ``Organisation`` table with a discriminator
   ``type ∈ {university, company}``; ``Person_studyAt_University.csv`` and
   ``Person_workAt_Company.csv`` reference Organisation ids of the matching
   type. ``Place`` similarly merges Continent/Country/City through a
   ``type`` field plus a self-referential ``PartOfPlaceId`` FK.

Logical edge labels (HAS_CREATOR, REPLY_OF_POST, CONTAINER_OF, KNOWS,
HAS_TAG_POST, ...) are kept as the canonical names used by per-driver query
templates — the loader is responsible for materialising them in the SUT.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Property:
    """A scalar column on a vertex or edge CSV.

    `name` is both the CSV header and the SUT-side property name.
    """

    name: str
    dtype: str  # id | int | long | string | datetime | date | enum


@dataclass(frozen=True, slots=True)
class ForeignKey:
    """An FK column embedded in a vertex CSV that synthesises one outgoing edge.

    `direction = "out"` means the edge runs from the vertex carrying the FK to
    the FK target (e.g., Person → Place via Person.LocationCityId).
    `direction = "in"` means the FK column points to the *source* of the edge
    rather than its target (e.g., Post.ContainerForumId carries the Forum → Post
    CONTAINER_OF edge: the FK target Forum is the source).
    """

    column: str
    edge_label: str
    target_label: str
    direction: str = "out"  # "out" | "in"
    nullable: bool = True


@dataclass(frozen=True, slots=True)
class VertexType:
    """One vertex CSV in the LDBC SNB Datagen output.

    `csv_subdir` is the path under
    ``graphs/csv/<mode>/composite-merged-fk/`` (e.g. ``dynamic/Person`` or
    ``static/Tag``); `csv_filename` is kept for the few callers that still
    treat the entity as a flat file (loader-side helpers glob ``part-*.csv``
    inside `csv_subdir`).
    """

    name: str
    properties: tuple[Property, ...]
    foreign_keys: tuple[ForeignKey, ...]
    csv_subdir: str

    @property
    def csv_filename(self) -> str:
        """Backwards-compatible shim: the legacy "<Entity>.csv" name."""
        return f"{self.name}.csv"


@dataclass(frozen=True, slots=True)
class EdgeType:
    """A standalone edge CSV in the LDBC SNB Datagen output."""

    name: str
    src_label: str
    dst_label: str
    src_column: str
    dst_column: str
    properties: tuple[Property, ...]
    csv_subdir: str

    @property
    def csv_filename(self) -> str:
        return f"{self.name}.csv"

    # Aliases used by older code paths that referred to vertex labels as `src`/`dst`.
    @property
    def src(self) -> str:
        return self.src_label

    @property
    def dst(self) -> str:
        return self.dst_label


# --------------------------------------------------------------------- vertices

VERTICES: tuple[VertexType, ...] = (
    VertexType(
        name="Person",
        csv_subdir="dynamic/Person",
        properties=(
            Property("creationDate", "datetime"),
            Property("firstName", "string"),
            Property("lastName", "string"),
            Property("gender", "string"),
            Property("birthday", "date"),
            Property("locationIP", "string"),
            Property("browserUsed", "string"),
            Property("language", "string"),
            Property("email", "string"),
        ),
        foreign_keys=(
            ForeignKey("LocationCityId", "IS_LOCATED_IN_PERSON", "Place"),
        ),
    ),
    VertexType(
        name="Forum",
        csv_subdir="dynamic/Forum",
        properties=(
            Property("creationDate", "datetime"),
            Property("title", "string"),
        ),
        foreign_keys=(
            ForeignKey("ModeratorPersonId", "HAS_MODERATOR", "Person"),
        ),
    ),
    VertexType(
        name="Post",
        csv_subdir="dynamic/Post",
        properties=(
            Property("creationDate", "datetime"),
            Property("imageFile", "string"),
            Property("locationIP", "string"),
            Property("browserUsed", "string"),
            Property("language", "string"),
            Property("content", "string"),
            Property("length", "int"),
        ),
        foreign_keys=(
            ForeignKey("CreatorPersonId", "HAS_CREATOR", "Person"),
            ForeignKey("ContainerForumId", "CONTAINER_OF", "Forum", direction="in"),
            ForeignKey("LocationCountryId", "IS_LOCATED_IN_POST", "Place"),
        ),
    ),
    VertexType(
        name="Comment",
        csv_subdir="dynamic/Comment",
        properties=(
            Property("creationDate", "datetime"),
            Property("locationIP", "string"),
            Property("browserUsed", "string"),
            Property("content", "string"),
            Property("length", "int"),
        ),
        foreign_keys=(
            ForeignKey("CreatorPersonId", "HAS_CREATOR_C", "Person"),
            ForeignKey("LocationCountryId", "IS_LOCATED_IN_COMMENT", "Place"),
            ForeignKey("ParentPostId", "REPLY_OF_POST", "Post"),
            ForeignKey("ParentCommentId", "REPLY_OF_COMMENT", "Comment"),
        ),
    ),
    VertexType(
        name="Organisation",
        csv_subdir="static/Organisation",
        properties=(
            Property("type", "string"),
            Property("name", "string"),
            Property("url", "string"),
        ),
        foreign_keys=(
            ForeignKey("LocationPlaceId", "IS_LOCATED_IN_ORG", "Place"),
        ),
    ),
    VertexType(
        name="Place",
        csv_subdir="static/Place",
        properties=(
            Property("name", "string"),
            Property("url", "string"),
            Property("type", "string"),
        ),
        foreign_keys=(
            ForeignKey("PartOfPlaceId", "IS_PART_OF_PLACE", "Place"),
        ),
    ),
    VertexType(
        name="Tag",
        csv_subdir="static/Tag",
        properties=(
            Property("name", "string"),
            Property("url", "string"),
        ),
        foreign_keys=(
            ForeignKey("TypeTagClassId", "HAS_TYPE", "TagClass"),
        ),
    ),
    VertexType(
        name="TagClass",
        csv_subdir="static/TagClass",
        properties=(
            Property("name", "string"),
            Property("url", "string"),
        ),
        foreign_keys=(
            ForeignKey("SubclassOfTagClassId", "IS_SUBCLASS_OF", "TagClass"),
        ),
    ),
)


# --------------------------------------------------------------------- edges
# Edges that live in their own CSVs. Edges synthesised from vertex foreign keys
# (CONTAINER_OF, HAS_CREATOR, REPLY_OF_*, IS_LOCATED_IN_*, HAS_TYPE,
# IS_SUBCLASS_OF, IS_PART_OF_PLACE, HAS_MODERATOR) are NOT listed here; they
# are recovered by the loader from `VertexType.foreign_keys`.

EDGES: tuple[EdgeType, ...] = (
    EdgeType(
        name="KNOWS",
        src_label="Person", dst_label="Person",
        src_column="Person1Id", dst_column="Person2Id",
        csv_subdir="dynamic/Person_knows_Person",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="HAS_INTEREST",
        src_label="Person", dst_label="Tag",
        src_column="PersonId", dst_column="TagId",
        csv_subdir="dynamic/Person_hasInterest_Tag",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="STUDY_AT",
        src_label="Person", dst_label="Organisation",
        src_column="PersonId", dst_column="UniversityId",
        csv_subdir="dynamic/Person_studyAt_University",
        properties=(Property("creationDate", "datetime"), Property("classYear", "int")),
    ),
    EdgeType(
        name="WORK_AT",
        src_label="Person", dst_label="Organisation",
        src_column="PersonId", dst_column="CompanyId",
        csv_subdir="dynamic/Person_workAt_Company",
        properties=(Property("creationDate", "datetime"), Property("workFrom", "int")),
    ),
    EdgeType(
        name="LIKES_POST",
        src_label="Person", dst_label="Post",
        src_column="PersonId", dst_column="PostId",
        csv_subdir="dynamic/Person_likes_Post",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="LIKES_COMMENT",
        src_label="Person", dst_label="Comment",
        src_column="PersonId", dst_column="CommentId",
        csv_subdir="dynamic/Person_likes_Comment",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="HAS_MEMBER",
        src_label="Forum", dst_label="Person",
        src_column="ForumId", dst_column="PersonId",
        csv_subdir="dynamic/Forum_hasMember_Person",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="HAS_TAG_FORUM",
        src_label="Forum", dst_label="Tag",
        src_column="ForumId", dst_column="TagId",
        csv_subdir="dynamic/Forum_hasTag_Tag",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="HAS_TAG_POST",
        src_label="Post", dst_label="Tag",
        src_column="PostId", dst_column="TagId",
        csv_subdir="dynamic/Post_hasTag_Tag",
        properties=(Property("creationDate", "datetime"),),
    ),
    EdgeType(
        name="HAS_TAG_COMMENT",
        src_label="Comment", dst_label="Tag",
        src_column="CommentId", dst_column="TagId",
        csv_subdir="dynamic/Comment_hasTag_Tag",
        properties=(Property("creationDate", "datetime"),),
    ),
)


# --------------------------------------------------------------------- helpers

def fk_edge_labels() -> tuple[str, ...]:
    """All edge labels synthesised from vertex foreign keys."""
    return tuple(fk.edge_label for v in VERTICES for fk in v.foreign_keys)


def all_edge_labels() -> tuple[str, ...]:
    """All edge labels (standalone CSVs + FK-synthesised)."""
    return tuple(e.name for e in EDGES) + fk_edge_labels()


def all_csv_filenames() -> tuple[str, ...]:
    """Backwards-compatible: the legacy ``Entity.csv`` filenames.

    Code that needs to glob the real datagen output should call
    `csv_glob(dataset_dir, vertex_or_edge.csv_subdir)` instead.
    """
    return tuple(v.csv_filename for v in VERTICES) + tuple(e.csv_filename for e in EDGES)
