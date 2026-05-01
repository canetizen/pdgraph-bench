# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench schema, aligned with the upstream Datagen output.

Authoritative source: ``ldbc/ldbc_finbench_datagen`` at tag ``0.1.0``,
packaged into ``graph-bench/finbench-datagen`` via
``data/generators/Dockerfile.finbench``. Output layout:

    <dataset>/raw/<entity>/part-*.csv

with ``<entity>`` in lower-camelCase (``account``, ``person``,
``personOwnAccount``, ``transfer``, ``loantransfer``, ...).

Unlike the SNB Iv2 schema, FinBench vertex CSVs do NOT embed foreign keys —
every relationship lives in a dedicated edge CSV. Loaders therefore iterate
vertices once and edges once, with no FK-synthesis pass.

Soft-delete columns (``deleteTime``, ``isExplicitDeleted``) appear on several
edge CSVs; the loader records them as edge properties but does not enforce the
deletion semantics (those belong to the temporal extension of FinBench, which
we do not benchmark).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Property:
    name: str
    dtype: str  # id | int | bigint | string | datetime | float


@dataclass(frozen=True, slots=True)
class VertexType:
    name: str
    properties: tuple[Property, ...]
    csv_subdir: str

    @property
    def csv_filename(self) -> str:
        return f"{self.name}.csv"


@dataclass(frozen=True, slots=True)
class EdgeType:
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

    @property
    def src(self) -> str:
        return self.src_label

    @property
    def dst(self) -> str:
        return self.dst_label


VERTICES: tuple[VertexType, ...] = (
    VertexType(
        name="Person",
        csv_subdir="person",
        properties=(
            Property("createTime", "datetime"),
            Property("name", "string"),
            Property("isBlocked", "string"),
            Property("gender", "string"),
            Property("birthday", "datetime"),
            Property("country", "string"),
            Property("city", "string"),
        ),
    ),
    VertexType(
        name="Company",
        csv_subdir="company",
        properties=(
            Property("createTime", "datetime"),
            Property("name", "string"),
            Property("isBlocked", "string"),
            Property("country", "string"),
            Property("city", "string"),
            Property("business", "string"),
            Property("description", "string"),
            Property("url", "string"),
        ),
    ),
    VertexType(
        name="Account",
        csv_subdir="account",
        properties=(
            Property("createTime", "datetime"),
            Property("isBlocked", "string"),
            Property("type", "string"),
            Property("nickname", "string"),
            Property("phonenum", "string"),
            Property("email", "string"),
            Property("freqLoginType", "string"),
            Property("lastLoginTime", "datetime"),
            Property("accountLevel", "string"),
        ),
    ),
    VertexType(
        name="Loan",
        csv_subdir="loan",
        properties=(
            Property("createTime", "datetime"),
            Property("loanAmount", "float"),
            Property("balance", "float"),
            Property("usage", "string"),
            Property("interestRate", "float"),
        ),
    ),
    VertexType(
        name="Medium",
        csv_subdir="medium",
        properties=(
            Property("createTime", "datetime"),
            Property("type", "string"),
            Property("isBlocked", "string"),
            Property("lastLogin", "datetime"),
            Property("riskLevel", "string"),
        ),
    ),
)


_TRANSFER_PROPS = (
    Property("createTime", "datetime"),
    Property("amount", "float"),
    Property("orderNum", "bigint"),
    Property("comment", "string"),
    Property("payType", "string"),
    Property("goodsType", "string"),
)


EDGES: tuple[EdgeType, ...] = (
    EdgeType(
        name="TRANSFER",
        src_label="Account", dst_label="Account",
        src_column="fromId", dst_column="toId",
        csv_subdir="transfer",
        properties=_TRANSFER_PROPS,
    ),
    EdgeType(
        name="LOAN_TRANSFER",
        src_label="Account", dst_label="Account",
        src_column="fromId", dst_column="toId",
        csv_subdir="loantransfer",
        properties=_TRANSFER_PROPS,
    ),
    EdgeType(
        name="WITHDRAW",
        src_label="Account", dst_label="Account",
        src_column="fromId", dst_column="toId",
        csv_subdir="withdraw",
        properties=(Property("createTime", "datetime"), Property("amount", "float")),
    ),
    EdgeType(
        name="DEPOSIT",
        src_label="Loan", dst_label="Account",
        src_column="loanId", dst_column="accountId",
        csv_subdir="deposit",
        properties=(Property("createTime", "datetime"), Property("amount", "float")),
    ),
    EdgeType(
        name="REPAY",
        src_label="Account", dst_label="Loan",
        src_column="accountId", dst_column="loanId",
        csv_subdir="repay",
        properties=(Property("createTime", "datetime"), Property("amount", "float")),
    ),
    EdgeType(
        name="SIGN_IN",
        src_label="Medium", dst_label="Account",
        src_column="mediumId", dst_column="accountId",
        csv_subdir="signIn",
        properties=(Property("createTime", "datetime"), Property("location", "string")),
    ),
    EdgeType(
        name="PERSON_OWN_ACCOUNT",
        src_label="Person", dst_label="Account",
        src_column="personId", dst_column="accountId",
        csv_subdir="personOwnAccount",
        properties=(Property("createTime", "datetime"),),
    ),
    EdgeType(
        name="COMPANY_OWN_ACCOUNT",
        src_label="Company", dst_label="Account",
        src_column="companyId", dst_column="accountId",
        csv_subdir="companyOwnAccount",
        properties=(Property("createTime", "datetime"),),
    ),
    EdgeType(
        name="PERSON_APPLY_LOAN",
        src_label="Person", dst_label="Loan",
        src_column="personId", dst_column="loanId",
        csv_subdir="personApplyLoan",
        properties=(
            Property("createTime", "datetime"),
            Property("loanAmount", "float"),
            Property("org", "string"),
        ),
    ),
    EdgeType(
        name="COMPANY_APPLY_LOAN",
        src_label="Company", dst_label="Loan",
        src_column="companyId", dst_column="loanId",
        csv_subdir="companyApplyLoan",
        properties=(
            Property("createTime", "datetime"),
            Property("loanAmount", "float"),
            Property("org", "string"),
        ),
    ),
    EdgeType(
        name="PERSON_GUARANTEE",
        src_label="Person", dst_label="Person",
        src_column="fromId", dst_column="toId",
        csv_subdir="personGuarantee",
        properties=(Property("createTime", "datetime"), Property("relation", "string")),
    ),
    EdgeType(
        name="COMPANY_GUARANTEE",
        src_label="Company", dst_label="Company",
        src_column="fromId", dst_column="toId",
        csv_subdir="companyGuarantee",
        properties=(Property("createTime", "datetime"), Property("relation", "string")),
    ),
    EdgeType(
        name="PERSON_INVEST",
        src_label="Person", dst_label="Company",
        src_column="investorId", dst_column="companyId",
        csv_subdir="personInvest",
        properties=(Property("createTime", "datetime"), Property("ratio", "float")),
    ),
    EdgeType(
        name="COMPANY_INVEST",
        src_label="Company", dst_label="Company",
        src_column="investorId", dst_column="companyId",
        csv_subdir="companyInvest",
        properties=(Property("createTime", "datetime"), Property("ratio", "float")),
    ),
)


def all_edge_labels() -> tuple[str, ...]:
    return tuple(e.name for e in EDGES)


def all_csv_filenames() -> tuple[str, ...]:
    return tuple(v.csv_filename for v in VERTICES) + tuple(e.csv_filename for e in EDGES)
