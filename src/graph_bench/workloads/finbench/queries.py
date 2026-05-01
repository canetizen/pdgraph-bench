# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench query catalog (subset SR1..SR6 + W1..W3) per SUT.

Edge labels match `data/loaders/ldbc_finbench_*.py` and
`graph_bench.workloads.finbench.schema`. SR5 uses both
`PERSON_OWN_ACCOUNT` and `COMPANY_OWN_ACCOUNT` because the dataset has both
ownership families.
"""

from __future__ import annotations


# --------------------------------------------------------------------- nGQL
NEBULAGRAPH: dict[str, str] = {
    # SR1 — account profile.
    "SR1": (
        "USE {space}; FETCH PROP ON `Account` {accountId} "
        "YIELD properties(vertex) AS props"
    ),
    # SR2 — outbound transfers from account (limit 100).
    "SR2": (
        "USE {space}; "
        "GO FROM {accountId} OVER `TRANSFER` YIELD dst(edge) AS dst, "
        "properties(edge).amount AS amount LIMIT 100"
    ),
    # SR3 — inbound transfers to account.
    "SR3": (
        "USE {space}; "
        "GO FROM {accountId} OVER `TRANSFER` REVERSELY YIELD src(edge) AS src, "
        "properties(edge).amount AS amount LIMIT 100"
    ),
    # SR4 — 2-hop downstream account ids.
    "SR4": (
        "USE {space}; "
        "GO 2 STEPS FROM {accountId} OVER `TRANSFER` "
        "YIELD DISTINCT dst(edge) AS dst LIMIT 100"
    ),
    # SR5 — owning person/company.
    "SR5": (
        "USE {space}; "
        "GO FROM {accountId} OVER `PERSON_OWN_ACCOUNT`,`COMPANY_OWN_ACCOUNT` REVERSELY "
        "YIELD src(edge) AS owner"
    ),
    # SR6 — outstanding loans deposited into the account.
    "SR6": (
        "USE {space}; "
        "GO FROM {accountId} OVER `DEPOSIT` REVERSELY YIELD src(edge) AS loan"
    ),
    # W1 — register a transfer.
    "W1": (
        "USE {space}; "
        "INSERT EDGE `TRANSFER`(amount, createTime, orderNum, comment, payType, goodsType) "
        "VALUES {srcId}->{dstId}:({amount}, {createTime}, {orderNum}, '{comment}', '{payType}', '{goodsType}')"
    ),
    # W2 — register a deposit (`Loan` -> `Account`).
    "W2": (
        "USE {space}; "
        "INSERT EDGE `DEPOSIT`(amount, createTime) "
        "VALUES {loanId}->{accountId}:({amount}, {createTime})"
    ),
    # W3 — register a withdraw.
    "W3": (
        "USE {space}; "
        "INSERT EDGE `WITHDRAW`(amount, createTime) "
        "VALUES {srcId}->{dstId}:({amount}, {createTime})"
    ),
}


# --------------------------------------------------------------------- AQL
ARANGODB: dict[str, str] = {
    "SR1": "FOR a IN Account FILTER a._key == @accountId LIMIT 1 RETURN a",
    "SR2": (
        "FOR a IN Account FILTER a._key == @accountId "
        "  FOR dst, e IN 1..1 OUTBOUND a TRANSFER LIMIT 100 "
        "  RETURN {dst: dst._key, amount: e.amount}"
    ),
    "SR3": (
        "FOR a IN Account FILTER a._key == @accountId "
        "  FOR src, e IN 1..1 INBOUND a TRANSFER LIMIT 100 "
        "  RETURN {src: src._key, amount: e.amount}"
    ),
    "SR4": (
        "FOR a IN Account FILTER a._key == @accountId "
        "  FOR dst IN 2..2 OUTBOUND a TRANSFER LIMIT 100 "
        "  RETURN DISTINCT dst._key"
    ),
    "SR5": (
        "FOR a IN Account FILTER a._key == @accountId "
        "  FOR owner IN 1..1 INBOUND a PERSON_OWN_ACCOUNT, COMPANY_OWN_ACCOUNT "
        "  RETURN owner._key"
    ),
    "SR6": (
        "FOR a IN Account FILTER a._key == @accountId "
        "  FOR loan IN 1..1 INBOUND a DEPOSIT RETURN loan._key"
    ),
    "W1": (
        "INSERT { _from: CONCAT('Account/', TO_STRING(@srcId)), "
        "         _to: CONCAT('Account/', TO_STRING(@dstId)), "
        "         amount: @amount, createTime: @createTime, orderNum: @orderNum, "
        "         comment: @comment, payType: @payType, goodsType: @goodsType "
        "       } INTO TRANSFER"
    ),
    "W2": (
        "INSERT { _from: CONCAT('Loan/', TO_STRING(@loanId)), "
        "         _to: CONCAT('Account/', TO_STRING(@accountId)), "
        "         amount: @amount, createTime: @createTime } INTO DEPOSIT"
    ),
    "W3": (
        "INSERT { _from: CONCAT('Account/', TO_STRING(@srcId)), "
        "         _to: CONCAT('Account/', TO_STRING(@dstId)), "
        "         amount: @amount, createTime: @createTime } INTO WITHDRAW"
    ),
}


# --------------------------------------------------------------------- DQL
DGRAPH: dict[str, str] = {
    "SR1": (
        "query sr1($accountId:int) { "
        "  a(func: eq(id, $accountId)) @filter(type(Account)) { "
        "    type isBlocked createTime nickname accountLevel "
        "  } "
        "}"
    ),
    "SR2": (
        "query sr2($accountId:int) { "
        "  a(func: eq(id, $accountId)) { "
        "    TRANSFER (first: 100) @facets(amount) { id } "
        "  } "
        "}"
    ),
    "SR3": (
        "query sr3($accountId:int) { "
        "  a(func: eq(id, $accountId)) { "
        "    ~TRANSFER (first: 100) @facets(amount) { id } "
        "  } "
        "}"
    ),
    "SR4": (
        "query sr4($accountId:int) { "
        "  a(func: eq(id, $accountId)) { "
        "    TRANSFER { TRANSFER (first: 100) { id } } "
        "  } "
        "}"
    ),
    "SR5": (
        "query sr5($accountId:int) { "
        "  a(func: eq(id, $accountId)) { ~PERSON_OWN_ACCOUNT { id } "
        "                                ~COMPANY_OWN_ACCOUNT { id } } "
        "}"
    ),
    "SR6": (
        "query sr6($accountId:int) { "
        "  a(func: eq(id, $accountId)) { ~DEPOSIT { id } } "
        "}"
    ),
    "W1": (
        "{ set { uid(src) <TRANSFER> uid(dst) (amount=$amount, createTime=$createTime) . }}"
    ),
    "W2": (
        "{ set { uid(loan) <DEPOSIT> uid(acc) (amount=$amount, createTime=$createTime) . }}"
    ),
    "W3": (
        "{ set { uid(src) <WITHDRAW> uid(dst) (amount=$amount, createTime=$createTime) . }}"
    ),
}


_BY_SYSTEM: dict[str, dict[str, str]] = {
    "nebulagraph": NEBULAGRAPH,
    "arangodb": ARANGODB,
    "dgraph": DGRAPH,
}


def query_template_for(system: str, ref_id: str) -> str | None:
    return _BY_SYSTEM.get(system, {}).get(ref_id)
