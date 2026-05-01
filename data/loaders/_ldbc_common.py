# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Shared helpers for LDBC-family loaders.

All LDBC datagens emit `|`-delimited CSV files with a known set of property
columns per vertex / edge type. The helpers here parse one row into the right
Python types so each per-SUT loader only has to focus on the wire format of
its target system.
"""

from __future__ import annotations

import datetime as dt
from typing import Protocol


class PropLike(Protocol):
    name: str
    dtype: str


def parse_value(value: str | None, dtype: str) -> object | None:
    """Coerce one CSV cell into the appropriate Python type.

    Returns `None` for empty strings; the per-SUT loader is then free to
    decide whether to omit the property or store a sentinel.
    """
    if value is None or value == "":
        return None
    if dtype in {"int", "id", "bigint"}:
        return int(value)
    if dtype == "float":
        return float(value)
    if dtype == "datetime":
        try:
            return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return int(dt.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").timestamp())
    if dtype == "date":
        return int(dt.datetime.fromisoformat(value).timestamp())
    return value


def coerce_for_string_literal(value: object | None) -> str:
    """Format a coerced Python value as an nGQL/AQL literal substring.

    - `None` becomes `0` for ints and `''` for strings (so DDL with `int` /
      `string` columns never sees NULL).
    - Strings are single-quoted with simple escape handling.
    """
    if value is None:
        return "''"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"
