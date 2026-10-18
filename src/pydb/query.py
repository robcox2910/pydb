"""Query model -- structured representations of database questions.

A query is like writing down your question on a slip of paper before
handing it to the librarian. It says exactly what you want: which table,
which columns, which rows match, what order, and how many.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from pydb.nulls import UNKNOWN, Truth, and3, in3, is_true, not3, or3
from pydb.record import Record, Value


class Operator(StrEnum):
    """Comparison operators for WHERE conditions."""

    EQ = "="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    IN = "IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class RowLike(Protocol):
    """Anything that answers ``row[column_name]`` with a value.

    Both ``Record`` objects (from a single table) and plain dicts (from
    JOINs, views or aggregate results) fit this shape, so a WHERE clause
    can be tested against either.
    """

    def __getitem__(self, key: str, /) -> Value:
        """Return the value stored under *key*."""
        ...


def compare3(left: Any, op: Operator, right: Any) -> Truth:
    """Compare two values in three-valued logic (see ``pydb.nulls``).

    This is the single source of truth for all comparison logic in PyDB.
    ``IS NULL`` and ``IS NOT NULL`` always answer TRUE or FALSE; ``IN``
    follows ``in3``; every other comparison with a NULL on either side is
    UNKNOWN -- even ``NULL = NULL``.

    Args:
        left: The left-hand value.
        op: The comparison operator.
        right: The right-hand value (ignored by IS NULL / IS NOT NULL).

    Returns:
        True, False, or None (UNKNOWN).

    """
    match op:
        case Operator.IS_NULL:
            return left is None
        case Operator.IS_NOT_NULL:
            return left is not None
        case Operator.IN:
            return in3(left, right)
        case _ if left is None or right is None:
            return UNKNOWN
        case _:
            return _compare_known(left, op, right)


def compare_values(left: Any, op: Operator, right: Any) -> bool:
    """Return whether a comparison is TRUE (UNKNOWN counts as not true).

    Args:
        left: The left-hand value.
        op: The comparison operator.
        right: The right-hand value.

    Returns:
        True only if the comparison is TRUE.

    """
    return is_true(compare3(left, op, right))


def _compare_known(left: Any, op: Operator, right: Any) -> bool:
    """Compare two non-NULL values."""
    match op:
        case Operator.EQ:
            return left == right
        case Operator.NE:
            return left != right
        case Operator.GT:
            return left > right
        case Operator.GE:
            return left >= right
        case Operator.LT:
            return left < right
        case Operator.LE:
            return left <= right
        case _:
            msg = f"Unknown operator: {op}"
            raise ValueError(msg)


class SortDirection(StrEnum):
    """Sort direction for ORDER BY."""

    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True, slots=True)
class Subquery:
    """Wrap an inner SELECT query used as a value in a WHERE clause.

    The executor evaluates the inner query and uses the result as a
    scalar value (for =, >, etc.) or a list (for IN).

    Args:
        query: The inner SELECT query.

    """

    query: Query


@dataclass(frozen=True, slots=True)
class Condition:
    """A single filter condition: column, operator, value.

    Example: ``Condition("power", Operator.GT, 50)`` means "power > 50".

    Args:
        column: The column name to test.
        operator: The comparison operator.
        value: The value to compare against, or a Subquery.

    """

    column: str
    operator: Operator
    value: Value | Subquery | list[Value]

    def evaluate(self, row: RowLike) -> Truth:
        """Return TRUE, FALSE or UNKNOWN for *row*."""
        return compare3(row[self.column], self.operator, self.value)

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies this condition.

        Args:
            record: The record to test.

        Returns:
            True only if the condition is TRUE (not UNKNOWN) for the record.

        """
        return is_true(self.evaluate(record))


@dataclass(frozen=True, slots=True)
class And:
    """Combine two conditions with AND logic.

    Both sides must be true for the record to match.

    Args:
        left: The first condition.
        right: The second condition.

    """

    left: WhereClause
    right: WhereClause

    def evaluate(self, row: RowLike) -> Truth:
        """Return ``left AND right`` for *row* in three-valued logic."""
        return and3(self.left.evaluate(row), self.right.evaluate(row))

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies both conditions."""
        return is_true(self.evaluate(record))


@dataclass(frozen=True, slots=True)
class Or:
    """Combine two conditions with OR logic.

    At least one side must be true for the record to match.

    Args:
        left: The first condition.
        right: The second condition.

    """

    left: WhereClause
    right: WhereClause

    def evaluate(self, row: RowLike) -> Truth:
        """Return ``left OR right`` for *row* in three-valued logic."""
        return or3(self.left.evaluate(row), self.right.evaluate(row))

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies at least one condition."""
        return is_true(self.evaluate(record))


@dataclass(frozen=True, slots=True)
class Not:
    """Turn a condition around: TRUE becomes FALSE and FALSE becomes TRUE.

    UNKNOWN stays UNKNOWN -- if we don't know whether a box holds 5, we
    don't know whether it doesn't, either.

    Args:
        operand: The condition to negate.

    """

    operand: WhereClause

    def evaluate(self, row: RowLike) -> Truth:
        """Return ``NOT operand`` for *row* in three-valued logic."""
        return not3(self.operand.evaluate(row))

    def matches(self, record: Record) -> bool:
        """Check whether a record makes the operand FALSE."""
        return is_true(self.evaluate(record))


# Union type for any filter expression.
WhereClause = Condition | And | Or | Not


@dataclass(frozen=True, slots=True)
class OrderBy:
    """Specify result ordering.

    Args:
        column: The column to sort by.
        direction: ASC (default) or DESC.

    """

    column: str
    direction: SortDirection = SortDirection.ASC


class AggFunc(StrEnum):
    """Supported aggregate functions."""

    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


@dataclass(frozen=True, slots=True)
class AggregateColumn:
    """An aggregate function call in a SELECT list.

    Args:
        function: The aggregate function (COUNT, SUM, etc.).
        column: The column to aggregate, or "*" for COUNT(*).
        alias: Display name for the result (e.g., "COUNT(*)").

    """

    function: AggFunc
    column: str
    alias: str


@dataclass(frozen=True, slots=True)
class JoinClause:
    """Describe a JOIN between two tables.

    Args:
        table: The table to join with.
        left_column: Column from the left (FROM) table (may use dot notation).
        right_column: Column from the right (JOIN) table (may use dot notation).

    """

    table: str
    left_column: str
    right_column: str


@dataclass(frozen=True)
class Query:
    """A complete database query.

    Describes what to retrieve: which table, which columns, which rows
    match, what order, and how many results.

    Args:
        table: The table name to query.
        columns: Column names to include (empty = all columns).
        join: An optional JOIN clause.
        where: An optional filter condition.
        order_by: An optional sort specification.
        limit: Maximum number of results (None = no limit).

    """

    table: str
    columns: list[str] = field(default_factory=lambda: [])
    aggregates: list[AggregateColumn] = field(default_factory=lambda: [])
    join: JoinClause | None = None
    where: WhereClause | None = None
    group_by: list[str] = field(default_factory=lambda: [])
    having: WhereClause | None = None
    order_by: OrderBy | None = None
    limit: int | None = None
