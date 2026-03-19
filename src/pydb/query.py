"""Query model -- structured representations of database questions.

A query is like writing down your question on a slip of paper before
handing it to the librarian. It says exactly what you want: which table,
which columns, which rows match, what order, and how many.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pydb.record import Record, Value


class Operator(StrEnum):
    """Comparison operators for WHERE conditions."""

    EQ = "="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="


class SortDirection(StrEnum):
    """Sort direction for ORDER BY."""

    ASC = "ASC"
    DESC = "DESC"


@dataclass(frozen=True, slots=True)
class Condition:
    """A single filter condition: column, operator, value.

    Example: ``Condition("power", Operator.GT, 50)`` means "power > 50".

    Args:
        column: The column name to test.
        operator: The comparison operator.
        value: The value to compare against.

    """

    column: str
    operator: Operator
    value: Value

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies this condition.

        Args:
            record: The record to test.

        Returns:
            True if the record matches the condition.

        """
        record_value: Any = record[self.column]
        target: Any = self.value
        match self.operator:
            case Operator.EQ:
                return record_value == target
            case Operator.NE:
                return record_value != target
            case Operator.GT:
                return record_value > target
            case Operator.GE:
                return record_value >= target
            case Operator.LT:
                return record_value < target
            case Operator.LE:
                return record_value <= target
        msg = f"Unknown operator: {self.operator}"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class And:
    """Combine two conditions with AND logic.

    Both sides must be true for the record to match.

    Args:
        left: The first condition.
        right: The second condition.

    """

    left: Condition | And | Or
    right: Condition | And | Or

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies both conditions."""
        return self.left.matches(record) and self.right.matches(record)


@dataclass(frozen=True, slots=True)
class Or:
    """Combine two conditions with OR logic.

    At least one side must be true for the record to match.

    Args:
        left: The first condition.
        right: The second condition.

    """

    left: Condition | And | Or
    right: Condition | And | Or

    def matches(self, record: Record) -> bool:
        """Check whether a record satisfies at least one condition."""
        return self.left.matches(record) or self.right.matches(record)


# Union type for any filter expression.
WhereClause = Condition | And | Or


@dataclass(frozen=True, slots=True)
class OrderBy:
    """Specify result ordering.

    Args:
        column: The column to sort by.
        direction: ASC (default) or DESC.

    """

    column: str
    direction: SortDirection = SortDirection.ASC


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
    join: JoinClause | None = None
    where: WhereClause | None = None
    order_by: OrderBy | None = None
    limit: int | None = None
