"""SQL statement models -- structured representations of write operations.

While a Query represents a SELECT (a question), these classes represent
actions that *change* the database: creating tables, inserting rows,
updating values, and deleting data.
"""

from dataclasses import dataclass

from pydb.query import WhereClause
from pydb.record import Value
from pydb.types import DataType


@dataclass(frozen=True, slots=True)
class CreateTableStatement:
    """Represent a CREATE TABLE statement.

    Args:
        table: The table name to create.
        columns: List of (name, data_type) pairs.

    """

    table: str
    columns: list[tuple[str, DataType]]


@dataclass(frozen=True, slots=True)
class DropTableStatement:
    """Represent a DROP TABLE statement.

    Args:
        table: The table name to drop.

    """

    table: str


@dataclass(frozen=True, slots=True)
class InsertStatement:
    """Represent an INSERT INTO ... VALUES statement.

    Args:
        table: The table to insert into.
        columns: Column names (empty = all columns in schema order).
        values: The values to insert.

    """

    table: str
    columns: list[str]
    values: list[Value]


@dataclass(frozen=True, slots=True)
class UpdateStatement:
    """Represent an UPDATE ... SET ... WHERE statement.

    Args:
        table: The table to update.
        assignments: Column-name-to-value mappings.
        where: Optional filter condition.

    """

    table: str
    assignments: dict[str, Value]
    where: WhereClause | None = None


@dataclass(frozen=True, slots=True)
class DeleteStatement:
    """Represent a DELETE FROM ... WHERE statement.

    Args:
        table: The table to delete from.
        where: Optional filter condition (None = delete all rows).

    """

    table: str
    where: WhereClause | None = None


# Union of all statement types the parser can produce.
Statement = (
    CreateTableStatement | DropTableStatement | InsertStatement | UpdateStatement | DeleteStatement
)
