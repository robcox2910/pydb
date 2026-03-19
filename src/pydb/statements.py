"""SQL statement models -- structured representations of write operations.

While a Query represents a SELECT (a question), these classes represent
actions that *change* the database: creating tables, inserting rows,
updating values, and deleting data.
"""

from dataclasses import dataclass

from pydb.query import WhereClause
from pydb.record import Value
from pydb.schema import Column


@dataclass(frozen=True, slots=True)
class CreateTableStatement:
    """Represent a CREATE TABLE statement.

    Args:
        table: The table name to create.
        columns: List of Column definitions with optional constraints.

    """

    table: str
    columns: list[Column]


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


@dataclass(frozen=True, slots=True)
class CreateIndexStatement:
    """Represent a CREATE INDEX statement.

    Args:
        index_name: The name of the index to create.
        table: The table to create the index on.
        column: The column to index.

    """

    index_name: str
    table: str
    column: str


@dataclass(frozen=True, slots=True)
class DropIndexStatement:
    """Represent a DROP INDEX statement.

    Args:
        index_name: The name of the index to drop.
        table: The table the index belongs to.

    """

    index_name: str
    table: str


@dataclass(frozen=True, slots=True)
class ExplainStatement:
    """Represent an EXPLAIN statement wrapping a SELECT query.

    Args:
        query: The SELECT query to explain.

    """

    query: Query


from pydb.query import Query  # noqa: E402  # Avoid circular import.

# Union of all statement types the parser can produce.
Statement = (
    CreateTableStatement
    | DropTableStatement
    | InsertStatement
    | UpdateStatement
    | DeleteStatement
    | CreateIndexStatement
    | DropIndexStatement
    | ExplainStatement
)
