"""Query executor -- the librarian who answers your questions.

The executor takes a query (the question on a slip of paper) and a
database (the collection room), then follows a clear plan:

1. Find the right table.
2. Filter rows that match the WHERE condition.
3. Sort by ORDER BY if requested.
4. Pick only the requested columns (projection).
5. Apply LIMIT to cap the number of results.
"""

from typing import Any

from pydb.database import Database
from pydb.errors import PyDBError
from pydb.query import Condition, Query, SortDirection, WhereClause
from pydb.record import Record, Value


class QueryError(PyDBError):
    """Raise when a query cannot be executed."""


def execute(query: Query, database: Database) -> list[dict[str, Value]]:
    """Execute a query against a database and return the results.

    Args:
        query: The query to execute.
        database: The database to query.

    Returns:
        A list of dictionaries, each representing a matching row with
        only the requested columns.

    Raises:
        QueryError: If the table or columns don't exist.

    """
    # Step 1: Find the table.
    try:
        table = database.get_table(query.table)
    except PyDBError as exc:
        msg = f"Query failed: {exc}"
        raise QueryError(msg) from exc

    # Step 2: Filter rows.
    valid_cols = set(table.schema.column_names)
    if query.where is not None:
        _validate_where_columns(query.where, valid_cols)
        records = table.select(where=query.where.matches)
    else:
        records = table.select()

    # Step 3: Sort.
    if query.order_by is not None:
        col = query.order_by.column
        if col not in valid_cols:
            msg = f"Cannot order by unknown column {col!r}"
            raise QueryError(msg)
        reverse = query.order_by.direction == SortDirection.DESC
        records = _sort_records(records, col, reverse=reverse)

    # Step 4: Limit.
    if query.limit is not None:
        records = records[: query.limit]

    # Step 5: Project columns.
    return _project(records, query.columns, table.schema.column_names)


def _validate_where_columns(clause: WhereClause, valid_columns: set[str]) -> None:
    """Recursively validate that all columns in a WHERE clause exist.

    Args:
        clause: A Condition, And, or Or object.
        valid_columns: The table's column names as a set.

    Raises:
        QueryError: If a column in the clause doesn't exist.

    """
    if isinstance(clause, Condition):
        if clause.column not in valid_columns:
            msg = f"Unknown column {clause.column!r} in WHERE clause"
            raise QueryError(msg)
    else:
        _validate_where_columns(clause.left, valid_columns)
        _validate_where_columns(clause.right, valid_columns)


def _sort_records(records: list[Record], column: str, *, reverse: bool) -> list[Record]:
    """Sort records by a column value.

    Args:
        records: The records to sort.
        column: The column to sort by.
        reverse: True for descending order.

    Returns:
        A new sorted list of records.

    """

    def sort_key(record: Record) -> Any:
        return record[column]

    return sorted(records, key=sort_key, reverse=reverse)


def _project(
    records: list[Record],
    columns: list[str],
    all_columns: list[str],
) -> list[dict[str, Value]]:
    """Extract only the requested columns from each record.

    Args:
        records: The records to project.
        columns: The columns to include (empty = all).
        all_columns: All valid column names from the schema.

    Returns:
        A list of dicts with only the requested columns.

    Raises:
        QueryError: If a requested column doesn't exist.

    """
    if not columns:
        target_cols = all_columns
    else:
        for col in columns:
            if col not in all_columns:
                msg = f"Unknown column {col!r} in SELECT"
                raise QueryError(msg)
        target_cols = columns

    return [{col: record[col] for col in target_cols} for record in records]
