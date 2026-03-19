"""Query executor -- the librarian who answers your questions.

The executor takes a query or statement and a database, then carries out
the requested operation:

- SELECT: filter → sort → limit → project → return results
- CREATE TABLE: create a new table with the given schema
- DROP TABLE: remove a table
- INSERT: add a new record
- UPDATE: modify matching records
- DELETE: remove matching records
"""

from typing import Any

from pydb.database import Database
from pydb.errors import PyDBError
from pydb.query import Condition, Query, SortDirection, WhereClause
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.statements import (
    CreateTableStatement,
    DeleteStatement,
    DropTableStatement,
    InsertStatement,
    Statement,
    UpdateStatement,
)


class QueryError(PyDBError):
    """Raise when a query or statement cannot be executed."""


# The result of executing any SQL statement.
ExecuteResult = list[dict[str, Value]]


def execute(query_or_stmt: Query | Statement, database: Database) -> ExecuteResult:
    """Execute a SQL query or statement against a database.

    Args:
        query_or_stmt: The parsed SQL to execute.
        database: The database to operate on.

    Returns:
        A list of row dicts (for SELECT), or a single-row result
        with a "result" key describing what happened (for writes).

    Raises:
        QueryError: If the operation fails.

    """
    if isinstance(query_or_stmt, Query):
        return _execute_select(query_or_stmt, database)
    if isinstance(query_or_stmt, CreateTableStatement):
        return _execute_create_table(query_or_stmt, database)
    if isinstance(query_or_stmt, DropTableStatement):
        return _execute_drop_table(query_or_stmt, database)
    if isinstance(query_or_stmt, InsertStatement):
        return _execute_insert(query_or_stmt, database)
    if isinstance(query_or_stmt, UpdateStatement):
        return _execute_update(query_or_stmt, database)
    return _execute_delete(query_or_stmt, database)


def _result_message(message: str) -> ExecuteResult:
    """Create a single-row result with a message."""
    return [{"result": message}]


def _execute_select(query: Query, database: Database) -> ExecuteResult:
    """Execute a SELECT query."""
    try:
        table = database.get_table(query.table)
    except PyDBError as exc:
        msg = f"Query failed: {exc}"
        raise QueryError(msg) from exc

    valid_cols = set(table.schema.column_names)
    if query.where is not None:
        _validate_where_columns(query.where, valid_cols)
        records = table.select(where=query.where.matches)
    else:
        records = table.select()

    if query.order_by is not None:
        col = query.order_by.column
        if col not in valid_cols:
            msg = f"Cannot order by unknown column {col!r}"
            raise QueryError(msg)
        reverse = query.order_by.direction == SortDirection.DESC
        records = _sort_records(records, col, reverse=reverse)

    if query.limit is not None:
        records = records[: query.limit]

    return _project(records, query.columns, table.schema.column_names)


def _execute_create_table(stmt: CreateTableStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE TABLE statement."""
    columns = [Column(name=name, data_type=dt) for name, dt in stmt.columns]
    schema = Schema(columns=columns)
    try:
        database.create_table(stmt.table, schema)
    except PyDBError as exc:
        msg = f"CREATE TABLE failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"Table {stmt.table!r} created")


def _execute_drop_table(stmt: DropTableStatement, database: Database) -> ExecuteResult:
    """Execute a DROP TABLE statement."""
    try:
        database.drop_table(stmt.table)
    except PyDBError as exc:
        msg = f"DROP TABLE failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"Table {stmt.table!r} dropped")


def _execute_insert(stmt: InsertStatement, database: Database) -> ExecuteResult:
    """Execute an INSERT INTO statement."""
    try:
        table = database.get_table(stmt.table)
    except PyDBError as exc:
        msg = f"INSERT failed: {exc}"
        raise QueryError(msg) from exc

    # Build the column-value mapping.
    if stmt.columns:
        if len(stmt.columns) != len(stmt.values):
            msg = (
                f"Column count ({len(stmt.columns)}) doesn't match value count ({len(stmt.values)})"
            )
            raise QueryError(msg)
        values = dict(zip(stmt.columns, stmt.values, strict=True))
    else:
        # No column list -- values must match schema column order.
        schema_cols = table.schema.column_names
        if len(schema_cols) != len(stmt.values):
            msg = f"Expected {len(schema_cols)} values, got {len(stmt.values)}"
            raise QueryError(msg)
        values = dict(zip(schema_cols, stmt.values, strict=True))

    try:
        table.insert(values)
    except PyDBError as exc:
        msg = f"INSERT failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message("1 row inserted")


def _execute_update(stmt: UpdateStatement, database: Database) -> ExecuteResult:
    """Execute an UPDATE statement."""
    try:
        table = database.get_table(stmt.table)
    except PyDBError as exc:
        msg = f"UPDATE failed: {exc}"
        raise QueryError(msg) from exc

    records = table.select(where=stmt.where.matches) if stmt.where is not None else table.select()

    count = 0
    for record in records:
        try:
            table.update(record_id=record.record_id, values=stmt.assignments)
        except PyDBError as exc:
            msg = f"UPDATE failed: {exc}"
            raise QueryError(msg) from exc
        count += 1

    row_word = "row" if count == 1 else "rows"
    return _result_message(f"{count} {row_word} updated")


def _execute_delete(stmt: DeleteStatement, database: Database) -> ExecuteResult:
    """Execute a DELETE FROM statement."""
    try:
        table = database.get_table(stmt.table)
    except PyDBError as exc:
        msg = f"DELETE failed: {exc}"
        raise QueryError(msg) from exc

    records = table.select(where=stmt.where.matches) if stmt.where is not None else table.select()

    count = 0
    for record in records:
        table.delete(record_id=record.record_id)
        count += 1

    row_word = "row" if count == 1 else "rows"
    return _result_message(f"{count} {row_word} deleted")


def _validate_where_columns(clause: WhereClause, valid_columns: set[str]) -> None:
    """Recursively validate that all columns in a WHERE clause exist."""
    if isinstance(clause, Condition):
        if clause.column not in valid_columns:
            msg = f"Unknown column {clause.column!r} in WHERE clause"
            raise QueryError(msg)
    else:
        _validate_where_columns(clause.left, valid_columns)
        _validate_where_columns(clause.right, valid_columns)


def _sort_records(records: list[Record], column: str, *, reverse: bool) -> list[Record]:
    """Sort records by a column value."""

    def sort_key(record: Record) -> Any:
        return record[column]

    return sorted(records, key=sort_key, reverse=reverse)


def _project(
    records: list[Record],
    columns: list[str],
    all_columns: list[str],
) -> list[dict[str, Value]]:
    """Extract only the requested columns from each record."""
    if not columns:
        target_cols = all_columns
    else:
        all_set = set(all_columns)
        for col in columns:
            if col not in all_set:
                msg = f"Unknown column {col!r} in SELECT"
                raise QueryError(msg)
        target_cols = columns

    return [{col: record[col] for col in target_cols} for record in records]
