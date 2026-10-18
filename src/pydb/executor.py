"""Query executor -- the librarian who answers your questions.

Think of this module like a librarian at a desk.  You hand them a slip of
paper (a parsed query or statement) and tell them which library to look in
(the database).  The librarian reads the slip, walks to the right shelf,
and comes back with an answer.

The executor takes a query or statement and a database, then carries out
the requested operation:

- SELECT: filter -> sort -> limit -> project -> return results
- CREATE TABLE: create a new table with the given schema
- DROP TABLE: remove a table
- INSERT: add a new record
- UPDATE: modify matching records
- DELETE: remove matching records
"""

from collections.abc import Sequence
from typing import Any

from pydb.aggregates import compute_aggregate, group_records
from pydb.database import Database
from pydb.errors import PyDBError, QueryError
from pydb.nulls import is_true, null_sort_key
from pydb.planner import plan_query
from pydb.query import (
    And,
    Condition,
    Not,
    Operator,
    Or,
    Query,
    RowLike,
    SortDirection,
    Subquery,
    WhereClause,
    compare3,
)
from pydb.record import Record, Value
from pydb.schema import Schema
from pydb.sql_literals import mismatched_date_comparison
from pydb.statement_commit import one_commit
from pydb.statements import (
    CreateIndexStatement,
    CreateTableStatement,
    CreateViewStatement,
    DeleteStatement,
    DropIndexStatement,
    DropTableStatement,
    DropViewStatement,
    ExplainStatement,
    InsertStatement,
    Statement,
    UpdateStatement,
)
from pydb.table import Table

__all__ = ["ExecuteResult", "QueryError", "execute"]

# The result of executing any SQL statement.
ExecuteResult = list[dict[str, Value]]


# ---------------------------------------------------------------------------
# Shared helpers -- small tools every handler can borrow
# ---------------------------------------------------------------------------


def _get_table(database: Database, table_name: str, operation: str) -> Table:
    """Look up a table in the database, raising a clear error on failure.

    Think of this like asking the librarian to fetch a specific book.
    If the book doesn't exist, you get a helpful error instead of a crash.

    Args:
        database: The database to search.
        table_name: The name of the table to find.
        operation: A label like "INSERT" for the error message.

    Raises:
        QueryError: If the table does not exist.

    """
    try:
        return database.get_table(table_name)
    except PyDBError as exc:
        msg = f"{operation} failed: {exc}"
        raise QueryError(msg) from exc


def _wrap_error(operation: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Run *fn* and re-raise any PyDBError as a QueryError.

    Like a safety net: if something goes wrong inside the library,
    this catches it and gives you a friendlier error message.

    Args:
        operation: A label like "CREATE TABLE" for the error message.
        fn: The function to call.
        *args: Positional arguments forwarded to *fn*.
        **kwargs: Keyword arguments forwarded to *fn*.

    Raises:
        QueryError: If *fn* raises a PyDBError.

    """
    try:
        return fn(*args, **kwargs)
    except PyDBError as exc:
        msg = f"{operation} failed: {exc}"
        raise QueryError(msg) from exc


def _result_message(message: str) -> ExecuteResult:
    """Create a single-row result with a message."""
    return [{"result": message}]


def _apply_order_by(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Sort result rows by the query's ORDER BY clause (if any).

    Like sorting a stack of index cards alphabetically or by number --
    this helper does the sorting step so every handler doesn't have to
    repeat the same code. NULLs come first ascending and last descending.
    """
    if query.order_by is None:
        return rows
    col = query.order_by.column
    reverse = query.order_by.direction == SortDirection.DESC
    return sorted(rows, key=lambda r: null_sort_key(r[col]), reverse=reverse)


def _apply_limit(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Trim result rows to the query's LIMIT count (if any).

    Like only taking the top N cards from a sorted pile.
    """
    if query.limit is None:
        return rows
    return rows[: query.limit]


def _apply_where_on_dicts(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Filter dict rows using the query's WHERE clause (if any).

    Works on plain dicts (e.g. from JOINs or views) rather than Record
    objects.
    """
    if query.where is None:
        return rows
    return [row for row in rows if _matches_dict(query.where, row)]


def _apply_column_projection(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Keep only the columns listed in the SELECT clause (if any).

    Like highlighting certain columns on a spreadsheet and hiding the rest.
    """
    if not query.columns:
        return rows
    return [{c: row[c] for c in query.columns} for row in rows]


def _apply_post_processing(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Apply the standard WHERE -> ORDER BY -> LIMIT -> PROJECT pipeline.

    Many query paths need the same four steps applied to dict rows.
    This helper chains them together in the correct order so we only
    write the logic once.
    """
    result = _apply_where_on_dicts(rows, query)
    result = _apply_order_by(result, query)
    result = _apply_limit(result, query)
    return _apply_column_projection(result, query)


def _finalize_dict_rows(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Finish a query whose rows are plain dicts (from a JOIN or a view).

    If the query asks for aggregates (like ``COUNT(*)``), we filter with
    WHERE first and then summarise. Otherwise we run the ordinary
    WHERE -> ORDER BY -> LIMIT -> PROJECT pipeline.
    """
    if query.aggregates:
        return _execute_aggregate(query, _apply_where_on_dicts(rows, query))
    return _apply_post_processing(rows, query)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def execute(query_or_stmt: Query | Statement, database: Database) -> ExecuteResult:
    """Execute a SQL query or statement against a database.

    This is the front desk of the library -- you hand in your request and
    the right specialist handles it.

    Args:
        query_or_stmt: The parsed SQL to execute.
        database: The database to operate on.

    Returns:
        A list of row dicts (for SELECT), or a single-row result
        with a "result" key describing what happened (for writes).

    Raises:
        QueryError: If the operation fails.

    """
    with database.lock:  # one statement is one turn: no writer runs half-way through it
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
        if isinstance(query_or_stmt, DeleteStatement):
            return _execute_delete(query_or_stmt, database)
        if isinstance(query_or_stmt, CreateIndexStatement):
            return _execute_create_index(query_or_stmt, database)
        if isinstance(query_or_stmt, DropIndexStatement):
            return _execute_drop_index(query_or_stmt, database)
        if isinstance(query_or_stmt, ExplainStatement):
            return _execute_explain(query_or_stmt, database)
        if isinstance(query_or_stmt, CreateViewStatement):
            return _execute_create_view(query_or_stmt, database)
        # Must be DropViewStatement (type narrowing guarantees this).
        return _execute_drop_view(query_or_stmt, database)


# ---------------------------------------------------------------------------
# SELECT execution
# ---------------------------------------------------------------------------


def _execute_select(query: Query, database: Database) -> ExecuteResult:
    """Execute a SELECT query, with optional JOIN and view support."""
    if query.aggregates:
        _check_aggregate_columns(query, _source_columns(query, database))
    # Check if the table name is actually a view.
    view_query = database.get_view(query.table)
    if view_query is not None:
        # Run the view's stored query, then apply outer clauses.
        view_results = execute(view_query, database)
        return _finalize_dict_rows(view_results, query)

    left_table = _get_table(database, query.table, "Query")

    # If there's a JOIN, produce combined rows from both tables.
    if query.join is not None:
        return _execute_select_with_join(query, database, left_table)

    return _execute_simple_select(query, left_table, database)


def _source_columns(query: Query, database: Database) -> list[str]:
    """Return the columns the rows behind *query* will have (before its SELECT list).

    Worked out from the schemas, not from the rows, so it is right even
    when there are no rows at all.
    """
    view_query = database.get_view(query.table)
    if view_query is not None:
        return _output_columns(view_query, database)
    left = _get_table(database, query.table, "Query")
    if query.join is None:
        return left.schema.column_names
    right = _get_table(database, query.join.table, "JOIN")
    return [f"{t.name}.{col}" for t in (left, right) for col in t.schema.column_names]


def _output_columns(query: Query, database: Database) -> list[str]:
    """Return the columns of the rows *query* produces (a view's columns)."""
    if query.aggregates:
        return [*query.group_by, *query.columns, *(agg.alias for agg in query.aggregates)]
    return query.columns or _source_columns(query, database)


def _check_aggregate_columns(query: Query, columns: list[str]) -> None:
    """Raise QueryError if an aggregate names a column the rows will not have."""
    for agg in query.aggregates:
        if agg.column != "*" and agg.column not in columns:
            msg = f"Unknown column {agg.column!r} in {agg.alias}"
            raise QueryError(msg)


def _execute_simple_select(query: Query, table: Table, database: Database) -> ExecuteResult:
    """Execute a SELECT without a JOIN."""
    valid_cols = set(table.schema.column_names)

    where = query.where
    if where is not None:
        # Resolve any subqueries in the WHERE clause before filtering.
        where = _resolve_subqueries(where, database)
        _validate_where_columns(where, valid_cols)
        _check_date_comparisons(where, table)
        records = _select_records(table, where)
    else:
        records = table.select()

    # If there are aggregate functions, delegate to aggregate execution.
    if query.aggregates:
        return _execute_aggregate(query, records)

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


def _select_records(table: Table, where: WhereClause) -> list[Record]:
    """Fetch matching records, using an index when the WHERE clause allows it.

    This is where the query planner's promise comes true: if the WHERE
    clause is a simple ``column = value`` test and that column has an
    index, we jump straight to the matching records (like using the card
    catalog) instead of walking every row (a full table scan).
    """
    if (
        isinstance(where, Condition)
        and where.operator == Operator.EQ
        and where.value is not None  # col = NULL is never TRUE (see the planner)
        and not isinstance(where.value, (Subquery, list))
    ):
        index = table.get_index_for_column(where.column)
        if index is not None:
            matches = table.select_by_index(index, where.value)
            # Preserve insertion (record-ID) order so results are
            # identical to a full scan.
            return sorted(matches, key=lambda record: record.record_id)

    return table.select(where=where.matches)


def _execute_aggregate(query: Query, records: Sequence[RowLike]) -> ExecuteResult:
    """Execute a query with aggregate functions and optional GROUP BY.

    Aggregates are like summary stats: instead of showing every row,
    you get a single answer (like COUNT or SUM) for each group.
    """
    groups = group_records(records, query.group_by) if query.group_by else {(): list(records)}

    result: ExecuteResult = []
    for members in groups.values():
        row: dict[str, Value] = {}

        # Add GROUP BY column values.
        if query.group_by and members:
            for col in query.group_by:
                row[col] = members[0][col]

        # Add plain columns (must be in GROUP BY).
        for col in query.columns:
            if col not in query.group_by:
                msg = f"Column {col!r} must appear in GROUP BY or an aggregate function"
                raise QueryError(msg)
            if members:
                row[col] = members[0][col]

        # Compute aggregates.
        for agg in query.aggregates:
            row[agg.alias] = compute_aggregate(agg, members)

        result.append(row)

    # Apply HAVING filter.
    if query.having is not None:
        result = [row for row in result if _matches_dict(query.having, row)]

    # Apply ORDER BY and LIMIT using shared helpers.
    result = _apply_order_by(result, query)
    return _apply_limit(result, query)


# ---------------------------------------------------------------------------
# JOIN execution
# ---------------------------------------------------------------------------


def _execute_select_with_join(
    query: Query,
    database: Database,
    left_table: Table,
) -> ExecuteResult:
    """Execute a SELECT with a JOIN clause (nested loop join)."""
    join = query.join
    assert join is not None  # Caller guarantees this.  # noqa: S101

    right_table = _get_table(database, join.table, "JOIN")

    left_name = left_table.name
    right_name = right_table.name

    # Resolve the ON columns (strip table prefix if present).
    left_join_col = _resolve_column(join.left_column, left_name, right_name)
    right_join_col = _resolve_column(join.right_column, left_name, right_name)

    left_records = left_table.select()
    right_records = right_table.select()

    # Build combined rows with qualified column names (table.column).
    combined: list[dict[str, Value]] = []
    for left_rec in left_records:
        for right_rec in right_records:
            # A NULL key never matches anything -- not even another NULL.
            if is_true(compare3(left_rec[left_join_col], Operator.EQ, right_rec[right_join_col])):
                row: dict[str, Value] = {}
                for col in left_table.schema.column_names:
                    row[f"{left_name}.{col}"] = left_rec[col]
                for col in right_table.schema.column_names:
                    row[f"{right_name}.{col}"] = right_rec[col]
                combined.append(row)

    # Apply WHERE, aggregates (if any), ORDER BY, LIMIT, and projection.
    return _finalize_dict_rows(combined, query)


def _resolve_column(qualified: str, left_table: str, right_table: str) -> str:
    """Strip table prefix from a qualified column name.

    Args:
        qualified: A column name like "table.col" or just "col".
        left_table: The left table name.
        right_table: The right table name.

    Returns:
        The bare column name.

    Raises:
        QueryError: If the table prefix doesn't match either table.

    """
    if "." not in qualified:
        return qualified
    parts = qualified.split(".", maxsplit=1)
    table_prefix = parts[0]
    if table_prefix not in (left_table, right_table):
        msg = f"Unknown table {table_prefix!r} in column reference {qualified!r}"
        raise QueryError(msg)
    return parts[1]


# ---------------------------------------------------------------------------
# WHERE helpers
# ---------------------------------------------------------------------------


def _matches_dict(clause: WhereClause, row: dict[str, Value]) -> bool:
    """Check if a combined row dict matches a WHERE clause.

    Unlike Condition.matches (which works on Record objects), this works
    on plain dicts produced by JOINs or views. Only TRUE keeps the row.
    """
    return is_true(clause.evaluate(row))


def _check_date_comparisons(clause: WhereClause, table: Table) -> None:
    """Raise QueryError if a DATE/TIMESTAMP column meets the wrong kind of value."""
    problem = mismatched_date_comparison(clause, table.schema)
    if problem is not None:
        raise QueryError(problem)


def _validate_where_columns(clause: WhereClause, valid_columns: set[str]) -> None:
    """Recursively validate that all columns in a WHERE clause exist."""
    if isinstance(clause, Condition):
        if clause.column not in valid_columns:
            msg = f"Unknown column {clause.column!r} in WHERE clause"
            raise QueryError(msg)
    elif isinstance(clause, Not):
        _validate_where_columns(clause.operand, valid_columns)
    else:
        _validate_where_columns(clause.left, valid_columns)
        _validate_where_columns(clause.right, valid_columns)


def _resolve_subqueries(clause: WhereClause, database: Database) -> WhereClause:
    """Recursively resolve any Subquery values in a WHERE clause.

    Run each subquery against the database and replace the Subquery
    object with the actual result value(s).
    """
    if isinstance(clause, Condition):
        if not isinstance(clause.value, Subquery):
            return clause
        inner_results = execute(clause.value.query, database)
        if clause.operator == Operator.IN:
            # IN subquery: collect all values from the first column.
            if not inner_results:
                resolved: Value | list[Value] = []
            else:
                first_col = next(iter(inner_results[0]))
                resolved = [row[first_col] for row in inner_results]
        else:
            # Scalar subquery: must return exactly one row, one column.
            if len(inner_results) != 1 or len(inner_results[0]) != 1:
                msg = "Scalar subquery must return exactly one row and one column"
                raise QueryError(msg)
            resolved = next(iter(inner_results[0].values()))
        return Condition(column=clause.column, operator=clause.operator, value=resolved)
    if isinstance(clause, Not):
        return Not(_resolve_subqueries(clause.operand, database))
    if isinstance(clause, And):
        return And(
            _resolve_subqueries(clause.left, database), _resolve_subqueries(clause.right, database)
        )
    # Must be Or.
    return Or(
        _resolve_subqueries(clause.left, database), _resolve_subqueries(clause.right, database)
    )


# ---------------------------------------------------------------------------
# Record helpers
# ---------------------------------------------------------------------------


def _sort_records(records: list[Record], column: str, *, reverse: bool) -> list[Record]:
    """Sort records by a column value (NULLs first ascending, last descending)."""
    return sorted(records, key=lambda record: null_sort_key(record[column]), reverse=reverse)


def _project(
    records: list[Record],
    columns: list[str],
    all_columns: list[str],
) -> list[dict[str, Value]]:
    """Extract only the requested columns from each record.

    Like using a ruler to cover columns you don't need on a spreadsheet --
    you only see the ones you asked for.
    """
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


# ---------------------------------------------------------------------------
# DML statement handlers (CREATE, DROP, INSERT, UPDATE, DELETE)
# ---------------------------------------------------------------------------


def _execute_create_table(stmt: CreateTableStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE TABLE statement."""
    schema = Schema(columns=stmt.columns)
    _wrap_error("CREATE TABLE", database.create_table, stmt.table, schema)
    return _result_message(f"Table {stmt.table!r} created")


def _execute_drop_table(stmt: DropTableStatement, database: Database) -> ExecuteResult:
    """Execute a DROP TABLE statement."""
    _wrap_error("DROP TABLE", database.drop_table, stmt.table)
    return _result_message(f"Table {stmt.table!r} dropped")


def _execute_insert(stmt: InsertStatement, database: Database) -> ExecuteResult:
    """Execute an INSERT INTO statement."""
    table = _get_table(database, stmt.table, "INSERT")

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

    _wrap_error("INSERT", table.insert, values)
    return _result_message("1 row inserted")


def _execute_update(stmt: UpdateStatement, database: Database) -> ExecuteResult:
    """Execute an UPDATE statement."""
    table = _get_table(database, stmt.table, "UPDATE")

    if stmt.where is not None:
        _check_date_comparisons(stmt.where, table)
    records = table.select(where=stmt.where.matches) if stmt.where is not None else table.select()

    count = 0
    with one_commit(database, len(records)):  # all rows or none, one journal line
        for record in records:
            _wrap_error("UPDATE", table.update, record_id=record.record_id, values=stmt.assignments)
            count += 1

    row_word = "row" if count == 1 else "rows"
    return _result_message(f"{count} {row_word} updated")


def _execute_delete(stmt: DeleteStatement, database: Database) -> ExecuteResult:
    """Execute a DELETE FROM statement."""
    table = _get_table(database, stmt.table, "DELETE")

    if stmt.where is not None:
        _check_date_comparisons(stmt.where, table)
    records = table.select(where=stmt.where.matches) if stmt.where is not None else table.select()

    count = 0
    with one_commit(database, len(records)):  # all rows or none, one journal line
        for record in records:
            table.delete(record_id=record.record_id)
            count += 1

    row_word = "row" if count == 1 else "rows"
    return _result_message(f"{count} {row_word} deleted")


# ---------------------------------------------------------------------------
# Index handlers
# ---------------------------------------------------------------------------


def _execute_create_index(stmt: CreateIndexStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE INDEX statement."""
    table = _get_table(database, stmt.table, "CREATE INDEX")
    _wrap_error("CREATE INDEX", table.create_index, stmt.index_name, stmt.column)
    return _result_message(f"Index {stmt.index_name!r} created on {stmt.table}({stmt.column})")


def _execute_drop_index(stmt: DropIndexStatement, database: Database) -> ExecuteResult:
    """Execute a DROP INDEX statement."""
    table = _get_table(database, stmt.table, "DROP INDEX")
    _wrap_error("DROP INDEX", table.drop_index, stmt.index_name)
    return _result_message(f"Index {stmt.index_name!r} dropped")


# ---------------------------------------------------------------------------
# EXPLAIN handler
# ---------------------------------------------------------------------------


def _execute_explain(stmt: ExplainStatement, database: Database) -> ExecuteResult:
    """Execute an EXPLAIN statement -- show the query plan."""
    table = _get_table(database, stmt.query.table, "EXPLAIN")
    plan = plan_query(stmt.query, table)
    return [{"plan": plan.strategy}]


# ---------------------------------------------------------------------------
# View handlers
# ---------------------------------------------------------------------------


def _execute_create_view(stmt: CreateViewStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE VIEW statement."""
    _wrap_error("CREATE VIEW", database.create_view, stmt.name, stmt.query)
    return _result_message(f"View {stmt.name!r} created")


def _execute_drop_view(stmt: DropViewStatement, database: Database) -> ExecuteResult:
    """Execute a DROP VIEW statement."""
    _wrap_error("DROP VIEW", database.drop_view, stmt.name)
    return _result_message(f"View {stmt.name!r} dropped")
