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
from pydb.planner import plan_query
from pydb.query import (
    AggFunc,
    AggregateColumn,
    And,
    Condition,
    Operator,
    Or,
    Query,
    SortDirection,
    Subquery,
    WhereClause,
)
from pydb.record import Record, Value
from pydb.schema import Schema
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


def _result_message(message: str) -> ExecuteResult:
    """Create a single-row result with a message."""
    return [{"result": message}]


def _execute_select(query: Query, database: Database) -> ExecuteResult:
    """Execute a SELECT query, with optional JOIN and view support."""
    # Check if the table name is actually a view.
    view_query = database.get_view(query.table)
    if view_query is not None:
        # Run the view's stored query, then apply outer clauses.
        view_results = execute(view_query, database)
        return _apply_outer_clauses(view_results, query)

    try:
        left_table = database.get_table(query.table)
    except PyDBError as exc:
        msg = f"Query failed: {exc}"
        raise QueryError(msg) from exc

    # If there's a JOIN, produce combined rows from both tables.
    if query.join is not None:
        return _execute_select_with_join(query, database, left_table)

    return _execute_simple_select(query, left_table, database)


def _execute_simple_select(query: Query, table: Table, database: Database) -> ExecuteResult:
    """Execute a SELECT without a JOIN."""
    valid_cols = set(table.schema.column_names)

    where = query.where
    if where is not None:
        # Resolve any subqueries in the WHERE clause before filtering.
        where = _resolve_subqueries(where, database)
        _validate_where_columns(where, valid_cols)
        records = table.select(where=where.matches)
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


def _execute_aggregate(query: Query, records: list[Record]) -> ExecuteResult:
    """Execute a query with aggregate functions and optional GROUP BY."""
    groups = _group_records(records, query.group_by) if query.group_by else {(): records}

    result: ExecuteResult = []
    for group_records in groups.values():
        row: dict[str, Value] = {}

        # Add GROUP BY column values.
        if query.group_by and group_records:
            for col in query.group_by:
                row[col] = group_records[0][col]

        # Add plain columns (must be in GROUP BY).
        for col in query.columns:
            if col not in query.group_by:
                msg = f"Column {col!r} must appear in GROUP BY or an aggregate function"
                raise QueryError(msg)
            if group_records:
                row[col] = group_records[0][col]

        # Compute aggregates.
        for agg in query.aggregates:
            row[agg.alias] = _compute_aggregate(agg, group_records)

        result.append(row)

    # Apply HAVING filter.
    if query.having is not None:
        result = [row for row in result if _matches_dict(query.having, row)]

    # Apply ORDER BY.
    if query.order_by is not None:
        col = query.order_by.column
        reverse = query.order_by.direction == SortDirection.DESC
        result = sorted(result, key=lambda r: r[col], reverse=reverse)  # type: ignore[return-value]

    # Apply LIMIT.
    if query.limit is not None:
        result = result[: query.limit]

    return result


def _group_records(
    records: list[Record], group_by: list[str]
) -> dict[tuple[Value, ...], list[Record]]:
    """Group records by the values of the GROUP BY columns.

    Returns:
        A dict mapping group keys to lists of records.

    """
    groups: dict[tuple[Value, ...], list[Record]] = {}
    for record in records:
        key = tuple(record[col] for col in group_by)
        groups.setdefault(key, []).append(record)
    return groups


def _compute_aggregate(agg: AggregateColumn, records: list[Record]) -> Value:
    """Compute a single aggregate function over a group of records."""
    match agg.function:
        case AggFunc.COUNT:
            return len(records)
        case AggFunc.SUM:
            values: list[Any] = [r[agg.column] for r in records]
            return sum(values)
        case AggFunc.AVG:
            total = sum(float(r[agg.column]) for r in records)
            return total / len(records) if records else 0.0
        case AggFunc.MIN:
            values: list[Any] = [r[agg.column] for r in records]
            return min(values) if values else 0
        case AggFunc.MAX:
            vals: list[Any] = [r[agg.column] for r in records]
            return max(vals) if vals else 0


def _execute_select_with_join(
    query: Query,
    database: Database,
    left_table: Table,
) -> ExecuteResult:
    """Execute a SELECT with a JOIN clause (nested loop join)."""
    join = query.join
    assert join is not None  # Caller guarantees this.  # noqa: S101

    try:
        right_table = database.get_table(join.table)
    except PyDBError as exc:
        msg = f"JOIN failed: {exc}"
        raise QueryError(msg) from exc

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
            if left_rec[left_join_col] == right_rec[right_join_col]:
                row: dict[str, Value] = {}
                for col in left_table.schema.column_names:
                    row[f"{left_name}.{col}"] = left_rec[col]
                for col in right_table.schema.column_names:
                    row[f"{right_name}.{col}"] = right_rec[col]
                combined.append(row)

    # Apply WHERE filter on combined rows.
    if query.where is not None:
        combined = [row for row in combined if _matches_dict(query.where, row)]

    # Apply ORDER BY.
    if query.order_by is not None:
        col = query.order_by.column
        reverse = query.order_by.direction == SortDirection.DESC
        combined = sorted(combined, key=lambda r: r[col], reverse=reverse)  # type: ignore[return-value]

    # Apply LIMIT.
    if query.limit is not None:
        combined = combined[: query.limit]

    # Project columns.
    if query.columns:
        combined = [{col: row[col] for col in query.columns} for row in combined]

    return combined


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


def _matches_dict(clause: WhereClause, row: dict[str, Value]) -> bool:
    """Check if a combined row dict matches a WHERE clause.

    Unlike Condition.matches (which works on Record objects), this works
    on plain dicts produced by JOINs.
    """
    if isinstance(clause, Condition):
        record_value: Any = row[clause.column]
        target: Any = clause.value
        return _compare(record_value, clause.operator, target)
    if isinstance(clause, And):
        return _matches_dict(clause.left, row) and _matches_dict(clause.right, row)
    return _matches_dict(clause.left, row) or _matches_dict(clause.right, row)


def _compare(left: Any, op: Operator, right: Any) -> bool:
    """Evaluate a comparison between two values."""
    match op:
        case Operator.EQ:
            result = left == right
        case Operator.NE:
            result = left != right
        case Operator.GT:
            result = left > right
        case Operator.GE:
            result = left >= right
        case Operator.LT:
            result = left < right
        case Operator.LE:
            result = left <= right
        case Operator.IN:
            result = left in right
        case _:
            msg = f"Unknown operator: {op}"
            raise ValueError(msg)
    return result


def _execute_create_table(stmt: CreateTableStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE TABLE statement."""
    schema = Schema(columns=stmt.columns)
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


def _execute_create_index(stmt: CreateIndexStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE INDEX statement."""
    try:
        table = database.get_table(stmt.table)
        table.create_index(stmt.index_name, stmt.column)
    except PyDBError as exc:
        msg = f"CREATE INDEX failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"Index {stmt.index_name!r} created on {stmt.table}({stmt.column})")


def _execute_drop_index(stmt: DropIndexStatement, database: Database) -> ExecuteResult:
    """Execute a DROP INDEX statement."""
    try:
        table = database.get_table(stmt.table)
        table.drop_index(stmt.index_name)
    except PyDBError as exc:
        msg = f"DROP INDEX failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"Index {stmt.index_name!r} dropped")


def _execute_explain(stmt: ExplainStatement, database: Database) -> ExecuteResult:
    """Execute an EXPLAIN statement -- show the query plan."""
    try:
        table = database.get_table(stmt.query.table)
    except PyDBError as exc:
        msg = f"EXPLAIN failed: {exc}"
        raise QueryError(msg) from exc

    plan = plan_query(stmt.query, table)
    return [{"plan": plan.strategy}]


def _resolve_subqueries(clause: WhereClause, database: Database) -> WhereClause:
    """Recursively resolve any Subquery values in a WHERE clause.

    Runs each subquery against the database and replaces the Subquery
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
    if isinstance(clause, And):
        return And(
            _resolve_subqueries(clause.left, database), _resolve_subqueries(clause.right, database)
        )
    # Must be Or.
    return Or(
        _resolve_subqueries(clause.left, database), _resolve_subqueries(clause.right, database)
    )


def _execute_create_view(stmt: CreateViewStatement, database: Database) -> ExecuteResult:
    """Execute a CREATE VIEW statement."""
    try:
        database.create_view(stmt.name, stmt.query)
    except PyDBError as exc:
        msg = f"CREATE VIEW failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"View {stmt.name!r} created")


def _execute_drop_view(stmt: DropViewStatement, database: Database) -> ExecuteResult:
    """Execute a DROP VIEW statement."""
    try:
        database.drop_view(stmt.name)
    except PyDBError as exc:
        msg = f"DROP VIEW failed: {exc}"
        raise QueryError(msg) from exc
    return _result_message(f"View {stmt.name!r} dropped")


def _apply_outer_clauses(rows: ExecuteResult, query: Query) -> ExecuteResult:
    """Apply WHERE, ORDER BY, LIMIT, and column projection from an outer query to view results."""
    result = rows

    # Filter.
    if query.where is not None:
        result = [row for row in result if _matches_dict(query.where, row)]

    # Sort.
    if query.order_by is not None:
        col = query.order_by.column
        reverse = query.order_by.direction == SortDirection.DESC
        result = sorted(result, key=lambda r: r[col], reverse=reverse)  # type: ignore[return-value]

    # Limit.
    if query.limit is not None:
        result = result[: query.limit]

    # Project columns.
    if query.columns:
        result = [{c: row[c] for c in query.columns} for row in result]

    return result
