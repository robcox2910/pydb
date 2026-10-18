"""SQL date helpers -- typed literals and date-aware comparison checks.

A typed literal is a value with its label stuck on, like ``DATE '2026-10-17'``.
The label tells PyDB exactly what kind of thing the quoted text is, so it
never has to guess. The same strictness applies when comparing: a DATE column
only ever meets DATE values, never a bare string.
"""

from datetime import date, datetime

from pydb.errors import SchemaError
from pydb.query import Condition, Not, Subquery, WhereClause
from pydb.record import Value
from pydb.schema import Schema
from pydb.types import DataType, from_iso

# Words that can label a quoted string as a date or timestamp.
LITERAL_TYPES: dict[str, DataType] = {
    "DATE": DataType.DATE,
    "TIMESTAMP": DataType.TIMESTAMP,
}


def parse_typed_literal(type_name: str, text: str) -> Value:
    """Turn ``DATE '...'`` or ``TIMESTAMP '...'`` text into a Python value.

    Args:
        type_name: ``DATE`` or ``TIMESTAMP`` (any case).
        text: The quoted text, e.g. ``2026-10-17`` or ``2026-10-17T09:30:00Z``.

    Returns:
        A ``date`` or a UTC ``datetime``.

    Raises:
        SchemaError: If the text is not valid for that type.

    """
    data_type = LITERAL_TYPES[type_name.upper()]
    try:
        return from_iso(text, data_type)
    except SchemaError as exc:
        msg = f"Invalid {data_type.value} literal: {exc}"
        raise SchemaError(msg) from exc


def mismatched_date_comparison(clause: WhereClause, schema: Schema) -> str | None:
    """Find a DATE/TIMESTAMP column compared with the wrong kind of value.

    There is no implicit conversion: a DATE column must meet a DATE literal,
    and a TIMESTAMP column a TIMESTAMP literal.

    Args:
        clause: The WHERE clause to inspect.
        schema: The schema of the table being queried.

    Returns:
        An error message for the first bad comparison, or None if all is well.

    """
    if isinstance(clause, Not):
        return mismatched_date_comparison(clause.operand, schema)
    if not isinstance(clause, Condition):
        return mismatched_date_comparison(clause.left, schema) or mismatched_date_comparison(
            clause.right, schema
        )
    if (
        clause.column not in schema.column_names
        or clause.value is None  # NULL meets any type (and IS NULL has no value)
        or isinstance(clause.value, (list, Subquery))
    ):
        return None
    wanted = next(c.data_type for c in schema.columns if c.name == clause.column)
    value = clause.value
    if wanted is DataType.DATE:
        ok = isinstance(value, date) and not isinstance(value, datetime)
    elif wanted is DataType.TIMESTAMP:
        ok = isinstance(value, datetime)
    else:
        return None
    if ok:
        return None
    return (
        f"Cannot compare {wanted.value} column {clause.column!r} with {type(value).__name__} "
        f"(write {wanted.value} '...' -- no implicit conversion)"
    )
