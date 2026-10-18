"""Aggregate functions -- COUNT, SUM, AVG, MIN and MAX over a pile of rows.

Aggregates are like summary stats: instead of showing every row, you get
a single answer (like a count or a total) for each group of rows.

NULL is an unknown value, so an aggregate cannot use it:

* ``COUNT(*)`` counts rows, whatever they hold.
* ``COUNT(col)``, ``SUM``, ``AVG``, ``MIN`` and ``MAX`` skip NULLs -- like
  adding up a test's marks while leaving out the pupils who were absent.
* With no known values left, ``COUNT`` is 0 and the others are NULL: the
  total of nothing we know is itself unknown.

GROUP BY puts every NULL into one group, the "unknown" pile.
"""

from collections.abc import Callable, Sequence
from datetime import date, datetime
from typing import Any

from pydb.errors import QueryError
from pydb.query import AggFunc, AggregateColumn, RowLike
from pydb.record import Value


def _average(values: list[Any]) -> float:
    """Return the mean of *values* (never empty here)."""
    return sum(float(v) for v in values) / len(values)


# How each aggregate (other than COUNT) folds its known values into one.
_FOLDS: dict[AggFunc, Callable[[list[Any]], Value]] = {
    AggFunc.SUM: sum,
    AggFunc.AVG: _average,
    AggFunc.MIN: min,
    AggFunc.MAX: max,
}


def group_records(
    records: Sequence[RowLike], group_by: list[str]
) -> dict[tuple[Value, ...], list[RowLike]]:
    """Group records by the values of the GROUP BY columns.

    Like sorting trading cards into piles by team -- each pile is a group.
    All NULLs of a column fall into the same pile.

    Returns:
        A dict mapping group keys to lists of records.

    """
    groups: dict[tuple[Value, ...], list[RowLike]] = {}
    for record in records:
        key = tuple(record[col] for col in group_by)
        groups.setdefault(key, []).append(record)
    return groups


def _known_values(agg: AggregateColumn, records: Sequence[RowLike]) -> list[Any]:
    """Return the non-NULL values of the aggregated column.

    Raises:
        QueryError: If the column does not exist.

    """
    try:
        values = [r[agg.column] for r in records]
    except KeyError:
        msg = f"Unknown column {agg.column!r} in {agg.alias}"
        raise QueryError(msg) from None
    return [v for v in values if v is not None]


def compute_aggregate(agg: AggregateColumn, records: Sequence[RowLike]) -> Value:
    """Compute a single aggregate function over a group of records.

    Raises:
        QueryError: If the column is unknown, or SUM / AVG meets dates.

    """
    if agg.function is AggFunc.COUNT and agg.column == "*":
        return len(records)
    values = _known_values(agg, records)
    if agg.function in (AggFunc.SUM, AggFunc.AVG) and any(
        isinstance(v, (date, datetime)) for v in values
    ):
        msg = f"{agg.function.value} is not supported on DATE or TIMESTAMP columns"
        raise QueryError(msg)
    if agg.function is AggFunc.COUNT:
        return len(values)
    if not values:
        return None
    return _FOLDS[agg.function](values)
