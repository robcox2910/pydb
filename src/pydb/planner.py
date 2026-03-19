"""Query planner -- decide the fastest way to answer a query.

The planner is the smart librarian. It looks at the WHERE clause and
the available indexes, then decides: should we walk every shelf (full
table scan) or use the card catalog (index lookup)?
"""

from dataclasses import dataclass

from pydb.query import Condition, Operator, Query
from pydb.table import Table


@dataclass(frozen=True, slots=True)
class QueryPlan:
    """Describe how a query will be executed.

    Args:
        strategy: A human-readable description of the plan.
        use_index: Whether an index will be used.
        index_name: The name of the index (if used).
        index_column: The column the index covers (if used).
        lookup_value: The value to look up in the index (if applicable).

    """

    strategy: str
    use_index: bool = False
    index_name: str = ""
    index_column: str = ""
    lookup_value: str = ""


def plan_query(query: Query, table: Table) -> QueryPlan:
    """Decide how to execute a query.

    The planner checks if the WHERE clause uses ``=`` on a column that
    has an index. If so, it chooses an index lookup. Otherwise, it
    falls back to a full table scan.

    Args:
        query: The query to plan.
        table: The table being queried.

    Returns:
        A QueryPlan describing the execution strategy.

    """
    if (
        query.where is not None
        and isinstance(query.where, Condition)
        and query.where.operator == Operator.EQ
    ):
        idx = table.get_index_for_column(query.where.column)
        if idx is not None:
            return QueryPlan(
                strategy=f"Index lookup on {idx.name} ({query.where.column} = ...)",
                use_index=True,
                index_name=idx.name,
                index_column=query.where.column,
                lookup_value=str(query.where.value),
            )

    return QueryPlan(strategy=f"Full table scan on {table.name}")
