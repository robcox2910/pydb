"""One SQL statement, one commit -- all of its rows or none of them.

Imagine a teacher marking ten spelling tests. If the bell rings after five,
you don't want five marked and five not: you want either the whole pile
marked or the pile untouched. A SQL ``UPDATE`` or ``DELETE`` that matches
many rows is the same. ``one_commit`` wraps the statement's row-by-row loop
in a ``Transaction``, so the rows become **one** commit: one journal line
and one ``fsync`` (instead of one per row), and one batch for the commit
listeners. If a row fails part-way -- say it breaks a ``UNIQUE`` rule --
the whole statement is rolled back and the error is passed on.

Inside a transaction you opened yourself, the statement just joins it; a
statement that touches at most one row is already a single commit.
"""

from collections.abc import Iterator
from contextlib import contextmanager

from pydb.database import Database
from pydb.transaction import Transaction


@contextmanager
def one_commit(database: Database, rows: int) -> Iterator[None]:
    """Make the changes inside the ``with`` block one all-or-nothing commit.

    The caller (``execute``) already holds the database's lock, so an open
    transaction seen here is this thread's own.

    Args:
        database: The database the statement changes.
        rows: How many rows the statement will change.

    """
    if rows <= 1 or database.in_transaction:
        yield
        return
    with Transaction(database):
        yield
