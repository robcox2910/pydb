"""Transactions -- the "no take-backs" rule with a safety net.

A transaction groups several database operations so they either all
happen (commit) or none happen (rollback). Under the hood, we snapshot
each table's state at the start and restore it on rollback.
"""

from dataclasses import dataclass

from pydb.database import Database
from pydb.errors import PyDBError
from pydb.record import Record
from pydb.schema import Schema
from pydb.table import Table


class TransactionError(PyDBError):
    """Raise when a transaction operation is invalid."""


@dataclass(frozen=True, slots=True)
class _TableSnapshot:
    """A frozen copy of a table's state for rollback.

    Args:
        name: The table name.
        schema: The table's schema.
        records: A copy of all records.
        next_id: The next auto-increment ID.

    """

    name: str
    schema: Schema
    records: list[Record]
    next_id: int


class Transaction:
    """Wrap a database session with commit/rollback semantics.

    On creation, snapshots every table. On commit, discards the
    snapshots. On rollback, restores every table from its snapshot.

    Args:
        database: The database to transact against.

    """

    __slots__ = ("_database", "_finished", "_snapshots")

    def __init__(self, database: Database) -> None:
        """Begin a transaction by snapshotting all tables."""
        self._database = database
        self._finished = False
        self._snapshots: dict[str, _TableSnapshot] = {}
        self._take_snapshots()

    def _take_snapshots(self) -> None:
        """Snapshot every table's current state."""
        for name in self._database.table_names():
            table = self._database.get_table(name)
            # Deep-copy records so in-place mutations (update_fields) don't
            # affect the snapshot.
            snapshot_records = [Record(record_id=r.record_id, data=r.data) for r in table.select()]
            self._snapshots[name] = _TableSnapshot(
                name=name,
                schema=table.schema,
                records=snapshot_records,
                next_id=table.next_id,
            )

    def get_table(self, name: str) -> Table:
        """Return a table from the database for use within this transaction.

        Args:
            name: The table name.

        Returns:
            The table object.

        Raises:
            TransactionError: If the transaction is already finished.

        """
        self._check_active()
        return self._database.get_table(name)

    def commit(self) -> None:
        """Commit the transaction -- make all changes permanent.

        Raises:
            TransactionError: If the transaction is already finished.

        """
        self._check_active()
        self._finished = True
        self._snapshots.clear()

    def rollback(self) -> None:
        """Roll back the transaction -- undo all changes.

        Restores every table to the state it was in when the transaction
        began.

        Raises:
            TransactionError: If the transaction is already finished.

        """
        self._check_active()
        self._finished = True

        for snapshot in self._snapshots.values():
            restored = Table.from_stored(
                name=snapshot.name,
                schema=snapshot.schema,
                records=snapshot.records,
                next_id=snapshot.next_id,
            )
            self._database.replace_table(restored)

        self._snapshots.clear()

    @property
    def is_active(self) -> bool:
        """Return whether this transaction is still active."""
        return not self._finished

    def _check_active(self) -> None:
        """Raise if the transaction is already finished."""
        if self._finished:
            msg = "Transaction is already finished (committed or rolled back)"
            raise TransactionError(msg)
