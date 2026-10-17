"""Transactions -- the "no take-backs" rule with a safety net.

A transaction groups several database operations so they either all
happen (commit) or none happen (rollback). Under the hood, we snapshot
each table's state (rows *and* index definitions) at the start and restore
it on rollback.

While a transaction is open, the database holds every ``Change`` note in a
bundle instead of committing it straight away. ``commit()`` hands the whole
bundle to the database's commit listeners in one go; ``rollback()`` throws
it away. If a listener fails during ``commit()``, the transaction rolls back
and the error is passed on -- so a commit is either fully done or not at
all. Only one transaction at a time may be open on a database.
"""

from dataclasses import dataclass

from pydb.database import Database, DatabaseError
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
        indexes: ``(name, column)`` for each index.

    """

    name: str
    schema: Schema
    records: list[Record]
    next_id: int
    indexes: list[tuple[str, str]]


class Transaction:
    """Wrap a database session with commit/rollback semantics.

    On creation, snapshots every table and starts holding changes. On
    commit, delivers the held changes as one batch and discards the
    snapshots. On rollback, restores every table from its snapshot and
    discards the held changes.

    Args:
        database: The database to transact against.

    Raises:
        TransactionError: If another transaction is already open on the
            database.

    """

    __slots__ = (
        "_commit_failed",
        "_database",
        "_finished",
        "_original_table_names",
        "_snapshots",
    )

    def __init__(self, database: Database) -> None:
        """Begin a transaction by snapshotting all tables."""
        self._database = database
        self._finished = False
        # Set when commit() failed and already rolled back, so a later
        # rollback() (the usual ``except: txn.rollback()``) is a no-op.
        self._commit_failed = False
        self._snapshots: dict[str, _TableSnapshot] = {}
        self._original_table_names: frozenset[str] = frozenset()
        try:
            database.begin_batch()
        except DatabaseError as exc:
            raise TransactionError(str(exc)) from exc
        self._take_snapshots()

    def _take_snapshots(self) -> None:
        """Snapshot every table's current state."""
        self._original_table_names = frozenset(self._database.table_names())
        for name in self._original_table_names:
            table = self._database.get_table(name)
            # Deep-copy records so in-place mutations (update_fields) don't
            # affect the snapshot.
            snapshot_records = [Record(record_id=r.record_id, data=r.data) for r in table.select()]
            self._snapshots[name] = _TableSnapshot(
                name=name,
                schema=table.schema,
                records=snapshot_records,
                next_id=table.next_id,
                indexes=table.index_definitions,
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

        The held changes go to the database's commit listeners as one
        batch. If a listener raises, the transaction is rolled back and the
        error propagates; calling ``rollback()`` afterwards is then a
        harmless no-op.

        Raises:
            TransactionError: If the transaction is already finished.

        """
        self._check_active()
        try:
            self._database.commit_batch()
        except BaseException:
            self._restore()
            self._commit_failed = True
            raise
        self._finished = True
        self._snapshots.clear()
        self._database.after_commit()  # outside the try: this commit is final

    def rollback(self) -> None:
        """Roll back the transaction -- undo all changes.

        Restores every table to the state it was in when the transaction
        began, and throws away the held changes. Does nothing if a failed
        ``commit()`` already rolled the transaction back.

        Raises:
            TransactionError: If the transaction was committed or already
                rolled back.

        """
        if self._commit_failed:
            return
        self._check_active()
        self._restore()

    def _restore(self) -> None:
        """Put every table back as it was at the start and end the transaction."""
        self._finished = True

        # Restore snapshotted tables.
        for snapshot in self._snapshots.values():
            restored = Table.from_stored(
                name=snapshot.name,
                schema=snapshot.schema,
                records=snapshot.records,
                next_id=snapshot.next_id,
                indexes=snapshot.indexes,
            )
            self._database.replace_table(restored)

        # Remove tables created during the transaction. The batch is still
        # open, so these drops are held -- and then thrown away below.
        current_names = set(self._database.table_names())
        for name in current_names - self._original_table_names:
            self._database.drop_table(name)

        self._database.discard_batch()
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
