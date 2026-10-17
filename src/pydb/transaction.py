"""Transactions -- the "no take-backs" rule with a safety net.

A transaction groups several database operations so they either all
happen (commit) or none happen (rollback). Under the hood, we snapshot a
table's state (rows *and* index definitions) and restore it on rollback.

The snapshots are *lazy* (copy-on-first-write): like only photocopying the
pages of a notebook you are about to scribble on, a table is copied just
before the transaction first changes it -- an insert, update, delete,
index change or a drop of the table -- whichever way the change is made
(a ``Table`` you got earlier, SQL, ...). A table the transaction never
touches is never copied, so a small transaction stays quick however big
the rest of the database is.

While a transaction is open, the database holds every ``Change`` note in a
bundle instead of committing it straight away. ``commit()`` hands the whole
bundle to the database's commit listeners in one go; ``rollback()`` throws
it away. If a listener fails during ``commit()``, the transaction rolls back
and the error is passed on -- so a commit is either fully done or not at
all.

A transaction also holds the database's lock (see ``pydb.locking``) from
the moment it is created until it commits or rolls back. Another thread
that wants the database -- to read, write or open its own transaction --
waits its turn, so it never sees a half-finished transaction. The thread
that opened the transaction can keep using the database normally, but
opening a *second* transaction there raises ``TransactionError``. Finish a
transaction in the thread that started it, or use it as a context manager::

    with Transaction(db) as txn:  # commits at the end, rolls back on error
        txn.get_table("cards").insert({"name": "Pikachu"})
"""

import threading
from dataclasses import dataclass
from types import TracebackType

from pydb.commits import is_committed
from pydb.database import Database, DatabaseError
from pydb.errors import PyDBError
from pydb.record import Record
from pydb.table import Table


class TransactionError(PyDBError):
    """Raise when a transaction operation is invalid."""


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """A frozen copy of a table's state for rollback.

    Args:
        table: The table object itself (rollback restores it in place).
        records: A copy of all records.
        next_id: The next auto-increment ID.
        indexes: ``(name, column)`` for each index.

    """

    table: Table
    records: list[Record]
    next_id: int
    indexes: list[tuple[str, str]]


def snapshot_table(table: Table) -> TableSnapshot:
    """Copy *table*'s rows, next ID and index definitions for a later rollback.

    Each record is copied, so the snapshot shares nothing with the live table.
    """
    return TableSnapshot(
        table=table,
        records=[Record(record_id=r.record_id, data=r.data) for r in table.select()],
        next_id=table.next_id,
        indexes=table.index_definitions,
    )


class Transaction:
    """Wrap a database session with commit/rollback semantics.

    On creation, takes the database's lock, starts holding changes and asks
    every table to tell it before its first change (``Table.watch``); a
    table is snapshotted only then. On commit, delivers the held changes as
    one batch and discards the snapshots. On rollback, restores every
    snapshotted table, puts back tables that were dropped or replaced,
    removes tables created inside the transaction, and discards the held
    changes. Either way the lock is handed back -- also when a commit fails.

    Args:
        database: The database to transact against.

    Raises:
        TransactionError: If this thread already has a transaction open on
            the database. (A transaction in another thread is waited for.)

    """

    __slots__ = (
        "_commit_failed",
        "_database",
        "_finished",
        "_holding",
        "_originals",
        "_owner",
        "_snapshots",
    )

    def __init__(self, database: Database) -> None:
        """Begin a transaction: lock, hold changes and watch every table."""
        self._database = database
        self._finished = False
        # Set when commit() failed and already rolled back, so a later
        # rollback() (the usual ``except: txn.rollback()``) is a no-op.
        self._commit_failed = False
        self._snapshots: list[TableSnapshot] = []
        # The tables at the start, by name (rollback puts these objects back).
        self._originals: dict[str, Table] = {}
        database.lock.acquire()  # other threads' transactions finish first
        self._owner = threading.get_ident()
        self._holding = True
        try:
            database.begin_batch()
        except DatabaseError as exc:
            self._release()
            raise TransactionError(str(exc)) from exc
        try:
            self._watch_tables()
        except BaseException:
            self._unwatch()
            database.discard_batch()
            self._finished = True
            self._release()
            raise

    def __enter__(self) -> Transaction:
        """Return the transaction for use in a ``with`` block."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Commit if the block finished cleanly, else roll back; errors propagate.

        If the block already committed or rolled back, there is nothing left
        to do.
        """
        if not self.is_active:
            return
        if exc is None:
            self.commit()
        else:
            self.rollback()

    def _check_owner(self) -> None:
        """Raise unless called by the thread that opened the transaction.

        The database's lock belongs to that thread, so only it can hand
        the lock back; another thread is refused and nothing changes.
        """
        if threading.get_ident() != self._owner:
            msg = "A transaction must be committed or rolled back by the thread that opened it"
            raise TransactionError(msg)

    def _release(self) -> None:
        """Hand the database's lock back (only once)."""
        if self._holding:
            self._holding = False
            self._database.lock.release()

    def _watch_tables(self) -> None:
        """Remember every table and ask each to call ``_snapshot`` before its first change."""
        for name in self._database.table_names():
            self._originals[name] = self._database.get_table(name)
        for table in self._originals.values():
            table.watch(self._snapshot)

    def _snapshot(self, table: Table) -> None:
        """Copy *table* just before this transaction first changes it."""
        self._snapshots.append(snapshot_table(table))

    def _unwatch(self) -> None:
        """Stop every table from calling back (the transaction is ending)."""
        for table in self._originals.values():
            table.watch(None)

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
        harmless no-op. An error (or Ctrl-C) that arrives only after the
        batch reached the journal leaves the commit standing: it propagates
        with ``pydb.commits.COMMITTED_NOTE`` attached.

        The automatic checkpoint (``Database.after_commit``) still runs
        while the lock is held, so no other thread commits half-way through
        it.

        Raises:
            TransactionError: If the transaction is already finished, or
                this is not the thread that opened it.

        """
        self._check_active()
        self._check_owner()
        try:
            self._database.commit_batch()
        except BaseException as exc:
            if is_committed(exc):  # it reached the journal just before the error
                try:
                    self._database.discard_batch()  # (it may not have been closed yet)
                    self._finish_committed()
                finally:
                    self._release()
                raise
            try:
                self._restore()
            finally:
                self._commit_failed = True
                self._release()
            raise
        try:
            self._finish_committed()
            self._database.after_commit()  # outside the undo: this commit is final
        finally:
            self._release()

    def _finish_committed(self) -> None:
        """Mark the transaction committed and drop its snapshots."""
        self._finished = True
        self._unwatch()
        self._snapshots.clear()
        self._originals.clear()

    def rollback(self) -> None:
        """Roll back the transaction -- undo all changes.

        Restores every table the transaction changed to the state it was in
        when the transaction began, and throws away the held changes. Does nothing if a failed
        ``commit()`` already rolled the transaction back.

        Raises:
            TransactionError: If the transaction was committed or already
                rolled back, or this is not the thread that opened it.

        """
        if self._commit_failed:
            return
        self._check_active()
        self._check_owner()
        try:
            self._restore()
        finally:
            self._release()

    def _restore(self) -> None:
        """Put every table back as it was at the start and end the transaction."""
        self._finished = True
        self._unwatch()

        # Restore the changed tables in place, so a reader holding one of
        # these Table objects never sees the rolled-back work. Untouched
        # tables need nothing but their place in the catalog.
        for snapshot in self._snapshots:
            snapshot.table.restore(snapshot.records, snapshot.next_id, snapshot.indexes)
        for table in self._originals.values():
            self._database.replace_table(table)

        # Remove tables created during the transaction. The batch is still
        # open, so these drops are held -- and then thrown away below.
        current_names = set(self._database.table_names())
        for name in current_names - self._originals.keys():
            self._database.drop_table(name)

        self._database.discard_batch()
        self._snapshots.clear()
        self._originals.clear()

    @property
    def is_active(self) -> bool:
        """Return whether this transaction is still active."""
        return not self._finished

    def _check_active(self) -> None:
        """Raise if the transaction is already finished."""
        if self._finished:
            msg = "Transaction is already finished (committed or rolled back)"
            raise TransactionError(msg)
