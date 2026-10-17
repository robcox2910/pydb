"""Transactions -- the "no take-backs" rule with a safety net.

A transaction groups several database operations so they either all
happen (commit) or none happen (rollback).

Under the hood there is an *undo log* (see ``pydb.rows``): instead of
photocopying a page before scribbling on it, you write down how to undo
each scribble. Every insert, update, delete and index change -- however
it is made (a ``Table`` you got earlier, SQL, ...) -- first notes its
opposite. Rollback follows the notes from newest to oldest; commit throws
them away. Each note costs the same however big the table is, and no
table is ever copied. Tables created inside the transaction are dropped
on rollback, and dropped ones are put back (the very same ``Table``).

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
from types import TracebackType

from pydb.commits import is_committed
from pydb.database import Database, DatabaseError
from pydb.errors import PyDBError
from pydb.rows import UndoLog
from pydb.table import Table


class TransactionError(PyDBError):
    """Raise when a transaction operation is invalid."""


class Transaction:
    """Wrap a database session with commit/rollback semantics.

    On creation, takes the database's lock, starts holding changes and
    hands every table an undo log (``Table.set_undo_log``). On commit,
    delivers the held changes as one batch and throws the undo log away.
    On rollback, replays the undo log newest first, puts back tables that
    were dropped or replaced, removes tables created inside the
    transaction, and discards the held changes. Either way the lock is handed back -- also when a commit fails.

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
        "_undo",
    )

    def __init__(self, database: Database) -> None:
        """Begin a transaction: lock, hold changes and give every table the undo log."""
        self._database = database
        self._finished = False
        # Set when commit() failed and already rolled back, so a later
        # rollback() (the usual ``except: txn.rollback()``) is a no-op.
        self._commit_failed = False
        self._undo = UndoLog()
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
            self._track_tables()
        except BaseException:
            self._untrack()
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

    def _track_tables(self) -> None:
        """Remember every table and have each write its undo notes into our log."""
        for name in self._database.table_names():
            self._originals[name] = self._database.get_table(name)
        for table in self._originals.values():
            table.set_undo_log(self._undo)

    def _untrack(self) -> None:
        """Stop every table writing undo notes (the transaction is ending)."""
        for table in self._originals.values():
            table.set_undo_log(None)

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
        """Mark the transaction committed and throw its undo log away."""
        self._finished = True
        self._untrack()
        self._undo.clear()
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
        self._untrack()

        # Undo every change in place, newest first, so a reader holding one
        # of these Table objects never sees the rolled-back work. Then put
        # every original table back in the catalog (dropped or replaced
        # ones included).
        self._undo.rollback()
        for table in self._originals.values():
            self._database.replace_table(table)

        # Remove tables created during the transaction. The batch is still
        # open, so these drops are held -- and then thrown away below.
        current_names = set(self._database.table_names())
        for name in current_names - self._originals.keys():
            self._database.drop_table(name)

        self._database.discard_batch()
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
