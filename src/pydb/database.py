"""The database -- a collection room for tables.

A database is the whole room full of shelves. It knows which binders (tables)
exist, creates new ones, drops old ones, and coordinates saving and loading
through the storage engine.

Every change -- a new table, a new row, a dropped index -- is also written
down as a ``Change`` note (see ``pydb.changes``) and handed to the
database's *commit listeners*, one batch per commit. Outside a transaction
each change is its own commit; inside a ``Transaction`` the notes wait until
``commit()``. A listener runs *before* the commit counts as done: if it
raises, the change (or the whole transaction) is undone in memory and the
error is passed on.

A *journalled* database (``Database(path, journal=True)``) also writes each
commit to ``journal.log`` before the commit counts, so committed work
survives a crash even without ``save()``. On opening, it rebuilds itself
from the folder, and now and then it takes a *checkpoint* so the journal
stays short (see ``pydb.durable`` and ``pydb.checkpoint``). It also keeps
a ``LOCK`` file so a second program can't open the same folder.

Every public method holds the database's ``lock``, which its tables share,
so threads take turns (see ``pydb.locking`` and ``pydb.transaction``).
"""

import logging
import threading
import weakref
from collections.abc import Callable, Sequence
from pathlib import Path
from types import TracebackType

from pydb.changes import Change, ChangeKind, CommitListener
from pydb.checkpoint import is_journalled_folder
from pydb.commits import CommitRouter, commit_point, is_committed
from pydb.durable_store import JournalStore
from pydb.errors import PyDBError
from pydb.locking import LockType, lock_public_methods
from pydb.query import Query
from pydb.schema import Schema
from pydb.storage import StorageEngine
from pydb.table import Table

logger = logging.getLogger(__name__)


class DatabaseError(PyDBError):
    """Raise when a database-level operation fails."""


def _refuse_detached(change: Change) -> None:
    """Refuse a change made through a table a journalled database no longer manages."""
    msg = (
        f"Table {change.table!r} is no longer part of the database "
        "(it was dropped, rolled back or replaced); get it again with get_table()"
    )
    raise DatabaseError(msg)


@lock_public_methods
class Database:
    """Manage a collection of named tables with disk persistence.

    The database keeps tables in memory for fast access and delegates
    persistence to a ``StorageEngine``. Call ``save()`` to flush all
    tables to disk, or ``save_table()`` for a single table.

    With ``journal=True`` every commit is written to ``journal.log`` (and,
    with ``sync=True``, forced onto the disk) before the change returns, and
    opening the database recovers everything committed: the saved table
    snapshots are loaded and the journal is replayed on top. A
    ``checkpoint()`` (also run by ``save()``, by ``close()``, and after a
    commit that makes the journal bigger than *checkpoint_bytes*) writes the
    changed tables and starts a fresh journal. Use ``close()`` (or
    ``with Database(...) as db:``) to finish; a closed journalled database
    refuses changes. If one journal write fails (``JournalWriteError``),
    every later commit is refused too until the database is reopened or a
    ``checkpoint()`` succeeds. Only one ``Database`` -- in any program -- may
    have a journalled folder open at a time.

    Args:
        path: Directory where the database stores its files.
        journal: Make every commit durable through a journal.
        sync: With a journal, ``fsync`` every commit (default True). False is
            faster but a power cut can lose recent commits.
        checkpoint_bytes: With a journal, the journal size that triggers a
            checkpoint (default 4 MiB).
        repair: With a journal, open even if its middle is damaged, keeping
            the commits before the damage (default False: raise).

    Raises:
        JournalCorruptError: If the journal is damaged mid-file and *repair*
            is False.
        RecoveryError: If the journal doesn't fit the saved tables.
        DatabaseError: If *journal* is False but the folder holds a
            journalled database.
        DatabaseLockedError: If another program (or another ``Database``
            here) has the journalled folder open.

    """

    __slots__ = (
        "__weakref__",
        "_checkpoint_bytes",
        "_closed",
        "_commits",
        "_crash_after",
        "_dropped",
        "_lock",
        "_path",
        "_release_store",
        "_storage",
        "_store",
        "_tables",
        "_views",
    )

    def __init__(
        self,
        path: str | Path,
        *,
        journal: bool = False,
        sync: bool = True,
        checkpoint_bytes: int = 4 * 1024 * 1024,
        repair: bool = False,
    ) -> None:
        """Create or open a database at the given path."""
        self._path = Path(path)
        if not journal and is_journalled_folder(self._path):
            msg = f"{self._path} holds a journalled database; open it with journal=True"
            raise DatabaseError(msg)
        self._storage = StorageEngine(data_dir=self._path)
        self._tables: dict[str, Table] = {}
        self._views: dict[str, Query] = {}
        # Tables dropped since the last save -- their files go on save().
        self._dropped: set[str] = set()
        self._commits = CommitRouter()
        self._checkpoint_bytes = checkpoint_bytes
        self._lock: LockType = threading.RLock()
        self._store: JournalStore | None = None
        self._closed = False
        self._release_store: Callable[[], object] | None = None
        # Test hook: a checkpoint step after which the process dies (see pydb.checkpoint).
        self._crash_after: str | None = None
        if journal:
            store = JournalStore(self._path, sync=sync)
            try:
                tables = store.recover(repair=repair)
            except BaseException:
                store.close()
                raise
            self._store = store
            # Forgotten without close()? Let go of the journal and LOCK anyway.
            self._release_store = weakref.finalize(self, store.close)
            for table in tables.values():
                self._adopt(table)

    @property
    def lock(self) -> LockType:
        """Return the re-entrant lock every public method (and every table) holds.

        Hold it yourself to make several calls in a row as one turn::

            with db.lock:
                rows = db.get_table("cards").select()
        """
        return self._lock

    def _live_store(self) -> JournalStore | None:
        """Return the journal store (None without a journal).

        Raises:
            DatabaseError: If the journalled database has been closed.

        """
        if self._closed:
            msg = "The database is closed; open it again to make changes"
            raise DatabaseError(msg)
        return self._store

    def __enter__(self) -> Database:
        """Return the database for use in a ``with`` block."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the database at the end of a ``with`` block.

        If the block is already failing, a failing closing checkpoint is
        logged instead, so the block's own error is the one you see.
        """
        if exc is None:
            self.close()
            return
        try:
            self.close()
        except Exception:
            logger.exception("closing checkpoint failed while handling another error")

    def close(self) -> None:
        """Take a checkpoint, release the journal and delete ``LOCK``.

        Does nothing without a journal, or when already closed. Inside an
        open transaction the checkpoint is skipped: the committed work is
        already safe in the journal.
        """
        store = self._store
        if store is None:
            return
        try:
            if not self._commits.batching:
                self.checkpoint()
        finally:
            self._store = None
            self._closed = True
            if self._release_store is not None:
                self._release_store()  # runs store.close() exactly once

    def checkpoint(self) -> None:
        """Write every table changed since the last checkpoint, then start a fresh journal.

        Snapshots of the changed tables are written first, then
        ``checkpoint.json``, then the journal is replaced by an empty one;
        a crash between any two steps still reopens to exactly the last
        committed state (see ``pydb.checkpoint``). Files of dropped tables
        are deleted.

        Raises:
            DatabaseError: Without a journal, once closed, or inside an open
                transaction.

        """
        store = self._live_store()
        if store is None:
            msg = "checkpoint() needs a journalled database (journal=True)"
            raise DatabaseError(msg)
        if self._commits.batching:
            msg = "Cannot checkpoint inside an open transaction"
            raise DatabaseError(msg)
        store.checkpoint(self._tables, crash_after=self._crash_after)
        self._dropped.clear()

    def after_commit(self) -> None:
        """Checkpoint if the journal grew past ``checkpoint_bytes`` (called after a commit).

        Tables, ``create_table``/``drop_table`` and ``Transaction.commit``
        call this *after* a commit is complete, outside the step that would
        undo it -- the commit is already durable, so nothing here may undo
        it. An error is logged and the next commit tries again; an
        interrupt (``KeyboardInterrupt``) propagates, but the commit stands.
        Inside an open transaction this does nothing.
        """
        if (
            self._store is None
            or self._commits.batching
            or self._store.size_bytes <= self._checkpoint_bytes
        ):
            return
        try:
            self.checkpoint()
        except Exception:  # the commit is durable: never undo or fail it
            logger.exception("automatic checkpoint failed; the journal keeps everything")

    @property
    def path(self) -> Path:
        """Return the database directory path."""
        return self._path

    @property
    def in_transaction(self) -> bool:
        """Return whether a transaction is open (holding changes until it commits)."""
        return self._commits.batching

    @property
    def journalled(self) -> bool:
        """Return whether commits are written to a journal."""
        return self._store is not None

    def replace_table(self, table: Table) -> None:
        """Replace a table in the catalog (used by transactions for rollback).

        Nothing is recorded; the table is hooked up to record its own
        future changes.

        Args:
            table: The table to insert or replace.

        """
        self._adopt(table)

    # -- change recording ----------------------------------------------------

    def add_commit_listener(self, listener: CommitListener) -> None:
        """Call *listener* once per commit with the changes, before the commit counts.

        If it raises, the commit is undone and the error propagates; see
        ``pydb.commits.CommitRouter`` for the details.
        """
        self._commits.add(listener)

    def remove_commit_listener(self, listener: CommitListener) -> None:
        """Stop calling a listener added with ``add_commit_listener``."""
        self._commits.remove(listener)

    def clear_commit_listeners(self) -> None:
        """Stop calling every commit listener."""
        self._commits.clear()

    def _adopt(self, table: Table) -> None:
        """Put *table* in the catalog, sharing this database's lock and recording.

        A different table already under that name is detached.
        """
        replaced = self._tables.get(table.name)
        if replaced is not None and replaced is not table:
            self._detach(replaced)
        table.share_lock(self._lock)
        table.set_sink(self._on_change, after=self.after_commit)
        self._tables[table.name] = table

    def _detach(self, table: Table) -> None:
        """Cut off a table this database stops managing (dropped, rolled back, replaced).

        Somebody may still hold the old ``Table``. On a journalled database
        every later change through it is refused with ``DatabaseError`` (and
        undone), so the journal never mentions a table that isn't there. On
        a plain database it becomes a table of its own, as before: changes
        stay in its memory and are never recorded or saved.
        """
        table.set_sink(_refuse_detached if self._store is not None else None)

    def _on_change(self, change: Change) -> None:
        """Commit *change* now, or keep it for the active transaction."""
        if not self._commits.batching:  # (a held change is checked at commit)
            self._live_store()  # a closed journalled database refuses changes
        with commit_point(self._store):
            self._commits.route(change, self._write_journal)

    def _write_journal(self, changes: Sequence[Change]) -> None:
        """Append a commit to the journal, if any: the commit point (runs last)."""
        if self._store is not None:
            self._store.record(changes)

    def begin_batch(self) -> None:
        """Start holding changes for a transaction (called by ``Transaction``).

        Raises:
            DatabaseError: If a transaction is already holding changes.

        """
        if self._commits.batching:
            msg = "Another transaction is already active on this database"
            raise DatabaseError(msg)
        self._commits.begin()

    def commit_batch(self) -> None:
        """Deliver the held changes as one commit, then stop holding (``Transaction``)."""
        self._live_store()
        with commit_point(self._store):
            self._commits.finish(self._write_journal)

    def discard_batch(self) -> None:
        """Throw away the held changes and stop holding (``Transaction.rollback``)."""
        self._commits.discard()

    def create_table(self, name: str, schema: Schema) -> Table:
        """Create a new table in the database.

        Args:
            name: The table name (must be unique).
            schema: The schema for the new table.

        Returns:
            The newly created table.

        Raises:
            DatabaseError: If a table with that name already exists.

        """
        if name in self._tables:
            msg = f"Table {name!r} already exists"
            raise DatabaseError(msg)
        table = Table(name=name, schema=schema)
        self._adopt(table)
        was_dropped = name in self._dropped
        self._dropped.discard(name)
        try:
            self._on_change(Change(ChangeKind.CREATE_TABLE, name, schema=schema))
        except BaseException as exc:
            if is_committed(exc):
                raise
            self._detach(self._tables.pop(name))
            if was_dropped:
                self._dropped.add(name)
            raise
        self.after_commit()
        return table

    def get_table(self, name: str) -> Table:
        """Return the table with the given name.

        Args:
            name: The table name.

        Returns:
            The requested table.

        Raises:
            DatabaseError: If no table with that name exists.

        """
        table = self._tables.get(name)
        if table is None:
            msg = f"Table {name!r} does not exist"
            raise DatabaseError(msg)
        return table

    def drop_table(self, name: str) -> None:
        """Remove a table from the database.

        Like every other change, this only reaches the disk when you call
        ``save()`` -- that's when the table's data file is deleted. Quit
        without saving and the table is still there next time.
        (On a journalled database the drop is durable at once, and the
        table's snapshot file goes at the next checkpoint.)

        The old ``Table`` object is detached (see ``_detach``): when
        journalled, changes made through it afterwards raise
        ``DatabaseError``.

        Args:
            name: The table name.

        Raises:
            DatabaseError: If no table with that name exists.

        """
        if name not in self._tables:
            msg = f"Table {name!r} does not exist"
            raise DatabaseError(msg)
        table = self._tables.pop(name)
        was_dropped = name in self._dropped
        self._dropped.add(name)
        self._detach(table)
        try:
            self._on_change(Change(ChangeKind.DROP_TABLE, name))
        except BaseException as exc:
            if is_committed(exc):
                raise
            # A note can't carry a whole table, so put the very same one back.
            self._adopt(table)
            if not was_dropped:
                self._dropped.discard(name)
            raise
        self.after_commit()

    def table_names(self) -> list[str]:
        """Return the names of all tables in the database."""
        return sorted(self._tables.keys())

    def create_view(self, name: str, query: Query) -> None:
        """Create a named view (a saved query).

        Args:
            name: The view name.
            query: The SELECT query that defines the view.

        Raises:
            DatabaseError: If a view or table with that name already exists.

        """
        if name in self._views or name in self._tables:
            msg = f"Name {name!r} already exists"
            raise DatabaseError(msg)
        self._views[name] = query

    def drop_view(self, name: str) -> None:
        """Remove a view by name.

        Args:
            name: The view name.

        Raises:
            DatabaseError: If no view with that name exists.

        """
        if name not in self._views:
            msg = f"View {name!r} does not exist"
            raise DatabaseError(msg)
        del self._views[name]

    def get_view(self, name: str) -> Query | None:
        """Return the query for a view, or None if not a view."""
        return self._views.get(name)

    def view_names(self) -> list[str]:
        """Return the names of all views in the database."""
        return sorted(self._views.keys())

    def save(self) -> None:
        """Save all tables to disk, and delete the files of dropped tables.

        Each table's file is replaced safely on its own, but a save of
        several tables is not one all-or-nothing step: a crash part-way can
        leave some tables saved and others not.

        On a journalled database this takes a ``checkpoint()`` instead
        (every commit is already safe in the journal).
        """
        if self._live_store() is not None:
            self.checkpoint()
            return
        for table in self._tables.values():
            self._storage.save_table_object(table)
        for name in self._dropped:
            if name not in self._tables and self._storage.table_exists(name):
                self._storage.delete_table(name)
        self._dropped.clear()

    def save_table(self, name: str) -> None:
        """Save a single table to disk (a ``checkpoint()`` on a journalled database).

        Args:
            name: The table name.

        Raises:
            DatabaseError: If no table with that name exists.

        """
        table = self.get_table(name)
        if self._live_store() is None:
            self._storage.save_table_object(table)
        else:
            self.checkpoint()

    def load(self) -> None:
        """Load all tables from disk into memory.

        Any tables already in memory are replaced by the on-disk versions.

        On a journalled database this does nothing: opening it already
        loaded everything, and the files on disk may be older than the
        journal.
        """
        if self._live_store() is not None:
            return
        self._dropped.clear()
        for name in self._storage.list_tables():
            self._adopt(self._storage.load_table_object(name))

    def load_table(self, name: str) -> Table:
        """Load a single table from disk into memory.

        Args:
            name: The table name.

        Returns:
            The loaded table.

        On a journalled database this just returns the table already in
        memory (see ``load()``).

        Raises:
            DatabaseError: If no data file exists for the table (or, when
                journalled, no such table exists).

        """
        if self._live_store() is not None:
            return self.get_table(name)
        table = self._storage.load_table_object(name)
        self._adopt(table)
        return table
