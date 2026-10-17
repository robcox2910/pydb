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
stays short (see ``pydb.durable`` and ``pydb.checkpoint``).
"""

import logging
from pathlib import Path
from types import TracebackType

from pydb.changes import Change, ChangeKind, CommitListener
from pydb.checkpoint import Checkpointer, is_journalled_folder
from pydb.durable import JOURNAL_FILE
from pydb.errors import PyDBError
from pydb.journal import Journal
from pydb.query import Query
from pydb.schema import Schema
from pydb.storage import StorageEngine
from pydb.table import Table

_log = logging.getLogger(__name__)


class DatabaseError(PyDBError):
    """Raise when a database-level operation fails."""


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
    ``with Database(...) as db:``) to finish. If one journal write fails
    (``JournalWriteError``), every later commit is refused too until the
    database is reopened or a ``checkpoint()`` succeeds.

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

    """

    __slots__ = (
        "_batch",
        "_checkpoint_bytes",
        "_checkpointer",
        "_crash_after",
        "_dropped",
        "_journal",
        "_listeners",
        "_path",
        "_storage",
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
        self._listeners: list[CommitListener] = []
        # Changes waiting for the active transaction to commit (None = no transaction).
        self._batch: list[Change] | None = None
        self._checkpoint_bytes = checkpoint_bytes
        self._journal: Journal | None = None
        self._checkpointer: Checkpointer | None = None
        # Test hook: a checkpoint step after which the process dies (see pydb.checkpoint).
        self._crash_after: str | None = None
        if journal:
            self._journal = Journal(self._path / JOURNAL_FILE, sync=sync)
            self._checkpointer = Checkpointer(self._path, self._journal)
            for table in self._checkpointer.recover(repair=repair).values():
                self._adopt(table)

    def __enter__(self) -> Database:
        """Return the database for use in a ``with`` block."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the database at the end of a ``with`` block."""
        self.close()

    def close(self) -> None:
        """Take a checkpoint and release the journal (does nothing without one).

        Inside an open transaction the checkpoint is skipped: the committed
        work is already safe in the journal.
        """
        if self._journal is None:
            return
        try:
            if self._batch is None:
                self.checkpoint()
        finally:
            self._journal.close()

    def checkpoint(self) -> None:
        """Write every table changed since the last checkpoint, then start a fresh journal.

        Snapshots of the changed tables are written first, then
        ``checkpoint.json``, then the journal is replaced by an empty one;
        a crash between any two steps still reopens to exactly the last
        committed state (see ``pydb.checkpoint``). Files of dropped tables
        are deleted.

        Raises:
            DatabaseError: Without a journal, or inside an open transaction.

        """
        if self._checkpointer is None:
            msg = "checkpoint() needs a journalled database (journal=True)"
            raise DatabaseError(msg)
        if self._batch is not None:
            msg = "Cannot checkpoint inside an open transaction"
            raise DatabaseError(msg)
        self._checkpointer.checkpoint(self._tables, crash_after=self._crash_after)
        self._dropped.clear()

    def _maybe_checkpoint(self) -> None:
        """Checkpoint if the journal grew past ``checkpoint_bytes`` (errors are logged).

        The commit before this is already durable, so a failure here must
        not undo it: it is logged and the next commit tries again.
        """
        if self._journal is None or self._journal.size_bytes <= self._checkpoint_bytes:
            return
        try:
            self.checkpoint()
        except (OSError, PyDBError) as exc:
            _log.warning("automatic checkpoint failed (%s); the journal keeps everything", exc)

    @property
    def path(self) -> Path:
        """Return the database directory path."""
        return self._path

    @property
    def journalled(self) -> bool:
        """Return whether commits are written to a journal."""
        return self._journal is not None

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
        """Call *listener* once per commit with the list of changes committed.

        The listener runs before the commit counts as done. If it raises,
        the change is undone in memory and the error propagates: a single
        change made outside a transaction is reversed, and a failing
        ``Transaction.commit()`` rolls the transaction back. A commit with
        no changes does not call listeners.

        Listeners must treat the changes as read-only -- never mutate
        ``Change.data``. With several listeners, they run in the order
        added, so an earlier listener may already have seen a batch that a
        later listener's failure then undid. On a journalled database the
        journal write comes after every listener, so a listener may also see
        a batch that a failed journal write then undoes.

        Args:
            listener: A function taking a list of ``Change`` notes.

        """
        self._listeners.append(listener)

    def remove_commit_listener(self, listener: CommitListener) -> None:
        """Stop calling a listener added with ``add_commit_listener``."""
        self._listeners.remove(listener)

    def clear_commit_listeners(self) -> None:
        """Stop calling every commit listener."""
        self._listeners.clear()

    def _adopt(self, table: Table) -> None:
        """Put *table* in the catalog and route its changes to this database."""
        table.set_sink(self._on_change)
        self._tables[table.name] = table

    def _on_change(self, change: Change) -> None:
        """Commit *change* now, or keep it for the active transaction."""
        if self._batch is not None:
            self._batch.append(change)
        else:
            self._commit([change])
            self._maybe_checkpoint()

    def _commit(self, changes: list[Change]) -> None:
        """Deliver one commit's changes to every listener, then to the journal.

        The journal goes last: once its entry is written the commit is
        final, so nothing that runs afterwards can fail and undo a commit
        that is already on disk. A failing append (``JournalWriteError``)
        undoes the commit like any other failing listener.
        """
        if not changes:
            return
        for listener in list(self._listeners):
            listener(list(changes))
        if self._journal is not None and self._checkpointer is not None:
            self._checkpointer.committed(self._journal.append(changes), changes)

    def begin_batch(self) -> None:
        """Start holding changes for a transaction (called by ``Transaction``).

        Raises:
            DatabaseError: If a transaction is already holding changes.

        """
        if self._batch is not None:
            msg = "Another transaction is already active on this database"
            raise DatabaseError(msg)
        self._batch = []

    def commit_batch(self) -> None:
        """Deliver the held changes as one commit, then stop holding (``Transaction``).

        If a listener raises, the batch stays open so the transaction can
        roll back (its undo steps are held and thrown away, not committed).
        """
        self._commit(self._batch or [])
        self._batch = None
        self._maybe_checkpoint()

    def discard_batch(self) -> None:
        """Throw away the held changes and stop holding (``Transaction.rollback``)."""
        self._batch = None

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
        except BaseException:
            del self._tables[name]
            if was_dropped:
                self._dropped.add(name)
            raise
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
        try:
            self._on_change(Change(ChangeKind.DROP_TABLE, name))
        except BaseException:
            # A note can't carry a whole table, so put the very same one back.
            self._tables[name] = table
            if not was_dropped:
                self._dropped.discard(name)
            raise

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
        if self._journal is not None:
            self.checkpoint()
            return
        for table in self._tables.values():
            self._save_one(table)
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
        if self._journal is None:
            self._save_one(table)
        else:
            self.checkpoint()

    def _save_one(self, table: Table) -> None:
        """Save a single table through the storage engine."""
        records = table.select()
        self._storage.save_table(
            name=table.name,
            schema=table.schema,
            records=records,
            next_id=table.next_id,
            indexes=table.index_definitions,
        )

    def load(self) -> None:
        """Load all tables from disk into memory.

        Any tables already in memory are replaced by the on-disk versions.

        On a journalled database this does nothing: opening it already
        loaded everything, and the files on disk may be older than the
        journal.
        """
        if self._journal is not None:
            return
        self._dropped.clear()
        for name in self._storage.list_tables():
            self._load_one(name)

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
        if self._journal is not None:
            return self.get_table(name)
        return self._load_one(name)

    def _load_one(self, name: str) -> Table:
        """Load a single table from the storage engine."""
        stored = self._storage.load_stored_table(name)
        table = Table.from_stored(
            name=stored.name,
            schema=stored.schema,
            records=stored.records,
            next_id=stored.next_id,
            indexes=stored.indexes,
        )
        self._adopt(table)
        return table
