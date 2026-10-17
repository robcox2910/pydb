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
"""

from pathlib import Path

from pydb.changes import Change, ChangeKind, CommitListener
from pydb.errors import PyDBError
from pydb.query import Query
from pydb.schema import Schema
from pydb.storage import StorageEngine
from pydb.table import Table


class DatabaseError(PyDBError):
    """Raise when a database-level operation fails."""


class Database:
    """Manage a collection of named tables with disk persistence.

    The database keeps tables in memory for fast access and delegates
    persistence to a ``StorageEngine``. Call ``save()`` to flush all
    tables to disk, or ``save_table()`` for a single table.

    Args:
        path: Directory where the database stores its files.

    """

    __slots__ = ("_batch", "_dropped", "_listeners", "_path", "_storage", "_tables", "_views")

    def __init__(self, path: str | Path) -> None:
        """Create or open a database at the given path."""
        self._path = Path(path)
        self._storage = StorageEngine(data_dir=self._path)
        self._tables: dict[str, Table] = {}
        self._views: dict[str, Query] = {}
        # Tables dropped since the last save -- their files go on save().
        self._dropped: set[str] = set()
        self._listeners: list[CommitListener] = []
        # Changes waiting for the active transaction to commit (None = no transaction).
        self._batch: list[Change] | None = None

    @property
    def path(self) -> Path:
        """Return the database directory path."""
        return self._path

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
        later listener's failure then undid.

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

    def _commit(self, changes: list[Change]) -> None:
        """Deliver one commit's changes to every listener (none if empty)."""
        if not changes:
            return
        for listener in list(self._listeners):
            listener(list(changes))

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
        """
        for table in self._tables.values():
            self._save_one(table)
        for name in self._dropped:
            if name not in self._tables and self._storage.table_exists(name):
                self._storage.delete_table(name)
        self._dropped.clear()

    def save_table(self, name: str) -> None:
        """Save a single table to disk.

        Args:
            name: The table name.

        Raises:
            DatabaseError: If no table with that name exists.

        """
        table = self.get_table(name)
        self._save_one(table)

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

        """
        self._dropped.clear()
        for name in self._storage.list_tables():
            self._load_one(name)

    def load_table(self, name: str) -> Table:
        """Load a single table from disk into memory.

        Args:
            name: The table name.

        Returns:
            The loaded table.

        Raises:
            DatabaseError: If no data file exists for the table.

        """
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
