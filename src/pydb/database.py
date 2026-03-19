"""The database -- a collection room for tables.

A database is the whole room full of shelves. It knows which binders (tables)
exist, creates new ones, drops old ones, and coordinates saving and loading
through the storage engine.
"""

from pathlib import Path

from pydb.errors import PyDBError
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

    __slots__ = ("_path", "_storage", "_tables")

    def __init__(self, path: str | Path) -> None:
        """Create or open a database at the given path."""
        self._path = Path(path)
        self._storage = StorageEngine(data_dir=self._path)
        self._tables: dict[str, Table] = {}

    @property
    def path(self) -> Path:
        """Return the database directory path."""
        return self._path

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
        self._tables[name] = table
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
        """Remove a table from the database and delete its data file.

        Args:
            name: The table name.

        Raises:
            DatabaseError: If no table with that name exists.

        """
        if name not in self._tables:
            msg = f"Table {name!r} does not exist"
            raise DatabaseError(msg)
        del self._tables[name]
        if self._storage.table_exists(name):
            self._storage.delete_table(name)

    def table_names(self) -> list[str]:
        """Return the names of all tables in the database."""
        return sorted(self._tables.keys())

    def save(self) -> None:
        """Save all tables to disk."""
        for table in self._tables.values():
            self._save_one(table)

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
        )

    def load(self) -> None:
        """Load all tables from disk into memory.

        Any tables already in memory are replaced by the on-disk versions.

        """
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
        loaded_name, schema, records, next_id = self._storage.load_table(name)
        table = Table.from_stored(
            name=loaded_name,
            schema=schema,
            records=records,
            next_id=next_id,
        )
        self._tables[loaded_name] = table
        return table
