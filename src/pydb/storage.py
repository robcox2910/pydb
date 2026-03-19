"""Storage engine -- persist tables to disk.

The storage engine is the notebook on the shelf. It takes tables from memory,
writes them to files, and reads them back later. Even if you close Python and
come back tomorrow, your data is still there.

Safety: writes use a temporary file and atomic rename so a crash mid-save
never corrupts your data.
"""

from pathlib import Path

from pydb.errors import PyDBError
from pydb.record import Record
from pydb.schema import Schema
from pydb.serializer import SerializationError, deserialize_table_data, serialize_table_data


class StorageError(PyDBError):
    """Raise when a storage operation fails (missing file, I/O error, etc.)."""


TABLE_FILE_SUFFIX = ".json"
TEMP_FILE_SUFFIX = ".json.tmp"


class StorageEngine:
    """Manage reading and writing table data to the filesystem.

    Each table is stored as a single JSON file inside a data directory.
    Writes use a temp-file-then-rename strategy for crash safety.

    Args:
        data_dir: Path to the directory where table files are stored.

    """

    __slots__ = ("_data_dir",)

    def __init__(self, data_dir: Path) -> None:
        """Create a storage engine backed by the given directory."""
        self._data_dir = data_dir
        self._data_dir.mkdir(parents=True, exist_ok=True)

    @property
    def data_dir(self) -> Path:
        """Return the data directory path."""
        return self._data_dir

    def _table_path(self, table_name: str) -> Path:
        """Return the file path for a table."""
        return self._data_dir / f"{table_name}{TABLE_FILE_SUFFIX}"

    def _temp_path(self, table_name: str) -> Path:
        """Return the temporary file path for a table."""
        return self._data_dir / f"{table_name}{TEMP_FILE_SUFFIX}"

    def save_table(
        self,
        name: str,
        schema: Schema,
        records: list[Record],
        next_id: int,
    ) -> None:
        """Save a table's complete state to disk.

        Uses write-then-rename for crash safety: data is written to a temp
        file first, then atomically renamed to the final path.

        Args:
            name: The table name.
            schema: The table's schema.
            records: All records in the table.
            next_id: The next auto-increment ID.

        Raises:
            StorageError: If the write fails.

        """
        json_str = serialize_table_data(name, schema, records, next_id)
        temp_path = self._temp_path(name)
        final_path = self._table_path(name)

        try:
            temp_path.write_text(json_str, encoding="utf-8")
            temp_path.replace(final_path)
        except OSError as exc:
            msg = f"Failed to save table {name!r}: {exc}"
            raise StorageError(msg) from exc

    def load_table(self, name: str) -> tuple[str, Schema, list[Record], int]:
        """Load a table's complete state from disk.

        Args:
            name: The table name (used to find the file).

        Returns:
            A tuple of (name, schema, records, next_id).

        Raises:
            StorageError: If the file doesn't exist or is corrupted.

        """
        file_path = self._table_path(name)

        if not file_path.exists():
            msg = f"No data file for table {name!r} at {file_path}"
            raise StorageError(msg)

        try:
            json_str = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            msg = f"Failed to read table {name!r}: {exc}"
            raise StorageError(msg) from exc

        try:
            return deserialize_table_data(json_str)
        except SerializationError as exc:
            msg = f"Corrupted data file for table {name!r}: {exc}"
            raise StorageError(msg) from exc

    def delete_table(self, name: str) -> None:
        """Remove a table's data file from disk.

        Args:
            name: The table name.

        Raises:
            StorageError: If the file doesn't exist or can't be deleted.

        """
        file_path = self._table_path(name)

        if not file_path.exists():
            msg = f"No data file for table {name!r} at {file_path}"
            raise StorageError(msg)

        try:
            file_path.unlink()
        except OSError as exc:
            msg = f"Failed to delete table {name!r}: {exc}"
            raise StorageError(msg) from exc

    def table_exists(self, name: str) -> bool:
        """Check whether a data file exists for the given table name."""
        return self._table_path(name).exists()

    def list_tables(self) -> list[str]:
        """Return the names of all tables with data files on disk."""
        return sorted(p.stem for p in self._data_dir.iterdir() if p.suffix == TABLE_FILE_SUFFIX)
