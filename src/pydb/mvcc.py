"""Multi-Version Concurrency Control (MVCC).

MVCC keeps multiple versions of each row so readers and writers don't
block each other. Each row has a "created at" and "deleted at" version
number. A reader at version 5 only sees rows created at version <= 5
that haven't been deleted yet (or were deleted after version 5).

Think of it like timestamped cards -- each reader sees a consistent
snapshot based on when they started looking.
"""

from dataclasses import dataclass

from pydb.record import Value


@dataclass(slots=True)
class VersionedRow:
    """A row with version metadata for MVCC.

    Args:
        row_id: Unique identifier for this row.
        data: The column values.
        created_at: The version (transaction ID) that created this row.
        deleted_at: The version that deleted this row (None if active).

    """

    row_id: int
    data: dict[str, Value]
    created_at: int
    deleted_at: int | None = None

    def visible_at(self, version: int) -> bool:
        """Check whether this row is visible at the given version.

        A row is visible if:
        - It was created at or before this version, AND
        - It has not been deleted, OR was deleted after this version.

        Args:
            version: The reader's snapshot version.

        Returns:
            True if the row should be visible to this reader.

        """
        if self.created_at > version:
            return False
        return self.deleted_at is None or self.deleted_at > version


class MVCCStore:
    """A versioned row store implementing MVCC.

    Tracks a global version counter and stores all row versions.
    Readers get a consistent snapshot; writers create new versions.

    """

    __slots__ = ("_current_version", "_next_row_id", "_rows")

    def __init__(self) -> None:
        """Create an empty MVCC store."""
        self._rows: list[VersionedRow] = []
        self._current_version = 0
        self._next_row_id = 1

    @property
    def current_version(self) -> int:
        """Return the current global version number."""
        return self._current_version

    def begin_version(self) -> int:
        """Start a new version (transaction) and return its ID.

        Returns:
            The new version number.

        """
        self._current_version += 1
        return self._current_version

    def insert(self, data: dict[str, Value], version: int) -> int:
        """Insert a new row at the given version.

        Args:
            data: The row data.
            version: The transaction version creating this row.

        Returns:
            The new row's ID.

        """
        row_id = self._next_row_id
        self._next_row_id += 1
        self._rows.append(VersionedRow(row_id=row_id, data=dict(data), created_at=version))
        return row_id

    def delete(self, row_id: int, version: int) -> bool:
        """Mark a row as deleted at the given version.

        Args:
            row_id: The row to delete.
            version: The transaction version deleting this row.

        Returns:
            True if the row was found and marked deleted.

        """
        for row in self._rows:
            if row.row_id == row_id and row.deleted_at is None:
                row.deleted_at = version
                return True
        return False

    def read(self, version: int) -> list[VersionedRow]:
        """Return all rows visible at the given version.

        Args:
            version: The reader's snapshot version.

        Returns:
            A list of visible rows.

        """
        return [row for row in self._rows if row.visible_at(version)]

    def vacuum(self, oldest_active_version: int) -> int:
        """Remove row versions that are no longer needed.

        Rows deleted before the oldest active version can never be seen
        by any current reader, so they can be safely removed.

        Args:
            oldest_active_version: The oldest version still in use.

        Returns:
            The number of rows cleaned up.

        """
        before = len(self._rows)
        self._rows = [
            row
            for row in self._rows
            if row.deleted_at is None or row.deleted_at > oldest_active_version
        ]
        return before - len(self._rows)
