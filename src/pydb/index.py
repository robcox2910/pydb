"""Database index -- a fast lookup structure for a table column.

An index is like the index at the back of a textbook. Instead of reading
every page to find "dinosaurs", you look it up in the index and jump
straight to page 42. A database index does the same thing -- it maps
column values to record IDs so you can find records without scanning
the whole table.
"""

from pydb.btree import DEFAULT_ORDER, BTree, BTreeKey


class Index:
    """Wrap a B-Tree to provide a named index on a table column.

    Each index tracks one column. When you insert a value and record ID,
    the index stores the mapping so later searches are fast.

    Args:
        name: A name for this index (e.g., "idx_name").
        column: The column this index covers.
        order: B-Tree order (max children per node).

    """

    __slots__ = ("_column", "_name", "_tree")

    def __init__(self, name: str, column: str, order: int = DEFAULT_ORDER) -> None:
        """Create an empty index for the given column."""
        self._name = name
        self._column = column
        self._tree = BTree(order=order)

    @property
    def name(self) -> str:
        """Return the index name."""
        return self._name

    @property
    def column(self) -> str:
        """Return the column this index covers."""
        return self._column

    def insert(self, key: BTreeKey, record_id: int) -> None:
        """Add a key-record mapping to the index.

        Args:
            key: The column value.
            record_id: The record ID that has this value.

        """
        self._tree.insert(key, record_id)

    def delete(self, key: BTreeKey, record_id: int) -> bool:
        """Remove a key-record mapping from the index.

        Args:
            key: The column value.
            record_id: The record ID to disassociate.

        Returns:
            True if the mapping was found and removed.

        """
        return self._tree.delete(key, record_id)

    def find(self, key: BTreeKey) -> list[int]:
        """Find all record IDs with the given column value.

        Args:
            key: The column value to search for.

        Returns:
            A list of matching record IDs (empty if none found).

        """
        return self._tree.search(key)

    def find_range(self, min_key: BTreeKey, max_key: BTreeKey) -> list[int]:
        """Find all record IDs with column values in [min_key, max_key].

        Args:
            min_key: Lower bound (inclusive).
            max_key: Upper bound (inclusive).

        Returns:
            A sorted list of unique matching record IDs.

        """
        return self._tree.find_range(min_key, max_key)

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"Index(name={self._name!r}, column={self._column!r})"
