"""A single row of data in a table.

Think of a record as one trading card in a binder. It has a unique serial
number (the ID) and a set of facts printed on it (the data fields).
"""

from typing import Any


# Type alias for values stored in a record.
Value = str | int | float | bool


class Record:
    """Represent a single row in a database table.

    Each record has a unique numeric ID and a dictionary of column-name to
    value mappings.

    Args:
        record_id: Unique identifier for this record.
        data: Column-name-to-value mapping.
    """

    __slots__ = ("_data", "_record_id")

    def __init__(self, record_id: int, data: dict[str, Value]) -> None:
        """Create a new record with the given ID and data."""
        self._record_id = record_id
        self._data = dict(data)

    @property
    def record_id(self) -> int:
        """Return the unique identifier for this record."""
        return self._record_id

    @property
    def data(self) -> dict[str, Value]:
        """Return a copy of the record's data."""
        return dict(self._data)

    @property
    def columns(self) -> list[str]:
        """Return the column names present in this record."""
        return list(self._data.keys())

    def __getitem__(self, column: str) -> Value:
        """Return the value for the given column name.

        Args:
            column: The column name to look up.

        Raises:
            KeyError: If the column does not exist in this record.
        """
        return self._data[column]

    def __eq__(self, other: object) -> bool:
        """Check equality based on record ID."""
        if not isinstance(other, Record):
            return NotImplemented
        return self._record_id == other._record_id

    def __hash__(self) -> int:
        """Hash based on record ID."""
        return hash(self._record_id)

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"Record(record_id={self._record_id}, data={self._data})"

    def _update(self, values: dict[str, Value]) -> None:
        """Update fields in this record.

        Args:
            values: Column-name-to-value mapping of fields to update.

        Raises:
            KeyError: If a column name does not exist in this record.
        """
        for column, value in values.items():
            if column not in self._data:
                msg = f"Unknown column: {column!r}"
                raise KeyError(msg)
            self._data[column] = value

    # Pyright needs this to accept Any for dict-style access patterns.
    def get(self, column: str, default: Any = None) -> Any:  # noqa: ANN401
        """Return the value for a column, or a default if not found.

        Args:
            column: The column name to look up.
            default: Value to return if the column is not found.
        """
        return self._data.get(column, default)
