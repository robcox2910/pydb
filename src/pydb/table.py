"""A table -- a collection of records sharing the same schema.

Think of a table as a binder full of trading cards. Every card in the binder
follows the same template (schema), and each card has a unique serial number
(record ID). You can add cards, find cards, change cards, and remove cards.
"""

from collections.abc import Callable, Mapping

from pydb.errors import RecordNotFoundError
from pydb.record import Record, Value
from pydb.schema import Schema


class Table:
    """Represent a database table that holds records.

    The table enforces its schema on every insert and update, assigns
    auto-incrementing IDs, and provides basic CRUD operations.

    Args:
        name: The name of this table (e.g., "pokemon_cards").
        schema: The schema that all records in this table must follow.

    """

    __slots__ = ("_name", "_next_id", "_records", "_schema")

    def __init__(self, name: str, schema: Schema) -> None:
        """Create a new empty table with the given name and schema."""
        self._name = name
        self._schema = schema
        self._records: dict[int, Record] = {}
        self._next_id = 1

    @property
    def name(self) -> str:
        """Return the table name."""
        return self._name

    @property
    def schema(self) -> Schema:
        """Return the table's schema."""
        return self._schema

    @classmethod
    def from_stored(
        cls,
        name: str,
        schema: Schema,
        records: list[Record],
        next_id: int,
    ) -> Table:
        """Reconstruct a table from stored components.

        Used by the storage engine to rebuild a table loaded from disk.

        Args:
            name: The table name.
            schema: The table schema.
            records: The records to populate the table with.
            next_id: The next auto-increment ID.

        Returns:
            A fully populated table.

        """
        table = cls(name=name, schema=schema)
        table._next_id = next_id
        for record in records:
            table._records[record.record_id] = record
        return table

    @property
    def next_id(self) -> int:
        """Return the next auto-increment ID (used by the storage engine)."""
        return self._next_id

    @property
    def row_count(self) -> int:
        """Return the number of records in the table."""
        return len(self._records)

    def insert(self, values: Mapping[str, Value]) -> Record:
        """Insert a new record into the table.

        The schema validates the values before insertion. The table assigns
        the next available ID automatically.

        Args:
            values: Column-name-to-value mapping for the new record.

        Returns:
            The newly created record with its assigned ID.

        Raises:
            SchemaError: If the values don't conform to the table's schema.

        """
        self._schema.validate(values)
        record_id = self._next_id
        self._next_id += 1
        record = Record(record_id=record_id, data=values)
        self._records[record_id] = record
        return record

    def select(
        self,
        where: Callable[[Record], bool] | None = None,
    ) -> list[Record]:
        """Return records from the table, optionally filtered.

        Args:
            where: An optional predicate function. Only records for which
                   this function returns True are included in the result.

        Returns:
            A list of matching records, ordered by ID.

        """
        records = sorted(self._records.values(), key=lambda r: r.record_id)
        if where is None:
            return records
        return [r for r in records if where(r)]

    def get(self, record_id: int) -> Record:
        """Return the record with the given ID.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            The matching record.

        Raises:
            RecordNotFoundError: If no record with that ID exists.

        """
        record = self._records.get(record_id)
        if record is None:
            msg = f"No record with id={record_id} in table {self._name!r}"
            raise RecordNotFoundError(msg)
        return record

    def update(self, record_id: int, values: Mapping[str, Value]) -> Record:
        """Update fields of an existing record.

        Only the columns specified in *values* are changed; other columns
        keep their current values. The schema validates the new values.

        Args:
            record_id: The ID of the record to update.
            values: Column-name-to-value mapping of fields to change.

        Returns:
            The updated record.

        Raises:
            RecordNotFoundError: If no record with that ID exists.
            SchemaError: If the new values don't conform to the schema.

        """
        record = self.get(record_id)
        # Build the full set of values for validation.
        merged = record.data
        merged.update(values)
        self._schema.validate(merged)
        record.update_fields(values)
        return record

    def delete(self, record_id: int) -> None:
        """Remove a record from the table.

        Args:
            record_id: The ID of the record to delete.

        Raises:
            RecordNotFoundError: If no record with that ID exists.

        """
        if record_id not in self._records:
            msg = f"No record with id={record_id} in table {self._name!r}"
            raise RecordNotFoundError(msg)
        del self._records[record_id]

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"Table(name={self._name!r}, rows={self.row_count})"
