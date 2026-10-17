"""A table -- a collection of records sharing the same schema.

Think of a table as a binder full of trading cards. Every card in the binder
follows the same template (schema), and each card has a unique serial number
(record ID). You can add cards, find cards, change cards, and remove cards.

Tables can also have **indexes** -- like a card catalog that makes finding
specific cards much faster.

A table that belongs to a ``Database`` also writes a note (a ``Change``)
after every successful change and hands it to its *sink* (see
``pydb.changes``). If the sink refuses the note -- say the journal could not
write it -- the table takes the change back before passing the error on, so
memory never holds a change nobody recorded.
"""

from collections.abc import Callable, Mapping, Sequence

from pydb.changes import Change, ChangeKind, ChangeSink
from pydb.errors import PyDBError, RecordNotFoundError, SchemaError
from pydb.index import Index
from pydb.record import Record, Value
from pydb.schema import Schema


class TableIndexError(PyDBError):
    """Raise when an index operation fails."""


class Table:
    """Represent a database table that holds records and indexes.

    The table enforces its schema on every insert and update, assigns
    auto-incrementing IDs, provides basic CRUD operations, and
    automatically maintains any indexes on insert, update, and delete.

    Args:
        name: The name of this table (e.g., "pokemon_cards").
        schema: The schema that all records in this table must follow.

    """

    __slots__ = ("_after_sink", "_indexes", "_name", "_next_id", "_records", "_schema", "_sink")

    def __init__(self, name: str, schema: Schema) -> None:
        """Create a new empty table with the given name and schema."""
        self._name = name
        self._schema = schema
        self._records: dict[int, Record] = {}
        self._next_id = 1
        self._indexes: dict[str, Index] = {}
        self._sink: ChangeSink | None = None
        self._after_sink: Callable[[], None] | None = None

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
        indexes: Sequence[tuple[str, str]] = (),
    ) -> Table:
        """Reconstruct a table from stored components.

        Used by the storage engine to rebuild a table loaded from disk, and
        by transactions to restore a snapshot. Nothing is recorded.

        Args:
            name: The table name.
            schema: The table schema.
            records: The records to populate the table with.
            next_id: The next auto-increment ID.
            indexes: ``(name, column)`` pairs of indexes to rebuild.

        Returns:
            A fully populated table.

        """
        table = cls(name=name, schema=schema)
        table._next_id = next_id
        for record in records:
            table._records[record.record_id] = record
        for index_name, column in indexes:
            table._add_index(index_name, column)
        return table

    def set_sink(self, sink: ChangeSink | None, after: Callable[[], None] | None = None) -> None:
        """Choose the function that receives a ``Change`` after each change.

        The owning ``Database`` calls this; ``None`` stops recording.

        Args:
            sink: Receives each change; if it raises, the change is undone.
            after: Called once the sink accepted a change, outside the undo
                step -- an error from it never undoes the change.

        """
        self._sink = sink
        self._after_sink = after

    @property
    def next_id(self) -> int:
        """Return the next auto-increment ID (used by the storage engine)."""
        return self._next_id

    @property
    def row_count(self) -> int:
        """Return the number of records in the table."""
        return len(self._records)

    @property
    def indexes(self) -> dict[str, Index]:
        """Return the table's indexes (name → Index)."""
        return dict(self._indexes)

    @property
    def index_definitions(self) -> list[tuple[str, str]]:
        """Return ``(name, column)`` for each index, oldest first."""
        return [(idx.name, idx.column) for idx in self._indexes.values()]

    def create_index(self, index_name: str, column: str) -> Index:
        """Create a new index on a column.

        Populates the index with all existing records.

        Args:
            index_name: A name for the index.
            column: The column to index.

        Returns:
            The newly created index.

        Raises:
            IndexError: If an index with that name already exists or
                the column doesn't exist in the schema.

        """
        if index_name in self._indexes:
            msg = f"Index {index_name!r} already exists on table {self._name!r}"
            raise TableIndexError(msg)
        if column not in self._schema.column_names:
            msg = f"Column {column!r} does not exist in table {self._name!r}"
            raise TableIndexError(msg)

        idx = self._add_index(index_name, column)
        definition = (index_name, column)
        self._emit(
            Change(ChangeKind.CREATE_INDEX, self._name, index=definition),
            undo=Change(ChangeKind.DROP_INDEX, self._name, index=definition),
        )
        return idx

    def drop_index(self, index_name: str) -> None:
        """Remove an index by name.

        Args:
            index_name: The index to remove.

        Raises:
            IndexError: If no index with that name exists.

        """
        if index_name not in self._indexes:
            msg = f"Index {index_name!r} does not exist on table {self._name!r}"
            raise TableIndexError(msg)
        definition = (index_name, self._indexes[index_name].column)
        self._remove_index(index_name)
        self._emit(
            Change(ChangeKind.DROP_INDEX, self._name, index=definition),
            undo=Change(ChangeKind.CREATE_INDEX, self._name, index=definition),
        )

    def get_index_for_column(self, column: str) -> Index | None:
        """Return an index covering the given column, if one exists."""
        for idx in self._indexes.values():
            if idx.column == column:
                return idx
        return None

    def insert(self, values: Mapping[str, Value]) -> Record:
        """Insert a new record into the table.

        The schema validates the values before insertion. The table assigns
        the next available ID automatically. All indexes are updated.

        Args:
            values: Column-name-to-value mapping for the new record.

        Returns:
            The newly created record with its assigned ID.

        Raises:
            SchemaError: If the values don't conform to the table's schema.

        """
        values = self._schema.coerce(values)
        self._schema.validate(values)
        self._check_constraints(values)
        record_id = self._next_id
        record = self._put_record(record_id, values)
        self._emit(
            Change(ChangeKind.INSERT, self._name, record_id=record_id, data=record.data),
            undo=Change(ChangeKind.DELETE, self._name, record_id=record_id),
        )
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
        # Dict preserves insertion order in Python 3.7+, and IDs are
        # assigned sequentially, so no sort is needed.
        records = list(self._records.values())
        if where is None:
            return records
        return [r for r in records if where(r)]

    def select_by_index(self, index: Index, key: Value) -> list[Record]:
        """Return records matching a key via an index lookup.

        Args:
            index: The index to use for the lookup.
            key: The value to search for.

        Returns:
            A list of matching records.

        """
        record_ids = index.find(key)
        return [self._records[rid] for rid in record_ids if rid in self._records]

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
        All affected indexes are updated.

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
        values = self._schema.coerce(values)
        old = record.data
        # Build the full set of values for validation.
        merged = record.data
        merged.update(values)
        self._schema.validate(merged)
        self._check_constraints(merged, exclude_record_id=record_id)

        self._replace_data(record_id, merged)
        self._emit(
            Change(ChangeKind.UPDATE, self._name, record_id=record_id, data=record.data),
            undo=Change(ChangeKind.UPDATE, self._name, record_id=record_id, data=old),
        )
        return record

    def delete(self, record_id: int) -> None:
        """Remove a record from the table. All indexes are updated.

        Args:
            record_id: The ID of the record to delete.

        Raises:
            RecordNotFoundError: If no record with that ID exists.

        """
        if record_id not in self._records:
            msg = f"No record with id={record_id} in table {self._name!r}"
            raise RecordNotFoundError(msg)

        old = self._records[record_id].data
        self._remove_record(record_id)
        self._emit(
            Change(ChangeKind.DELETE, self._name, record_id=record_id),
            undo=Change(ChangeKind.INSERT, self._name, record_id=record_id, data=old),
        )

    # -- recording -----------------------------------------------------------

    def _emit(self, change: Change, *, undo: Change) -> None:
        """Hand *change* to the sink; if the sink fails, apply *undo* and re-raise.

        Args:
            change: The note describing the change just made in memory.
            undo: The opposite change, which puts memory back as it was.

        """
        if self._sink is None:
            return
        try:
            self._sink(change)
        except BaseException:
            self._apply_change(undo)
            if change.kind is ChangeKind.INSERT and change.record_id is not None:
                # Hand back the ID number the failed insert used up.
                self._next_id = change.record_id
            raise
        if self._after_sink is not None:
            self._after_sink()

    def replay_change(self, change: Change) -> None:
        """Redo a journalled row or index change exactly (used by recovery).

        No checks run and nothing is recorded: the change was checked when
        it first happened. The record keeps its original ID.

        Args:
            change: The change to redo.

        Raises:
            PyDBError: If the change is not a complete row or index change.
            KeyError: If it names a record or index that isn't there.

        """
        self._apply_change(change)

    def _apply_change(self, change: Change) -> None:
        """Apply a row or index change exactly, without checks or recording.

        Used to undo a change whose note was refused, and (through
        ``replay_change``) to replay a journal. Table-level kinds (create/drop table) belong to the
        ``Database``.

        Args:
            change: The change to apply.

        Raises:
            PyDBError: If the change is not a complete row or index change.

        """
        match change:
            case Change(kind=ChangeKind.INSERT, record_id=int() as rid, data=dict() as data):
                self._put_record(rid, data)
            case Change(kind=ChangeKind.UPDATE, record_id=int() as rid, data=dict() as data):
                self._replace_data(rid, data)
            case Change(kind=ChangeKind.DELETE, record_id=int() as rid):
                self._remove_record(rid)
            case Change(kind=ChangeKind.CREATE_INDEX, index=(index_name, column)):
                self._add_index(index_name, column)
            case Change(kind=ChangeKind.DROP_INDEX, index=(index_name, _)):
                self._remove_index(index_name)
            case _:
                msg = f"Cannot apply {change!r} to table {self._name!r}"
                raise PyDBError(msg)

    # -- raw building blocks (no validation, no recording) -------------------

    def _put_record(self, record_id: int, data: Mapping[str, Value]) -> Record:
        """Store a record under *record_id*, keeping records in ID order."""
        record = Record(record_id=record_id, data=data)
        out_of_order = bool(self._records) and record_id < next(reversed(self._records))
        self._records[record_id] = record
        if out_of_order:
            self._records = dict(sorted(self._records.items()))
        self._next_id = max(self._next_id, record_id + 1)
        for idx in self._indexes.values():
            idx.insert(record[idx.column], record_id)
        return record

    def _replace_data(self, record_id: int, data: Mapping[str, Value]) -> None:
        """Overwrite a record's values, moving its index entries to match."""
        record = self._records[record_id]
        for idx in self._indexes.values():
            if record[idx.column] != data[idx.column]:
                idx.delete(record[idx.column], record_id)
                idx.insert(data[idx.column], record_id)
        record.update_fields(data)

    def _remove_record(self, record_id: int) -> None:
        """Remove a record and its index entries."""
        record = self._records.pop(record_id)
        for idx in self._indexes.values():
            idx.delete(record[idx.column], record_id)

    def _add_index(self, index_name: str, column: str) -> Index:
        """Build an index on *column* filled with every current record."""
        idx = Index(name=index_name, column=column)
        for record in self._records.values():
            idx.insert(record[column], record.record_id)
        self._indexes[index_name] = idx
        return idx

    def _remove_index(self, index_name: str) -> None:
        """Forget an index."""
        del self._indexes[index_name]

    def _check_constraints(
        self,
        values: Mapping[str, Value],
        exclude_record_id: int | None = None,
    ) -> None:
        """Check NOT NULL, UNIQUE, and PRIMARY KEY constraints.

        Args:
            values: The column values to check.
            exclude_record_id: A record ID to exclude from uniqueness
                checks (used during UPDATE so the record doesn't
                conflict with itself).

        Raises:
            SchemaError: If any constraint is violated.

        """
        for col in self._schema.columns:
            if not (col.primary_key or col.not_null or col.unique):
                continue

            val = values.get(col.name)

            # NOT NULL / PRIMARY KEY: value must be present.
            if (col.not_null or col.primary_key) and val is None:
                msg = f"Column {col.name!r} cannot be NULL"
                raise SchemaError(msg)

            # UNIQUE / PRIMARY KEY: no duplicate values.
            if (col.unique or col.primary_key) and val is not None:
                for record in self._records.values():
                    if exclude_record_id is not None and record.record_id == exclude_record_id:
                        continue
                    if record[col.name] == val:
                        constraint = "PRIMARY KEY" if col.primary_key else "UNIQUE"
                        msg = f"{constraint} violation: {col.name!r} value {val!r} already exists"
                        raise SchemaError(msg)

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"Table(name={self._name!r}, rows={self.row_count})"
