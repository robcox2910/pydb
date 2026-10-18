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

The rows themselves live in a ``RowStore`` (see ``pydb.rows``), which
keeps the indexes and the register of UNIQUE values (see ``pydb.unique``)
in step -- so a UNIQUE or PRIMARY KEY check never walks through every row
-- and, inside a ``Transaction``, writes down how to undo each change.

Every public method holds the table's ``lock`` while it runs (see
``pydb.locking``). A table on its own has a lock of its own; a table that
belongs to a ``Database`` shares the database's lock.
"""

import threading
from collections.abc import Callable, Mapping, Sequence

from pydb.changes import Change, ChangeKind, ChangeSink
from pydb.commits import is_committed
from pydb.errors import PyDBError, RecordNotFoundError
from pydb.index import Index
from pydb.locking import LockType, lock_public_methods
from pydb.record import Record, Value
from pydb.rows import RowStore, UndoLog
from pydb.schema import Schema


class TableIndexError(PyDBError):
    """Raise when an index operation fails."""


@lock_public_methods
class Table:
    """Represent a database table that holds records and indexes.

    The table enforces its schema on every insert and update, assigns
    auto-incrementing IDs, provides basic CRUD operations, and
    automatically maintains any indexes on insert, update, and delete.

    Args:
        name: The name of this table (e.g., "pokemon_cards").
        schema: The schema that all records in this table must follow.

    """

    __slots__ = (
        "_after_sink",
        "_lock",
        "_name",
        "_rows",
        "_schema",
        "_sink",
    )

    def __init__(self, name: str, schema: Schema) -> None:
        """Create a new empty table with the given name and schema."""
        self._name = name
        self._schema = schema
        self._rows = RowStore(schema.unique_columns)
        self._sink: ChangeSink | None = None
        self._after_sink: Callable[[], None] | None = None
        self._lock: LockType = threading.RLock()

    @property
    def lock(self) -> LockType:
        """Return the lock every public method holds (the database's, once adopted)."""
        return self._lock

    def share_lock(self, lock: LockType) -> None:
        """Use *lock* from now on (the owning ``Database`` hands over its own)."""
        self._lock = lock

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

        Used by the storage engine and recovery to rebuild a table loaded
        from disk. Nothing is recorded.

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
        table._rows.reset(records, next_id)
        for index_name, column in indexes:
            table._rows.add_index(index_name, column)
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

    def set_undo_log(self, log: UndoLog | None) -> None:
        """Write how to undo each change into *log* from now on (``None`` stops).

        An open ``Transaction`` uses this, so rollback can take back exactly
        the changes made, without ever copying the table.
        """
        self._rows.undo = log

    @property
    def next_id(self) -> int:
        """Return the next auto-increment ID (used by the storage engine)."""
        return self._rows.next_id

    @property
    def row_count(self) -> int:
        """Return the number of records in the table."""
        return len(self._rows.records)

    @property
    def indexes(self) -> dict[str, Index]:
        """Return the table's indexes (name → Index)."""
        return dict(self._rows.indexes)

    @property
    def index_definitions(self) -> list[tuple[str, str]]:
        """Return ``(name, column)`` for each index, oldest first."""
        return [(idx.name, idx.column) for idx in self._rows.indexes.values()]

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
        if index_name in self._rows.indexes:
            msg = f"Index {index_name!r} already exists on table {self._name!r}"
            raise TableIndexError(msg)
        if column not in self._schema.column_names:
            msg = f"Column {column!r} does not exist in table {self._name!r}"
            raise TableIndexError(msg)

        idx = self._rows.add_index(index_name, column)
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
        if index_name not in self._rows.indexes:
            msg = f"Index {index_name!r} does not exist on table {self._name!r}"
            raise TableIndexError(msg)
        definition = (index_name, self._rows.indexes[index_name].column)
        self._rows.remove_index(index_name)
        self._emit(
            Change(ChangeKind.DROP_INDEX, self._name, index=definition),
            undo=Change(ChangeKind.CREATE_INDEX, self._name, index=definition),
        )

    def get_index_for_column(self, column: str) -> Index | None:
        """Return an index covering the given column, if one exists."""
        for idx in self._rows.indexes.values():
            if idx.column == column:
                return idx
        return None

    def insert(self, values: Mapping[str, Value]) -> Record:
        """Insert a new record into the table.

        A nullable column left out of *values* is stored as NULL (``None``).
        The schema validates the values before insertion. The table assigns
        the next available ID automatically. All indexes are updated.

        Args:
            values: Column-name-to-value mapping for the new record.

        Returns:
            The newly created record with its assigned ID.

        Raises:
            SchemaError: If the values don't conform to the table's schema.

        """
        values = self._schema.fill_nulls(self._schema.coerce(values))
        self._schema.validate(values)
        self._schema.check_constraints(values, unique=self._rows.unique)
        record_id = self._rows.next_id
        record = self._rows.put(record_id, values)
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
        records = list(self._rows.records.values())
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
        return [self._rows.records[rid] for rid in record_ids if rid in self._rows.records]

    def get(self, record_id: int) -> Record:
        """Return the record with the given ID.

        Args:
            record_id: The ID of the record to retrieve.

        Returns:
            The matching record.

        Raises:
            RecordNotFoundError: If no record with that ID exists.

        """
        record = self._rows.records.get(record_id)
        if record is None:
            msg = f"No record with id={record_id} in table {self._name!r}"
            raise RecordNotFoundError(msg)
        return record

    def update(self, record_id: int, values: Mapping[str, Value]) -> Record:
        """Update fields of an existing record.

        Only the columns specified in *values* are changed; other columns
        keep their current values. The schema validates the new values.
        All affected indexes are updated.

        This returns a new ``Record``; Records you fetched earlier keep
        their old values (so rows you are reading never change under you).

        Args:
            record_id: The ID of the record to update.
            values: Column-name-to-value mapping of fields to change.

        Returns:
            The updated record (a new ``Record`` object).

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
        self._schema.check_constraints(
            merged, exclude_record_id=record_id, unique=self._rows.unique
        )

        updated = self._rows.replace(record_id, merged)
        self._emit(
            Change(ChangeKind.UPDATE, self._name, record_id=record_id, data=updated.data),
            undo=Change(ChangeKind.UPDATE, self._name, record_id=record_id, data=old),
        )
        return updated

    def delete(self, record_id: int) -> None:
        """Remove a record from the table. All indexes are updated.

        Args:
            record_id: The ID of the record to delete.

        Raises:
            RecordNotFoundError: If no record with that ID exists.

        """
        if record_id not in self._rows.records:
            msg = f"No record with id={record_id} in table {self._name!r}"
            raise RecordNotFoundError(msg)

        old = self._rows.records[record_id].data
        self._rows.remove(record_id)
        self._emit(
            Change(ChangeKind.DELETE, self._name, record_id=record_id),
            undo=Change(ChangeKind.INSERT, self._name, record_id=record_id, data=old),
        )

    def restore(
        self, records: Sequence[Record], next_id: int, indexes: Sequence[tuple[str, str]]
    ) -> None:
        """Put the table back to an earlier state, in place (used by ``Transaction``).

        Anyone holding this ``Table`` object sees the restored rows. Nothing
        is recorded. (``Transaction`` no longer needs this: it rolls back
        with an undo log; see ``pydb.rows``.)

        Args:
            records: The records to hold (they are used, not copied).
            next_id: The next auto-increment ID.
            indexes: ``(name, column)`` pairs of indexes to rebuild.

        """
        self._rows.reset(records, next_id)
        for index_name, column in indexes:
            self._rows.add_index(index_name, column)

    # -- recording -----------------------------------------------------------

    def _emit(self, change: Change, *, undo: Change) -> None:
        """Hand *change* to the sink; if the sink fails, apply *undo* and re-raise.

        An exception marked ``is_committed`` (it came after the change was
        already durable) is passed on without undoing anything.

        Args:
            change: The note describing the change just made in memory.
            undo: The opposite change, which puts memory back as it was.

        """
        if self._sink is None:
            return
        try:
            self._sink(change)
        except BaseException as exc:
            if is_committed(exc):  # already in the journal: it stands
                raise
            self._apply_change(undo)
            if change.kind is ChangeKind.INSERT and change.record_id is not None:
                # Hand back the ID number the failed insert used up.
                self._rows.set_next_id(change.record_id)
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
                self._rows.put(rid, data)
            case Change(kind=ChangeKind.UPDATE, record_id=int() as rid, data=dict() as data):
                self._rows.replace(rid, data)
            case Change(kind=ChangeKind.DELETE, record_id=int() as rid):
                self._rows.remove(rid)
            case Change(kind=ChangeKind.CREATE_INDEX, index=(index_name, column)):
                self._rows.add_index(index_name, column)
            case Change(kind=ChangeKind.DROP_INDEX, index=(index_name, _)):
                self._rows.remove_index(index_name)
            case _:
                msg = f"Cannot apply {change!r} to table {self._name!r}"
                raise PyDBError(msg)

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"Table(name={self._name!r}, rows={self.row_count})"
