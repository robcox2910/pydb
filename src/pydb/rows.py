"""The rows of a table, and a list of how to undo each change to them.

A ``Table`` checks the rules (schema, constraints) and records its changes;
the ``RowStore`` underneath just *holds* things: the records by ID, the
next ID to hand out, the indexes, and the register of UNIQUE values (see
``pydb.unique``). Every change goes through one of its five building
blocks -- ``put``, ``replace``, ``remove``, ``add_index`` and
``remove_index`` -- which keep the indexes and the register in step.

While a ``Transaction`` is open, each building block first writes down how
to undo itself in the transaction's ``UndoLog``. Think of scribbling in a
notebook you borrowed: instead of photocopying the whole page before you
start, you jot on a sticky note "I wrote 'cat' on line 3 -- rub it out" or
"I crossed out 'dog' on line 5 -- here it is again". Each note is tiny, so
writing one costs the same however big the notebook is. To give the
notebook back unchanged, you follow the sticky notes from the newest to
the oldest. To keep your changes, you just throw the notes away.

Undoing never copies a row. A record is never changed in place (an update
swaps in a new ``Record``), so a note simply keeps the old ``Record``
object and puts it back.
"""

from collections.abc import Callable, Iterable, Mapping

from pydb.index import Index
from pydb.record import Record, Value
from pydb.unique import UniqueRegister


class UndoLog:
    """Notes on how to undo each change, replayed newest first on rollback."""

    __slots__ = ("_notes", "_stores")

    def __init__(self) -> None:
        """Start an empty log."""
        self._notes: list[Callable[[], None]] = []
        self._stores: dict[int, RowStore] = {}

    def note(self, store: RowStore, undo: Callable[[], None]) -> None:
        """Remember *undo*, which takes back a change just about to happen in *store*."""
        self._notes.append(undo)
        self._stores.setdefault(id(store), store)

    def rollback(self) -> None:
        """Undo every noted change, newest first, then empty the log."""
        for undo in reversed(self._notes):
            undo()
        for store in self._stores.values():
            store.tidy()
        self.clear()

    def clear(self) -> None:
        """Forget every note (the changes are kept)."""
        self._notes.clear()
        self._stores.clear()


class RowStore:
    """Hold a table's records, next ID, indexes and unique register.

    No checks happen here and nothing is recorded in the journal; the
    ``Table`` on top does that. When ``undo`` is set, every change first
    writes its opposite into that log.

    Args:
        unique_columns: The PRIMARY KEY and UNIQUE columns to register.

    """

    __slots__ = ("_untidy", "indexes", "next_id", "records", "undo", "unique")

    def __init__(self, unique_columns: Iterable[str]) -> None:
        """Create an empty store."""
        self.records: dict[int, Record] = {}
        self.next_id = 1
        self.indexes: dict[str, Index] = {}
        self.unique = UniqueRegister(unique_columns)
        self.undo: UndoLog | None = None
        # Set when undo put a record back out of ID order (see ``tidy``).
        self._untidy = False

    def _note(self, undo: Callable[[], None]) -> None:
        """Write *undo* into the open undo log, if any."""
        if self.undo is not None:
            self.undo.note(self, undo)

    # -- the five building blocks --------------------------------------------

    def put(self, record_id: int, data: Mapping[str, Value]) -> Record:
        """Store a record under *record_id*, keeping records in ID order."""
        previous, prior = self.records.get(record_id), self.next_id
        self._note(lambda: self._unput(record_id, previous, prior))
        if previous is not None:
            self._unlink(previous)
        record = Record(record_id=record_id, data=data)
        out_of_order = bool(self.records) and record_id < next(reversed(self.records))
        self.records[record_id] = record
        if out_of_order:
            self.records = dict(sorted(self.records.items()))
        self.next_id = max(self.next_id, record_id + 1)
        self._link(record)
        return record

    def replace(self, record_id: int, data: Mapping[str, Value]) -> Record:
        """Swap in a new record with *data*; the old ``Record`` is left untouched."""
        record = self.records[record_id]
        self._note(lambda: self._reinstate(record))
        replacement = Record(record_id=record_id, data=data)
        self._swap(record, replacement)
        return replacement

    def remove(self, record_id: int) -> None:
        """Remove a record and its index and register entries."""
        record = self.records[record_id]
        self._note(lambda: self._reinstate(record))
        del self.records[record_id]
        self._unlink(record)

    def add_index(self, index_name: str, column: str) -> Index:
        """Build an index on *column* filled with every current record."""
        self._note(lambda: self._drop_index(index_name))
        idx = Index(name=index_name, column=column)
        for record in self.records.values():
            idx.insert(record[column], record.record_id)
        self.indexes[index_name] = idx
        return idx

    def remove_index(self, index_name: str) -> None:
        """Forget an index."""
        position = list(self.indexes).index(index_name)
        idx = self.indexes[index_name]
        self._note(lambda: self._reinstate_index(position, idx))
        self._drop_index(index_name)

    def set_next_id(self, next_id: int) -> None:
        """Change the next ID (a refused insert hands its ID back)."""
        prior = self.next_id
        self._note(lambda: setattr(self, "next_id", prior))
        self.next_id = next_id

    def reset(self, records: Iterable[Record], next_id: int) -> None:
        """Replace every record and the next ID; indexes are emptied (rebuild them)."""
        old = (self.records, self.next_id, self.indexes, self.unique)
        self._note(lambda: self._unreset(*old))
        self.records = {record.record_id: record for record in records}
        self.next_id = next_id
        self.indexes = {}
        self.unique = UniqueRegister(old[3].column_names)
        self.unique.rebuild(self.records.values())

    # -- undoing (no notes are written: the log is replaying) ----------------

    def _unput(self, record_id: int, previous: Record | None, next_id: int) -> None:
        """Take back ``put``: drop the new record and bring back any it replaced."""
        self._unlink(self.records.pop(record_id))
        if previous is not None:
            self._put_back(previous)
        self.next_id = next_id

    def _reinstate(self, record: Record) -> None:
        """Put *record* (the very same object) back under its ID."""
        current = self.records.get(record.record_id)
        if current is not None:
            self._swap(current, record)
        else:
            self._put_back(record)

    def _put_back(self, record: Record) -> None:
        """Add a missing *record* again; remember if that broke the ID order."""
        if self.records and record.record_id < next(reversed(self.records)):
            self._untidy = True
        self.records[record.record_id] = record
        self._link(record)

    def _reinstate_index(self, position: int, idx: Index) -> None:
        """Put a dropped index back where it was in the list."""
        items = list(self.indexes.items())
        items.insert(position, (idx.name, idx))
        self.indexes = dict(items)

    def _unreset(
        self,
        records: dict[int, Record],
        next_id: int,
        indexes: dict[str, Index],
        unique: UniqueRegister,
    ) -> None:
        """Take back ``reset``: bring back the earlier state objects."""
        self.records, self.next_id, self.indexes, self.unique = records, next_id, indexes, unique

    def tidy(self) -> None:
        """Sort the records by ID again if undo put some back out of order."""
        if self._untidy:
            self.records = dict(sorted(self.records.items()))
            self._untidy = False

    # -- helpers -------------------------------------------------------------

    def _drop_index(self, index_name: str) -> None:
        """Delete an index by name."""
        del self.indexes[index_name]

    def _link(self, record: Record) -> None:
        """Add *record* to every index and the register."""
        self.unique.add(record)
        for idx in self.indexes.values():
            idx.insert(record[idx.column], record.record_id)

    def _unlink(self, record: Record) -> None:
        """Take *record* out of every index and the register."""
        self.unique.remove(record)
        for idx in self.indexes.values():
            idx.delete(record[idx.column], record.record_id)

    def _swap(self, old: Record, new: Record) -> None:
        """Replace *old* by *new* (same ID), moving index and register entries."""
        for idx in self.indexes.values():
            if old[idx.column] != new[idx.column]:
                idx.delete(old[idx.column], old.record_id)
                idx.insert(new[idx.column], new.record_id)
        self.records[new.record_id] = new
        self.unique.remove(old)
        self.unique.add(new)
