"""A register of names -- checking UNIQUE and PRIMARY KEY in one look.

Imagine a teacher who must make sure no two children in a class share a
name. She could walk along every desk and read every name tag each time a
new child arrives -- slow, and slower the bigger the class gets. Or she
can keep a **register**: a list of names, each pointing at a desk. A new
child arrives, she looks the name up once, and she knows straight away.

A ``UniqueRegister`` is that register for one table. For each UNIQUE or
PRIMARY KEY column it keeps a dictionary from value to record ID, so a
duplicate check is one dictionary look-up instead of a walk through every
row. The table keeps it in step on every insert, update, delete, rollback,
load and journal replay.

Two values count as "the same" exactly when ``==`` says so -- the rule
the old walk-every-row check used. A Python dictionary already works like
that: ``1``, ``1.0`` and ``True`` are equal *and* hash alike, so they find
each other. Two values are never registered:

* ``None`` (a missing value) -- several rows may leave a UNIQUE column empty.
* NaN ("not a number") -- ``nan == nan`` is False, so a NaN never clashes.

Normally each value belongs to one row. If rows that clash were loaded
anyway (say, from a hand-edited file), the extra rows are remembered too,
so the checks still behave exactly as the old walk did.
"""

import math
from collections.abc import Iterable
from typing import TypeGuard

from pydb.record import Record, Value


def _registrable(value: Value | None) -> TypeGuard[Value]:
    """Return whether *value* takes part in uniqueness (not None, not NaN)."""
    if value is None:
        return False
    return not (isinstance(value, float) and math.isnan(value))


class UniqueRegister:
    """Map each value of each UNIQUE / PRIMARY KEY column to the rows holding it.

    Args:
        columns: The names of the columns to register.

    """

    __slots__ = ("_extra", "_first")

    def __init__(self, columns: Iterable[str]) -> None:
        """Create an empty register for *columns*."""
        # value -> the record ID that holds it (the usual, one-row case).
        self._first: dict[str, dict[Value, int]] = {column: {} for column in columns}
        # value -> further record IDs holding an equal value (only clashing data).
        self._extra: dict[str, dict[Value, set[int]]] = {column: {} for column in self._first}

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return the registered column names."""
        return tuple(self._first)

    def is_taken(
        self, column: str, value: Value | None, exclude_record_id: int | None = None
    ) -> bool:
        """Return whether a row other than *exclude_record_id* holds *value* in *column*.

        Args:
            column: A registered column.
            value: The value to look up.
            exclude_record_id: A row to ignore (the row being updated).

        """
        if not _registrable(value):
            return False
        first = self._first[column].get(value)
        if first is None:
            return False
        if first != exclude_record_id:
            return True
        return value in self._extra[column]

    def add(self, record: Record) -> None:
        """Register every registered column's value of *record*."""
        rid = record.record_id
        for column, holders in self._first.items():
            value: Value | None = record.get(column)
            if not _registrable(value):
                continue
            if value in holders:
                self._extra[column].setdefault(value, set()).add(rid)
            else:
                holders[value] = rid

    def remove(self, record: Record) -> None:
        """Forget the values of *record* (it is being deleted or replaced)."""
        rid = record.record_id
        for column, holders in self._first.items():
            value: Value | None = record.get(column)
            if not _registrable(value):
                continue
            extra = self._extra[column]
            others = extra.get(value)
            if holders.get(value) == rid:
                if others:
                    holders[value] = others.pop()
                else:
                    del holders[value]
            elif others is not None:
                others.discard(rid)
            if others is not None and not others:
                del extra[value]

    def rebuild(self, records: Iterable[Record]) -> None:
        """Forget everything, then register every record in *records*."""
        for column in self._first:
            self._first[column] = {}
            self._extra[column] = {}
        for record in records:
            self.add(record)
