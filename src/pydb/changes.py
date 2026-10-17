"""Changes -- little notes describing what happened to the database.

Imagine a class diary. Every time somebody adds a trading card to a binder,
swaps one, or throws one away, a helper writes a short note: "added card 3
to *cards*: Squirtle, power 48". A ``Change`` is one of those notes.

On their own the notes don't do anything -- they're just a record. Later,
the journal (see ``pydb.journal``) copies them to disk so that, after a
crash, PyDB can read the diary back and redo everything that was committed.

Who gets the notes?

* Every ``Table`` owned by a ``Database`` has a *sink*: a function it calls
  with each note right after the change has happened in memory.
* The ``Database`` gathers the notes into *batches*. Outside a transaction
  each note is its own batch of one. Inside a ``Transaction`` the notes wait
  in a bundle until ``commit()`` -- and are thrown away on ``rollback()``.
* Each finished batch is handed to the database's *commit listeners*
  (``Database.add_commit_listener``).
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from pydb.record import Value
from pydb.schema import Schema


class ChangeKind(StrEnum):
    """Name the kinds of change a note can describe."""

    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"


@dataclass(frozen=True)
class Change:
    """Describe one change to one table.

    Which fields are filled in depends on the kind:

    * ``CREATE_TABLE``: ``schema``.
    * ``DROP_TABLE``: just ``table``.
    * ``INSERT`` / ``UPDATE``: ``record_id`` and ``data`` (every column's
      value after the change -- a copy, never the live record).
    * ``DELETE``: ``record_id``.
    * ``CREATE_INDEX`` / ``DROP_INDEX``: ``index`` as ``(name, column)``.

    Args:
        kind: What happened.
        table: The name of the table it happened to.
        record_id: The record's ID, for row changes.
        data: The record's full data, for inserts and updates.
        schema: The new table's schema, for ``CREATE_TABLE``.
        index: The index's ``(name, column)``, for index changes.

    """

    kind: ChangeKind
    table: str
    record_id: int | None = None
    data: dict[str, Value] | None = None
    schema: Schema | None = None
    index: tuple[str, str] | None = None


type ChangeSink = Callable[[Change], None]
"""A function a table calls with each change it makes."""

type CommitListener = Callable[[list[Change]], None]
"""A function a database calls once per commit with that commit's changes."""
