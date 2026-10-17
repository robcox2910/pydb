"""Durable databases -- rebuilding the room from the diary after a power cut.

Imagine a librarian who writes every change in a diary *before* saying
"done". One morning the lights come back on and the shelves are in a mess.
No problem: she puts back the binders she photocopied last time (the table
snapshots), then reads the diary from the first line to the last and redoes
each change, in order. When she reaches the end, the room looks exactly as
it did the moment before the lights went out.

That's what ``Database(path, journal=True)`` does when it opens:

1. Load the table snapshots the last checkpoint lists (see
   ``pydb.checkpoint``; an old folder's ``<table>.json`` files if there is
   no checkpoint yet).
2. Read the journal (``journal.log``) and *replay* each committed change
   made after that checkpoint.

This module holds the building blocks; ``pydb.checkpoint`` puts them together.

Replaying is careful in three ways:

* It uses the tables' raw "apply exactly this" path, so no checks run twice
  and every row keeps the very ID it had. ``next_id`` and the indexes end up
  exactly as they were.
* It is done on plain tables that belong to no database yet, so nothing is
  written to the journal again and no commit listener is called.
* Entries are applied in LSN (commit number) order -- the order the journal
  hands them back.

A half-written last line (a power cut mid-append) is simply dropped. Damage
in the middle of the journal stops the open with ``JournalCorruptError``,
unless ``repair=True`` -- then everything before the damage is kept, the
rest is cut off, and a warning is logged.
"""

import logging
from collections.abc import Iterable

from pydb.changes import Change, ChangeKind
from pydb.errors import PyDBError
from pydb.journal import Entry, Journal, JournalCorruptError
from pydb.storage import StorageEngine
from pydb.table import Table

JOURNAL_FILE = "journal.log"
"""The journal's file name inside the database folder."""

_log = logging.getLogger(__name__)


class RecoveryError(PyDBError):
    """Raise when the journal can't be replayed onto the saved tables."""


def load_snapshots(storage: StorageEngine) -> dict[str, Table]:
    """Load every saved table in the folder, with its indexes.

    Args:
        storage: The storage engine for the database folder.

    Returns:
        The loaded tables by name. They belong to no database yet.

    """
    tables: dict[str, Table] = {}
    for name in storage.list_tables():
        stored = storage.load_stored_table(name)
        tables[name] = Table.from_stored(
            name=stored.name,
            schema=stored.schema,
            records=stored.records,
            next_id=stored.next_id,
            indexes=stored.indexes,
        )
    return tables


def read_entries(journal: Journal, *, after: int = 0, repair: bool = False) -> list[Entry]:
    """Return every committed entry ``(lsn, changes)`` after LSN *after*, oldest first.

    Args:
        journal: The journal to read.
        after: Skip entries up to and including this LSN.
        repair: On mid-file damage, keep what comes before it (and log a
            warning) instead of raising.

    Raises:
        JournalCorruptError: On mid-file damage when *repair* is False.

    """
    try:
        return journal.read(after=after)
    except JournalCorruptError as exc:
        if not repair:
            raise
        _log.warning("journal damaged (%s); repair keeps the entries before it", exc)
        return journal.read(after=after, repair=True)


def replay(tables: dict[str, Table], changes: Iterable[Change]) -> None:
    """Apply journalled *changes* to *tables* in order, without any checks.

    Args:
        tables: The tables by name; created and dropped tables are added
            to and removed from this dict.
        changes: The changes to apply, oldest first.

    Raises:
        RecoveryError: If a change doesn't fit the tables (for example it
            names a table that doesn't exist).

    """
    for change in changes:
        _replay_one(tables, change)


def _replay_one(tables: dict[str, Table], change: Change) -> None:
    """Apply one change to *tables*."""
    name = change.table
    if change.kind is ChangeKind.CREATE_TABLE:
        if name in tables or change.schema is None:
            msg = f"Cannot replay creating table {name!r}: it already exists or has no schema"
            raise RecoveryError(msg)
        tables[name] = Table(name=name, schema=change.schema)
        return
    table = tables.get(name)
    if table is None:
        msg = f"Cannot replay {change.kind.value} on table {name!r}: no such table"
        raise RecoveryError(msg)
    if change.kind is ChangeKind.DROP_TABLE:
        del tables[name]
        return
    try:
        table.replay_change(change)
    except (KeyError, PyDBError) as exc:
        msg = f"Cannot replay {change.kind.value} on table {name!r}: {exc!r}"
        raise RecoveryError(msg) from exc
