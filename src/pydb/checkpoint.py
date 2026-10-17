"""Checkpoints -- photocopy the binders, then start a fresh diary.

The journal (see ``pydb.journal``) grows with every commit. Replaying a
huge diary on every open would be slow, so now and then the librarian
takes a *checkpoint*:

1. **Photocopy** every binder that changed since the last checkpoint.
   Each copy is a *new* file, ``snapshots/<table>.<lsn>.json``, named after
   the commit number (LSN) it is up to date with. Old copies are not touched.
2. **Write the index card** ``checkpoint.json`` -- in one safe step -- saying
   "the copies listed here are up to date with commit N"::

       {"lsn": 7, "tables": [{"name": "cards", "file": "cards.7.json"}]}

3. **Start a fresh diary**: ``journal.log`` is replaced by an empty one
   that begins after commit N.
4. Throw away the copies no index card mentions any more.

Why new file names? Because the power can go out between any two steps.
Opening a database reads the index card, loads *exactly* the copies it
lists, and replays the diary entries with an LSN above the card's. If a
crash comes after step 1, the old card still points at the old copies and
the whole diary is still there, so nothing is lost -- and nothing is
counted twice, because the new copies (which already contain the diary's
changes) are not listed yet. After step 2 the new card points at the new
copies and the diary entries up to N are skipped. After step 3 the diary is
simply empty.

A folder saved by an older PyDB has ``<table>.json`` files and no index
card; those load as they always did, and the first checkpoint moves them
into ``snapshots/``.
"""

import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from pydb.atomic import atomic_write_text, sync_folder
from pydb.changes import Change
from pydb.durable import JOURNAL_FILE, RecoveryError, load_snapshots, read_entries, replay
from pydb.journal import Journal
from pydb.serializer import SerializationError, deserialize_table, serialize_table_data
from pydb.storage import TABLE_FILE_SUFFIX, StorageEngine
from pydb.table import Table

CHECKPOINT_FILE = "checkpoint.json"
"""The index card naming the snapshots and the LSN they cover."""

SNAPSHOT_DIR = "snapshots"
"""The folder (inside the database folder) holding checkpoint snapshots."""

_CRASH_EXIT = 1


@dataclass(slots=True)
class _Card:
    """What ``checkpoint.json`` says: the LSN covered and each table's snapshot file."""

    lsn: int = 0
    files: dict[str, str] = field(default_factory=dict[str, str])


def _parse_card(raw: object) -> _Card:
    """Turn the parsed JSON of ``checkpoint.json`` into a card.

    Raises:
        KeyError: If ``"lsn"``, ``"tables"`` or a table's ``"name"``/``"file"`` is missing.
        TypeError: If a value has the wrong type.

    """
    if not isinstance(raw, dict):
        raise TypeError
    obj: dict[str, object] = raw  # pyright: ignore[reportUnknownVariableType]
    lsn, tables = obj["lsn"], obj["tables"]
    if not isinstance(lsn, int) or not isinstance(tables, list):
        raise TypeError
    files: dict[str, str] = {}
    for item in tables:  # pyright: ignore[reportUnknownVariableType]
        name, file = item["name"], item["file"]  # pyright: ignore[reportUnknownVariableType]
        if not isinstance(name, str) or not isinstance(file, str):
            raise TypeError
        files[name] = file
    return _Card(lsn, files)


def _read_raw(folder: Path) -> object | None:
    """Return the parsed ``checkpoint.json`` if it is a checkpoint, else None.

    A file that isn't valid JSON or has no ``"lsn"`` key is not a checkpoint
    -- it may be an old-style snapshot of a table called ``checkpoint``.
    """
    try:
        raw: object = json.loads((folder / CHECKPOINT_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError, ValueError:
        return None
    if isinstance(raw, dict) and "lsn" in raw:
        return raw  # pyright: ignore[reportUnknownVariableType]
    return None


def is_journalled_folder(folder: Path) -> bool:
    """Return whether *folder* holds a journalled database (journal or checkpoint)."""
    return (folder / JOURNAL_FILE).exists() or _read_raw(folder) is not None


def _snapshot_text(table: Table) -> str:
    """Return the snapshot JSON for *table*."""
    return serialize_table_data(
        table.name,
        table.schema,
        table.select(),
        table.next_id,
        indexes=table.index_definitions,
    )


@dataclass(slots=True)
class Checkpointer:
    """Recover a journalled database and take its checkpoints.

    Args:
        folder: The database folder.
        journal: The database's journal.

    """

    folder: Path
    journal: Journal
    _card: _Card = field(default_factory=_Card)
    _lsn: int = 0  # the newest committed LSN, as this process knows it
    _dirty: set[str] = field(default_factory=set[str])
    _old_files: set[str] = field(default_factory=set[str])  # pre-checkpoint <table>.json

    def recover(self, *, repair: bool = False) -> dict[str, Table]:
        """Load the checkpoint's snapshots and replay the journal after it.

        Args:
            repair: Passed to ``read_entries`` (keep the commits before
                mid-file damage instead of raising).

        Returns:
            The recovered tables by name. They belong to no database yet.

        Raises:
            RecoveryError: If ``checkpoint.json`` or a snapshot it lists
                can't be read, commits between it and the journal are
                missing, or the journal doesn't fit the tables.

        """
        raw = _read_raw(self.folder)
        if raw is None:
            tables = load_snapshots(StorageEngine(self.folder))
            self._old_files = set(tables)
        else:
            try:
                self._card = _parse_card(raw)
            except (KeyError, TypeError, ValueError) as exc:
                msg = f"Cannot read {CHECKPOINT_FILE}: {exc!r}"
                raise RecoveryError(msg) from exc
            tables = {name: self._load(name, file) for name, file in self._card.files.items()}
        self._dirty = set(tables) if raw is None else set()
        after = self._card.lsn
        entries = read_entries(self.journal, after=after, repair=repair)
        if (entries and entries[0][0] != after + 1) or (
            not entries and self.journal.last_lsn > after
        ):
            msg = f"Journal commits right after checkpoint LSN {after} are missing"
            raise RecoveryError(msg)
        changes = [change for _lsn, batch in entries for change in batch]
        replay(tables, changes)
        self._note(changes)
        self._lsn = max(after, self.journal.last_lsn)
        if self.journal.last_lsn < after:
            self.journal.rotate(after)  # journal.log was lost; restart it after the card
        return tables

    def _load(self, name: str, file: str) -> Table:
        """Load one listed snapshot."""
        try:
            stored = deserialize_table((self.folder / SNAPSHOT_DIR / file).read_text("utf-8"))
        except (OSError, SerializationError) as exc:
            msg = f"Cannot load the snapshot {file!r} of table {name!r}: {exc!r}"
            raise RecoveryError(msg) from exc
        return Table.from_stored(
            name=name,
            schema=stored.schema,
            records=stored.records,
            next_id=stored.next_id,
            indexes=stored.indexes,
        )

    def committed(self, lsn: int, changes: Iterable[Change]) -> None:
        """Remember a commit that reached the journal as entry *lsn*."""
        self._lsn = lsn
        self._note(changes)

    def _note(self, changes: Iterable[Change]) -> None:
        """Mark the tables *changes* touch as needing a new snapshot."""
        self._dirty.update(change.table for change in changes)

    def checkpoint(self, tables: Mapping[str, Table], *, crash_after: str | None = None) -> None:
        """Snapshot changed tables, write ``checkpoint.json``, then rotate the journal.

        Args:
            tables: Every table of the database, all committed.
            crash_after: Test hook: end the process (``os._exit``) right after
                this step -- ``"snapshots"``, ``"checkpoint_file"`` or ``"rotate"``.

        """
        lsn = self._lsn
        if lsn == self._card.lsn and not self._dirty and not self._old_files:
            if self.journal.failed:
                self.journal.rotate(lsn)
            return
        files = {
            name: file
            for name, file in self._card.files.items()
            if name in tables and name not in self._dirty
        }
        snapshots = self.folder / SNAPSHOT_DIR
        if not snapshots.exists():
            snapshots.mkdir()
            sync_folder(self.folder)
        for name in sorted(set(tables) - set(files)):
            files[name] = f"{name}.{lsn}{TABLE_FILE_SUFFIX}"
            atomic_write_text(snapshots / files[name], _snapshot_text(tables[name]))
        _maybe_crash(crash_after, "snapshots")
        card = {"lsn": lsn, "tables": [{"name": n, "file": f} for n, f in sorted(files.items())]}
        atomic_write_text(self.folder / CHECKPOINT_FILE, json.dumps(card, indent=2) + "\n")
        self._card = _Card(lsn, files)
        self._dirty.clear()
        _maybe_crash(crash_after, "checkpoint_file")
        self.journal.rotate(lsn)
        _maybe_crash(crash_after, "rotate")
        self._remove_leftovers(snapshots, set(files.values()))

    def _remove_leftovers(self, snapshots: Path, keep: set[str]) -> None:
        """Delete snapshot files no checkpoint lists, and pre-checkpoint table files."""
        for path in snapshots.iterdir():
            if path.name not in keep:
                path.unlink(missing_ok=True)
        for name in self._old_files:
            old = self.folder / f"{name}{TABLE_FILE_SUFFIX}"
            if old.name != CHECKPOINT_FILE:  # now the card itself
                old.unlink(missing_ok=True)
        self._old_files.clear()


def _maybe_crash(crash_after: str | None, step: str) -> None:
    """End the process right now if the test hook asks for a crash after *step*."""
    if crash_after == step:
        os._exit(_CRASH_EXIT)  # a real crash: no clean-up at all
