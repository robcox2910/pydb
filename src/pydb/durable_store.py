"""The journalled database's desk drawer -- its LOCK, diary and photo album.

A journalled ``Database`` keeps three things on disk, and this module keeps
them together so the database itself stays short:

- the ``LOCK`` file that tells other programs "busy!" (``pydb.locking``),
- the journal, the diary every commit is written in (``pydb.journal``),
- the checkpointer, which tidies the diary into table snapshots
  (``pydb.checkpoint``).
"""

from collections.abc import Mapping, Sequence
from pathlib import Path

from pydb.changes import Change
from pydb.checkpoint import Checkpointer
from pydb.durable import JOURNAL_FILE
from pydb.journal import Journal
from pydb.locking import LockFile
from pydb.table import Table


class JournalStore:
    """Own the ``LOCK`` file, journal and checkpoints of one journalled folder.

    Creating a store takes the folder's ``LOCK`` first, so two programs
    never write the same journal.

    Args:
        folder: The database folder.
        sync: ``fsync`` every commit.

    Raises:
        DatabaseLockedError: If another program has the folder open.

    """

    __slots__ = ("_checkpointer", "_journal", "_lock_file")

    def __init__(self, folder: Path, *, sync: bool) -> None:
        """Take the folder's lock and open its journal."""
        self._lock_file = LockFile(folder)
        self._lock_file.acquire()
        self._journal = Journal(folder / JOURNAL_FILE, sync=sync)
        self._checkpointer = Checkpointer(folder, self._journal)

    def recover(self, *, repair: bool) -> dict[str, Table]:
        """Return every table as last committed (snapshots plus journal replay)."""
        return self._checkpointer.recover(repair=repair)

    def record(self, changes: Sequence[Change]) -> None:
        """Append one commit to the journal (this is the commit point).

        The tables it touches are marked for the next checkpoint *before*
        the append, so an interrupt right after the append can't leave a
        committed change out of the next snapshot.
        """
        self._checkpointer.will_commit(changes)
        self._checkpointer.committed(self._journal.append(changes), changes)

    @property
    def last_lsn(self) -> int:
        """Return the LSN of the newest commit in the journal."""
        return self._journal.last_lsn

    @property
    def size_bytes(self) -> int:
        """Return the journal's size on disk in bytes."""
        return self._journal.size_bytes

    def checkpoint(self, tables: Mapping[str, Table], *, crash_after: str | None) -> None:
        """Write the changed tables and start a fresh journal (see ``pydb.checkpoint``)."""
        self._checkpointer.checkpoint(tables, crash_after=crash_after)

    def close(self) -> None:
        """Close the journal and delete the ``LOCK`` file."""
        try:
            self._journal.close()
        finally:
            self._lock_file.release()
