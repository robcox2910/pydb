r"""The journal -- a diary that survives a power cut.

Imagine writing each change in a notebook, one line per commit. If the
lights go out halfway through a line, the next day you can see the last
line is unfinished and simply ignore it -- everything before it is safe.
The journal does exactly that, and it adds a **checksum** to every line so
it can also tell when a line has been smudged or altered.

File format
-----------

Each commit is one line of bytes::

    <len> <crc32 hex8> <json>\n

* ``len`` -- how many bytes the JSON text has (a torn line is usually too short).
* ``crc32`` -- an 8-digit hex checksum of exactly those JSON bytes.
* ``json`` -- ``{"lsn": 7, "changes": [...]}``. The LSN (log sequence number)
  counts commits 1, 2, 3, ... with no gaps and no repeats.

A journal made by ``rotate(start_after)`` begins with one *header* line,
``PYDBJ1 <start_after>\n``, which remembers the last LSN already covered by
a checkpoint, so ``last_lsn`` survives even when no entries follow it.
A missing or empty file means "no entries, last LSN 0".

Each change is a JSON object: ``{"k": kind, "t": table}`` plus, as the kind
needs, ``"id"``, ``"data"``, ``"schema"`` (see ``serialize_schema``) and
``"index"`` (``[name, column]``).

Value tagging
-------------

Strings, integers, floats and booleans are plain JSON. Dates and timestamps
are *tagged*: ``{"$date": "2026-10-17"}`` and ``{"$ts": "2026-10-17T12:30:00Z"}``.
Only a dict with exactly one such key is a tag; change data is never a dict
otherwise, so a user's *string* ``"$date"`` round-trips untouched.

Torn versus corrupt
-------------------

An unreadable entry (bad length, checksum, JSON, or LSN) with **nothing valid
after it** is a *torn tail* -- an interrupted append -- and is dropped (and cut
off the file on disk). An unreadable entry with a valid one after it means
damage in the middle of the diary, and raises ``JournalCorruptError``.
"""

import contextlib
import json
import os
import zlib
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any, BinaryIO, cast

from pydb.atomic import atomic_write_text, sync_folder
from pydb.changes import Change, ChangeKind
from pydb.errors import PyDBError
from pydb.record import Value
from pydb.serializer import deserialize_schema, serialize_schema
from pydb.types import DataType, from_iso, to_iso

_HEADER_PREFIX = b"PYDBJ1 "
_DATE_TAG = "$date"
_TS_TAG = "$ts"
_LINE_PARTS = 3

type Entry = tuple[int, list[Change]]


class JournalWriteError(PyDBError):
    """Raise when a write to the journal failed.

    After one failed write the journal refuses every later append, so the
    database refuses every later commit, until it is reopened (or a
    checkpoint starts a fresh journal).
    """


class JournalCorruptError(PyDBError):
    """Raise when an entry in the middle of the journal is damaged.

    Attributes:
        line: The 1-based line number of the damaged entry.

    """

    def __init__(self, line: int) -> None:
        """Remember which line is damaged.

        Args:
            line: The 1-based line number of the damaged entry.

        """
        super().__init__(f"journal is corrupt at line {line}")
        self.line = line


def _encode_value(value: Value) -> object:
    """Tag dates and timestamps; pass other values through."""
    if isinstance(value, datetime):
        return {_TS_TAG: to_iso(value)}
    if isinstance(value, date):
        return {_DATE_TAG: to_iso(value)}
    return value


def _decode_value(raw: object) -> Value:
    """Undo ``_encode_value``."""
    if isinstance(raw, dict):
        tag = cast(dict[str, str], raw)
        if set(tag) == {_TS_TAG}:
            return cast(Value, from_iso(tag[_TS_TAG], DataType.TIMESTAMP))
        if set(tag) == {_DATE_TAG}:
            return cast(Value, from_iso(tag[_DATE_TAG], DataType.DATE))
        msg = "unknown tag"
        raise ValueError(msg)
    return cast(Value, raw)


def _encode_change(change: Change) -> dict[str, Any]:
    """Turn a change into a JSON-friendly dict."""
    out: dict[str, Any] = {"k": change.kind.value, "t": change.table}
    if change.record_id is not None:
        out["id"] = change.record_id
    if change.data is not None:
        out["data"] = {name: _encode_value(v) for name, v in change.data.items()}
    if change.schema is not None:
        out["schema"] = serialize_schema(change.schema)
    if change.index is not None:
        out["index"] = list(change.index)
    return out


def _decode_change(raw: dict[str, Any]) -> Change:
    """Turn a JSON dict back into a change."""
    data = raw.get("data")
    schema = raw.get("schema")
    index = raw.get("index")
    return Change(
        kind=ChangeKind(raw["k"]),
        table=raw["t"],
        record_id=raw.get("id"),
        data=None if data is None else {n: _decode_value(v) for n, v in data.items()},
        schema=None if schema is None else deserialize_schema(schema),
        index=None if index is None else (index[0], index[1]),
    )


def encode_entry(lsn: int, changes: Sequence[Change]) -> bytes:
    r"""Encode one commit as a journal line.

    Args:
        lsn: The commit's log sequence number.
        changes: The changes that made up the commit.

    Returns:
        The bytes ``<len> <crc32 hex8> <json>\n``.

    """
    body = json.dumps(
        {"lsn": lsn, "changes": [_encode_change(c) for c in changes]},
        separators=(",", ":"),
    ).encode("utf-8")
    return b"%d %08x %s\n" % (len(body), zlib.crc32(body), body)


def _parse_line(line: bytes) -> Entry | None:
    """Parse one line (without its newline); return None if it is unreadable."""
    parts = line.split(b" ", _LINE_PARTS - 1)
    if len(parts) != _LINE_PARTS:
        return None
    try:
        length = int(parts[0])
        crc = int(parts[1], 16)
        body = parts[2]
        if len(parts[1]) != len("00000000") or length != len(body) or crc != zlib.crc32(body):
            return None
        obj = json.loads(body)
        return int(obj["lsn"]), [_decode_change(c) for c in obj["changes"]]
    except ValueError, KeyError, TypeError, IndexError, AttributeError:
        return None


def decode_entries(
    data: bytes, *, start_after: int = 0, first_line: int = 1
) -> tuple[list[Entry], int]:
    """Decode journal entry lines (without any header).

    Args:
        data: The raw bytes of the entry lines.
        start_after: The LSN before the first entry; entries must count up from it.
        first_line: The line number of the first entry, for error messages.

    Returns:
        The entries and the number of valid bytes (a torn tail is excluded).

    Raises:
        JournalCorruptError: If a bad entry is followed by a valid one.

    """
    entries: list[Entry] = []
    pieces = data.split(b"\n")
    complete = pieces[:-1]  # the last piece has no newline: never a finished entry
    expected = start_after + 1
    valid = 0
    for number, piece in enumerate(complete):
        parsed = _parse_line(piece)
        if parsed is None or parsed[0] != expected:
            for later in complete[number + 1 :]:
                if _parse_line(later) is not None:
                    raise JournalCorruptError(first_line + number)
            return entries, valid
        entries.append(parsed)
        expected += 1
        valid += len(piece) + 1
    return entries, valid


def _read_header(data: bytes) -> tuple[int, int]:
    """Return ``(start_after, header_bytes)``; no header means ``(0, 0)``."""
    if not data.startswith(_HEADER_PREFIX):
        return 0, 0
    end = data.find(b"\n")
    if end == -1:
        raise JournalCorruptError(1)
    try:
        return int(data[len(_HEADER_PREFIX) : end]), end + 1
    except ValueError:
        raise JournalCorruptError(1) from None


class Journal:
    """Append commits to a checksummed file and read them back.

    Args:
        path: The journal file (created on first append).
        sync: Force every append onto the disk with ``fsync`` (default True).

    """

    def __init__(self, path: Path, *, sync: bool = True) -> None:
        """Open (lazily) the journal at *path*."""
        self._path = path
        self._sync = sync
        self._file: BinaryIO | None = None
        self._last_lsn: int | None = None
        self._size = 0
        self._failed = False
        self._dir_synced = True

    def _raw(self) -> bytes:
        """Return the file's bytes, or nothing if it doesn't exist."""
        try:
            return self._path.read_bytes()
        except FileNotFoundError:
            return b""

    def _scan(self, *, repair: bool = False) -> list[Entry]:
        """Read every entry, truncating a torn tail; remember size and last LSN."""
        raw = self._raw()
        start_after, header = _read_header(raw)
        first_line = 2 if header else 1
        try:
            entries, valid = decode_entries(
                raw[header:], start_after=start_after, first_line=first_line
            )
        except JournalCorruptError:
            if not repair:
                raise
            entries, valid = self._salvage(raw[header:], start_after)
        length = header + valid
        if length < len(raw):
            self._close_file()
            with self._path.open("r+b") as handle:
                handle.truncate(length)
                handle.flush()
                os.fsync(handle.fileno())
        self._size = length
        self._last_lsn = entries[-1][0] if entries else start_after
        return entries

    @staticmethod
    def _salvage(data: bytes, start_after: int) -> tuple[list[Entry], int]:
        """Keep entries up to the first damaged line (used by ``repair=True``)."""
        entries: list[Entry] = []
        valid = 0
        expected = start_after + 1
        for piece in data.split(b"\n")[:-1]:
            parsed = _parse_line(piece)
            if parsed is None or parsed[0] != expected:
                break
            entries.append(parsed)
            expected += 1
            valid += len(piece) + 1
        return entries, valid

    def _close_file(self) -> None:
        """Close the append handle if open."""
        if self._file is not None:
            self._file.close()
            self._file = None

    @property
    def last_lsn(self) -> int:
        """The LSN of the newest entry (or the rotate point; 0 if brand new)."""
        if self._last_lsn is None:
            self._scan()
        return cast(int, self._last_lsn)

    @property
    def failed(self) -> bool:
        """Whether an append failed (appends are refused until ``rotate`` or reopen)."""
        return self._failed

    @property
    def size_bytes(self) -> int:
        """The journal's size on disk in bytes."""
        if self._last_lsn is None:
            self._scan()
        return self._size

    def append(self, changes: Sequence[Change]) -> int:
        """Write one commit as the next entry.

        Args:
            changes: The commit's changes.

        Returns:
            The LSN given to the entry.

        """
        if self._failed:
            msg = (
                "an earlier journal write failed, so no more commits are accepted "
                "until the database is reopened (or checkpointed)"
            )
            raise JournalWriteError(msg)
        lsn = self.last_lsn + 1
        line = encode_entry(lsn, changes)
        try:
            if self._file is None:
                self._dir_synced = self._path.exists()
                self._file = self._path.open("ab")
            self._file.write(line)
            self._file.flush()
            if self._sync:
                os.fsync(self._file.fileno())
                if not self._dir_synced:
                    sync_folder(self._path.parent)
                    self._dir_synced = True
        except BaseException as exc:
            self._fail()
            msg = f"could not write the journal: {exc}"
            raise JournalWriteError(msg) from exc
        self._size += len(line)
        self._last_lsn = lsn
        return lsn

    def _fail(self) -> None:
        """Undo a failed append: cut back to the last good size and refuse more writes.

        We never retry ``fsync`` on the same handle -- after a failure the
        disk's state is unknown, so the safe move is to reopen and re-read.
        """
        self._failed = True
        with contextlib.suppress(OSError):
            self._close_file()
        with contextlib.suppress(OSError), self._path.open("r+b") as handle:
            handle.truncate(self._size)
            handle.flush()
            os.fsync(handle.fileno())

    def read(self, *, after: int = 0, repair: bool = False) -> list[Entry]:
        """Return the entries with an LSN above *after*.

        A torn tail is cut off the file on disk.

        Args:
            after: Skip entries up to and including this LSN.
            repair: On mid-file damage, keep the entries before it and cut
                the rest off, instead of raising.

        Raises:
            JournalCorruptError: On mid-file damage when *repair* is False.

        """
        return [e for e in self._scan(repair=repair) if e[0] > after]

    def rotate(self, start_after: int) -> None:
        """Atomically replace the journal with an empty one.

        The old file is never read, so a damaged journal can be rotated
        away. A fresh, fully synced file also ends the "an earlier write
        failed" state: appends work again.

        Args:
            start_after: The LSN already covered by a checkpoint; the next
                append gets ``start_after + 1``.

        """
        if self._last_lsn is not None and start_after < self._last_lsn:
            msg = f"start_after {start_after} is below last LSN {self._last_lsn}"
            raise ValueError(msg)
        self._close_file()
        header = _HEADER_PREFIX + b"%d\n" % start_after
        atomic_write_text(self._path, header.decode("ascii"))
        self._last_lsn = start_after
        self._size = len(header)
        self._failed = False

    def close(self) -> None:
        """Close the file."""
        self._close_file()
