"""Tests for the checksummed journal."""

import os
import zlib
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from pydb.changes import Change, ChangeKind
from pydb.journal import (
    Journal,
    JournalCorruptError,
    JournalWriteError,
    decode_entries,
    encode_entry,
)
from pydb.record import Value
from pydb.schema import Column, Schema
from pydb.serializer import serialize_schema
from pydb.types import DataType

_ENTRY_COUNT = 3
_TS = datetime(2026, 10, 17, 12, 30, 45, 123456, tzinfo=UTC)
_DAY = date(2026, 10, 17)
_ROW_ID = 7
_START = 41
_GAP_LINE = 3
_EIO = 5
_ENOSPC = 28


def _all_changes() -> list[Change]:
    """Build one change of every kind."""
    schema = Schema(
        columns=[
            Column("id", DataType.INTEGER, primary_key=True),
            Column("born", DataType.DATE, not_null=True),
        ]
    )
    data: dict[str, Value] = {
        "id": 1,
        "born": _DAY,
        "seen": _TS,
        "name": "$date",
        "ok": True,
        "x": 1.5,
        "gone": None,
    }
    return [
        Change(ChangeKind.CREATE_TABLE, "t", schema=schema),
        Change(ChangeKind.INSERT, "t", record_id=1, data=data),
        Change(ChangeKind.UPDATE, "t", record_id=1, data=data),
        Change(ChangeKind.DELETE, "t", record_id=_ROW_ID),
        Change(ChangeKind.CREATE_INDEX, "t", index=("ix", "id")),
        Change(ChangeKind.DROP_INDEX, "t", index=("ix", "id")),
        Change(ChangeKind.DROP_TABLE, "t"),
    ]


def _shape(changes: list[Change]) -> list[object]:
    """Describe changes comparably (Schema has no equality of its own)."""
    return [
        (c.kind, c.table, c.record_id, c.data, c.index, c.schema and serialize_schema(c.schema))
        for c in changes
    ]


def _small(n: int) -> list[Change]:
    """Build a one-change commit."""
    return [Change(ChangeKind.DELETE, "t", record_id=n)]


class TestRoundTrip:
    """Check encoding and decoding."""

    def test_all_kinds_round_trip(self) -> None:
        """Decode exactly what was encoded, including dates."""
        changes = _all_changes()
        entries, valid = decode_entries(encode_entry(1, changes))
        assert [lsn for lsn, _ in entries] == [1]
        assert _shape(entries[0][1]) == _shape(changes)
        assert valid == len(encode_entry(1, changes))

    def test_line_format(self) -> None:
        """Start with length and checksum, end with a newline."""
        line = encode_entry(1, _small(1))
        length, crc, body = line[:-1].split(b" ", 2)
        assert int(length) == len(body)
        assert len(crc) == len("00000000")
        assert line.endswith(b"\n")

    def test_journal_round_trip(self, tmp_path: Path) -> None:
        """Write through a Journal and read it back."""
        journal = Journal(tmp_path / "j")
        journal.append(_all_changes())
        ((lsn, got),) = journal.read()
        assert lsn == 1
        assert _shape(got) == _shape(_all_changes())
        journal.close()


class TestTornTail:
    """Check power cuts mid-append."""

    def test_torn_tail_is_ignored_and_truncated(self, tmp_path: Path) -> None:
        """Cut the last entry at every byte offset: earlier commits survive."""
        path = tmp_path / "j"
        journal = Journal(path)
        for n in range(_ENTRY_COUNT):
            journal.append(_small(n))
        journal.close()
        full = path.read_bytes()
        last_len = len(encode_entry(_ENTRY_COUNT, _small(_ENTRY_COUNT - 1)))
        good = len(full) - last_len
        for cut in range(good, len(full)):
            path.write_bytes(full[:cut])
            reopened = Journal(path)
            entries = reopened.read()
            assert [lsn for lsn, _ in entries] == list(range(1, _ENTRY_COUNT))
            assert path.stat().st_size == good
            assert reopened.last_lsn == _ENTRY_COUNT - 1
            reopened.close()

    def test_repeated_lsn_at_end_is_torn(self) -> None:
        """Treat a repeated LSN at the end as a torn tail."""
        data = encode_entry(1, _small(1)) + encode_entry(1, _small(2))
        entries, valid = decode_entries(data)
        assert len(entries) == 1
        assert valid == len(encode_entry(1, _small(1)))

    def test_append_after_torn_tail(self, tmp_path: Path) -> None:
        """Continue cleanly after a torn tail."""
        path = tmp_path / "j"
        path.write_bytes(encode_entry(1, _small(1)) + b"12 abc")
        journal = Journal(path)
        assert journal.append(_small(2)) == _ENTRY_COUNT - 1
        journal.close()
        assert [lsn for lsn, _ in Journal(path).read()] == [1, 2]


class TestCorruption:
    """Check damage in the middle."""

    def _three(self, path: Path) -> bytes:
        """Write three entries and return the bytes."""
        journal = Journal(path)
        for n in range(_ENTRY_COUNT):
            journal.append(_small(n))
        journal.close()
        return path.read_bytes()

    def test_flipped_byte_in_middle(self, tmp_path: Path) -> None:
        """Raise with the right line number."""
        path = tmp_path / "j"
        raw = bytearray(self._three(path))
        first = len(encode_entry(1, _small(0)))
        raw[first + len(encode_entry(2, _small(1))) - 3] ^= 0x01
        path.write_bytes(bytes(raw))
        with pytest.raises(JournalCorruptError) as info:
            Journal(path).read()
        assert info.value.line == _ENTRY_COUNT - 1

    def test_repair_stops_there(self, tmp_path: Path) -> None:
        """Keep entries before the damage and cut the rest."""
        path = tmp_path / "j"
        raw = bytearray(self._three(path))
        first = len(encode_entry(1, _small(0)))
        raw[first + 5] ^= 0x01
        path.write_bytes(bytes(raw))
        journal = Journal(path)
        assert [lsn for lsn, _ in journal.read(repair=True)] == [1]
        assert path.stat().st_size == first
        assert journal.append(_small(9)) == _ENTRY_COUNT - 1


class TestJournal:
    """Check the Journal object."""

    def test_lsns_continue_after_reopen(self, tmp_path: Path) -> None:
        """Keep counting after a restart."""
        path = tmp_path / "j"
        first = Journal(path)
        first.append(_small(1))
        first.close()
        second = Journal(path)
        assert second.last_lsn == 1
        assert second.append(_small(2)) == _ENTRY_COUNT - 1
        assert second.size_bytes == path.stat().st_size
        assert [lsn for lsn, _ in second.read(after=1)] == [2]
        second.close()

    def test_missing_file(self, tmp_path: Path) -> None:
        """Treat a missing file as empty."""
        journal = Journal(tmp_path / "nope")
        assert journal.read() == []
        assert journal.last_lsn == 0
        assert journal.size_bytes == 0

    def test_rotate_keeps_last_lsn(self, tmp_path: Path) -> None:
        """Remember the start point across an empty journal and reopen."""
        path = tmp_path / "j"
        journal = Journal(path)
        journal.append(_small(1))
        journal.rotate(_START)
        assert journal.read() == []
        assert journal.last_lsn == _START
        assert journal.append(_small(2)) == _START + 1
        journal.close()
        reopened = Journal(path)
        assert reopened.last_lsn == _START + 1
        assert [lsn for lsn, _ in reopened.read()] == [_START + 1]
        reopened.rotate(_START + 1)
        reopened.close()
        assert Journal(path).last_lsn == _START + 1

    def test_bad_header(self, tmp_path: Path) -> None:
        """Reject a damaged header."""
        path = tmp_path / "j"
        path.write_bytes(b"PYDBJ1 xx\n")
        with pytest.raises(JournalCorruptError):
            Journal(path).read()
        path.write_bytes(b"PYDBJ1 4")
        with pytest.raises(JournalCorruptError):
            Journal(path).read()

    @pytest.mark.parametrize(("sync", "expected"), [(True, 1), (False, 0)])
    def test_sync_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sync: bool, expected: int
    ) -> None:
        """Call fsync per append only when sync is on."""
        journal = Journal(tmp_path / "j", sync=sync)
        journal.append(_small(1))  # the first append also syncs the folder
        calls: list[int] = []
        monkeypatch.setattr(os, "fsync", calls.append)
        journal.append(_small(2))
        assert len(calls) == expected
        journal.close()


def _join(*lsns: int) -> bytes:
    """Encode one small entry per LSN given."""
    return b"".join(encode_entry(n, _small(n)) for n in lsns)


class TestLsnOrder:
    """Check gaps and repeats."""

    def test_gap_in_middle_is_corrupt(self, tmp_path: Path) -> None:
        """Report the first entry after the gap; no header, so line 3."""
        path = tmp_path / "j"
        path.write_bytes(_join(1, 2, 4, 5))
        with pytest.raises(JournalCorruptError) as info:
            Journal(path).read()
        assert info.value.line == _GAP_LINE

    def test_gap_in_middle_after_header(self, tmp_path: Path) -> None:
        """Count the header as line 1, so the same gap is line 4."""
        path = tmp_path / "j"
        path.write_bytes(b"PYDBJ1 0\n" + _join(1, 2, 4, 5))
        with pytest.raises(JournalCorruptError) as info:
            Journal(path).read()
        assert info.value.line == _GAP_LINE + 1

    def test_repeat_in_middle_is_corrupt(self, tmp_path: Path) -> None:
        """Treat a repeated LSN with entries after it as damage."""
        path = tmp_path / "j"
        path.write_bytes(_join(1, 2, 2, 3))
        with pytest.raises(JournalCorruptError) as info:
            Journal(path).read()
        assert info.value.line == _GAP_LINE

    def test_gap_at_end_is_torn(self, tmp_path: Path) -> None:
        """Truncate a gapped final entry."""
        path = tmp_path / "j"
        good = _join(1, 2)
        path.write_bytes(good + _join(4))
        assert [lsn for lsn, _ in Journal(path).read()] == [1, 2]
        assert path.read_bytes() == good

    def test_non_dict_change_is_unreadable(self) -> None:
        """Treat a change that is not an object as a bad line."""
        body = b'{"lsn":1,"changes":[5]}'
        line = b"%d %08x %s\n" % (len(body), zlib.crc32(body), body)
        assert decode_entries(line) == ([], 0)


class TestWriteFailure:
    """Check that failed appends never corrupt the journal."""

    def _prepare(self, path: Path) -> tuple[Journal, int]:
        """Append three entries and return the journal and the good size."""
        journal = Journal(path)
        for n in range(_ENTRY_COUNT):
            journal.append(_small(n))
        return journal, path.stat().st_size

    def _check(self, path: Path, journal: Journal, good: int) -> None:
        """Assert a failed journal refuses writes and reopens cleanly."""
        with pytest.raises(JournalWriteError):
            journal.append(_small(8))
        assert path.stat().st_size == good
        reopened = Journal(path)
        assert [lsn for lsn, _ in reopened.read()] == [1, 2, 3]
        assert reopened.append(_small(9)) == _ENTRY_COUNT + 1
        reopened.close()

    def test_fsync_failure(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Roll back and refuse further appends after an fsync error."""
        path = tmp_path / "j"
        journal, good = self._prepare(path)
        real = os.fsync
        calls: list[int] = []

        def flaky(fd: int) -> None:
            if not calls:
                calls.append(fd)
                raise OSError(_EIO, "boom")
            real(fd)

        monkeypatch.setattr(os, "fsync", flaky)
        with pytest.raises(JournalWriteError):
            journal.append(_small(7))
        self._check(path, journal, good)

    @pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
    def test_interrupt_is_passed_on_unchanged(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        interrupt: type[BaseException],
    ) -> None:
        """Cut back and refuse more appends, but re-raise Ctrl-C/exit as itself."""
        path = tmp_path / "j"
        journal, good = self._prepare(path)
        real = os.fsync
        calls: list[int] = []

        def interrupted(fd: int) -> None:
            if not calls:
                calls.append(fd)
                raise interrupt
            real(fd)

        monkeypatch.setattr(os, "fsync", interrupted)
        with pytest.raises(interrupt):
            journal.append(_small(7))
        assert journal.failed
        self._check(path, journal, good)

    def test_short_write(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Roll back a half-written line."""
        path = tmp_path / "j"
        journal, good = self._prepare(path)
        real_file: Any = journal._file
        line = encode_entry(_ENTRY_COUNT + 1, _small(7))

        class Half:
            """File wrapper that writes half a line and then fails."""

            def write(self, data: bytes) -> int:
                """Write half of *data*, then raise."""
                real_file.write(data[: len(line) // 2])
                real_file.flush()
                raise OSError(_ENOSPC, "full")

            def __getattr__(self, name: str) -> Any:
                """Delegate everything else."""
                return getattr(real_file, name)

        monkeypatch.setattr(journal, "_file", Half())
        with pytest.raises(JournalWriteError):
            journal.append(_small(7))
        self._check(path, journal, good)

    def test_new_file_syncs_directory_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sync the parent folder after the first append only."""
        calls: list[Path] = []
        monkeypatch.setattr("pydb.journal.sync_folder", calls.append)
        journal = Journal(tmp_path / "j")
        journal.append(_small(1))
        journal.append(_small(2))
        assert calls == [tmp_path]
        journal.close()
        again = Journal(tmp_path / "j")
        again.append(_small(3))
        assert calls == [tmp_path]
        again.close()

    def test_rotate_below_last_lsn_rejected(self, tmp_path: Path) -> None:
        """Refuse to rotate backwards."""
        journal = Journal(tmp_path / "j")
        journal.append(_small(1))
        journal.append(_small(2))
        with pytest.raises(ValueError, match="below"):
            journal.rotate(1)
        journal.close()

    def test_rotate_clears_a_failed_append(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Accept appends again once rotate wrote a fresh journal."""
        journal, _good = self._prepare(tmp_path / "j")

        def failing_fsync(fd: int) -> None:
            """Fail every call."""
            raise OSError(_EIO, f"I/O error on {fd}")

        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(JournalWriteError):
            journal.append(_small(8))
        assert journal.failed
        monkeypatch.undo()
        journal.rotate(_ENTRY_COUNT)
        assert not journal.failed
        assert journal.append(_small(9)) == _ENTRY_COUNT + 1
        journal.close()

    def test_rotate_never_reads_a_damaged_journal(self, tmp_path: Path) -> None:
        """Rotate away a journal whose middle is damaged without raising."""
        path = tmp_path / "j"
        good = encode_entry(1, _small(1))
        path.write_bytes(b"garbage\n" + good)
        Journal(path).rotate(_START)
        assert Journal(path).last_lsn == _START
