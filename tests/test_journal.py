"""Tests for the checksummed journal."""

import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from pydb.changes import Change, ChangeKind
from pydb.journal import Journal, JournalCorruptError, decode_entries, encode_entry
from pydb.schema import Column, Schema
from pydb.serializer import serialize_schema
from pydb.types import DataType

_ENTRY_COUNT = 3
_TS = datetime(2026, 10, 17, 12, 30, 45, 123456, tzinfo=UTC)
_DAY = date(2026, 10, 17)
_ROW_ID = 7
_START = 41


def _all_changes() -> list[Change]:
    """Build one change of every kind."""
    schema = Schema(
        columns=[
            Column("id", DataType.INTEGER, primary_key=True),
            Column("born", DataType.DATE, not_null=True),
        ]
    )
    data = {"id": 1, "born": _DAY, "seen": _TS, "name": "$date", "ok": True, "x": 1.5}
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

    def test_lsn_gap_at_end_is_torn(self) -> None:
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
        reopened.rotate(_START)
        reopened.close()
        assert Journal(path).last_lsn == _START

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
        calls: list[int] = []
        monkeypatch.setattr(os, "fsync", calls.append)
        journal.append(_small(1))
        assert len(calls) == expected
        journal.close()
