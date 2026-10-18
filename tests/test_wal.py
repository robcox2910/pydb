"""Tests for the Write-Ahead Log (WAL).

The WAL writes every change to a log file before applying it. If the
database crashes, it reads the log to recover.
"""

import os
from pathlib import Path

import pytest

from pydb.wal import WALError, WriteAheadLog

TXN_1 = "txn_001"
TXN_2 = "txn_002"
ONE_ENTRY = 1
TWO_ENTRIES = 2
THREE_ENTRIES = 3


class TestWALBasics:
    """Verify basic WAL operations."""

    def test_empty_wal(self, tmp_path: Path) -> None:
        """A new WAL should have no entries."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        assert wal.read_entries() == []

    def test_log_begin(self, tmp_path: Path) -> None:
        """BEGIN should be logged."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        entries = wal.read_entries()
        assert len(entries) == ONE_ENTRY
        assert entries[0]["type"] == "BEGIN"
        assert entries[0]["txn"] == TXN_1

    def test_log_operation(self, tmp_path: Path) -> None:
        """Operations should be logged with table, op, and data."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_operation(TXN_1, "cards", "INSERT", {"name": "Pikachu"})
        entries = wal.read_entries()
        assert len(entries) == ONE_ENTRY
        assert entries[0]["op"] == "INSERT"
        assert entries[0]["table"] == "cards"

    def test_log_commit(self, tmp_path: Path) -> None:
        """COMMIT should be logged."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_commit(TXN_1)
        entries = wal.read_entries()
        assert len(entries) == TWO_ENTRIES
        assert entries[1]["type"] == "COMMIT"

    def test_log_rollback(self, tmp_path: Path) -> None:
        """ROLLBACK should be logged."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_rollback(TXN_1)
        entries = wal.read_entries()
        assert entries[1]["type"] == "ROLLBACK"


class TestWALRecovery:
    """Verify recovery-related operations."""

    def test_committed_txns(self, tmp_path: Path) -> None:
        """get_committed_txns should return only committed transaction IDs."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_commit(TXN_1)
        wal.log_begin(TXN_2)
        # TXN_2 never committed.
        assert wal.get_committed_txns() == {TXN_1}

    def test_get_operations(self, tmp_path: Path) -> None:
        """get_operations should return ops for a specific transaction."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_operation(TXN_1, "cards", "INSERT", {"name": "Pikachu"})
        wal.log_operation(TXN_1, "cards", "INSERT", {"name": "Charmander"})
        wal.log_begin(TXN_2)
        wal.log_operation(TXN_2, "cards", "INSERT", {"name": "Squirtle"})
        wal.log_commit(TXN_1)

        ops = wal.get_operations(TXN_1)
        assert len(ops) == TWO_ENTRIES
        ops2 = wal.get_operations(TXN_2)
        assert len(ops2) == ONE_ENTRY

    def test_uncommitted_ops_excluded(self, tmp_path: Path) -> None:
        """Operations from uncommitted transactions should be identifiable."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_operation(TXN_1, "cards", "INSERT", {"name": "Pikachu"})
        # No commit -- this is a crashed transaction.
        committed = wal.get_committed_txns()
        assert TXN_1 not in committed


class TestWALClear:
    """Verify WAL clearing."""

    def test_clear(self, tmp_path: Path) -> None:
        """Clearing the WAL should remove all entries."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.log_begin(TXN_1)
        wal.log_commit(TXN_1)
        wal.clear()
        assert wal.read_entries() == []

    def test_clear_nonexistent(self, tmp_path: Path) -> None:
        """Clearing a WAL that doesn't exist should not raise."""
        wal = WriteAheadLog(log_path=tmp_path / "wal.log")
        wal.clear()  # Should not raise.


class TestWALPath:
    """Verify WAL path property."""

    def test_path(self, tmp_path: Path) -> None:
        """The path property should return the log file path."""
        log_path = tmp_path / "wal.log"
        wal = WriteAheadLog(log_path=log_path)
        assert wal.path == log_path


class TestRecovery:
    """After a crash, recover() hands back exactly the committed work, in order."""

    def test_recover_returns_committed_operations_in_order(self, tmp_path: Path) -> None:
        """Only committed transactions are replayed, in the order they were logged."""
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        wal.log_operation("t1", "cards", "INSERT", {"id": "1"})
        wal.log_begin("t2")
        wal.log_operation("t2", "cards", "INSERT", {"id": "2"})
        wal.log_operation("t1", "cards", "INSERT", {"id": "3"})
        wal.log_commit("t1")
        wal.log_begin("t3")
        wal.log_operation("t3", "cards", "DELETE", {"id": "1"})
        wal.log_rollback("t3")
        ops = wal.recover()
        assert [op["data"] for op in ops] == [{"id": "1"}, {"id": "3"}]

    def test_half_written_last_line_is_ignored(self, tmp_path: Path) -> None:
        """A crash mid-write leaves a torn final line; recovery skips just that."""
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        wal.log_operation("t1", "cards", "INSERT", {"id": "1"})
        wal.log_commit("t1")
        with wal.path.open("a", encoding="utf-8") as f:
            f.write('{"type": "OP", "txn": "t2", "ta')
        assert len(wal.recover()) == 1

    def test_last_line_torn_inside_a_character_is_ignored(self, tmp_path: Path) -> None:
        """A torn final line that isn't even valid UTF-8 is skipped like any torn line."""
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        wal.log_operation("t1", "cards", "INSERT", {"id": "1"})
        wal.log_commit("t1")
        with wal.path.open("ab") as f:
            f.write(b'{"type": "OP", "txn": "t\xc3')  # cut half-way through "\xc3\xa9"
        assert len(wal.recover()) == 1

    def test_bytes_that_are_not_text_in_the_middle_are_an_error(self, tmp_path: Path) -> None:
        """A line that isn't UTF-8 before the last line is corruption, so WALError."""
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        with wal.path.open("ab") as f:
            f.write(b"\x80\n")
        wal.log_commit("t1")
        with pytest.raises(WALError, match="line 2"):
            wal.recover()

    def test_damage_in_the_middle_is_an_error(self, tmp_path: Path) -> None:
        """Regression: a corrupt middle entry used to be skipped silently."""
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        with wal.path.open("a", encoding="utf-8") as f:
            f.write("not json at all\n")
        wal.log_commit("t1")
        with pytest.raises(WALError, match="line 2"):
            wal.recover()

    def test_commit_is_forced_to_disk(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A COMMIT must really reach the disk (fsync), or it isn't durable."""
        synced: list[int] = []
        real_fsync = os.fsync

        def recording_fsync(fd: int) -> None:
            synced.append(fd)
            real_fsync(fd)

        monkeypatch.setattr(os, "fsync", recording_fsync)
        wal = WriteAheadLog(tmp_path / "wal.log")
        wal.log_begin("t1")
        assert synced == []
        wal.log_commit("t1")
        assert len(synced) == 1
