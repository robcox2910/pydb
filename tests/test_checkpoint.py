"""Tests for checkpoints: snapshots, checkpoint.json and a fresh journal."""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pydb.changes import Change, ChangeKind
from pydb.checkpoint import CHECKPOINT_FILE, SNAPSHOT_DIR, Checkpointer
from pydb.database import Database, DatabaseError
from pydb.durable import JOURNAL_FILE, RecoveryError
from pydb.journal import Journal, JournalWriteError
from pydb.record import Value
from pydb.schema import Column, Schema
from pydb.transaction import Transaction
from pydb.types import DataType
from tests.crash_helpers import checkpoint_workload

_CHILD_TIMEOUT = 5
_CRASH_EXIT = 1
_ROWS = 20
_TWO_ROWS = 2
_THREE_ROWS = 3
_FIRST_ID = 1
_SECOND_ID = 2
_LSN_ONE = 1
_LSN_TWO = 2
_LSN_THREE = 3
_TINY = 1
_HELPER = Path(__file__).with_name("crash_helpers.py")

type TableState = tuple[list[tuple[int, dict[str, Value]]], int, list[tuple[str, str]]]


def _schema() -> Schema:
    """Return a two-column schema."""
    return Schema(columns=[Column("name", DataType.TEXT), Column("power", DataType.INTEGER)])


def _open(folder: Path, checkpoint_bytes: int = 4 * 1024 * 1024) -> Database:
    """Open a journalled database in *folder*."""
    return Database(folder, journal=True, checkpoint_bytes=checkpoint_bytes)


def _state(db: Database) -> dict[str, TableState]:
    """Return every table's rows, next ID and indexes."""
    out: dict[str, TableState] = {}
    for name in db.table_names():
        table = db.get_table(name)
        rows = [(r.record_id, dict(r.data)) for r in table.select()]
        out[name] = (rows, table.next_id, sorted(table.index_definitions))
    return out


def _checkpoint_json(folder: Path) -> dict[str, object]:
    """Return the parsed checkpoint.json."""
    return json.loads((folder / CHECKPOINT_FILE).read_text(encoding="utf-8"))


def _snapshot_files(folder: Path) -> set[str]:
    """Return the names of the files in the snapshot folder."""
    return {p.name for p in (folder / SNAPSHOT_DIR).iterdir()}


def _fill(db: Database, count: int = _ROWS) -> None:
    """Create ``cards`` and commit *count* rows one by one."""
    cards = db.create_table("cards", _schema())
    for number in range(count):
        cards.insert({"name": f"card{number}", "power": number})


class TestCheckpoint:
    """checkpoint() writes snapshots, checkpoint.json, then a fresh journal."""

    def test_checkpoint_shrinks_the_journal(self, tmp_path: Path) -> None:
        """Rotate the journal down to its header and record the LSN."""
        db = _open(tmp_path)
        _fill(db)
        before = (tmp_path / JOURNAL_FILE).stat().st_size
        db.checkpoint()
        after = (tmp_path / JOURNAL_FILE).stat().st_size
        assert after < before
        assert Journal(tmp_path / JOURNAL_FILE).read() == []
        last = _ROWS + 1
        assert _checkpoint_json(tmp_path) == {
            "lsn": last,
            "tables": [{"name": "cards", "file": f"cards.{last}.json"}],
        }
        assert _snapshot_files(tmp_path) == {f"cards.{last}.json"}

    def test_reopen_after_checkpoint_and_more_commits(self, tmp_path: Path) -> None:
        """Load the snapshot, then replay only the later commits."""
        db = _open(tmp_path)
        _fill(db)
        db.checkpoint()
        cards = db.get_table("cards")
        cards.update(_FIRST_ID, {"power": -1})
        cards.delete(_SECOND_ID)
        cards.insert({"name": "late", "power": 0})
        expected = _state(db)
        del db
        again = _open(tmp_path)
        assert _state(again) == expected
        assert Journal(tmp_path / JOURNAL_FILE).last_lsn == _ROWS + 1 + _THREE_ROWS

    def test_only_changed_tables_are_rewritten(self, tmp_path: Path) -> None:
        """Keep the old snapshot of a table nobody touched."""
        db = _open(tmp_path)
        db.create_table("quiet", _schema())
        db.create_table("busy", _schema())
        db.checkpoint()
        db.get_table("busy").insert({"name": "A", "power": 1})
        db.checkpoint()
        files = {t["name"]: t["file"] for t in _tables(tmp_path)}
        assert files == {"quiet": f"quiet.{_LSN_TWO}.json", "busy": f"busy.{_LSN_THREE}.json"}
        assert _snapshot_files(tmp_path) == set(files.values())

    def test_dropped_table_files_go_at_checkpoint(self, tmp_path: Path) -> None:
        """Remove a dropped table's snapshot and forget the drop."""
        db = _open(tmp_path)
        db.create_table("gone", _schema())
        db.checkpoint()
        db.drop_table("gone")
        assert db._dropped == {"gone"}
        db.checkpoint()
        assert db._dropped == set()
        assert _tables(tmp_path) == []
        assert _snapshot_files(tmp_path) == set()
        assert _open(tmp_path).table_names() == []

    def test_nothing_new_writes_nothing(self, tmp_path: Path) -> None:
        """Leave the folder alone when nothing changed since the last checkpoint."""
        db = _open(tmp_path)
        db.checkpoint()
        assert not (tmp_path / CHECKPOINT_FILE).exists()
        assert not (tmp_path / JOURNAL_FILE).exists()

    def test_save_close_and_save_table_checkpoint(self, tmp_path: Path) -> None:
        """Make save(), save_table() and close() take a checkpoint."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        db.save()
        assert _checkpoint_json(tmp_path)["lsn"] == _LSN_ONE
        db.get_table("cards").insert({"name": "A", "power": 1})
        db.save_table("cards")
        assert _checkpoint_json(tmp_path)["lsn"] == _LSN_TWO
        db.get_table("cards").insert({"name": "B", "power": 2})
        db.close()
        assert _checkpoint_json(tmp_path)["lsn"] == _LSN_THREE
        assert len(_state(_open(tmp_path))["cards"][0]) == _TWO_ROWS

    def test_plain_database_cannot_checkpoint(self, tmp_path: Path) -> None:
        """Refuse checkpoint() without a journal."""
        with pytest.raises(DatabaseError, match="journal"):
            Database(tmp_path).checkpoint()

    def test_no_checkpoint_inside_a_transaction(self, tmp_path: Path) -> None:
        """Refuse to snapshot uncommitted work; close() just skips it."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "maybe", "power": 1})
        with pytest.raises(DatabaseError, match="transaction"):
            db.checkpoint()
        db.close()
        assert not (tmp_path / CHECKPOINT_FILE).exists()
        assert _state(_open(tmp_path))["cards"][0] == []


class TestAutoCheckpoint:
    """A commit that makes the journal too big triggers a checkpoint."""

    def test_tiny_threshold_checkpoints_after_each_commit(self, tmp_path: Path) -> None:
        """Checkpoint after the commit that crossed checkpoint_bytes."""
        db = _open(tmp_path, checkpoint_bytes=_TINY)
        _fill(db, _TWO_ROWS)
        assert _checkpoint_json(tmp_path)["lsn"] == _TWO_ROWS + 1
        assert Journal(tmp_path / JOURNAL_FILE).read() == []
        assert len(_state(_open(tmp_path))["cards"][0]) == _TWO_ROWS

    def test_transaction_commit_auto_checkpoints(self, tmp_path: Path) -> None:
        """Checkpoint after a transaction's commit, outside the transaction."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        db._checkpoint_bytes = _TINY
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "A", "power": 1})
        txn.commit()
        assert _checkpoint_json(tmp_path)["lsn"] == _LSN_TWO

    def test_failed_auto_checkpoint_keeps_the_commit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Log a failed automatic checkpoint; the commit is already durable."""
        db = _open(tmp_path, checkpoint_bytes=_TINY)

        def broken(*_args: object, **_kwargs: object) -> None:
            """Fail like a full disk."""
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(Checkpointer, "checkpoint", broken)
        with caplog.at_level(logging.WARNING, logger="pydb.database"):
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        assert "checkpoint failed" in caplog.text
        assert db.get_table("cards").row_count == 1
        monkeypatch.undo()
        del db
        assert len(_state(_open(tmp_path))["cards"][0]) == 1


class TestRecoveryFromCheckpoint:
    """Opening uses checkpoint.json and only the journal entries after it."""

    def test_old_snapshots_migrate_into_the_first_checkpoint(self, tmp_path: Path) -> None:
        """Load <table>.json files, then replace them at the first checkpoint."""
        plain = Database(tmp_path)
        plain.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        plain.create_table("checkpoint", _schema()).insert({"name": "B", "power": 2})
        plain.save()
        with _open(tmp_path) as db:
            expected = _state(db)
        assert not (tmp_path / "cards.json").exists()
        assert _checkpoint_json(tmp_path)["lsn"] == 0
        assert _state(_open(tmp_path)) == expected

    def test_missing_snapshot_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to open when a snapshot listed in checkpoint.json is gone."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema())
        for path in (tmp_path / SNAPSHOT_DIR).iterdir():
            path.unlink()
        with pytest.raises(RecoveryError, match="cards"):
            _open(tmp_path)

    def test_bad_checkpoint_file_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to open with a checkpoint.json that can't be read."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema())
        (tmp_path / CHECKPOINT_FILE).write_text('{"lsn": "x", "tables": 3}', encoding="utf-8")
        with pytest.raises(RecoveryError, match="checkpoint"):
            _open(tmp_path)

    def test_journal_behind_the_checkpoint_is_restarted(self, tmp_path: Path) -> None:
        """Number new commits after the checkpoint even if journal.log was lost."""
        with _open(tmp_path) as db:
            _fill(db, _TWO_ROWS)
        (tmp_path / JOURNAL_FILE).unlink()
        db = _open(tmp_path)
        db.get_table("cards").insert({"name": "C", "power": 3})
        del db
        assert len(_state(_open(tmp_path))["cards"][0]) == _THREE_ROWS

    def test_journal_starting_after_the_checkpoint_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to open when commits between checkpoint and journal are missing."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema())
        journal = Journal(tmp_path / JOURNAL_FILE)
        journal.rotate(_LSN_THREE)
        journal.append([Change(ChangeKind.DROP_TABLE, "cards")])
        journal.close()
        with pytest.raises(RecoveryError, match="missing"):
            _open(tmp_path)


class TestJournalModeSafety:
    """A journalled folder can't be opened without the journal."""

    def test_plain_open_of_a_journalled_folder_is_refused(self, tmp_path: Path) -> None:
        """Raise DatabaseError when journal.log or checkpoint.json is present."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        with pytest.raises(DatabaseError, match="journal=True"):
            Database(tmp_path)
        db.close()
        (tmp_path / JOURNAL_FILE).unlink()
        with pytest.raises(DatabaseError, match="journal=True"):
            Database(tmp_path)

    def test_plain_table_named_checkpoint_is_fine(self, tmp_path: Path) -> None:
        """Keep allowing a plain database with a table called ``checkpoint``."""
        plain = Database(tmp_path)
        plain.create_table("checkpoint", _schema())
        plain.save()
        assert Database(tmp_path).load_table("checkpoint").row_count == 0

    def test_checkpoint_heals_a_failed_journal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Accept commits again after a checkpoint starts a fresh journal."""
        db = _open(tmp_path)
        cards = db.create_table("cards", _schema())
        real_fsync = os.fsync
        calls: list[int] = []

        def failing_fsync(fd: int) -> None:
            """Fail the first call, then behave normally."""
            calls.append(fd)
            if len(calls) == 1:
                raise OSError(5, "I/O error")
            real_fsync(fd)

        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(JournalWriteError):
            cards.insert({"name": "lost", "power": 0})
        with pytest.raises(JournalWriteError, match="reopened"):
            cards.insert({"name": "refused", "power": 0})
        db.checkpoint()
        cards.insert({"name": "kept", "power": 1})
        del db
        rows = _state(_open(tmp_path))["cards"][0]
        assert rows == [(_FIRST_ID, {"name": "kept", "power": 1})]

    def test_checkpoint_with_nothing_new_still_heals(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Start a fresh journal after a failed write even if no table changed."""
        db = _open(tmp_path)
        cards = db.create_table("cards", _schema())
        db.checkpoint()

        def failing_fsync(fd: int) -> None:
            """Fail every call."""
            raise OSError(5, f"I/O error on {fd}")

        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(JournalWriteError):
            cards.insert({"name": "lost", "power": 0})
        monkeypatch.undo()
        db.checkpoint()
        cards.insert({"name": "kept", "power": 1})
        del db
        assert len(_state(_open(tmp_path))["cards"][0]) == 1


def _tables(folder: Path) -> list[dict[str, str]]:
    """Return the table list of checkpoint.json."""
    tables = _checkpoint_json(folder)["tables"]
    assert isinstance(tables, list)
    return [dict(t) for t in tables]  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]


def _expected_state(folder: Path) -> dict[str, TableState]:
    """Run the checkpoint workload without crashing and return the state."""
    db = _open(folder)
    checkpoint_workload(db, prior_checkpoint=False)
    return _state(db)


class TestCrashDuringCheckpoint:
    """A real child process dies between the steps of a checkpoint."""

    @pytest.mark.parametrize("when", ["first", "second"])
    @pytest.mark.parametrize("step", ["snapshots", "checkpoint_file", "rotate"])
    def test_crash_at_every_checkpoint_step(self, tmp_path: Path, step: str, when: str) -> None:
        """Reopen to exactly the last committed state, nothing doubled."""
        folder = tmp_path / "db"
        result = subprocess.run(
            [sys.executable, str(_HELPER), "checkpoint", str(folder), step, when],
            check=False,
            timeout=_CHILD_TIMEOUT,
            capture_output=True,
        )
        assert result.returncode == _CRASH_EXIT, result.stderr.decode()
        expected = _expected_state(tmp_path / "expected")
        again = _open(folder)
        assert _state(again) == expected
        cards = again.get_table("cards")
        next_id = expected["cards"][1]
        assert cards.insert({"name": "after", "power": 0}).record_id == next_id
        again.close()
        reopened = _open(folder)
        assert len(reopened.get_table("cards").select()) == len(expected["cards"][0]) + 1
        referenced = {t["file"] for t in _tables(folder)}
        assert _snapshot_files(folder) == referenced
