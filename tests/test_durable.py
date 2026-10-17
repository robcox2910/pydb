"""Tests for journalled databases: durable commits and recovery on open."""

import logging
import os
import subprocess
import sys
import threading
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from pydb.changes import Change, ChangeKind
from pydb.checkpoint import Checkpointer
from pydb.commits import COMMITTED_NOTE
from pydb.database import Database
from pydb.durable import JOURNAL_FILE, RecoveryError
from pydb.executor import execute
from pydb.journal import Journal, JournalCorruptError, JournalWriteError
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.transaction import Transaction
from pydb.types import DataType
from tests.crash_helpers import abandon

_CRASH_ROWS = 25
_TXN_ROWS = 4
_CHILD_TIMEOUT = 5
_CRASH_EXIT = 1
_FIRST_ID = 1
_SECOND_ID = 2
_THIRD_ID = 3
_FOURTH_ID = 4
_POWER = 55
_NEW_POWER = 99
_OTHER_POWER = 48
_THREE_ROWS = 3
_TWO_ROWS = 2
_TWO_ENTRIES = 2
_SMALL_JOURNAL = 32  # bytes: a freshly rotated journal is just its header
_DAY = date(2026, 10, 17)
_TS = datetime(2026, 10, 17, 12, 30, 45, tzinfo=UTC)
_HELPER = Path(__file__).with_name("crash_helpers.py")


def _schema() -> Schema:
    """Return a schema with text, number and date columns."""
    return Schema(
        columns=[
            Column("name", DataType.TEXT),
            Column("power", DataType.INTEGER),
        ]
    )


def _open(folder: Path, **options: bool) -> Database:
    """Open a journalled database in *folder*."""
    return Database(folder, journal=True, **options)


def _rows(db: Database, table: str) -> list[tuple[int, dict[str, object]]]:
    """Return ``(id, data)`` for every row of *table*."""
    return [(r.record_id, dict(r.data)) for r in db.get_table(table).select()]


def _run_child(scenario: str, folder: Path, count: int) -> None:
    """Run a crash scenario in a real child process and check it crashed."""
    result = subprocess.run(
        [sys.executable, str(_HELPER), scenario, str(folder), str(count)],
        check=False,
        timeout=_CHILD_TIMEOUT,
        capture_output=True,
    )
    assert result.returncode == _CRASH_EXIT, result.stderr.decode()


class TestDurableCommits:
    """Commits survive without save()."""

    def test_commit_survives_without_save(self, tmp_path: Path) -> None:
        """Reopen after dropping the object and see the committed row."""
        db = _open(tmp_path)
        db.create_table("cards", _schema()).insert({"name": "Pikachu", "power": _POWER})
        abandon(db)
        again = _open(tmp_path)
        assert _rows(again, "cards") == [(_FIRST_ID, {"name": "Pikachu", "power": _POWER})]

    def test_each_autocommit_is_one_entry(self, tmp_path: Path) -> None:
        """Write one journal entry per change made outside a transaction."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        assert Journal(tmp_path / JOURNAL_FILE).last_lsn == _TWO_ENTRIES

    def test_transaction_is_one_entry(self, tmp_path: Path) -> None:
        """Write a whole transaction as a single journal entry."""
        db = _open(tmp_path)  # no close(): that would checkpoint the journal away
        db.create_table("cards", _schema())
        txn = Transaction(db)
        cards = txn.get_table("cards")
        cards.insert({"name": "A", "power": 1})
        cards.insert({"name": "B", "power": 2})
        txn.commit()
        abandon(db)
        entries = Journal(tmp_path / JOURNAL_FILE).read()
        assert len(entries) == _TWO_ENTRIES
        assert [c.kind for c in entries[1][1]] == [ChangeKind.INSERT, ChangeKind.INSERT]
        assert len(_rows(_open(tmp_path), "cards")) == _TWO_ROWS

    def test_rolled_back_transaction_is_absent(self, tmp_path: Path) -> None:
        """Leave no trace of a rolled-back transaction after reopening."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
            txn = Transaction(db)
            txn.get_table("cards").insert({"name": "B", "power": 2})
            txn.get_table("cards").delete(_FIRST_ID)
            db.create_table("temp", _schema())
            txn.rollback()
        again = _open(tmp_path)
        assert again.table_names() == ["cards"]
        assert _rows(again, "cards") == [(_FIRST_ID, {"name": "A", "power": 1})]

    def test_dates_and_timestamps_survive(self, tmp_path: Path) -> None:
        """Recover DATE and TIMESTAMP values exactly."""
        schema = Schema(
            columns=[Column("day", DataType.DATE), Column("at", DataType.TIMESTAMP)],
        )
        with _open(tmp_path) as db:
            db.create_table("events", schema).insert({"day": _DAY, "at": _TS})
        assert _rows(_open(tmp_path), "events") == [(_FIRST_ID, {"day": _DAY, "at": _TS})]

    def test_sql_statements_are_durable(self, tmp_path: Path) -> None:
        """Recover changes made through SQL."""
        with _open(tmp_path) as db:
            execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
            execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
            execute(parse_sql("UPDATE cards SET power = 99 WHERE name = 'Pikachu'"), db)
        assert _rows(_open(tmp_path), "cards") == [
            (_FIRST_ID, {"name": "Pikachu", "power": _NEW_POWER})
        ]

    def test_sync_false_still_recovers(self, tmp_path: Path) -> None:
        """Recover commits written without fsync (the process didn't crash)."""
        with _open(tmp_path, sync=False) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        assert len(_rows(_open(tmp_path), "cards")) == 1


class TestReplay:
    """Replay rebuilds exactly the state before the crash."""

    def test_every_change_kind_replays(self, tmp_path: Path) -> None:
        """Replay inserts, updates, deletes, index changes and table drops."""
        with _open(tmp_path) as db:
            cards = db.create_table("cards", _schema())
            cards.insert({"name": "A", "power": _POWER})
            cards.insert({"name": "B", "power": _OTHER_POWER})
            cards.insert({"name": "C", "power": 1})
            cards.update(_SECOND_ID, {"power": _NEW_POWER})
            cards.delete(_THIRD_ID)
            cards.create_index("by_power", "power")
            cards.create_index("by_name", "name")
            cards.drop_index("by_name")
            db.create_table("gone", _schema()).insert({"name": "X", "power": 1})
            db.drop_table("gone")
            db.create_table("gone", _schema())
        again = _open(tmp_path)
        assert again.table_names() == ["cards", "gone"]
        assert _rows(again, "gone") == []
        assert _rows(again, "cards") == [
            (_FIRST_ID, {"name": "A", "power": _POWER}),
            (_SECOND_ID, {"name": "B", "power": _NEW_POWER}),
        ]
        table = again.get_table("cards")
        assert table.index_definitions == [("by_power", "power")]
        index = table.indexes["by_power"]
        found = table.select_by_index(index, _NEW_POWER)
        assert [r.record_id for r in found] == [_SECOND_ID]

    def test_next_id_matches_after_deleting_the_last_row(self, tmp_path: Path) -> None:
        """Keep handing out new IDs after the highest one ever used."""
        with _open(tmp_path) as db:
            cards = db.create_table("cards", _schema())
            for name in "ABC":
                cards.insert({"name": name, "power": 1})
            cards.delete(_THIRD_ID)
        again = _open(tmp_path)
        record = again.get_table("cards").insert({"name": "D", "power": 1})
        assert record.record_id == _FOURTH_ID

    def test_replay_does_not_rejournal_or_call_listeners(self, tmp_path: Path) -> None:
        """Leave the journal untouched and call no listener while recovering."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        size = (tmp_path / JOURNAL_FILE).stat().st_size
        again = _open(tmp_path)
        seen: list[list[Change]] = []
        again.add_commit_listener(seen.append)
        again.close()
        assert (tmp_path / JOURNAL_FILE).stat().st_size == size
        assert seen == []

    def test_commits_after_reopen_continue_the_lsn(self, tmp_path: Path) -> None:
        """Number new entries after the recovered ones."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema())
        with _open(tmp_path) as db:
            db.get_table("cards").insert({"name": "A", "power": 1})
        assert Journal(tmp_path / JOURNAL_FILE).last_lsn == _TWO_ENTRIES
        assert len(_rows(_open(tmp_path), "cards")) == 1

    def test_existing_snapshots_load_before_replay(self, tmp_path: Path) -> None:
        """Turn on the journal for a folder of saved tables."""
        plain = Database(tmp_path)
        plain.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        plain.save()
        with _open(tmp_path) as db:
            db.get_table("cards").insert({"name": "B", "power": 2})
        again = _open(tmp_path)
        assert [rid for rid, _ in _rows(again, "cards")] == [_FIRST_ID, _SECOND_ID]

    def test_change_for_unknown_table_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to open when the journal mentions a table that isn't there."""
        Journal(tmp_path / JOURNAL_FILE).append(
            [Change(ChangeKind.INSERT, "ghost", record_id=1, data={"x": 1})]
        )
        with pytest.raises(RecoveryError, match="ghost"):
            _open(tmp_path)

    def test_delete_of_missing_row_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to replay deleting a row that isn't there."""
        journal = Journal(tmp_path / JOURNAL_FILE)
        journal.append([Change(ChangeKind.CREATE_TABLE, "cards", schema=_schema())])
        journal.append([Change(ChangeKind.DELETE, "cards", record_id=_FIRST_ID)])
        journal.close()
        with pytest.raises(RecoveryError, match="delete"):
            _open(tmp_path)

    def test_create_of_existing_table_is_an_error(self, tmp_path: Path) -> None:
        """Refuse to replay a table creation over a table that already exists."""
        journal = Journal(tmp_path / JOURNAL_FILE)
        create = Change(ChangeKind.CREATE_TABLE, "cards", schema=_schema())
        journal.append([create])
        journal.append([create])
        journal.close()
        with pytest.raises(RecoveryError, match="cards"):
            _open(tmp_path)


class TestDamagedJournal:
    """Torn tails are dropped; mid-file damage needs repair=True."""

    def test_torn_tail_is_dropped(self, tmp_path: Path) -> None:
        """Open with every complete commit when the last line is half-written."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        with (tmp_path / JOURNAL_FILE).open("ab") as handle:
            handle.write(b"120 0000")
        again = _open(tmp_path)
        assert len(_rows(again, "cards")) == 1

    def test_mid_file_damage_raises_unless_repair(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Raise on damage before good entries; repair keeps the entries before it."""
        db = _open(tmp_path)  # no close(): that would checkpoint the journal away
        cards = db.create_table("cards", _schema())
        cards.insert({"name": "A", "power": 1})
        cards.insert({"name": "B", "power": 2})
        abandon(db)
        path = tmp_path / JOURNAL_FILE
        lines = path.read_bytes().split(b"\n")
        lines[1] = lines[1].replace(b"A", b"Z")
        path.write_bytes(b"\n".join(lines))
        with pytest.raises(JournalCorruptError):
            _open(tmp_path)
        with caplog.at_level(logging.WARNING, logger="pydb.durable"):
            repaired = _open(tmp_path, repair=True)
        assert repaired.table_names() == ["cards"]
        assert _rows(repaired, "cards") == []
        assert "repair" in caplog.text


class TestJournalFailure:
    """A commit whose journal write fails is undone."""

    def test_failed_append_undoes_the_insert(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Raise JournalWriteError and leave the row out of memory and disk."""
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
            cards.insert({"name": "A", "power": 1})
        assert cards.row_count == 0
        assert cards.next_id == _FIRST_ID
        db.close()
        assert _rows(_open(tmp_path), "cards") == []

    def test_failed_append_rolls_back_a_transaction(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Roll the whole transaction back when its entry can't be written."""
        db = _open(tmp_path)
        db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "B", "power": 2})

        def failing_fsync(fd: int) -> None:
            """Fail every call."""
            raise OSError(5, f"I/O error on {fd}")

        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(JournalWriteError):
            txn.commit()
        monkeypatch.undo()
        txn.rollback()
        assert len(_rows(db, "cards")) == 1
        db.close()
        assert len(_rows(_open(tmp_path), "cards")) == 1


def _interrupt_after_append(_self: Checkpointer, _lsn: int, _changes: object) -> None:
    """Stand in for the bookkeeping right after a journal append: press Ctrl-C."""
    raise KeyboardInterrupt


class TestInterruptAfterCommitPoint:
    """Ctrl-C landing just after the journal append never undoes the commit."""

    def test_autocommit_stands(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep the row in memory, say so in a note, and in the closing checkpoint."""
        db = _open(tmp_path)
        cards = db.create_table("cards", _schema())
        db.checkpoint()  # so only the interrupted commit can make "cards" dirty
        monkeypatch.setattr(Checkpointer, "committed", _interrupt_after_append)
        with pytest.raises(KeyboardInterrupt) as caught:
            cards.insert({"name": "A", "power": 1})
        monkeypatch.undo()
        assert COMMITTED_NOTE in caught.value.__notes__
        assert cards.row_count == 1
        db.close()
        assert (tmp_path / JOURNAL_FILE).stat().st_size < _SMALL_JOURNAL  # rotated away
        assert len(_rows(_open(tmp_path), "cards")) == 1

    def test_create_table_stands(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep a table whose creation reached the journal just before Ctrl-C."""
        db = _open(tmp_path)
        monkeypatch.setattr(Checkpointer, "committed", _interrupt_after_append)
        with pytest.raises(KeyboardInterrupt):
            db.create_table("cards", _schema())
        monkeypatch.undo()
        assert db.table_names() == ["cards"]
        db.get_table("cards").insert({"name": "A", "power": 1})
        abandon(db)
        assert len(_rows(_open(tmp_path), "cards")) == 1

    def test_drop_table_stands(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep a table dropped for good when the drop reached the journal before Ctrl-C."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        monkeypatch.setattr(Checkpointer, "committed", _interrupt_after_append)
        with pytest.raises(KeyboardInterrupt):
            db.drop_table("cards")
        monkeypatch.undo()
        assert db.table_names() == []
        abandon(db)
        assert _open(tmp_path).table_names() == []

    def test_transaction_stands(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep the committed rows, end the transaction and hand the lock back."""
        db = _open(tmp_path)
        db.create_table("cards", _schema())
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "A", "power": 1})
        txn.get_table("cards").insert({"name": "B", "power": 2})
        monkeypatch.setattr(Checkpointer, "committed", _interrupt_after_append)
        with pytest.raises(KeyboardInterrupt):
            txn.commit()
        monkeypatch.undo()
        assert not txn.is_active
        assert not db.in_transaction
        got: list[bool] = []

        def take_the_lock() -> None:
            """Take the lock from another thread (it must be free), then give it back."""
            got.append(db.lock.acquire(timeout=_CHILD_TIMEOUT))
            if got[-1]:
                db.lock.release()

        other = threading.Thread(target=take_the_lock)
        other.start()
        other.join()
        assert got == [True]
        assert len(_rows(db, "cards")) == _TWO_ROWS
        abandon(db)
        assert len(_rows(_open(tmp_path), "cards")) == _TWO_ROWS


class TestLifecycle:
    """Opening, closing, and load/save on a journalled database."""

    def test_load_is_a_no_op(self, tmp_path: Path) -> None:
        """Keep the recovered state when load() is called."""
        db = _open(tmp_path)
        db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        db.load()
        assert len(_rows(db, "cards")) == 1
        assert db.load_table("cards") is db.get_table("cards")

    def test_save_writes_no_old_style_snapshots(self, tmp_path: Path) -> None:
        """Checkpoint on save(): no ``<table>.json`` beside the journal."""
        with _open(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
            db.save()
            db.save_table("cards")
        assert not (tmp_path / "cards.json").exists()
        assert len(_rows(_open(tmp_path), "cards")) == 1

    def test_journalled_property(self, tmp_path: Path) -> None:
        """Report whether the database keeps a journal."""
        assert _open(tmp_path / "j").journalled
        assert not Database(tmp_path / "p").journalled

    def test_context_manager_closes(self, tmp_path: Path) -> None:
        """Return the database from ``with`` and close it afterwards."""
        with _open(tmp_path / "new") as db:
            db.create_table("cards", _schema())
        assert (tmp_path / "new" / JOURNAL_FILE).exists()

    def test_plain_database_has_no_journal(self, tmp_path: Path) -> None:
        """Create no journal file without journal=True."""
        with Database(tmp_path) as db:
            db.create_table("cards", _schema()).insert({"name": "A", "power": 1})
        assert not (tmp_path / JOURNAL_FILE).exists()
        assert Database(tmp_path).table_names() == []


class TestRealCrashes:
    """A child process dies with os._exit(1); the parent reopens."""

    def test_committed_rows_survive_a_crash(self, tmp_path: Path) -> None:
        """See every row the child committed before dying."""
        _run_child("commit", tmp_path, _CRASH_ROWS)
        again = _open(tmp_path)
        rows = _rows(again, "cards")
        assert len(rows) == _CRASH_ROWS
        assert rows[-1] == (
            _CRASH_ROWS,
            {"name": f"card{_CRASH_ROWS - 1}", "power": _CRASH_ROWS - 1},
        )

    def test_unfinished_transaction_is_lost_entirely(self, tmp_path: Path) -> None:
        """See none of a transaction the child was in the middle of."""
        _run_child("mid_txn", tmp_path, _TXN_ROWS)
        again = _open(tmp_path)
        rows = _rows(again, "cards")
        assert [rid for rid, _ in rows] == list(range(1, _TXN_ROWS + 1))
        assert all(not str(data["name"]).startswith("lost") for _, data in rows)
        record = again.get_table("cards").insert({"name": "next", "power": 0})
        assert record.record_id == _TXN_ROWS + 1
