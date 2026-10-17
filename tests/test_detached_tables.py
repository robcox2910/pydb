"""Tests for *detached* tables -- ``Table`` objects the database stopped managing.

A table is detached when it is dropped, when the transaction that created
it rolls back, when a failed ``create_table`` takes it back, or when
``load()`` / a rollback puts a different table under its name. Somebody may
still hold the old ``Table`` object. On a journalled database every change
through it is refused (and undone), so the journal never holds a change for
a table that isn't there. On a plain database the old object simply becomes
a table of its own: changes stay in memory and are never recorded or saved.
"""

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from pydb.changes import Change
from pydb.database import Database, DatabaseError
from pydb.schema import Column, Schema
from pydb.table import Table
from pydb.transaction import Transaction
from pydb.types import DataType
from tests.crash_helpers import STALE_REFUSED

_HELPER = Path(__file__).with_name("crash_helpers.py")
_CHILD_TIMEOUT = 5
_CRASH_EXIT = 1
_CHILD_ROWS = 3
_POWER = 10
_NEW_POWER = 20
_FIRST_ID = 1
_GHOST = {"name": "ghost", "power": -1}

type Write = Callable[[Table], object]

_WRITES: dict[str, Write] = {
    "insert": lambda t: t.insert(_GHOST),
    "update": lambda t: t.update(_FIRST_ID, {"power": _NEW_POWER}),
    "delete": lambda t: t.delete(_FIRST_ID),
    "create_index": lambda t: t.create_index("by_power", "power"),
}


class ListenerBoomError(Exception):
    """Raise from a test listener to make a commit fail."""


def _schema() -> Schema:
    """Return a name/power schema."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _journalled_cards(folder: Path) -> tuple[Database, Table]:
    """Return a journalled database with a one-row cards table."""
    db = Database(folder, journal=True)
    cards = db.create_table("cards", _schema())
    cards.insert({"name": "Pikachu", "power": _POWER})
    return db, cards


def _snapshot(table: Table) -> list[tuple[int, dict[str, object]]]:
    """Return ``(id, data)`` for every row of *table*."""
    return [(r.record_id, dict(r.data)) for r in table.select()]


class TestJournalledRefuses:
    """On a journalled database, writes through a detached table are refused."""

    @pytest.mark.parametrize("kind", sorted(_WRITES))
    def test_write_after_drop_is_refused(self, tmp_path: Path, kind: str) -> None:
        """Refuse every kind of write and leave the dropped table unchanged."""
        db, cards = _journalled_cards(tmp_path)
        before = _snapshot(cards)
        db.drop_table("cards")
        with pytest.raises(DatabaseError, match="no longer part of the database"):
            _WRITES[kind](cards)
        assert _snapshot(cards) == before
        assert cards.indexes == {}
        db.close()
        assert Database(tmp_path, journal=True).table_names() == []

    def test_write_after_rollback_of_create_is_refused(self, tmp_path: Path) -> None:
        """Refuse a write to a table whose creating transaction rolled back."""
        db, _ = _journalled_cards(tmp_path)
        txn = Transaction(db)
        extra = db.create_table("extra", _schema())
        txn.rollback()
        with pytest.raises(DatabaseError):
            extra.insert(_GHOST)
        assert extra.row_count == 0
        db.close()
        assert Database(tmp_path, journal=True).table_names() == ["cards"]

    def test_write_after_recreate_is_refused(self, tmp_path: Path) -> None:
        """Refuse a write through the old object once a new table has the name."""
        db, cards = _journalled_cards(tmp_path)
        db.drop_table("cards")
        fresh = db.create_table("cards", _schema())
        with pytest.raises(DatabaseError):
            cards.insert(_GHOST)
        assert fresh.row_count == 0
        db.close()
        assert Database(tmp_path, journal=True).get_table("cards").row_count == 0

    def test_failed_create_detaches_the_table(self, tmp_path: Path) -> None:
        """Refuse writes to the table a failed ``create_table`` took back."""
        db, _ = _journalled_cards(tmp_path)
        caught: list[Table] = []

        def grab_and_fail(changes: list[Change]) -> None:
            """Keep hold of the new table, then fail the commit."""
            caught.append(db.get_table(changes[0].table))
            raise ListenerBoomError

        db.add_commit_listener(grab_and_fail)
        with pytest.raises(ListenerBoomError):
            db.create_table("extra", _schema())
        db.remove_commit_listener(grab_and_fail)
        with pytest.raises(DatabaseError):
            caught[0].insert(_GHOST)
        assert db.table_names() == ["cards"]

    def test_drop_inside_transaction_refuses_until_rollback(self, tmp_path: Path) -> None:
        """Refuse writes after a held drop; the rollback hooks the table back up."""
        db, cards = _journalled_cards(tmp_path)
        txn = Transaction(db)
        db.drop_table("cards")
        with pytest.raises(DatabaseError):
            cards.insert(_GHOST)
        txn.rollback()
        cards.insert({"name": "Eevee", "power": _NEW_POWER})
        db.close()
        names = [r["name"] for r in Database(tmp_path, journal=True).get_table("cards").select()]
        assert names == ["Pikachu", "Eevee"]

    def test_rollback_detaches_a_table_recreated_inside(self, tmp_path: Path) -> None:
        """Detach the in-transaction copy when rollback puts the original back."""
        db, cards = _journalled_cards(tmp_path)
        txn = Transaction(db)
        db.drop_table("cards")
        inside = db.create_table("cards", _schema())
        txn.rollback()
        assert db.get_table("cards") is cards
        with pytest.raises(DatabaseError):
            inside.insert(_GHOST)


class TestPlainDatabase:
    """On a plain database a detached table keeps working on its own (as before)."""

    def test_write_after_drop_stays_in_memory(self, tmp_path: Path) -> None:
        """Accept the write, record nothing and save nothing."""
        db = Database(tmp_path)
        cards = db.create_table("cards", _schema())
        batches: list[list[Change]] = []
        db.add_commit_listener(batches.append)
        db.drop_table("cards")
        batches.clear()
        cards.insert(_GHOST)
        assert cards.row_count == 1
        assert batches == []
        db.save()
        assert Database(tmp_path).table_names() == []

    def test_load_detaches_the_replaced_tables(self, tmp_path: Path) -> None:
        """Stop recording changes through a table ``load()`` replaced."""
        db = Database(tmp_path)
        old = db.create_table("cards", _schema())
        db.save()
        db.load()
        batches: list[list[Change]] = []
        db.add_commit_listener(batches.append)
        old.insert(_GHOST)
        assert batches == []
        assert db.get_table("cards").row_count == 0


def _run_child(scenario: str, folder: Path) -> str:
    """Run a stale-reference crash scenario in a real child; return its output."""
    result = subprocess.run(
        [sys.executable, str(_HELPER), scenario, str(folder), str(_CHILD_ROWS)],
        check=False,
        timeout=_CHILD_TIMEOUT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == _CRASH_EXIT, result.stderr
    return result.stdout


class TestStaleWritesAndCrashes:
    """A child writes through a stale table and dies; the parent reopens."""

    def test_drop_then_stale_write_then_crash(self, tmp_path: Path) -> None:
        """Reopen with the table gone -- no 'no such table' recovery error."""
        assert STALE_REFUSED in _run_child("stale_drop", tmp_path)
        with Database(tmp_path, journal=True) as db:
            assert db.table_names() == []

    def test_rolled_back_create_then_stale_write_then_crash(self, tmp_path: Path) -> None:
        """Reopen with only the committed table and its rows."""
        assert STALE_REFUSED in _run_child("stale_rollback", tmp_path)
        with Database(tmp_path, journal=True) as db:
            assert db.table_names() == ["cards"]
            assert db.get_table("cards").row_count == _CHILD_ROWS

    def test_recreate_then_stale_write_then_crash(self, tmp_path: Path) -> None:
        """Reopen with the recreated table empty -- no ghost row."""
        assert STALE_REFUSED in _run_child("stale_recreate", tmp_path)
        with Database(tmp_path, journal=True) as db:
            assert db.get_table("cards").row_count == 0
