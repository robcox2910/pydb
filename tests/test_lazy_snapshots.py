"""Tests for copy-on-first-write transactions.

A transaction only photocopies the pages it is about to scribble on: a
table is snapshotted just before the transaction first changes it, and
tables it never touches are never copied. Rollback must still undo every
kind of change, whichever path made it.
"""

from collections.abc import Callable
from pathlib import Path

import pytest

from pydb import transaction as transaction_module
from pydb.changes import Change
from pydb.database import Database
from pydb.executor import execute
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.table import Table
from pydb.transaction import TableSnapshot, Transaction
from pydb.types import DataType
from tests.crash_helpers import abandon

BIG_ROWS = 50_000
SMALL_ROWS = 3
POWER_55 = 55
POWER_99 = 99
CHANGES_IN_ONE_TABLE = 4
WATCH_FAILS_AT = 2

Rows = list[tuple[int, dict[str, object]]]


def _schema() -> Schema:
    """Return a two-column schema: a unique name and a power."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT, unique=True),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _fill(table: Table, count: int, prefix: str = "card") -> None:
    """Insert *count* rows into *table*."""
    for number in range(count):
        table.insert({"name": f"{prefix}{number}", "power": number})


def _make_db(path: Path, *, journal: bool = False) -> Database:
    """Create a database with a small ``cards`` table and a small ``decks`` table."""
    db = Database(path, journal=journal)
    _fill(db.create_table("cards", _schema()), SMALL_ROWS)
    _fill(db.create_table("decks", _schema()), SMALL_ROWS, prefix="deck")
    return db


def _rows(table: Table) -> Rows:
    """Return ``(id, data)`` for every row of *table*."""
    return [(r.record_id, dict(r.data)) for r in table.select()]


def _state(db: Database) -> dict[str, tuple[Rows, int, list[tuple[str, str]]]]:
    """Return everything rollback must put back, for every table."""
    return {
        name: (
            _rows(db.get_table(name)),
            db.get_table(name).next_id,
            db.get_table(name).index_definitions,
        )
        for name in db.table_names()
    }


@pytest.fixture
def snapshot_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Count table snapshots: return the list of snapshotted table names."""
    seen: list[str] = []
    real: Callable[[Table], TableSnapshot] = transaction_module.snapshot_table

    def spy(table: Table) -> TableSnapshot:
        seen.append(table.name)
        return real(table)

    monkeypatch.setattr(transaction_module, "snapshot_table", spy)
    return seen


class TestOnlyTouchedTablesAreCopied:
    """A transaction copies a table only when it first changes it."""

    def test_large_untouched_table_is_not_copied(
        self, tmp_path: Path, snapshot_spy: list[str]
    ) -> None:
        """Insert one row into a small table: the big history table is never copied."""
        db = _make_db(tmp_path)
        big = db.create_table("history", _schema())
        _fill(big, BIG_ROWS)
        with Transaction(db) as txn:
            txn.get_table("cards").insert({"name": "Pikachu", "power": POWER_55})
        assert snapshot_spy == ["cards"]
        assert big.row_count == BIG_ROWS

    def test_a_table_is_copied_once_however_often_it_changes(
        self, tmp_path: Path, snapshot_spy: list[str]
    ) -> None:
        """Many changes to one table make one snapshot."""
        db = _make_db(tmp_path)
        with Transaction(db) as txn:
            cards = txn.get_table("cards")
            cards.insert({"name": "Pikachu", "power": POWER_55})
            cards.update(1, {"power": POWER_99})
            cards.delete(2)
            cards.create_index("by_power", "power")
        assert snapshot_spy == ["cards"]
        assert len(snapshot_spy) < CHANGES_IN_ONE_TABLE

    def test_snapshot_is_taken_before_the_change(self, tmp_path: Path) -> None:
        """The copy holds the rows as they were before the first change."""
        db = _make_db(tmp_path)
        before = _rows(db.get_table("cards"))
        copies: list[Rows] = []
        real = transaction_module.snapshot_table

        def spy(table: Table) -> TableSnapshot:
            copies.append(_rows(table))
            return real(table)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(transaction_module, "snapshot_table", spy)
            txn = Transaction(db)
            txn.get_table("cards").update(1, {"power": POWER_99})
            txn.rollback()
        assert copies == [before]

    def test_failed_change_still_rolls_back(self, tmp_path: Path) -> None:
        """A change rejected by its constraint leaves the table as it was."""
        db = _make_db(tmp_path)
        before = _state(db)
        txn = Transaction(db)
        cards = txn.get_table("cards")
        with pytest.raises(Exception, match="UNIQUE violation"):
            cards.insert({"name": "card0", "power": 1})
        cards.insert({"name": "new", "power": 1})
        txn.rollback()
        assert _state(db) == before

    def test_nothing_is_copied_after_the_transaction_ends(
        self, tmp_path: Path, snapshot_spy: list[str]
    ) -> None:
        """Changes after commit or rollback copy nothing."""
        db = _make_db(tmp_path)
        Transaction(db).commit()
        txn = Transaction(db)
        txn.rollback()
        db.get_table("cards").insert({"name": "later", "power": 1})
        db.get_table("decks").delete(1)
        assert snapshot_spy == []

    def test_snapshot_failure_stops_the_change(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the copy can't be made, the change doesn't happen and rollback still works."""
        db = _make_db(tmp_path)
        before = _state(db)

        def broken(table: Table) -> TableSnapshot:
            msg = f"no room to copy {table.name}"
            raise MemoryError(msg)

        txn = Transaction(db)
        monkeypatch.setattr(transaction_module, "snapshot_table", broken)
        with pytest.raises(MemoryError):
            txn.get_table("cards").insert({"name": "Pikachu", "power": POWER_55})
        monkeypatch.undo()
        assert _state(db) == before
        txn.get_table("cards").insert({"name": "Pikachu", "power": POWER_55})
        txn.rollback()
        assert _state(db) == before

    def test_failure_while_starting_lets_go(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If watching the tables fails, the lock is handed back and nothing is watched."""
        db = _make_db(tmp_path)
        real = Table.watch
        calls = 0

        def flaky(table: Table, watcher: Callable[[Table], None] | None) -> None:
            nonlocal calls
            calls += 1
            if calls == WATCH_FAILS_AT:
                msg = "cannot watch"
                raise RuntimeError(msg)
            real(table, watcher)

        monkeypatch.setattr(Table, "watch", flaky)
        with pytest.raises(RuntimeError, match="cannot watch"):
            Transaction(db)
        monkeypatch.undo()
        assert not db.in_transaction
        with Transaction(db) as txn:  # the lock and the batch were handed back
            txn.get_table("cards").insert({"name": "Pikachu", "power": POWER_55})
        assert db.get_table("cards").row_count == SMALL_ROWS + 1


class TestRollbackEveryPath:
    """Rollback undoes changes made by every mutation path."""

    def test_table_fetched_before_the_transaction(self, tmp_path: Path) -> None:
        """Changes through a ``Table`` got before the transaction are undone in place."""
        db = _make_db(tmp_path)
        cards = db.get_table("cards")
        before = _state(db)
        txn = Transaction(db)
        cards.insert({"name": "Pikachu", "power": POWER_55})
        cards.update(1, {"power": POWER_99})
        cards.delete(2)
        txn.rollback()
        assert _state(db) == before
        assert db.get_table("cards") is cards
        assert _rows(cards) == before["cards"][0]

    def test_sql_insert_update_delete(self, tmp_path: Path) -> None:
        """SQL INSERT, UPDATE and DELETE inside a transaction are undone."""
        db = _make_db(tmp_path)
        before = _state(db)
        txn = Transaction(db)
        execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
        execute(parse_sql("UPDATE cards SET power = 99"), db)
        execute(parse_sql("DELETE FROM decks WHERE power = 1"), db)
        execute(parse_sql("DELETE FROM decks"), db)
        txn.rollback()
        assert _state(db) == before

    def test_sql_statement_outside_a_transaction_rolls_back_on_failure(
        self, tmp_path: Path
    ) -> None:
        """A multi-row UPDATE that breaks UNIQUE half-way is fully undone."""
        db = _make_db(tmp_path)
        before = _state(db)
        with pytest.raises(Exception, match="UNIQUE violation"):
            execute(parse_sql("UPDATE cards SET name = 'same'"), db)
        assert _state(db) == before

    def test_drop_of_a_pre_existing_table(self, tmp_path: Path) -> None:
        """A dropped table comes back, rows and all, as the very same object."""
        db = _make_db(tmp_path)
        cards = db.get_table("cards")
        before = _state(db)
        txn = Transaction(db)
        db.drop_table("cards")
        txn.rollback()
        assert _state(db) == before
        assert db.get_table("cards") is cards

    def test_change_through_a_dropped_table(self, tmp_path: Path) -> None:
        """Changes through a table dropped in the transaction are undone too."""
        db = _make_db(tmp_path)
        cards = db.get_table("cards")
        before = _state(db)
        txn = Transaction(db)
        db.drop_table("cards")
        cards.insert({"name": "ghost", "power": 1})
        txn.rollback()
        assert _state(db) == before

    def test_drop_and_recreate_same_name(self, tmp_path: Path) -> None:
        """Drop a table, create a new one with its name: rollback brings the old one back."""
        db = _make_db(tmp_path)
        cards = db.get_table("cards")
        before = _state(db)
        txn = Transaction(db)
        db.drop_table("cards")
        _fill(db.create_table("cards", _schema()), 1, prefix="new")
        txn.rollback()
        assert _state(db) == before
        assert db.get_table("cards") is cards

    def test_create_then_drop(self, tmp_path: Path) -> None:
        """A table created and dropped in the transaction leaves no trace."""
        db = _make_db(tmp_path)
        before = _state(db)
        txn = Transaction(db)
        _fill(db.create_table("temp", _schema()), SMALL_ROWS)
        db.drop_table("temp")
        _fill(db.create_table("kept", _schema()), SMALL_ROWS)
        txn.rollback()
        assert _state(db) == before

    def test_index_create_and_drop(self, tmp_path: Path) -> None:
        """Index changes are undone and the restored index still finds rows."""
        db = _make_db(tmp_path)
        db.get_table("decks").create_index("by_power", "power")
        before = _state(db)
        txn = Transaction(db)
        db.get_table("cards").create_index("by_name", "name")
        db.get_table("decks").drop_index("by_power")
        execute(parse_sql("CREATE INDEX by_p ON cards (power)"), db)
        txn.rollback()
        assert _state(db) == before
        decks = db.get_table("decks")
        index = decks.get_index_for_column("power")
        assert index is not None
        assert [r["name"] for r in decks.select_by_index(index, 1)] == ["deck1"]

    def test_load_inside_a_transaction(self, tmp_path: Path) -> None:
        """Tables replaced by ``load()`` inside a transaction come back on rollback."""
        db = _make_db(tmp_path)
        db.save()
        cards = db.get_table("cards")
        cards.insert({"name": "unsaved", "power": 1})
        before = _state(db)
        txn = Transaction(db)
        db.load()
        txn.rollback()
        assert _state(db) == before
        assert db.get_table("cards") is cards

    def test_failed_commit_restores_touched_tables(self, tmp_path: Path) -> None:
        """A commit listener that refuses the batch rolls the touched table back."""
        db = _make_db(tmp_path)
        before = _state(db)

        def refuse(changes: list[Change]) -> None:
            msg = f"refused {len(changes)} changes"
            raise RuntimeError(msg)

        db.add_commit_listener(refuse)
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "Pikachu", "power": POWER_55})
        with pytest.raises(RuntimeError, match="refused"):
            txn.commit()
        assert _state(db) == before


class TestJournalledRollback:
    """On a journalled database, rolled-back work never reaches the disk."""

    def _churn(self, db: Database) -> None:
        """Make one change of every kind."""
        cards = db.get_table("cards")
        cards.insert({"name": "Pikachu", "power": POWER_55})
        cards.update(1, {"power": POWER_99})
        cards.create_index("by_power", "power")
        execute(parse_sql("DELETE FROM decks"), db)
        db.drop_table("decks")
        _fill(db.create_table("temp", _schema()), SMALL_ROWS)

    def test_rollback_then_reopen_shows_no_trace(self, tmp_path: Path) -> None:
        """Roll back, close and reopen: the state is exactly the pre-transaction one."""
        db = _make_db(tmp_path, journal=True)
        before = _state(db)
        txn = Transaction(db)
        self._churn(db)
        txn.rollback()
        assert _state(db) == before
        db.close()
        with Database(tmp_path, journal=True) as reopened:
            assert _state(reopened) == before

    def test_rollback_then_crash_recovers_without_a_trace(self, tmp_path: Path) -> None:
        """Roll back, then die without a checkpoint: recovery shows no trace."""
        db = _make_db(tmp_path, journal=True)
        before = _state(db)
        txn = Transaction(db)
        self._churn(db)
        txn.rollback()
        db.get_table("cards").insert({"name": "after", "power": 1})
        expected = _state(db)
        abandon(db)
        with Database(tmp_path, journal=True) as reopened:
            assert _state(reopened) == expected
            assert _state(reopened)["decks"] == before["decks"]
