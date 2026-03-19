"""Tests for the Transaction class.

A transaction is the "no take-backs" rule with a safety net. These tests
verify that commit makes changes permanent and rollback undoes them.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.transaction import Transaction, TransactionError
from pydb.types import DataType

# Named constants.
POWER_55 = 55
POWER_52 = 52
POWER_48 = 48
INITIAL_ROW_COUNT = 2
POST_INSERT_COUNT = 3
THIRD_RECORD_ID = 3


def _make_db(tmp_path: Path) -> Database:
    """Create a test database with a populated cards table."""
    db = Database(path=tmp_path)
    schema = Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )
    table = db.create_table("cards", schema)
    table.insert({"name": "Pikachu", "power": POWER_55})
    table.insert({"name": "Charmander", "power": POWER_52})
    return db


class TestCommit:
    """Verify that committed changes persist."""

    def test_commit_preserves_insert(self, tmp_path: Path) -> None:
        """An insert inside a committed transaction should be visible."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.insert({"name": "Squirtle", "power": POWER_48})
        txn.commit()
        assert db.get_table("cards").row_count == POST_INSERT_COUNT

    def test_commit_preserves_delete(self, tmp_path: Path) -> None:
        """A delete inside a committed transaction should be visible."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.delete(record_id=1)
        txn.commit()
        assert db.get_table("cards").row_count == 1

    def test_commit_marks_finished(self, tmp_path: Path) -> None:
        """After commit, the transaction should no longer be active."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        assert txn.is_active
        txn.commit()
        assert not txn.is_active


class TestRollback:
    """Verify that rollback undoes all changes."""

    def test_rollback_undoes_insert(self, tmp_path: Path) -> None:
        """An insert should be undone after rollback."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.insert({"name": "Squirtle", "power": POWER_48})
        assert db.get_table("cards").row_count == POST_INSERT_COUNT
        txn.rollback()
        assert db.get_table("cards").row_count == INITIAL_ROW_COUNT

    def test_rollback_undoes_delete(self, tmp_path: Path) -> None:
        """A delete should be undone after rollback."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.delete(record_id=1)
        assert db.get_table("cards").row_count == 1
        txn.rollback()
        assert db.get_table("cards").row_count == INITIAL_ROW_COUNT

    def test_rollback_undoes_update(self, tmp_path: Path) -> None:
        """An update should be undone after rollback."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.update(record_id=1, values={"power": POWER_48})
        txn.rollback()
        assert db.get_table("cards").get(1)["power"] == POWER_55

    def test_rollback_restores_next_id(self, tmp_path: Path) -> None:
        """After rollback, next_id should be restored."""
        db = _make_db(tmp_path)
        original_next_id = db.get_table("cards").next_id
        txn = Transaction(db)
        table = txn.get_table("cards")
        table.insert({"name": "Squirtle", "power": POWER_48})
        txn.rollback()
        assert db.get_table("cards").next_id == original_next_id

    def test_rollback_marks_finished(self, tmp_path: Path) -> None:
        """After rollback, the transaction should no longer be active."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        txn.rollback()
        assert not txn.is_active


class TestDoubleFinish:
    """Verify that double-commit or double-rollback raises."""

    def test_double_commit_raises(self, tmp_path: Path) -> None:
        """Committing twice should raise TransactionError."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        txn.commit()
        with pytest.raises(TransactionError, match="already finished"):
            txn.commit()

    def test_double_rollback_raises(self, tmp_path: Path) -> None:
        """Rolling back twice should raise TransactionError."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        txn.rollback()
        with pytest.raises(TransactionError, match="already finished"):
            txn.rollback()

    def test_commit_then_rollback_raises(self, tmp_path: Path) -> None:
        """Rolling back after commit should raise TransactionError."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        txn.commit()
        with pytest.raises(TransactionError, match="already finished"):
            txn.rollback()

    def test_get_table_after_finish_raises(self, tmp_path: Path) -> None:
        """Getting a table after commit should raise TransactionError."""
        db = _make_db(tmp_path)
        txn = Transaction(db)
        txn.commit()
        with pytest.raises(TransactionError, match="already finished"):
            txn.get_table("cards")


class TestMultipleTables:
    """Verify transactions across multiple tables."""

    def test_rollback_restores_all_tables(self, tmp_path: Path) -> None:
        """Rollback should restore all tables, not just one."""
        db = Database(path=tmp_path)
        schema = Schema(
            columns=[
                Column(name="val", data_type=DataType.INTEGER),
            ]
        )
        t1 = db.create_table("table_a", schema)
        t2 = db.create_table("table_b", schema)
        t1.insert({"val": 1})
        t2.insert({"val": 2})

        txn = Transaction(db)
        txn.get_table("table_a").insert({"val": 10})
        txn.get_table("table_b").insert({"val": 20})
        txn.rollback()

        assert db.get_table("table_a").row_count == 1
        assert db.get_table("table_b").row_count == 1
