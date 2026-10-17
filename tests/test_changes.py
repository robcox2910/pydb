"""Tests for change recording and commit listeners.

Every time a table changes, PyDB writes a little note (a ``Change``) saying
what happened. Notes are handed to *commit listeners* -- one note at a time
outside a transaction, or the whole bundle when a transaction commits.
"""

from pathlib import Path

import pytest

from pydb.changes import Change, ChangeKind
from pydb.database import Database
from pydb.errors import PyDBError, SchemaError
from pydb.executor import execute
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.table import Table
from pydb.transaction import Transaction, TransactionError
from pydb.types import DataType

POWER_55 = 55
POWER_52 = 52
POWER_99 = 99
FIRST_ID = 1
SECOND_ID = 2
THIRD_ID = 3
TWO_ROWS = 2
THREE_CHANGES = 3


class ListenerBoomError(Exception):
    """Raise from a test listener to simulate a failing journal."""


def _schema() -> Schema:
    """Return a name/power schema."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _db_with_recorder(tmp_path: Path) -> tuple[Database, list[list[Change]]]:
    """Return a database with a cards table and a list collecting batches."""
    db = Database(path=tmp_path)
    cards = db.create_table("cards", _schema())
    cards.insert({"name": "Pikachu", "power": POWER_55})
    cards.insert({"name": "Charmander", "power": POWER_52})
    batches: list[list[Change]] = []
    db.add_commit_listener(batches.append)
    return db, batches


def _boom(_changes: list[Change]) -> None:
    """Fail every commit."""
    raise ListenerBoomError


def _only(batches: list[list[Change]]) -> Change:
    """Return the single change in the single batch."""
    assert len(batches) == 1
    assert len(batches[0]) == 1
    return batches[0][0]


class TestEachMutationEmits:
    """Verify each kind of mutation emits the right change."""

    def test_create_table(self, tmp_path: Path) -> None:
        """Creating a table records its name and schema."""
        db, batches = _db_with_recorder(tmp_path)
        schema = _schema()
        db.create_table("trainers", schema)
        assert _only(batches) == Change(ChangeKind.CREATE_TABLE, "trainers", schema=schema)

    def test_drop_table(self, tmp_path: Path) -> None:
        """Dropping a table records its name."""
        db, batches = _db_with_recorder(tmp_path)
        db.drop_table("cards")
        assert _only(batches) == Change(ChangeKind.DROP_TABLE, "cards")

    def test_insert(self, tmp_path: Path) -> None:
        """An insert records the assigned id and the full data."""
        db, batches = _db_with_recorder(tmp_path)
        db.get_table("cards").insert({"name": "Squirtle", "power": POWER_99})
        assert _only(batches) == Change(
            ChangeKind.INSERT,
            "cards",
            record_id=THIRD_ID,
            data={"name": "Squirtle", "power": POWER_99},
        )

    def test_update_records_full_new_data(self, tmp_path: Path) -> None:
        """An update records the id and every column's new value."""
        db, batches = _db_with_recorder(tmp_path)
        db.get_table("cards").update(FIRST_ID, {"power": POWER_99})
        assert _only(batches) == Change(
            ChangeKind.UPDATE,
            "cards",
            record_id=FIRST_ID,
            data={"name": "Pikachu", "power": POWER_99},
        )

    def test_delete(self, tmp_path: Path) -> None:
        """A delete records the id."""
        db, batches = _db_with_recorder(tmp_path)
        db.get_table("cards").delete(SECOND_ID)
        assert _only(batches) == Change(ChangeKind.DELETE, "cards", record_id=SECOND_ID)

    def test_create_index(self, tmp_path: Path) -> None:
        """Creating an index records its name and column."""
        db, batches = _db_with_recorder(tmp_path)
        db.get_table("cards").create_index("idx_power", "power")
        assert _only(batches) == Change(
            ChangeKind.CREATE_INDEX, "cards", index=("idx_power", "power")
        )

    def test_drop_index(self, tmp_path: Path) -> None:
        """Dropping an index records its name and column."""
        db = Database(path=tmp_path)
        db.create_table("cards", _schema()).create_index("idx_power", "power")
        batches: list[list[Change]] = []
        db.add_commit_listener(batches.append)
        db.get_table("cards").drop_index("idx_power")
        assert _only(batches) == Change(
            ChangeKind.DROP_INDEX, "cards", index=("idx_power", "power")
        )

    def test_change_data_is_a_copy(self, tmp_path: Path) -> None:
        """Changing a record later does not change an earlier note."""
        db, batches = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        cards.insert({"name": "Squirtle", "power": POWER_52})
        cards.update(THIRD_ID, {"power": POWER_99})
        first = batches[0][0]
        assert first.data == {"name": "Squirtle", "power": POWER_52}
        assert cards.get(THIRD_ID)["power"] == POWER_99

    def test_failed_insert_emits_nothing(self, tmp_path: Path) -> None:
        """A rejected insert leaves no note behind."""
        db, batches = _db_with_recorder(tmp_path)
        with pytest.raises(SchemaError):
            db.get_table("cards").insert({"name": "Bad", "power": "lots"})
        assert batches == []

    def test_free_standing_table_records_nothing(self) -> None:
        """A table that belongs to no database works without a sink."""
        table = Table("loose", _schema())
        record = table.insert({"name": "Eevee", "power": POWER_55})
        assert record.record_id == FIRST_ID

    def test_loaded_tables_still_record(self, tmp_path: Path) -> None:
        """Tables loaded from disk are hooked up to the database too."""
        db, _ = _db_with_recorder(tmp_path)
        db.save()
        reopened = Database(path=tmp_path)
        reopened.load()
        batches: list[list[Change]] = []
        reopened.add_commit_listener(batches.append)
        reopened.get_table("cards").delete(FIRST_ID)
        assert _only(batches) == Change(ChangeKind.DELETE, "cards", record_id=FIRST_ID)


class TestSqlFlowsThroughRecording:
    """Verify SQL statements produce the same changes as the Python API."""

    def test_sql_statements_emit_changes(self, tmp_path: Path) -> None:
        """Each mutating SQL statement emits one matching change."""
        db = Database(path=tmp_path)
        batches: list[list[Change]] = []
        db.add_commit_listener(batches.append)
        for sql in (
            "CREATE TABLE t (name TEXT, power INTEGER)",
            "INSERT INTO t VALUES ('Mew', 55)",
            "UPDATE t SET power = 99 WHERE name = 'Mew'",
            "CREATE INDEX idx_p ON t (power)",
            "DROP INDEX idx_p ON t",
            "DELETE FROM t WHERE name = 'Mew'",
            "DROP TABLE t",
        ):
            execute(parse_sql(sql), db)
        kinds = [batch[0].kind for batch in batches]
        assert kinds == [
            ChangeKind.CREATE_TABLE,
            ChangeKind.INSERT,
            ChangeKind.UPDATE,
            ChangeKind.CREATE_INDEX,
            ChangeKind.DROP_INDEX,
            ChangeKind.DELETE,
            ChangeKind.DROP_TABLE,
        ]
        assert all(len(batch) == 1 for batch in batches)
        assert batches[1][0] == Change(
            ChangeKind.INSERT, "t", record_id=FIRST_ID, data={"name": "Mew", "power": POWER_55}
        )
        assert batches[2][0] == Change(
            ChangeKind.UPDATE, "t", record_id=FIRST_ID, data={"name": "Mew", "power": POWER_99}
        )


class TestListeners:
    """Verify adding and removing commit listeners."""

    def test_remove_listener(self, tmp_path: Path) -> None:
        """A removed listener hears nothing more."""
        db, batches = _db_with_recorder(tmp_path)
        db.remove_commit_listener(batches.append)
        db.get_table("cards").delete(FIRST_ID)
        assert batches == []

    def test_clear_listeners(self, tmp_path: Path) -> None:
        """Clearing listeners silences all of them."""
        db, batches = _db_with_recorder(tmp_path)
        db.clear_commit_listeners()
        db.get_table("cards").delete(FIRST_ID)
        assert batches == []


class TestTransactionBatches:
    """Verify transactions deliver one batch on commit and none on rollback."""

    def test_commit_delivers_one_batch(self, tmp_path: Path) -> None:
        """All changes in a transaction arrive together, in order."""
        db, batches = _db_with_recorder(tmp_path)
        txn = Transaction(db)
        cards = txn.get_table("cards")
        cards.insert({"name": "Squirtle", "power": POWER_99})
        cards.update(FIRST_ID, {"power": POWER_99})
        cards.delete(SECOND_ID)
        assert batches == []
        txn.commit()
        assert len(batches) == 1
        assert [c.kind for c in batches[0]] == [
            ChangeKind.INSERT,
            ChangeKind.UPDATE,
            ChangeKind.DELETE,
        ]
        assert len(batches[0]) == THREE_CHANGES

    def test_rollback_delivers_nothing(self, tmp_path: Path) -> None:
        """A rolled-back transaction leaves no notes behind."""
        db, batches = _db_with_recorder(tmp_path)
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "Squirtle", "power": POWER_99})
        db.create_table("temp", _schema())
        txn.rollback()
        assert batches == []

    def test_empty_commit_delivers_nothing(self, tmp_path: Path) -> None:
        """Committing without changes does not call the listeners."""
        db, batches = _db_with_recorder(tmp_path)
        Transaction(db).commit()
        assert batches == []

    def test_after_transaction_changes_autocommit_again(self, tmp_path: Path) -> None:
        """Once a transaction ends, each change is its own commit again."""
        db, batches = _db_with_recorder(tmp_path)
        Transaction(db).rollback()
        db.get_table("cards").delete(FIRST_ID)
        assert _only(batches).kind is ChangeKind.DELETE

    def test_second_transaction_is_refused(self, tmp_path: Path) -> None:
        """Only one transaction at a time can collect changes."""
        db, _ = _db_with_recorder(tmp_path)
        txn = Transaction(db)
        with pytest.raises(TransactionError):
            Transaction(db)
        txn.commit()
        Transaction(db).commit()


class TestListenerFailure:
    """Verify a failing listener never leaves a half-committed state."""

    def test_failing_commit_rolls_back_transaction(self, tmp_path: Path) -> None:
        """If the listener fails at commit, the transaction rolls back."""
        db, _ = _db_with_recorder(tmp_path)
        db.add_commit_listener(_boom)
        txn = Transaction(db)
        txn.get_table("cards").insert({"name": "Squirtle", "power": POWER_99})
        db.create_table("temp", _schema())
        with pytest.raises(ListenerBoomError):
            txn.commit()
        assert not txn.is_active
        assert db.get_table("cards").row_count == TWO_ROWS
        assert "temp" not in db.table_names()

    def test_rollback_after_failed_commit_is_a_no_op(self, tmp_path: Path) -> None:
        """Rolling back a transaction its failed commit already undid is harmless."""
        db, _ = _db_with_recorder(tmp_path)
        db.add_commit_listener(_boom)
        txn = Transaction(db)
        txn.get_table("cards").delete(FIRST_ID)
        with pytest.raises(ListenerBoomError):
            txn.commit()
        txn.rollback()
        assert db.get_table("cards").row_count == TWO_ROWS

    def test_rollback_after_successful_commit_still_raises(self, tmp_path: Path) -> None:
        """A transaction that really committed cannot be rolled back."""
        db, _ = _db_with_recorder(tmp_path)
        txn = Transaction(db)
        txn.commit()
        with pytest.raises(TransactionError):
            txn.rollback()

    def test_failed_transaction_frees_the_database(self, tmp_path: Path) -> None:
        """After a failed commit a new transaction can start."""
        db, batches = _db_with_recorder(tmp_path)
        db.add_commit_listener(_boom)
        txn = Transaction(db)
        txn.get_table("cards").delete(FIRST_ID)
        with pytest.raises(ListenerBoomError):
            txn.commit()
        db.remove_commit_listener(_boom)
        batches.clear()
        Transaction(db).commit()
        db.get_table("cards").delete(FIRST_ID)
        assert _only(batches).kind is ChangeKind.DELETE

    def test_failed_insert_is_undone(self, tmp_path: Path) -> None:
        """An autocommit insert is taken back, id number included."""
        db, _ = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        cards.create_index("idx_power", "power")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            cards.insert({"name": "Squirtle", "power": POWER_99})
        assert cards.row_count == TWO_ROWS
        assert cards.next_id == THIRD_ID
        assert cards.indexes["idx_power"].find(POWER_99) == []

    def test_failed_update_is_undone(self, tmp_path: Path) -> None:
        """An autocommit update puts the old values back."""
        db, _ = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        cards.create_index("idx_power", "power")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            cards.update(FIRST_ID, {"power": POWER_99})
        assert cards.get(FIRST_ID)["power"] == POWER_55
        index = cards.indexes["idx_power"]
        assert index.find(POWER_99) == []
        assert index.find(POWER_55) == [FIRST_ID]

    def test_failed_delete_is_undone(self, tmp_path: Path) -> None:
        """An autocommit delete brings the record back in its old place."""
        db, _ = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        cards.create_index("idx_power", "power")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            cards.delete(FIRST_ID)
        assert [r.record_id for r in cards.select()] == [FIRST_ID, SECOND_ID]
        assert cards.get(FIRST_ID)["name"] == "Pikachu"
        assert cards.indexes["idx_power"].find(POWER_55) == [FIRST_ID]

    def test_failed_create_index_is_undone(self, tmp_path: Path) -> None:
        """An autocommit index creation is taken back."""
        db, _ = _db_with_recorder(tmp_path)
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            db.get_table("cards").create_index("idx_power", "power")
        assert db.get_table("cards").indexes == {}

    def test_failed_drop_index_is_undone(self, tmp_path: Path) -> None:
        """An autocommit index drop is taken back, entries rebuilt."""
        db, _ = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        cards.create_index("idx_power", "power")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            cards.drop_index("idx_power")
        assert cards.indexes["idx_power"].find(POWER_52) == [SECOND_ID]

    def test_failed_create_table_is_undone(self, tmp_path: Path) -> None:
        """An autocommit table creation is taken back."""
        db, _ = _db_with_recorder(tmp_path)
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            db.create_table("trainers", _schema())
        assert db.table_names() == ["cards"]

    def test_failed_recreate_still_deletes_old_file(self, tmp_path: Path) -> None:
        """Undoing a re-create of a dropped table keeps its file marked for deletion."""
        db, _ = _db_with_recorder(tmp_path)
        db.save()
        db.drop_table("cards")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            db.create_table("cards", _schema())
        db.save()
        assert not (tmp_path / "cards.json").exists()

    def test_table_refuses_database_level_change(self) -> None:
        """A table cannot apply a create-table note to itself."""
        table = Table("loose", _schema())
        with pytest.raises(PyDBError):
            table._apply_change(Change(ChangeKind.CREATE_TABLE, "loose"))

    def test_failed_drop_table_is_undone(self, tmp_path: Path) -> None:
        """An autocommit table drop is taken back, rows and all."""
        db, _ = _db_with_recorder(tmp_path)
        cards = db.get_table("cards")
        db.add_commit_listener(_boom)
        with pytest.raises(ListenerBoomError):
            db.drop_table("cards")
        assert db.get_table("cards") is cards
        assert cards.row_count == TWO_ROWS


class TestRollbackRestoresIndexes:
    """Verify rollback leaves indexes consistent with the restored rows."""

    def test_rollback_restores_index_entries(self, tmp_path: Path) -> None:
        """Index entries made inside a rolled-back transaction vanish."""
        db, _ = _db_with_recorder(tmp_path)
        db.get_table("cards").create_index("idx_power", "power")
        txn = Transaction(db)
        cards = txn.get_table("cards")
        cards.insert({"name": "Squirtle", "power": POWER_99})
        cards.update(FIRST_ID, {"power": POWER_52})
        txn.rollback()
        index = db.get_table("cards").indexes["idx_power"]
        assert index.find(POWER_99) == []
        assert index.find(POWER_55) == [FIRST_ID]
        assert index.find(POWER_52) == [SECOND_ID]

    def test_rollback_undoes_index_create_and_drop(self, tmp_path: Path) -> None:
        """Index definitions go back to how they were at the start."""
        db, _ = _db_with_recorder(tmp_path)
        db.get_table("cards").create_index("idx_power", "power")
        txn = Transaction(db)
        cards = txn.get_table("cards")
        cards.drop_index("idx_power")
        cards.create_index("idx_name", "name")
        txn.rollback()
        assert sorted(db.get_table("cards").indexes) == ["idx_power"]

    def test_restored_table_still_records(self, tmp_path: Path) -> None:
        """A table put back by rollback keeps sending notes."""
        db, batches = _db_with_recorder(tmp_path)
        Transaction(db).rollback()
        db.get_table("cards").delete(FIRST_ID)
        assert _only(batches) == Change(ChangeKind.DELETE, "cards", record_id=FIRST_ID)
