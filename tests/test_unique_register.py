"""Tests for hash-indexed UNIQUE and PRIMARY KEY checks.

Each table keeps a register (value -> row) for every UNIQUE or PRIMARY KEY
column, so a duplicate check is one look-up instead of a walk through
every row. The register must agree with the old "compare with ``==``
against every row" rule exactly, and stay in step with every change.
"""

import enum
import itertools
import math
from collections.abc import Callable
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path

import pytest

from pydb.changes import Change, ChangeKind
from pydb.database import Database
from pydb.errors import SchemaError
from pydb.executor import execute
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.sql_parser import parse_sql
from pydb.table import Table
from pydb.transaction import Transaction
from pydb.types import DataType
from pydb.unique import UniqueRegister
from tests.crash_helpers import abandon

MANY_ROWS = 20_000
SCAN_ROWS = 5_000
MAX_LOOKUPS_PER_INSERT = 20
POWER_1 = 1
POWER_2 = 2
NAN = float("nan")


class Level(enum.IntEnum):
    """An ``int`` subclass: still an INTEGER, and ``Level.ONE == 1``."""

    ONE = 1


def _outcome(check: Callable[[], None]) -> str | None:
    """Run *check* and return its error message (None if it passed)."""
    try:
        check()
    except SchemaError as exc:
        return str(exc)
    return None


def _schema() -> Schema:
    """Return a schema with a PRIMARY KEY ``id``, a UNIQUE ``code`` and a plain ``power``."""
    return Schema(
        columns=[
            Column(name="id", data_type=DataType.INTEGER, primary_key=True),
            Column(name="code", data_type=DataType.TEXT, unique=True),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _table() -> Table:
    """Return a table with two rows: ids 1 and 2, codes 'A' and 'B'."""
    table = Table("cards", _schema())
    table.insert({"id": 1, "code": "A", "power": POWER_1})
    table.insert({"id": 2, "code": "B", "power": POWER_2})
    return table


def _code_taken(table: Table, code: str) -> bool:
    """Return whether inserting a row with *code* is refused as a duplicate."""
    try:
        record = table.insert({"id": 1000 + table.next_id, "code": code, "power": 0})
    except SchemaError:
        return True
    table.delete(record.record_id)
    return False


class TestSameErrors:
    """Duplicates are refused with exactly the old messages."""

    def test_unique_duplicate_message(self) -> None:
        """A duplicate UNIQUE value raises the old message."""
        table = _table()
        with pytest.raises(SchemaError) as info:
            table.insert({"id": 3, "code": "A", "power": 0})
        assert str(info.value) == "UNIQUE violation: 'code' value 'A' already exists"

    def test_primary_key_duplicate_message(self) -> None:
        """A duplicate PRIMARY KEY raises the old message."""
        table = _table()
        with pytest.raises(SchemaError) as info:
            table.insert({"id": 1, "code": "Z", "power": 0})
        assert str(info.value) == "PRIMARY KEY violation: 'id' value 1 already exists"

    def test_columns_are_checked_in_schema_order(self) -> None:
        """When two columns clash, the first column in the schema is reported."""
        table = _table()
        with pytest.raises(SchemaError, match="PRIMARY KEY violation: 'id'"):
            table.insert({"id": 1, "code": "A", "power": 0})

    def test_update_to_a_taken_value(self) -> None:
        """UPDATE to another row's value is refused; keeping your own is fine."""
        table = _table()
        with pytest.raises(SchemaError, match="UNIQUE violation: 'code' value 'B'"):
            table.update(1, {"code": "B"})
        table.update(1, {"code": "A", "power": POWER_2})
        assert table.get(1)["power"] == POWER_2

    def test_sql_duplicate_is_refused(self, tmp_path: Path) -> None:
        """SQL INSERT of a duplicate is refused too."""
        db = Database(tmp_path)
        execute(parse_sql("CREATE TABLE t (code TEXT UNIQUE)"), db)
        execute(parse_sql("INSERT INTO t VALUES ('x')"), db)
        with pytest.raises(Exception, match="UNIQUE violation"):
            execute(parse_sql("INSERT INTO t VALUES ('x')"), db)


class TestEqualityMatchesTheOldScan:
    """The register treats values as equal exactly when ``==`` did."""

    VALUES: tuple[Value, ...] = (
        1,
        1.0,
        True,
        0,
        0.0,
        -0.0,
        False,
        2,
        "1",
        "",
        NAN,
        Level.ONE,
        date(2026, 1, 1),
        datetime(2026, 1, 1, tzinfo=UTC),
    )

    def test_every_pair_agrees_with_old_scan(self) -> None:
        """For every pair of values, register and scan give the same verdict and message."""
        schema = Schema(columns=[Column(name="k", data_type=DataType.FLOAT, unique=True)])
        for stored, probe in itertools.product(self.VALUES, repeat=2):
            records = [Record(record_id=1, data={"k": stored})]
            register = UniqueRegister(["k"])
            register.add(records[0])
            old = _outcome(partial(schema.check_constraints, {"k": probe}, records))
            new = _outcome(partial(schema.check_constraints, {"k": probe}, (), unique=register))
            assert old == new, (stored, probe)

    def test_one_and_one_point_oh_clash_in_a_float_column(self) -> None:
        """``1`` is widened to ``1.0``, so the second insert is a duplicate, as before."""
        table = Table("t", Schema(columns=[Column("k", DataType.FLOAT, unique=True)]))
        table.insert({"k": 1})
        with pytest.raises(SchemaError, match=r"UNIQUE violation: 'k' value 1\.0"):
            table.insert({"k": 1.0})

    def test_int_subclass_clashes_with_its_int(self) -> None:
        """``Level.ONE == 1``, so it is a duplicate of ``1``, as before."""
        table = Table("t", Schema(columns=[Column("k", DataType.INTEGER, unique=True)]))
        table.insert({"k": 1})
        with pytest.raises(SchemaError, match="UNIQUE violation"):
            table.insert({"k": Level.ONE})

    def test_nan_never_clashes(self) -> None:
        """``nan != nan``, so several NaNs are allowed, as before."""
        table = Table("t", Schema(columns=[Column("k", DataType.FLOAT, unique=True)]))
        table.insert({"k": NAN})
        table.insert({"k": NAN})
        values = [r["k"] for r in table.select()]
        assert all(isinstance(v, float) and math.isnan(v) for v in values)

    def test_missing_values_are_not_checked(self) -> None:
        """A missing (NULL) value is never a duplicate, however many there are."""
        schema = Schema(columns=[Column(name="k", data_type=DataType.TEXT, unique=True)])
        register = UniqueRegister(["k"])
        schema.check_constraints({}, (), unique=register)
        assert not register.is_taken("k", None)


class TestRegisterKeepsInStep:
    """The register follows every change to the rows."""

    def test_update_moves_the_value(self) -> None:
        """After changing a row's code, the old code is free and the new one taken."""
        table = _table()
        table.update(1, {"code": "C"})
        assert not _code_taken(table, "A")
        assert _code_taken(table, "C")

    def test_delete_frees_the_value(self) -> None:
        """After deleting a row, its code can be used again."""
        table = _table()
        table.delete(1)
        assert not _code_taken(table, "A")

    def test_rollback_restores_the_register(self, tmp_path: Path) -> None:
        """Rollback of insert, update and delete leaves the register as before."""
        db = Database(tmp_path)
        cards = db.create_table("cards", _schema())
        cards.insert({"id": 1, "code": "A", "power": POWER_1})
        cards.insert({"id": 2, "code": "B", "power": POWER_2})
        txn = Transaction(db)
        cards.insert({"id": 3, "code": "C", "power": 0})
        cards.update(1, {"code": "D"})
        cards.delete(2)
        txn.rollback()
        assert _code_taken(cards, "A")
        assert _code_taken(cards, "B")
        assert not _code_taken(cards, "C")
        assert not _code_taken(cards, "D")

    def test_refused_change_is_undone_in_the_register(self, tmp_path: Path) -> None:
        """A change a commit listener refuses leaves the register as before."""
        db = Database(tmp_path)
        cards = db.create_table("cards", _schema())
        cards.insert({"id": 1, "code": "A", "power": POWER_1})

        def refuse(changes: list[Change]) -> None:
            msg = f"refused {len(changes)}"
            raise RuntimeError(msg)

        db.add_commit_listener(refuse)
        with pytest.raises(RuntimeError):
            cards.insert({"id": 2, "code": "B", "power": 0})
        with pytest.raises(RuntimeError):
            cards.update(1, {"code": "C"})
        with pytest.raises(RuntimeError):
            cards.delete(1)
        db.remove_commit_listener(refuse)
        assert _code_taken(cards, "A")
        assert not _code_taken(cards, "B")
        assert not _code_taken(cards, "C")

    def test_load_from_disk_rebuilds_the_register(self, tmp_path: Path) -> None:
        """A table loaded from its file still refuses duplicates."""
        db = Database(tmp_path)
        _fill_codes(db.create_table("cards", _schema()))
        db.save()
        loaded = Database(tmp_path)
        loaded.load()
        assert _code_taken(loaded.get_table("cards"), "A")
        assert not _code_taken(loaded.get_table("cards"), "Z")

    def test_recovery_replay_rebuilds_the_register(self, tmp_path: Path) -> None:
        """After a crash, replayed rows still refuse duplicates."""
        db = Database(tmp_path, journal=True)
        cards = db.create_table("cards", _schema())
        _fill_codes(cards)
        db.checkpoint()
        cards.insert({"id": 3, "code": "C", "power": 0})
        cards.update(1, {"code": "D"})
        cards.delete(2)
        abandon(db)
        with Database(tmp_path, journal=True) as reopened:
            table = reopened.get_table("cards")
            assert _code_taken(table, "C")
            assert _code_taken(table, "D")
            assert not _code_taken(table, "A")
            assert not _code_taken(table, "B")

    def test_duplicates_already_stored_are_still_found(self) -> None:
        """Rows that already clash (say, a hand-edited file) behave as the old scan did."""
        records = [
            Record(record_id=1, data={"id": 1, "code": "A", "power": 0}),
            Record(record_id=2, data={"id": 2, "code": "A", "power": 0}),
        ]
        table = Table.from_stored("cards", _schema(), records, next_id=3)
        with pytest.raises(SchemaError, match="UNIQUE violation"):
            table.update(1, {"power": 1})
        table.delete(1)
        table.update(2, {"power": 1})
        assert _code_taken(table, "A")
        table.delete(2)
        assert not _code_taken(table, "A")

    def test_removing_a_second_holder_keeps_the_first(self) -> None:
        """Deleting the later of two clashing rows leaves the value taken by the first."""
        records = [
            Record(record_id=1, data={"id": 1, "code": "A", "power": 0}),
            Record(record_id=2, data={"id": 2, "code": "A", "power": 0}),
        ]
        table = Table.from_stored("cards", _schema(), records, next_id=3)
        table.delete(2)
        table.update(1, {"power": 1})
        assert _code_taken(table, "A")

    def test_nan_rows_can_be_deleted(self) -> None:
        """A NaN row is never registered, so deleting it is fine."""
        table = Table("t", Schema(columns=[Column("k", DataType.FLOAT, unique=True)]))
        record = table.insert({"k": NAN})
        table.delete(record.record_id)
        assert table.row_count == 0

    def test_replayed_insert_over_an_existing_row(self) -> None:
        """A replayed insert that reuses a row ID swaps that row's value in the register."""
        table = _table()
        table.replay_change(
            Change(ChangeKind.INSERT, "cards", record_id=1, data={"id": 1, "code": "Z", "power": 0})
        )
        assert not _code_taken(table, "A")
        assert _code_taken(table, "Z")


def _fill_codes(table: Table) -> None:
    """Insert rows with codes 'A' and 'B'."""
    table.insert({"id": 1, "code": "A", "power": POWER_1})
    table.insert({"id": 2, "code": "B", "power": POWER_2})


class TestNoScan:
    """A uniqueness check is a look-up, not a walk through every row."""

    def test_insert_does_not_read_every_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """One insert into a big table reads only a handful of record fields."""
        table = Table("t", Schema(columns=[Column("k", DataType.INTEGER, unique=True)]))
        for number in range(SCAN_ROWS):
            table.insert({"k": number})
        reads = 0
        real = Record.__getitem__

        def counting(record: Record, column: str) -> Value:
            nonlocal reads
            reads += 1
            return real(record, column)

        monkeypatch.setattr(Record, "__getitem__", counting)
        table.insert({"k": SCAN_ROWS})
        table.update(1, {"k": SCAN_ROWS + 1})
        assert reads < MAX_LOOKUPS_PER_INSERT

    def test_many_inserts_stay_fast(self) -> None:
        """20 000 unique inserts finish well inside the test timeout (quadratic would not)."""
        table = Table("t", Schema(columns=[Column("k", DataType.INTEGER, primary_key=True)]))
        for number in range(MANY_ROWS):
            table.insert({"k": number})
        assert table.row_count == MANY_ROWS
