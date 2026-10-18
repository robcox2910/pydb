"""Tests for NULL in tables, indexes, files and the journal.

A NULL is an empty box with a question mark on it: the value is unknown.
These tests check that the box can be stored, found (or rather, *not*
found by an index), saved, journalled and recovered.
"""

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from pydb.changes import Change
from pydb.database import Database
from pydb.errors import SchemaError
from pydb.index import Index
from pydb.journal import decode_entries, encode_entry
from pydb.record import Value
from pydb.schema import Column, Schema
from pydb.serializer import deserialize_table, serialize_table_data
from pydb.table import Table
from pydb.transaction import Transaction
from pydb.types import DataType
from tests.crash_helpers import abandon

FIRST_ID = 1
SECOND_ID = 2
THIRD_ID = 3
CODE_0 = 0
CODE_7 = 7
DAY = date(2026, 10, 18)
MOMENT = datetime(2026, 10, 18, 9, 30, tzinfo=UTC)


def _runs_schema() -> Schema:
    """Return a schema with a NOT NULL name and nullable columns of every type."""
    return Schema(
        columns=[
            Column("name", DataType.TEXT, not_null=True),
            Column("code", DataType.INTEGER),
            Column("score", DataType.FLOAT),
            Column("ok", DataType.BOOLEAN),
            Column("day", DataType.DATE),
            Column("ended", DataType.TIMESTAMP),
            Column("note", DataType.TEXT),
        ]
    )


def _runs() -> Table:
    """Return an empty runs table."""
    return Table("runs", _runs_schema())


def _all_null(name: str) -> dict[str, Value]:
    """Return a row with *name* and NULL everywhere else."""
    return {
        "name": name,
        "code": None,
        "score": None,
        "ok": None,
        "day": None,
        "ended": None,
        "note": None,
    }


class TestNullableColumns:
    """A column without NOT NULL accepts None."""

    def test_missing_nullable_columns_default_to_null(self) -> None:
        """Leaving out a nullable column stores NULL."""
        record = _runs().insert({"name": "build"})
        assert record.data == _all_null("build")

    def test_explicit_null_for_every_type(self) -> None:
        """None passes the type check of every type."""
        record = _runs().insert(_all_null("build"))
        assert record["code"] is None
        assert record["ended"] is None

    def test_missing_not_null_column_is_still_an_error(self) -> None:
        """A missing NOT NULL column keeps the old message."""
        with pytest.raises(SchemaError, match="Missing required column"):
            _runs().insert({"code": CODE_0})

    def test_missing_primary_key_is_still_an_error(self) -> None:
        """A PRIMARY KEY column is never filled with NULL."""
        table = Table("t", Schema([Column("id", DataType.INTEGER, primary_key=True)]))
        with pytest.raises(SchemaError, match="Missing required column"):
            table.insert({})

    def test_null_in_a_not_null_column_is_rejected(self) -> None:
        """None in a NOT NULL column gets the NOT NULL message."""
        with pytest.raises(SchemaError, match="cannot be NULL"):
            _runs().insert({"name": None})

    def test_null_in_a_primary_key_is_rejected(self) -> None:
        """A PRIMARY KEY is NOT NULL even if not_null was not set."""
        table = Table("t", Schema([Column("id", DataType.INTEGER, primary_key=True)]))
        with pytest.raises(SchemaError, match="cannot be NULL"):
            table.insert({"id": None})

    def test_wrong_type_still_rejected(self) -> None:
        """A real value of the wrong type is still refused."""
        with pytest.raises(SchemaError, match="expects INTEGER"):
            _runs().insert({"name": "build", "code": "seven"})

    def test_update_to_null_and_back(self) -> None:
        """Update may set a nullable column to None, and back to a value."""
        table = _runs()
        table.insert({"name": "build", "code": CODE_7})
        assert table.update(FIRST_ID, {"code": None})["code"] is None
        assert table.update(FIRST_ID, {"code": CODE_0})["code"] == CODE_0

    def test_update_not_null_to_null_is_rejected(self) -> None:
        """Update cannot empty a NOT NULL column."""
        table = _runs()
        table.insert({"name": "build"})
        with pytest.raises(SchemaError, match="cannot be NULL"):
            table.update(FIRST_ID, {"name": None})

    def test_several_nulls_in_a_unique_column(self) -> None:
        """UNIQUE ignores NULL, so many rows may leave it empty."""
        table = Table("t", Schema([Column("email", DataType.TEXT, unique=True)]))
        table.insert({"email": None})
        table.insert({})
        table.insert({"email": "a@b"})
        with pytest.raises(SchemaError, match="UNIQUE"):
            table.insert({"email": "a@b"})


class TestIndexesSkipNull:
    """B-tree indexes never store a NULL key."""

    def test_index_ignores_null_keys(self) -> None:
        """Inserting, finding and deleting None does nothing."""
        idx = Index("i", "code")
        idx.insert(None, FIRST_ID)
        idx.insert(CODE_7, SECOND_ID)
        assert idx.find(None) == []
        assert idx.find(CODE_7) == [SECOND_ID]
        assert idx.delete(None, FIRST_ID) is False
        assert idx.find_range(None, CODE_7) == []

    def test_select_by_index_with_null_is_empty(self) -> None:
        """Looking up NULL in an index finds nothing."""
        table = _runs()
        table.insert({"name": "a"})
        idx = table.create_index("by_code", "code")
        assert table.select_by_index(idx, None) == []

    def test_update_null_value_null_keeps_find_right(self) -> None:
        """NULL -> value -> NULL moves the index entry in and out again."""
        table = _runs()
        table.insert({"name": "a"})
        table.insert({"name": "b", "code": CODE_7})
        idx = table.create_index("by_code", "code")
        assert idx.find(CODE_7) == [SECOND_ID]
        table.update(FIRST_ID, {"code": CODE_7})
        assert idx.find(CODE_7) == [FIRST_ID, SECOND_ID]
        table.update(FIRST_ID, {"code": None})
        assert idx.find(CODE_7) == [SECOND_ID]
        table.delete(SECOND_ID)
        assert idx.find(CODE_7) == []

    def test_delete_of_null_row_with_index(self) -> None:
        """Deleting a row whose indexed value is NULL works."""
        table = _runs()
        table.create_index("by_day", "day")
        table.insert({"name": "a"})
        table.delete(FIRST_ID)
        assert table.row_count == 0

    def test_rollback_restores_index_contents(self, tmp_path: Path) -> None:
        """Rolling back NULL changes puts the index back exactly."""
        db = Database(tmp_path)
        runs = db.create_table("runs", _runs_schema())
        runs.insert({"name": "a", "code": CODE_7})
        runs.insert({"name": "b"})
        idx = runs.create_index("by_code", "code")
        txn = Transaction(db)
        runs.update(FIRST_ID, {"code": None})
        runs.update(SECOND_ID, {"code": CODE_7})
        runs.insert({"name": "c", "code": CODE_7})
        runs.delete(SECOND_ID)
        txn.rollback()
        assert idx.find(CODE_7) == [FIRST_ID]
        assert runs.get(SECOND_ID)["code"] is None


class TestNullOnDisk:
    """NULL is JSON ``null`` in snapshots and journal entries."""

    def test_snapshot_round_trip(self) -> None:
        """serialize_table_data writes null and reads None back."""
        table = _runs()
        table.insert(_all_null("a"))
        text = serialize_table_data("runs", table.schema, table.select(), table.next_id)
        assert json.loads(text)["records"][0]["data"]["code"] is None
        stored = deserialize_table(text)
        assert stored.records[0].data == _all_null("a")

    def test_save_and_load(self, tmp_path: Path) -> None:
        """Database.save / load keeps NULLs and real dates side by side."""
        db = Database(tmp_path)
        runs = db.create_table("runs", _runs_schema())
        runs.insert({"name": "a"})
        runs.insert({"name": "b", "day": DAY, "ended": MOMENT})
        db.save()
        again = Database(tmp_path)
        again.load()
        rows = [r.data for r in again.get_table("runs").select()]
        assert rows[0] == _all_null("a")
        assert rows[1]["day"] == DAY
        assert rows[1]["ended"] == MOMENT
        assert rows[1]["code"] is None

    def test_journal_entry_round_trip(self) -> None:
        """A journal line keeps None as null next to tagged dates."""
        table = _runs()
        changes: list[Change] = []
        table.set_sink(changes.append)
        table.insert({"name": "a", "day": DAY})
        line = encode_entry(FIRST_ID, changes)
        assert b'"code":null' in line
        entries, _ = decode_entries(line)
        lsn, decoded = entries[0]
        assert lsn == FIRST_ID
        assert decoded[0].data is not None
        assert decoded[0].data["code"] is None
        assert decoded[0].data["day"] == DAY

    def test_crash_recovery(self, tmp_path: Path) -> None:
        """NULLs written only to the journal come back after a crash."""
        db = Database(tmp_path, journal=True)
        runs = db.create_table("runs", _runs_schema())
        runs.insert({"name": "a", "ended": MOMENT})
        runs.update(FIRST_ID, {"ended": None})
        runs.insert({"name": "b", "code": CODE_7})
        abandon(db)
        again = Database(tmp_path, journal=True)
        rows = [r.data for r in again.get_table("runs").select()]
        assert rows[0] == _all_null("a")
        assert rows[1]["code"] == CODE_7
        assert rows[1]["day"] is None
        again.close()

    def test_checkpoint_and_reopen(self, tmp_path: Path) -> None:
        """NULLs survive a checkpoint snapshot and a reopen."""
        with Database(tmp_path, journal=True) as db:
            runs = db.create_table("runs", _runs_schema())
            runs.create_index("by_code", "code")
            runs.insert({"name": "a"})
            db.checkpoint()
            runs.insert({"name": "b", "code": CODE_7})
        with Database(tmp_path, journal=True) as again:
            runs = again.get_table("runs")
            assert runs.get(FIRST_ID).data == _all_null("a")
            idx = runs.get_index_for_column("code")
            assert idx is not None
            assert idx.find(CODE_7) == [SECOND_ID]
            assert runs.select_by_index(idx, None) == []

    def test_old_files_without_nulls_load_unchanged(self) -> None:
        """A snapshot with no nulls in it loads exactly as before."""
        schema = Schema([Column("name", DataType.TEXT), Column("power", DataType.INTEGER)])
        payload = {
            "name": "cards",
            "schema": {
                "columns": [
                    {"name": "name", "data_type": "TEXT"},
                    {"name": "power", "data_type": "INTEGER"},
                ]
            },
            "next_id": THIRD_ID,
            "records": [
                {"record_id": FIRST_ID, "data": {"name": "A", "power": 1}},
                {"record_id": SECOND_ID, "data": {"name": "B", "power": 2}},
            ],
        }
        stored = deserialize_table(json.dumps(payload))
        assert repr(stored.schema) == repr(schema)
        assert [r.data for r in stored.records] == [
            {"name": "A", "power": 1},
            {"name": "B", "power": 2},
        ]
