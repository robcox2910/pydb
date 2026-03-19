"""Tests for the storage engine.

The storage engine is the notebook on the shelf -- it saves tables to disk
and loads them back. These tests verify that data survives the round trip.
"""

from pathlib import Path

import pytest

from pydb.record import Record
from pydb.schema import Column, Schema
from pydb.storage import StorageEngine, StorageError
from pydb.types import DataType

# Named constants.
RECORD_ID_1 = 1
RECORD_ID_2 = 2
NEXT_ID = 3
POWER_55 = 55
POWER_52 = 52
PI = 3.14


def _make_schema() -> Schema:
    """Create a standard test schema."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _make_records() -> list[Record]:
    """Create a standard set of test records."""
    return [
        Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55}),
        Record(record_id=RECORD_ID_2, data={"name": "Charmander", "power": POWER_52}),
    ]


class TestStorageEngineCreation:
    """Verify storage engine initialization."""

    def test_creates_data_directory(self, tmp_path: Path) -> None:
        """The storage engine should create the data directory if needed."""
        data_dir = tmp_path / "db_data"
        StorageEngine(data_dir=data_dir)
        assert data_dir.is_dir()

    def test_data_dir_property(self, tmp_path: Path) -> None:
        """The data_dir property should return the configured path."""
        engine = StorageEngine(data_dir=tmp_path)
        assert engine.data_dir == tmp_path


class TestSaveAndLoad:
    """Verify saving and loading tables."""

    def test_save_creates_file(self, tmp_path: Path) -> None:
        """Saving a table should create a JSON file."""
        engine = StorageEngine(data_dir=tmp_path)
        engine.save_table("cards", _make_schema(), _make_records(), NEXT_ID)
        assert (tmp_path / "cards.json").exists()

    def test_load_restores_data(self, tmp_path: Path) -> None:
        """Loading a saved table should restore all data."""
        engine = StorageEngine(data_dir=tmp_path)
        schema = _make_schema()
        records = _make_records()
        engine.save_table("cards", schema, records, NEXT_ID)

        name, loaded_schema, loaded_records, next_id = engine.load_table("cards")
        assert name == "cards"
        assert loaded_schema.column_names == schema.column_names
        assert len(loaded_records) == len(records)
        assert next_id == NEXT_ID
        assert loaded_records[0]["name"] == "Pikachu"
        assert loaded_records[1]["power"] == POWER_52

    def test_save_overwrites_existing(self, tmp_path: Path) -> None:
        """Saving again should overwrite the previous file."""
        engine = StorageEngine(data_dir=tmp_path)
        schema = _make_schema()
        engine.save_table("cards", schema, _make_records(), NEXT_ID)

        new_records = [
            Record(record_id=RECORD_ID_1, data={"name": "Squirtle", "power": POWER_55}),
        ]
        new_next_id = 2
        engine.save_table("cards", schema, new_records, new_next_id)

        _, _, loaded, next_id = engine.load_table("cards")
        assert len(loaded) == 1
        assert loaded[0]["name"] == "Squirtle"
        assert next_id == new_next_id

    def test_temp_file_cleaned_up(self, tmp_path: Path) -> None:
        """After a successful save, the temp file should not remain."""
        engine = StorageEngine(data_dir=tmp_path)
        engine.save_table("cards", _make_schema(), _make_records(), NEXT_ID)
        assert not (tmp_path / "cards.json.tmp").exists()


class TestLoadErrors:
    """Verify error handling during load."""

    def test_load_nonexistent_raises(self, tmp_path: Path) -> None:
        """Loading a table that doesn't exist should raise StorageError."""
        engine = StorageEngine(data_dir=tmp_path)
        with pytest.raises(StorageError, match="No data file"):
            engine.load_table("missing")

    def test_load_corrupted_raises(self, tmp_path: Path) -> None:
        """Loading a corrupted file should raise StorageError."""
        engine = StorageEngine(data_dir=tmp_path)
        corrupted = tmp_path / "bad.json"
        corrupted.write_text("not valid json {{{", encoding="utf-8")
        with pytest.raises(StorageError, match="Corrupted"):
            engine.load_table("bad")


class TestDeleteTable:
    """Verify table deletion from disk."""

    def test_delete_removes_file(self, tmp_path: Path) -> None:
        """Deleting a table should remove its file."""
        engine = StorageEngine(data_dir=tmp_path)
        engine.save_table("cards", _make_schema(), _make_records(), NEXT_ID)
        engine.delete_table("cards")
        assert not (tmp_path / "cards.json").exists()

    def test_delete_nonexistent_raises(self, tmp_path: Path) -> None:
        """Deleting a table that doesn't exist should raise StorageError."""
        engine = StorageEngine(data_dir=tmp_path)
        with pytest.raises(StorageError, match="No data file"):
            engine.delete_table("missing")


class TestTableExists:
    """Verify table existence checks."""

    def test_exists_true(self, tmp_path: Path) -> None:
        """table_exists should return True for a saved table."""
        engine = StorageEngine(data_dir=tmp_path)
        engine.save_table("cards", _make_schema(), [], RECORD_ID_1)
        assert engine.table_exists("cards")

    def test_exists_false(self, tmp_path: Path) -> None:
        """table_exists should return False for a missing table."""
        engine = StorageEngine(data_dir=tmp_path)
        assert not engine.table_exists("missing")


class TestListTables:
    """Verify listing tables on disk."""

    def test_list_empty(self, tmp_path: Path) -> None:
        """An empty directory should list no tables."""
        engine = StorageEngine(data_dir=tmp_path)
        assert engine.list_tables() == []

    def test_list_multiple(self, tmp_path: Path) -> None:
        """Multiple saved tables should all appear in the list."""
        engine = StorageEngine(data_dir=tmp_path)
        schema = _make_schema()
        engine.save_table("alpha", schema, [], RECORD_ID_1)
        engine.save_table("beta", schema, [], RECORD_ID_1)
        assert engine.list_tables() == ["alpha", "beta"]


class TestDataTypeRoundTrips:
    """Verify all data types survive the disk round-trip."""

    def test_float_survives(self, tmp_path: Path) -> None:
        """Float values should survive save → load."""
        engine = StorageEngine(data_dir=tmp_path)
        schema = Schema(columns=[Column(name="value", data_type=DataType.FLOAT)])
        records = [Record(record_id=RECORD_ID_1, data={"value": PI})]
        engine.save_table("floats", schema, records, RECORD_ID_2)

        _, _, loaded, _ = engine.load_table("floats")
        assert loaded[0]["value"] == PI

    def test_boolean_survives(self, tmp_path: Path) -> None:
        """Boolean values should survive save → load."""
        engine = StorageEngine(data_dir=tmp_path)
        schema = Schema(columns=[Column(name="flag", data_type=DataType.BOOLEAN)])
        records = [Record(record_id=RECORD_ID_1, data={"flag": True})]
        engine.save_table("bools", schema, records, RECORD_ID_2)

        _, _, loaded, _ = engine.load_table("bools")
        assert loaded[0]["flag"] is True
