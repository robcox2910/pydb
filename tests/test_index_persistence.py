"""Tests for saving and loading index definitions.

An index is the card catalog at the front of the binder. When we put the
binder on the shelf, we now write down which catalogs it had, so they can
be rebuilt when the binder comes back down.
"""

import json
from pathlib import Path

import pytest

from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.serializer import SerializationError, deserialize_table, serialize_table_data
from pydb.storage import StorageError
from pydb.types import DataType

POWER_55 = 55
POWER_52 = 52
FIRST_ID = 1
SECOND_ID = 2
NEXT_ID = 3


def _schema() -> Schema:
    """Return a name/power schema."""
    return Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )


def _saved_db(tmp_path: Path) -> Database:
    """Return a saved database whose cards table has two indexes."""
    db = Database(path=tmp_path)
    cards = db.create_table("cards", _schema())
    cards.insert({"name": "Pikachu", "power": POWER_55})
    cards.insert({"name": "Charmander", "power": POWER_52})
    cards.create_index("idx_power", "power")
    cards.create_index("idx_name", "name")
    db.save()
    return db


class TestIndexesSurviveSaveAndLoad:
    """Verify index definitions round-trip through the disk."""

    def test_file_lists_indexes(self, tmp_path: Path) -> None:
        """The table file names each index and its column."""
        _saved_db(tmp_path)
        payload = json.loads((tmp_path / "cards.json").read_text(encoding="utf-8"))
        assert payload["indexes"] == [
            {"name": "idx_power", "column": "power"},
            {"name": "idx_name", "column": "name"},
        ]

    def test_load_rebuilds_indexes(self, tmp_path: Path) -> None:
        """Loading recreates each index, filled with the stored rows."""
        _saved_db(tmp_path)
        db = Database(path=tmp_path)
        db.load()
        cards = db.get_table("cards")
        assert sorted(cards.indexes) == ["idx_name", "idx_power"]
        assert cards.indexes["idx_power"].find(POWER_52) == [SECOND_ID]
        assert cards.indexes["idx_name"].find("Pikachu") == [FIRST_ID]

    def test_load_table_rebuilds_indexes(self, tmp_path: Path) -> None:
        """Loading a single table also rebuilds its indexes."""
        _saved_db(tmp_path)
        db = Database(path=tmp_path)
        cards = db.load_table("cards")
        assert cards.get_index_for_column("power") is not None

    def test_dropped_index_stays_dropped(self, tmp_path: Path) -> None:
        """An index dropped before saving is not brought back."""
        db = _saved_db(tmp_path)
        db.get_table("cards").drop_index("idx_name")
        db.save()
        reopened = Database(path=tmp_path)
        reopened.load()
        assert list(reopened.get_table("cards").indexes) == ["idx_power"]


class TestOldFormat:
    """Verify files written before index persistence still load."""

    def test_file_without_indexes_key_loads(self, tmp_path: Path) -> None:
        """A table file with no index list loads with no indexes."""
        _saved_db(tmp_path)
        path = tmp_path / "cards.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        del payload["indexes"]
        path.write_text(json.dumps(payload), encoding="utf-8")
        db = Database(path=tmp_path)
        db.load()
        cards = db.get_table("cards")
        assert cards.indexes == {}
        assert cards.row_count == SECOND_ID


class TestSerializer:
    """Verify the serializer handles index lists."""

    def test_round_trip(self) -> None:
        """Index definitions come back exactly as written."""
        text = serialize_table_data("cards", _schema(), [], NEXT_ID, indexes=[("idx_p", "power")])
        stored = deserialize_table(text)
        assert stored.indexes == [("idx_p", "power")]
        assert stored.next_id == NEXT_ID

    def test_bad_index_entry_is_rejected(self) -> None:
        """A malformed index entry is a serialization error."""
        text = serialize_table_data("cards", _schema(), [], NEXT_ID)
        payload = json.loads(text)
        payload["indexes"] = [{"name": "idx_p"}]
        with pytest.raises(SerializationError):
            deserialize_table(json.dumps(payload))

    def test_non_object_index_entry_is_rejected(self) -> None:
        """An index entry that isn't an object is a serialization error."""
        text = serialize_table_data("cards", _schema(), [], NEXT_ID)
        payload = json.loads(text)
        payload["indexes"] = ["idx_p"]
        with pytest.raises(SerializationError):
            deserialize_table(json.dumps(payload))

    def test_bad_index_entry_in_file_is_storage_error(self, tmp_path: Path) -> None:
        """Loading a file with a malformed index entry reports corruption."""
        _saved_db(tmp_path)
        path = tmp_path / "cards.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["indexes"] = "not a list"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(StorageError):
            Database(path=tmp_path).load()
