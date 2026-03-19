"""Tests for the Database class.

The database is the whole collection room -- it manages multiple tables,
knows where each one is stored, and coordinates saving and loading.
"""

from pathlib import Path

import pytest

from pydb.database import Database, DatabaseError
from pydb.schema import Column, Schema
from pydb.types import DataType

# Named constants.
POWER_55 = 55
POWER_52 = 52
SCORE_9001 = 9001
FIRST_ID = 1


def _make_schema() -> Schema:
    """Create a standard test schema."""
    return Schema(columns=[
        Column(name="name", data_type=DataType.TEXT),
        Column(name="power", data_type=DataType.INTEGER),
    ])


class TestDatabaseCreation:
    """Verify database initialization."""

    def test_creates_data_directory(self, tmp_path: Path) -> None:
        """Opening a database should create the data directory."""
        db_path = tmp_path / "mydb"
        Database(path=db_path)
        assert db_path.is_dir()

    def test_starts_with_no_tables(self, tmp_path: Path) -> None:
        """A new database should have no tables."""
        db = Database(path=tmp_path)
        assert db.table_names() == []


class TestCreateTable:
    """Verify table creation."""

    def test_create_adds_table(self, tmp_path: Path) -> None:
        """Creating a table should make it available by name."""
        db = Database(path=tmp_path)
        db.create_table("cards", _make_schema())
        assert "cards" in db.table_names()

    def test_create_returns_table(self, tmp_path: Path) -> None:
        """create_table should return the new table object."""
        db = Database(path=tmp_path)
        table = db.create_table("cards", _make_schema())
        assert table.name == "cards"

    def test_duplicate_name_raises(self, tmp_path: Path) -> None:
        """Creating a table with an existing name should raise."""
        db = Database(path=tmp_path)
        db.create_table("cards", _make_schema())
        with pytest.raises(DatabaseError, match="already exists"):
            db.create_table("cards", _make_schema())


class TestGetTable:
    """Verify table retrieval."""

    def test_get_existing_table(self, tmp_path: Path) -> None:
        """Getting a table by name should return the correct table."""
        db = Database(path=tmp_path)
        db.create_table("cards", _make_schema())
        table = db.get_table("cards")
        assert table.name == "cards"

    def test_get_nonexistent_raises(self, tmp_path: Path) -> None:
        """Getting a table that doesn't exist should raise."""
        db = Database(path=tmp_path)
        with pytest.raises(DatabaseError, match="does not exist"):
            db.get_table("missing")


class TestDropTable:
    """Verify table deletion."""

    def test_drop_removes_from_catalog(self, tmp_path: Path) -> None:
        """Dropping a table should remove it from table_names."""
        db = Database(path=tmp_path)
        db.create_table("cards", _make_schema())
        db.drop_table("cards")
        assert "cards" not in db.table_names()

    def test_drop_nonexistent_raises(self, tmp_path: Path) -> None:
        """Dropping a table that doesn't exist should raise."""
        db = Database(path=tmp_path)
        with pytest.raises(DatabaseError, match="does not exist"):
            db.drop_table("missing")

    def test_drop_deletes_file(self, tmp_path: Path) -> None:
        """Dropping a saved table should remove its file from disk."""
        db = Database(path=tmp_path)
        db.create_table("cards", _make_schema())
        db.save()
        assert (tmp_path / "cards.json").exists()
        db.drop_table("cards")
        assert not (tmp_path / "cards.json").exists()


class TestSaveAndLoad:
    """Verify persistence -- the whole point of the storage layer."""

    def test_save_and_load_round_trip(self, tmp_path: Path) -> None:
        """Tables should survive save → new Database → load."""
        db_path = tmp_path / "mydb"

        # Create and populate.
        db1 = Database(path=db_path)
        table = db1.create_table("cards", _make_schema())
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.insert({"name": "Charmander", "power": POWER_52})
        db1.save()

        # Load in a fresh database instance.
        db2 = Database(path=db_path)
        db2.load()
        loaded = db2.get_table("cards")
        assert loaded.row_count == 2  # noqa: PLR2004
        records = loaded.select()
        assert records[0]["name"] == "Pikachu"
        assert records[1]["power"] == POWER_52

    def test_load_preserves_next_id(self, tmp_path: Path) -> None:
        """After load, the next inserted record should get the correct ID."""
        db_path = tmp_path / "mydb"

        db1 = Database(path=db_path)
        table = db1.create_table("cards", _make_schema())
        table.insert({"name": "Pikachu", "power": POWER_55})
        db1.save()

        db2 = Database(path=db_path)
        db2.load()
        loaded = db2.get_table("cards")
        new_record = loaded.insert({"name": "Charmander", "power": POWER_52})
        assert new_record.record_id == 2  # noqa: PLR2004

    def test_save_single_table(self, tmp_path: Path) -> None:
        """save_table should persist just the named table."""
        db = Database(path=tmp_path)
        table = db.create_table("cards", _make_schema())
        table.insert({"name": "Pikachu", "power": POWER_55})
        db.save_table("cards")
        assert (tmp_path / "cards.json").exists()

    def test_save_table_nonexistent_raises(self, tmp_path: Path) -> None:
        """save_table for a non-existent table should raise."""
        db = Database(path=tmp_path)
        with pytest.raises(DatabaseError, match="does not exist"):
            db.save_table("missing")

    def test_load_multiple_tables(self, tmp_path: Path) -> None:
        """Multiple tables should all be restored on load."""
        db_path = tmp_path / "mydb"

        db1 = Database(path=db_path)
        db1.create_table("cards", _make_schema())
        score_schema = Schema(columns=[
            Column(name="player", data_type=DataType.TEXT),
            Column(name="score", data_type=DataType.INTEGER),
        ])
        scores = db1.create_table("scores", score_schema)
        scores.insert({"player": "Alice", "score": SCORE_9001})
        db1.save()

        db2 = Database(path=db_path)
        db2.load()
        assert sorted(db2.table_names()) == ["cards", "scores"]
        assert db2.get_table("scores").row_count == FIRST_ID

    def test_load_single_table(self, tmp_path: Path) -> None:
        """load_table should load just the named table."""
        db_path = tmp_path / "mydb"

        db1 = Database(path=db_path)
        table = db1.create_table("cards", _make_schema())
        table.insert({"name": "Pikachu", "power": POWER_55})
        db1.save()

        db2 = Database(path=db_path)
        loaded = db2.load_table("cards")
        assert loaded.row_count == FIRST_ID
        assert loaded.select()[0]["name"] == "Pikachu"


class TestTableNames:
    """Verify the table listing."""

    def test_names_sorted(self, tmp_path: Path) -> None:
        """table_names should return names in sorted order."""
        db = Database(path=tmp_path)
        db.create_table("zebra", _make_schema())
        db.create_table("alpha", _make_schema())
        assert db.table_names() == ["alpha", "zebra"]
