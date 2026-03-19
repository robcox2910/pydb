"""Tests for the Table class.

A table is a binder full of trading cards. These tests verify that you can
add cards, find cards, change cards, and remove cards -- and that the binder
enforces its template (schema) on every operation.
"""

import pytest

from pydb.errors import RecordNotFoundError, SchemaError
from pydb.schema import Column, Schema
from pydb.table import Table
from pydb.types import DataType

# Named constants.
POWER_55 = 55
POWER_52 = 52
POWER_48 = 48
POWER_60 = 60
POWER_THRESHOLD = 50
FIRST_ID = 1
SECOND_ID = 2
THIRD_ID = 3
NONEXISTENT_ID = 999


def _make_table() -> Table:
    """Create a standard test table with name and power columns."""
    schema = Schema(columns=[
        Column(name="name", data_type=DataType.TEXT),
        Column(name="power", data_type=DataType.INTEGER),
    ])
    return Table(name="cards", schema=schema)


class TestTableCreation:
    """Verify initial state of a new table."""

    def test_new_table_is_empty(self) -> None:
        """A freshly created table should have zero rows."""
        table = _make_table()
        assert table.row_count == 0

    def test_table_has_name(self) -> None:
        """A table should know its own name."""
        table = _make_table()
        assert table.name == "cards"

    def test_table_has_schema(self) -> None:
        """A table should expose its schema."""
        table = _make_table()
        assert table.schema.column_names == ["name", "power"]


class TestTableInsert:
    """Verify that records are inserted correctly."""

    def test_insert_increases_row_count(self) -> None:
        """Inserting a record should increase the row count by one."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        assert table.row_count == FIRST_ID

    def test_insert_returns_record_with_id(self) -> None:
        """The returned record should have an auto-assigned ID."""
        table = _make_table()
        record = table.insert({"name": "Pikachu", "power": POWER_55})
        assert record.record_id == FIRST_ID

    def test_insert_assigns_sequential_ids(self) -> None:
        """Each insert should get the next ID in sequence."""
        table = _make_table()
        r1 = table.insert({"name": "Pikachu", "power": POWER_55})
        r2 = table.insert({"name": "Charmander", "power": POWER_52})
        assert r1.record_id == FIRST_ID
        assert r2.record_id == SECOND_ID

    def test_insert_validates_schema(self) -> None:
        """Inserting data that doesn't match the schema should raise."""
        table = _make_table()
        with pytest.raises(SchemaError):
            table.insert({"name": "Pikachu", "power": "banana"})

    def test_insert_rejects_missing_columns(self) -> None:
        """Inserting with missing columns should raise SchemaError."""
        table = _make_table()
        with pytest.raises(SchemaError, match="Missing required"):
            table.insert({"name": "Pikachu"})

    def test_insert_rejects_extra_columns(self) -> None:
        """Inserting with unknown columns should raise SchemaError."""
        table = _make_table()
        with pytest.raises(SchemaError, match="Unknown column"):
            table.insert({"name": "Pikachu", "power": POWER_55, "colour": "yellow"})


class TestTableSelect:
    """Verify that records can be retrieved."""

    def test_select_all_from_empty_table(self) -> None:
        """Selecting from an empty table should return an empty list."""
        table = _make_table()
        assert table.select() == []

    def test_select_all_returns_all_records(self) -> None:
        """Selecting without a filter should return every record."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.insert({"name": "Charmander", "power": POWER_52})
        results = table.select()
        assert len(results) == SECOND_ID

    def test_select_with_filter(self) -> None:
        """Selecting with a where clause should return only matches."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.insert({"name": "Charmander", "power": POWER_52})
        table.insert({"name": "Squirtle", "power": POWER_48})

        strong = table.select(where=lambda r: r["power"] > POWER_THRESHOLD)
        assert len(strong) == SECOND_ID
        names = [r["name"] for r in strong]
        assert "Pikachu" in names
        assert "Charmander" in names
        assert "Squirtle" not in names

    def test_select_returns_records_ordered_by_id(self) -> None:
        """Results should be ordered by record ID."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.insert({"name": "Charmander", "power": POWER_52})
        results = table.select()
        assert results[0].record_id < results[1].record_id


class TestTableGet:
    """Verify that individual records can be retrieved by ID."""

    def test_get_existing_record(self) -> None:
        """Getting a record by its ID should return the correct record."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        record = table.get(FIRST_ID)
        assert record["name"] == "Pikachu"

    def test_get_nonexistent_raises(self) -> None:
        """Getting a record that doesn't exist should raise."""
        table = _make_table()
        with pytest.raises(RecordNotFoundError):
            table.get(NONEXISTENT_ID)


class TestTableUpdate:
    """Verify that records can be updated."""

    def test_update_changes_values(self) -> None:
        """Updating should change the specified fields."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.update(record_id=FIRST_ID, values={"power": POWER_60})
        record = table.get(FIRST_ID)
        assert record["power"] == POWER_60

    def test_update_preserves_other_fields(self) -> None:
        """Fields not mentioned in the update should stay the same."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.update(record_id=FIRST_ID, values={"power": POWER_60})
        record = table.get(FIRST_ID)
        assert record["name"] == "Pikachu"

    def test_update_returns_updated_record(self) -> None:
        """The update method should return the modified record."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        result = table.update(record_id=FIRST_ID, values={"power": POWER_60})
        assert result["power"] == POWER_60

    def test_update_nonexistent_raises(self) -> None:
        """Updating a record that doesn't exist should raise."""
        table = _make_table()
        with pytest.raises(RecordNotFoundError):
            table.update(record_id=NONEXISTENT_ID, values={"power": POWER_60})

    def test_update_validates_schema(self) -> None:
        """Updating with wrong types should raise SchemaError."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        with pytest.raises(SchemaError):
            table.update(record_id=FIRST_ID, values={"power": "banana"})


class TestTableDelete:
    """Verify that records can be deleted."""

    def test_delete_removes_record(self) -> None:
        """Deleting a record should reduce the row count."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.delete(record_id=FIRST_ID)
        assert table.row_count == 0

    def test_delete_nonexistent_raises(self) -> None:
        """Deleting a record that doesn't exist should raise."""
        table = _make_table()
        with pytest.raises(RecordNotFoundError):
            table.delete(record_id=NONEXISTENT_ID)

    def test_ids_not_reused_after_delete(self) -> None:
        """Deleting a record should not cause its ID to be reused."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.delete(record_id=FIRST_ID)
        record = table.insert({"name": "Charmander", "power": POWER_52})
        assert record.record_id == SECOND_ID

    def test_deleted_record_not_in_select(self) -> None:
        """A deleted record should not appear in select results."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        table.insert({"name": "Charmander", "power": POWER_52})
        table.delete(record_id=FIRST_ID)
        results = table.select()
        assert len(results) == FIRST_ID
        assert results[0]["name"] == "Charmander"


class TestTableRepr:
    """Verify the table string representation."""

    def test_repr_shows_name_and_row_count(self) -> None:
        """The repr should include the table name and row count."""
        table = _make_table()
        table.insert({"name": "Pikachu", "power": POWER_55})
        result = repr(table)
        assert "cards" in result
        assert "1" in result
