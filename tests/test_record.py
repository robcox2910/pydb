"""Tests for the Record class.

A record is one trading card -- it has a serial number (ID) and facts (data).
These tests verify that records store data correctly and behave as expected.
"""

import pytest

from pydb.record import Record

# Named constants to avoid magic values (PLR2004).
RECORD_ID_1 = 1
RECORD_ID_2 = 2
POWER_55 = 55
POWER_60 = 60
EXPECTED_COLUMN_COUNT = 3


class TestRecordCreation:
    """Verify that records are created with the correct ID and data."""

    def test_record_stores_id(self) -> None:
        """A record should know its own ID."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        assert record.record_id == RECORD_ID_1

    def test_record_stores_data(self) -> None:
        """A record should store and return its data."""
        data = {"name": "Pikachu", "type": "Electric", "power": POWER_55}
        record = Record(record_id=RECORD_ID_1, data=data)
        assert record.data == data

    def test_data_is_a_copy(self) -> None:
        """Modifying the returned data dict should not affect the record."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        returned = record.data
        returned["name"] = "Charmander"
        assert record["name"] == "Pikachu"


class TestRecordAccess:
    """Verify field access via bracket notation and helper methods."""

    def test_getitem_returns_value(self) -> None:
        """record['column'] should return the stored value."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55})
        assert record["name"] == "Pikachu"
        assert record["power"] == POWER_55

    def test_getitem_raises_for_unknown_column(self) -> None:
        """Accessing a column that doesn't exist should raise KeyError."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        with pytest.raises(KeyError):
            _ = record["missing"]

    def test_columns_returns_field_names(self) -> None:
        """The columns property should list all field names."""
        data = {"name": "Pikachu", "type": "Electric", "power": POWER_55}
        record = Record(record_id=RECORD_ID_1, data=data)
        assert len(record.columns) == EXPECTED_COLUMN_COUNT
        assert set(record.columns) == {"name", "type", "power"}

    def test_get_returns_value(self) -> None:
        """The get method should return a value for existing columns."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        assert record.get("name") == "Pikachu"

    def test_get_returns_default_for_missing(self) -> None:
        """The get method should return the default for missing columns."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        assert record.get("missing", "N/A") == "N/A"

    def test_get_returns_none_by_default(self) -> None:
        """The get method should return None when no default is given."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        assert record.get("missing") is None


class TestRecordEquality:
    """Verify that equality and hashing are based on record ID."""

    def test_same_id_means_equal(self) -> None:
        """Two records with the same ID should be equal."""
        r1 = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        r2 = Record(record_id=RECORD_ID_1, data={"name": "Charmander"})
        assert r1 == r2

    def test_different_id_means_not_equal(self) -> None:
        """Two records with different IDs should not be equal."""
        r1 = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        r2 = Record(record_id=RECORD_ID_2, data={"name": "Pikachu"})
        assert r1 != r2

    def test_not_equal_to_non_record(self) -> None:
        """A record should not be equal to a non-Record object."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        assert record != "not a record"

    def test_same_id_same_hash(self) -> None:
        """Records with the same ID should have the same hash."""
        r1 = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        r2 = Record(record_id=RECORD_ID_1, data={"name": "Charmander"})
        assert hash(r1) == hash(r2)


class TestRecordUpdate:
    """Verify that internal update modifies fields correctly."""

    def test_update_changes_values(self) -> None:
        """Updating a field should change its value."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55})
        record._update({"power": POWER_60})
        assert record["power"] == POWER_60

    def test_update_unknown_column_raises(self) -> None:
        """Updating a column that doesn't exist should raise KeyError."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        with pytest.raises(KeyError, match="Unknown column"):
            record._update({"missing": "value"})


class TestRecordRepr:
    """Verify the string representation of records."""

    def test_repr_includes_id_and_data(self) -> None:
        """The repr should show the record ID and data."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu"})
        result = repr(record)
        assert "record_id=1" in result
        assert "Pikachu" in result
