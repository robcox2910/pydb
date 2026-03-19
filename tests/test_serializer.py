"""Tests for the serializer module.

The serializer translates Python objects to JSON and back. These tests
verify that nothing gets lost in translation -- like checking that your
letter arrives exactly as you wrote it.
"""

import pytest

from pydb.record import Record
from pydb.schema import Column, Schema
from pydb.serializer import (
    SerializationError,
    deserialize_record,
    deserialize_schema,
    deserialize_table_data,
    serialize_record,
    serialize_schema,
    serialize_table_data,
)
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


class TestSerializeSchema:
    """Verify schema serialization to dict."""

    def test_schema_to_dict(self) -> None:
        """A serialized schema should contain column names and types."""
        schema = _make_schema()
        result = serialize_schema(schema)
        assert len(result["columns"]) == 2  # noqa: PLR2004
        assert result["columns"][0] == {"name": "name", "data_type": "TEXT"}
        assert result["columns"][1] == {"name": "power", "data_type": "INTEGER"}


class TestDeserializeSchema:
    """Verify schema deserialization from dict."""

    def test_round_trip(self) -> None:
        """A schema should survive serialize → deserialize unchanged."""
        schema = _make_schema()
        data = serialize_schema(schema)
        restored = deserialize_schema(data)
        assert restored.column_names == schema.column_names

    def test_missing_columns_key_raises(self) -> None:
        """A dict without 'columns' should raise SerializationError."""
        with pytest.raises(SerializationError, match="missing 'columns'"):
            deserialize_schema({})

    def test_invalid_column_raises(self) -> None:
        """A column dict missing required fields should raise."""
        with pytest.raises(SerializationError, match="Invalid column"):
            deserialize_schema({"columns": [{"name": "x"}]})


class TestSerializeRecord:
    """Verify record serialization to dict."""

    def test_record_to_dict(self) -> None:
        """A serialized record should have record_id and data."""
        record = Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55})
        result = serialize_record(record)
        assert result["record_id"] == RECORD_ID_1
        assert result["data"] == {"name": "Pikachu", "power": POWER_55}


class TestDeserializeRecord:
    """Verify record deserialization from dict."""

    def test_round_trip(self) -> None:
        """A record should survive serialize → deserialize unchanged."""
        schema = _make_schema()
        original = Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55})
        data = serialize_record(original)
        restored = deserialize_record(data, schema)
        assert restored.record_id == original.record_id
        assert restored["name"] == "Pikachu"
        assert restored["power"] == POWER_55

    def test_missing_record_id_raises(self) -> None:
        """A dict without 'record_id' should raise SerializationError."""
        schema = _make_schema()
        with pytest.raises(SerializationError, match="Invalid record"):
            deserialize_record({"data": {"name": "x", "power": 1}}, schema)

    def test_non_dict_data_raises(self) -> None:
        """If 'data' is not a dict, it should raise SerializationError."""
        schema = _make_schema()
        with pytest.raises(SerializationError, match="must be a dictionary"):
            deserialize_record({"record_id": 1, "data": "not a dict"}, schema)

    def test_unknown_column_raises(self) -> None:
        """A column not in the schema should raise SerializationError."""
        schema = _make_schema()
        with pytest.raises(SerializationError, match="Unknown column"):
            deserialize_record(
                {"record_id": 1, "data": {"name": "x", "power": 1, "extra": "y"}},
                schema,
            )


class TestSerializeTableData:
    """Verify full table serialization to JSON string."""

    def test_produces_valid_json(self) -> None:
        """The result should be a valid JSON string."""
        schema = _make_schema()
        records = [
            Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55}),
        ]
        result = serialize_table_data("cards", schema, records, NEXT_ID)
        assert '"name": "cards"' in result
        assert '"next_id": 3' in result


class TestDeserializeTableData:
    """Verify full table deserialization from JSON string."""

    def test_round_trip(self) -> None:
        """A full table should survive serialize → deserialize."""
        schema = _make_schema()
        records = [
            Record(record_id=RECORD_ID_1, data={"name": "Pikachu", "power": POWER_55}),
            Record(record_id=RECORD_ID_2, data={"name": "Charmander", "power": POWER_52}),
        ]
        json_str = serialize_table_data("cards", schema, records, NEXT_ID)
        name, restored_schema, restored_records, next_id = deserialize_table_data(json_str)

        assert name == "cards"
        assert restored_schema.column_names == ["name", "power"]
        assert len(restored_records) == 2  # noqa: PLR2004
        assert next_id == NEXT_ID
        assert restored_records[0]["name"] == "Pikachu"
        assert restored_records[1]["name"] == "Charmander"

    def test_invalid_json_raises(self) -> None:
        """Invalid JSON should raise SerializationError."""
        with pytest.raises(SerializationError, match="Invalid JSON"):
            deserialize_table_data("not json at all {{{")

    def test_missing_field_raises(self) -> None:
        """JSON missing required fields should raise SerializationError."""
        with pytest.raises(SerializationError, match="missing required field"):
            deserialize_table_data('{"name": "cards"}')

    def test_non_object_json_raises(self) -> None:
        """A JSON array (not object) should raise SerializationError."""
        with pytest.raises(SerializationError, match="must be a JSON object"):
            deserialize_table_data("[1, 2, 3]")


class TestDataTypeRoundTrips:
    """Verify that all data types survive serialization."""

    def test_float_round_trip(self) -> None:
        """Float values should round-trip correctly."""
        schema = Schema(columns=[Column(name="value", data_type=DataType.FLOAT)])
        record = Record(record_id=RECORD_ID_1, data={"value": PI})
        data = serialize_record(record)
        restored = deserialize_record(data, schema)
        assert restored["value"] == PI

    def test_boolean_round_trip(self) -> None:
        """Boolean values should round-trip correctly."""
        schema = Schema(columns=[Column(name="active", data_type=DataType.BOOLEAN)])
        record = Record(record_id=RECORD_ID_1, data={"active": True})
        data = serialize_record(record)
        restored = deserialize_record(data, schema)
        assert restored["active"] is True

    def test_text_round_trip(self) -> None:
        """Text values should round-trip correctly."""
        schema = Schema(columns=[Column(name="msg", data_type=DataType.TEXT)])
        record = Record(record_id=RECORD_ID_1, data={"msg": "hello world"})
        data = serialize_record(record)
        restored = deserialize_record(data, schema)
        assert restored["msg"] == "hello world"
