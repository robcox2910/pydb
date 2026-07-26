"""Tests for the Schema class.

A schema is the bouncer at the door -- it checks every new record to make
sure it follows the rules. These tests verify that the bouncer does its job.
"""

import pytest

from pydb.errors import SchemaError
from pydb.schema import Column, Schema
from pydb.types import DataType

EXPECTED_COLUMN_COUNT = 3
TWO_COLUMNS = 2
POWER_55 = 55
PI = 3.14
FIVE_INT = 5
FIVE_FLOAT = 5.0


class TestSchemaCreation:
    """Verify that schemas are created correctly."""

    def test_schema_stores_columns(self) -> None:
        """A schema should remember its column definitions."""
        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        assert len(schema.columns) == TWO_COLUMNS

    def test_schema_column_names(self) -> None:
        """The column_names property should return names in order."""
        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="type", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        assert schema.column_names == ["name", "type", "power"]

    def test_empty_schema_raises(self) -> None:
        """A schema with no columns should raise SchemaError."""
        with pytest.raises(SchemaError, match="at least one column"):
            Schema(columns=[])


class TestSchemaValidation:
    """Verify that schema validation catches bad data."""

    def _make_schema(self) -> Schema:
        """Create a standard test schema."""
        return Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="type", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )

    def test_valid_data_passes(self) -> None:
        """Values that match the schema should pass validation."""
        schema = self._make_schema()
        # Should not raise.
        schema.validate({"name": "Pikachu", "type": "Electric", "power": POWER_55})

    def test_unknown_column_rejected(self) -> None:
        """A column not in the schema should be rejected."""
        schema = self._make_schema()
        with pytest.raises(SchemaError, match="Unknown column"):
            schema.validate(
                {
                    "name": "Pikachu",
                    "type": "Electric",
                    "power": POWER_55,
                    "colour": "yellow",
                }
            )

    def test_missing_column_rejected(self) -> None:
        """A missing required column should be rejected."""
        schema = self._make_schema()
        with pytest.raises(SchemaError, match="Missing required column"):
            schema.validate({"name": "Pikachu", "type": "Electric"})

    def test_wrong_type_rejected(self) -> None:
        """A value with the wrong type should be rejected."""
        schema = self._make_schema()
        with pytest.raises(SchemaError, match="expects INTEGER"):
            schema.validate({"name": "Pikachu", "type": "Electric", "power": "banana"})

    def test_bool_rejected_for_integer_column(self) -> None:
        """A boolean should not be accepted as an integer."""
        schema = self._make_schema()
        with pytest.raises(SchemaError, match=r"expects INTEGER.*got bool"):
            schema.validate({"name": "Pikachu", "type": "Electric", "power": True})

    def test_float_column_accepts_float(self) -> None:
        """A FLOAT column should accept float values."""
        schema = Schema(
            columns=[
                Column(name="value", data_type=DataType.FLOAT),
            ]
        )
        schema.validate({"value": PI})

    def test_boolean_column_accepts_bool(self) -> None:
        """A BOOLEAN column should accept boolean values."""
        schema = Schema(
            columns=[
                Column(name="active", data_type=DataType.BOOLEAN),
            ]
        )
        schema.validate({"active": True})


class TestColumnDataclass:
    """Verify the Column dataclass."""

    def test_column_is_frozen(self) -> None:
        """Column instances should be immutable."""
        col = Column(name="name", data_type=DataType.TEXT)
        with pytest.raises(AttributeError):
            col.name = "changed"  # type: ignore[misc]

    def test_column_repr(self) -> None:
        """Column repr should show name and data type."""
        col = Column(name="power", data_type=DataType.INTEGER)
        result = repr(col)
        assert "power" in result
        assert "INTEGER" in result


class TestSchemaRepr:
    """Verify the schema string representation."""

    def test_repr_shows_columns(self) -> None:
        """The schema repr should list column names and types."""
        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        result = repr(schema)
        assert "name:TEXT" in result
        assert "power:INTEGER" in result


class TestFloatCoercion:
    """Verify int -> float widening for FLOAT columns."""

    def test_int_widened_to_float(self) -> None:
        """A whole number for a FLOAT column becomes a float."""
        schema = Schema(columns=[Column(name="ratio", data_type=DataType.FLOAT)])
        coerced = schema.coerce({"ratio": FIVE_INT})
        assert coerced["ratio"] == FIVE_FLOAT
        assert isinstance(coerced["ratio"], float)
        # A coerced int should now pass validation for a FLOAT column.
        schema.validate(coerced)

    def test_float_left_unchanged(self) -> None:
        """An existing float is passed through untouched."""
        schema = Schema(columns=[Column(name="ratio", data_type=DataType.FLOAT)])
        coerced = schema.coerce({"ratio": PI})
        assert coerced["ratio"] == PI

    def test_bool_not_widened_for_float_column(self) -> None:
        """A bool is not silently turned into a float (it is still rejected)."""
        schema = Schema(columns=[Column(name="ratio", data_type=DataType.FLOAT)])
        coerced = schema.coerce({"ratio": True})
        assert coerced["ratio"] is True
        with pytest.raises(SchemaError):
            schema.validate(coerced)

    def test_int_column_not_affected(self) -> None:
        """Ints for INTEGER columns are left as ints."""
        schema = Schema(columns=[Column(name="power", data_type=DataType.INTEGER)])
        coerced = schema.coerce({"power": POWER_55})
        assert coerced["power"] == POWER_55
        assert not isinstance(coerced["power"], float)
