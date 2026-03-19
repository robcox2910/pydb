"""Tests for PyDB data types.

Data types are like labelled drawers -- each one only accepts the right kind
of thing. These tests verify that our type definitions are correct.
"""

from pydb.types import PYTHON_TYPES, DataType

EXPECTED_TYPE_COUNT = 4


class TestDataType:
    """Verify the DataType enumeration."""

    def test_text_value(self) -> None:
        """TEXT should have string value 'TEXT'."""
        assert DataType.TEXT == "TEXT"

    def test_integer_value(self) -> None:
        """INTEGER should have string value 'INTEGER'."""
        assert DataType.INTEGER == "INTEGER"

    def test_float_value(self) -> None:
        """FLOAT should have string value 'FLOAT'."""
        assert DataType.FLOAT == "FLOAT"

    def test_boolean_value(self) -> None:
        """BOOLEAN should have string value 'BOOLEAN'."""
        assert DataType.BOOLEAN == "BOOLEAN"

    def test_all_types_have_python_mapping(self) -> None:
        """Every DataType variant should map to a Python built-in type."""
        assert len(PYTHON_TYPES) == EXPECTED_TYPE_COUNT
        for dt in DataType:
            assert dt in PYTHON_TYPES


class TestPythonTypeMapping:
    """Verify the mapping from DataType to Python types."""

    def test_text_maps_to_str(self) -> None:
        """TEXT should map to Python str."""
        assert PYTHON_TYPES[DataType.TEXT] is str

    def test_integer_maps_to_int(self) -> None:
        """INTEGER should map to Python int."""
        assert PYTHON_TYPES[DataType.INTEGER] is int

    def test_float_maps_to_float(self) -> None:
        """FLOAT should map to Python float."""
        assert PYTHON_TYPES[DataType.FLOAT] is float

    def test_boolean_maps_to_bool(self) -> None:
        """BOOLEAN should map to Python bool."""
        assert PYTHON_TYPES[DataType.BOOLEAN] is bool
