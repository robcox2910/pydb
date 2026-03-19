"""Tests for PyDB custom exceptions.

Clear error messages help people understand what went wrong. These tests
verify that our exceptions exist and work as expected.
"""

from pydb.errors import PyDBError, RecordNotFoundError, SchemaError


class TestExceptionHierarchy:
    """Verify that all exceptions inherit from PyDBError."""

    def test_schema_error_is_pydb_error(self) -> None:
        """SchemaError should be a subclass of PyDBError."""
        assert issubclass(SchemaError, PyDBError)

    def test_record_not_found_is_pydb_error(self) -> None:
        """RecordNotFoundError should be a subclass of PyDBError."""
        assert issubclass(RecordNotFoundError, PyDBError)

    def test_pydb_error_is_exception(self) -> None:
        """PyDBError should be a subclass of Exception."""
        assert issubclass(PyDBError, Exception)


class TestExceptionMessages:
    """Verify that exceptions carry their messages."""

    def test_schema_error_message(self) -> None:
        """SchemaError should carry the provided message."""
        err = SchemaError("wrong type")
        assert str(err) == "wrong type"

    def test_record_not_found_message(self) -> None:
        """RecordNotFoundError should carry the provided message."""
        err = RecordNotFoundError("no such record")
        assert str(err) == "no such record"
