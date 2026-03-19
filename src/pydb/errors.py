"""Custom exceptions for PyDB.

Every database needs clear error messages. When something goes wrong -- like
trying to insert text into a number column -- the error should explain exactly
what happened and why, so the person using the database can fix it.
"""


class PyDBError(Exception):
    """Base exception for all PyDB errors."""


class SchemaError(PyDBError):
    """Raise when data does not conform to a table's schema.

    This is the "bouncer at the door" -- it rejects records that don't match
    the table's column definitions.
    """


class RecordNotFoundError(PyDBError):
    """Raise when a record with the given ID does not exist."""
