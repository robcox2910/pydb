"""Data types supported by PyDB.

Every column in a table has a data type -- like labelled drawers that only
accept the right kind of thing. You wouldn't put socks in the cutlery drawer,
and you shouldn't put text in an integer column.
"""

from enum import StrEnum


class DataType(StrEnum):
    """Enumerate the data types a column can hold.

    Each variant maps to a Python built-in type for validation.
    """

    TEXT = "TEXT"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"


# Mapping from DataType to the Python type used for validation.
PYTHON_TYPES: dict[DataType, type] = {
    DataType.TEXT: str,
    DataType.INTEGER: int,
    DataType.FLOAT: float,
    DataType.BOOLEAN: bool,
}
