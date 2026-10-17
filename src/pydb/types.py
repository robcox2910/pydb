"""Data types supported by PyDB.

Every column in a table has a data type -- like labelled drawers that only
accept the right kind of thing. You wouldn't put socks in the cutlery drawer,
and you shouldn't put text in an integer column.
"""

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import cast

from pydb.errors import SchemaError
from pydb.record import Value


class DataType(StrEnum):
    """Enumerate the data types a column can hold.

    Each variant maps to a Python built-in type for validation.
    """

    TEXT = "TEXT"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"


# Mapping from DataType to the Python type used for validation.
PYTHON_TYPES: dict[DataType, type] = {
    DataType.TEXT: str,
    DataType.INTEGER: int,
    DataType.FLOAT: float,
    DataType.BOOLEAN: bool,
    DataType.DATE: date,
    DataType.TIMESTAMP: datetime,
}


def check_value(value: object, data_type: DataType) -> Value:
    """Validate a value against a column type and normalise it.

    This is the one bouncer for every type. Subclasses are welcome (a
    ``StrEnum`` member is still text), with three exceptions: ``bool`` is
    not an INTEGER, an ``int`` is not a FLOAT (the schema widens those
    before validating), and a ``datetime`` is not a DATE. A TIMESTAMP must
    carry a timezone and is converted to UTC.

    Args:
        value: The value to check.
        data_type: The column type it must fit.

    Returns:
        The value, with TIMESTAMPs converted to UTC.

    Raises:
        SchemaError: If the value does not fit the type.

    """
    expected = PYTHON_TYPES[data_type]
    wrong_kind = (data_type is DataType.INTEGER and isinstance(value, bool)) or (
        data_type is DataType.DATE and isinstance(value, datetime)
    )
    if wrong_kind or not isinstance(value, expected):
        msg = f"expects {data_type.value}, got {type(value).__name__}"
        raise SchemaError(msg)
    if isinstance(value, datetime):
        return _to_utc(value)
    return cast(Value, value)


def _to_utc(value: datetime) -> datetime:
    """Convert an aware datetime to UTC, rejecting naive ones."""
    if value.utcoffset() is None:
        msg = "TIMESTAMP values must have a timezone (naive datetime rejected)"
        raise SchemaError(msg)
    return value.astimezone(UTC)


def to_iso(value: date | datetime) -> str:
    """Write a date or timestamp as ISO text.

    DATE becomes ``YYYY-MM-DD``; TIMESTAMP becomes UTC with a ``Z`` suffix,
    e.g. ``2026-10-17T12:30:45.123456Z``.

    Raises:
        SchemaError: If a datetime has no timezone.

    """
    if isinstance(value, datetime):
        utc = _to_utc(value)
        return utc.replace(tzinfo=None).isoformat() + "Z"
    return value.isoformat()


def from_iso(text: str, data_type: DataType) -> date | datetime:
    """Read ISO text back into a DATE or TIMESTAMP value.

    TIMESTAMP text must include ``Z`` or an offset such as ``+02:00``; the
    result is always converted to UTC.

    Raises:
        SchemaError: If the text is malformed or the type is not temporal.

    """
    try:
        if data_type is DataType.DATE:
            if len(text) != len("YYYY-MM-DD"):
                msg = f"bad DATE text {text!r}"
                raise SchemaError(msg)
            return date.fromisoformat(text)
        if data_type is DataType.TIMESTAMP:
            if "T" not in text:
                msg = f"bad TIMESTAMP text {text!r}"
                raise SchemaError(msg)
            parsed = datetime.fromisoformat(text)
            return _to_utc(parsed)
    except ValueError as exc:
        msg = f"bad {data_type.value} text {text!r}"
        raise SchemaError(msg) from exc
    msg = f"{data_type.value} is not a date type"
    raise SchemaError(msg)
