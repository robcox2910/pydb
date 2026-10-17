"""Schema definitions for PyDB tables.

A schema is the template for a table -- like the design for a trading card
that says "every card must have a Name (text), a Type (text), and a Power
(number)." The schema acts as a bouncer at the door, checking every new
record before letting it in.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from pydb.errors import SchemaError
from pydb.record import Value
from pydb.types import DataType, check_value


@dataclass(frozen=True, slots=True)
class Column:
    """Define a single column in a table schema.

    Args:
        name: The column name (e.g., "age").
        data_type: The type of data this column holds.
        primary_key: Whether this column is the primary key.
        not_null: Whether this column rejects NULL/missing values.
        unique: Whether this column requires unique values.

    """

    name: str
    data_type: DataType
    primary_key: bool = False
    not_null: bool = False
    unique: bool = False


class Schema:
    """Define the structure of a table -- its columns and their types.

    A schema is immutable once created. To change a table's structure, you
    create a new schema.

    Args:
        columns: The column definitions for this schema.

    """

    __slots__ = ("_column_map", "_columns")

    def __init__(self, columns: list[Column]) -> None:
        """Create a new schema from a list of column definitions."""
        if not columns:
            msg = "A schema must have at least one column"
            raise SchemaError(msg)
        self._columns = tuple(columns)
        self._column_map: dict[str, DataType] = {col.name: col.data_type for col in columns}

    @property
    def columns(self) -> tuple[Column, ...]:
        """Return the column definitions."""
        return self._columns

    @property
    def column_names(self) -> list[str]:
        """Return the column names in order."""
        return [col.name for col in self._columns]

    def coerce(self, values: Mapping[str, Value]) -> dict[str, Value]:
        """Adjust values to fit their column types where it's safe.

        Like a helpful clerk who writes "5.0" when a form asks for a
        decimal but you only wrote "5" -- a whole number is a perfectly
        good decimal, so we widen an ``int`` to a ``float`` for FLOAT
        columns. Everything else is passed through unchanged.

        Args:
            values: Column-name-to-value mapping to adjust.

        Returns:
            A new mapping with ints widened to floats for FLOAT columns and aware timestamps
            converted to UTC.

        """
        result = dict(values)
        for name, value in result.items():
            expected = self._column_map.get(name)
            # bool is a subclass of int, so exclude it explicitly.
            if (
                expected == DataType.FLOAT
                and isinstance(value, int)
                and not isinstance(value, bool)
            ):
                result[name] = float(value)
            elif (
                expected == DataType.TIMESTAMP
                and isinstance(value, datetime)
                and value.utcoffset() is not None
            ):
                result[name] = check_value(value, DataType.TIMESTAMP)
        return result

    def validate(self, values: Mapping[str, Value]) -> None:
        """Check that a set of values conforms to this schema.

        Validation checks (the bouncer's checklist):
        1. No unknown columns (no gate-crashers).
        2. All required columns are present (no missing tickets).
        3. Every value has the correct type (no fake IDs).

        Args:
            values: Column-name-to-value mapping to validate.

        Raises:
            SchemaError: If validation fails, with a message explaining why.

        """
        # Check for unknown columns.
        schema_cols = set(self._column_map)
        unknown = set(values.keys()) - schema_cols
        if unknown:
            msg = f"Unknown column(s): {', '.join(sorted(unknown))}"
            raise SchemaError(msg)

        # Check for missing columns.
        missing = schema_cols - set(values.keys())
        if missing:
            msg = f"Missing required column(s): {', '.join(sorted(missing))}"
            raise SchemaError(msg)

        # Check types (check_value is the single exact-type gate).
        for col_name, value in values.items():
            try:
                check_value(value, self._column_map[col_name])
            except SchemaError as exc:
                msg = f"Column {col_name!r} {exc}"
                raise SchemaError(msg) from exc

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        col_strs = [f"{c.name}:{c.data_type.value}" for c in self._columns]
        return f"Schema([{', '.join(col_strs)}])"
