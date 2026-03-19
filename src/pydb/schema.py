"""Schema definitions for PyDB tables.

A schema is the template for a table -- like the design for a trading card
that says "every card must have a Name (text), a Type (text), and a Power
(number)." The schema acts as a bouncer at the door, checking every new
record before letting it in.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from pydb.errors import SchemaError
from pydb.record import Value
from pydb.types import PYTHON_TYPES, DataType


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

        # Check types.
        for col_name, value in values.items():
            expected_type = PYTHON_TYPES[self._column_map[col_name]]
            # In Python, bool is a subclass of int. We need to distinguish them:
            # if the column expects INTEGER, reject booleans; if it expects
            # BOOLEAN, reject plain ints.
            if self._column_map[col_name] == DataType.INTEGER and isinstance(value, bool):
                msg = f"Column {col_name!r} expects {self._column_map[col_name].value}, got bool"
                raise SchemaError(msg)
            if not isinstance(value, expected_type):
                msg = (
                    f"Column {col_name!r} expects {self._column_map[col_name].value}, "
                    f"got {type(value).__name__}"
                )
                raise SchemaError(msg)

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        col_strs = [f"{c.name}:{c.data_type.value}" for c in self._columns]
        return f"Schema([{', '.join(col_strs)}])"
