"""Serialize and deserialize tables to and from JSON.

Think of the serializer as a translator. Your trading cards exist as Python
objects in memory, but to save them to a file you need to write them down in
a language that files understand -- JSON. The serializer translates back and
forth so nothing gets lost.
"""

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, cast

from pydb.errors import PyDBError, SchemaError
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.types import DataType, from_iso, to_iso


class SerializationError(PyDBError):
    """Raise when data cannot be serialized or deserialized."""


# What Python raises when a file has the right labels but the wrong things
# under them: a column with no "name" (KeyError), a number where a list
# belongs (TypeError), text where a number belongs (ValueError),
# ``Infinity`` as a record ID (OverflowError), a schema with no columns or a
# bad date (SchemaError).
_MALFORMED = (KeyError, TypeError, ValueError, OverflowError, SchemaError)


@contextmanager
def _refuse_malformed(what: str) -> Iterator[None]:
    """Turn any error from reading malformed *what* data into a ``SerializationError``."""
    try:
        yield
    except _MALFORMED as exc:
        msg = f"Invalid {what} data: {exc}"
        raise SerializationError(msg) from exc


def _value_to_json(value: Value) -> str | int | float | bool | None:
    """Convert a record value to a JSON-safe Python type.

    Text, numbers and booleans are already JSON-safe, and NULL (``None``)
    becomes JSON ``null``; dates and timestamps become ISO text.
    """
    if isinstance(value, date):
        return to_iso(value)
    return value


def _value_from_json(raw: str | int | float | bool | None, data_type: DataType) -> Value:
    """Convert a JSON value back to the correct Python type.

    JSON doesn't distinguish int from float in all cases, so we cast
    explicitly based on the column's declared type.

    Args:
        raw: The value read from JSON.
        data_type: The expected data type from the schema.

    Returns:
        The value cast to the correct Python type (``null`` stays NULL).

    """
    if raw is None:
        return None
    match data_type:
        case DataType.TEXT:
            return str(raw)
        case DataType.INTEGER:
            return int(raw)
        case DataType.FLOAT:
            return float(raw)
        case DataType.BOOLEAN:
            return raw if isinstance(raw, bool) else bool(raw)
        case DataType.DATE | DataType.TIMESTAMP:
            return cast(Value, from_iso(str(raw), data_type))
        case _:  # pragma: no cover
            msg = f"Unsupported data type: {data_type}"
            raise SerializationError(msg)


def serialize_schema(schema: Schema) -> dict[str, Any]:
    """Convert a schema to a JSON-friendly dictionary.

    Args:
        schema: The schema to serialize.

    Returns:
        A dictionary with a "columns" key containing column definitions.

    """
    col_defs: list[dict[str, str | bool]] = []
    for col in schema.columns:
        col_dict: dict[str, str | bool] = {"name": col.name, "data_type": col.data_type.value}
        if col.primary_key:
            col_dict["primary_key"] = True
        if col.not_null:
            col_dict["not_null"] = True
        if col.unique:
            col_dict["unique"] = True
        col_defs.append(col_dict)
    return {"columns": col_defs}


def deserialize_schema(data: dict[str, Any]) -> Schema:
    """Reconstruct a schema from a JSON-friendly dictionary.

    Args:
        data: A dictionary produced by ``serialize_schema``.

    Returns:
        The reconstructed schema.

    Raises:
        SerializationError: If the data is missing required fields or is
            the wrong shape.

    """
    with _refuse_malformed("schema"):
        return _read_schema(data)


def _read_schema(data: dict[str, Any]) -> Schema:
    """Build a schema from its dictionary (``deserialize_schema`` catches the errors)."""
    try:
        columns_data: list[dict[str, Any]] = data["columns"]
    except KeyError:
        msg = "Schema data missing 'columns' key"
        raise SerializationError(msg) from None

    columns: list[Column] = []
    for col_data in columns_data:
        name: Any = col_data["name"]
        if not isinstance(name, str):
            msg = f"Column name must be text, got {name!r}"
            raise SerializationError(msg)
        try:
            columns.append(
                Column(
                    name=name,
                    data_type=DataType(col_data["data_type"]),
                    primary_key=bool(col_data.get("primary_key", False)),
                    not_null=bool(col_data.get("not_null", False)),
                    unique=bool(col_data.get("unique", False)),
                )
            )
        except (KeyError, ValueError) as exc:
            msg = f"Invalid column definition: {col_data}"
            raise SerializationError(msg) from exc

    return Schema(columns=columns)


def serialize_record(record: Record) -> dict[str, Any]:
    """Convert a record to a JSON-friendly dictionary.

    Args:
        record: The record to serialize.

    Returns:
        A dictionary with "record_id" and "data" keys.

    """
    return {
        "record_id": record.record_id,
        "data": {k: _value_to_json(v) for k, v in record.data.items()},
    }


def deserialize_record(
    data: dict[str, Any],
    schema: Schema,
) -> Record:
    """Reconstruct a record from a JSON-friendly dictionary.

    Args:
        data: A dictionary produced by ``serialize_record``.
        schema: The schema to use for type coercion.

    Returns:
        The reconstructed record.

    Raises:
        SerializationError: If the data is missing required fields or is
            the wrong shape.

    """
    with _refuse_malformed("record"):
        return _read_record(data, schema)


def _read_record(data: dict[str, Any], schema: Schema) -> Record:
    """Build a record from its dictionary (``deserialize_record`` catches the errors)."""
    try:
        record_id = int(data["record_id"])
        raw_data: Any = data["data"]
    except (KeyError, TypeError, ValueError) as exc:
        msg = f"Invalid record data: {data}"
        raise SerializationError(msg) from exc

    if not isinstance(raw_data, dict):
        msg = f"Record 'data' must be a dictionary, got {type(raw_data).__name__}"
        raise SerializationError(msg)

    # Cast to a known type after the isinstance guard.
    typed_data = cast(dict[str, Any], raw_data)

    # Build a column-name → DataType lookup from the schema.
    type_map = {col.name: col.data_type for col in schema.columns}

    values: dict[str, Value] = {}
    for col_name, raw_value in typed_data.items():
        if col_name not in type_map:
            msg = f"Unknown column {col_name!r} not in schema"
            raise SerializationError(msg)
        if raw_value is not None and not isinstance(raw_value, str | int | float | bool):
            msg = f"Unsupported value type for column {col_name!r}: {type(raw_value).__name__}"
            raise SerializationError(msg)
        values[col_name] = _value_from_json(raw_value, type_map[col_name])

    return Record(record_id=record_id, data=values)


@dataclass(frozen=True, slots=True)
class StoredTable:
    """Hold everything a table file contains.

    Args:
        name: The table name.
        schema: The table's schema.
        records: All records in the table.
        next_id: The next auto-increment ID.
        indexes: ``(name, column)`` for each index (empty for old files).

    """

    name: str
    schema: Schema
    records: list[Record]
    next_id: int
    indexes: list[tuple[str, str]]


def serialize_table_data(
    name: str,
    schema: Schema,
    records: list[Record],
    next_id: int,
    *,
    indexes: Sequence[tuple[str, str]] = (),
) -> str:
    """Serialize a table's complete state to a JSON string.

    Args:
        name: The table name.
        schema: The table's schema.
        records: All records in the table.
        next_id: The next auto-increment ID.
        indexes: ``(name, column)`` for each index, stored as
            ``"indexes": [{"name": ..., "column": ...}]``.

    Returns:
        A JSON string representing the full table state.

    """
    payload: dict[str, Any] = {
        "name": name,
        "schema": serialize_schema(schema),
        "next_id": next_id,
        "indexes": [{"name": index_name, "column": column} for index_name, column in indexes],
        "records": [serialize_record(r) for r in records],
    }
    return json.dumps(payload, indent=2)


def _deserialize_indexes(raw: Any) -> list[tuple[str, str]]:
    """Read the ``"indexes"`` list, checking each entry has a name and column.

    Raises:
        SerializationError: If the list or an entry is malformed.

    """
    if not isinstance(raw, list):
        msg = "Table 'indexes' must be a list"
        raise SerializationError(msg)
    definitions: list[tuple[str, str]] = []
    for entry in cast(list[Any], raw):
        if not isinstance(entry, dict):
            msg = f"Invalid index definition: {entry!r}"
            raise SerializationError(msg)
        typed = cast(dict[str, Any], entry)
        index_name, column = typed.get("name"), typed.get("column")
        if not isinstance(index_name, str) or not isinstance(column, str):
            msg = f"Invalid index definition: {entry!r}"
            raise SerializationError(msg)
        definitions.append((index_name, column))
    return definitions


def deserialize_table_data(
    json_str: str,
) -> tuple[str, Schema, list[Record], int]:
    """Deserialize a JSON string back into table components.

    Args:
        json_str: A JSON string produced by ``serialize_table_data``.

    Returns:
        A tuple of (name, schema, records, next_id).

    Raises:
        SerializationError: If the JSON is invalid or missing fields.

    """
    stored = deserialize_table(json_str)
    return stored.name, stored.schema, stored.records, stored.next_id


def deserialize_table(json_str: str) -> StoredTable:
    """Deserialize a JSON string into a ``StoredTable``, index list included.

    Files written before index definitions were saved have no
    ``"indexes"`` key; they load with no indexes.

    Args:
        json_str: A JSON string produced by ``serialize_table_data``.

    Returns:
        The table's stored state.

    Raises:
        SerializationError: If the JSON is invalid, missing fields or the
            wrong shape.

    """
    try:
        payload: Any = json.loads(json_str)
    # ValueError covers JSONDecodeError and a number too long to read;
    # RecursionError is lists nested thousands deep.
    except (ValueError, RecursionError) as exc:
        msg = f"Invalid JSON: {exc}"
        raise SerializationError(msg) from exc

    with _refuse_malformed("table"):
        return _read_table(payload)


def _read_table(payload: Any) -> StoredTable:
    """Build a ``StoredTable`` from parsed JSON (``deserialize_table`` catches the errors)."""
    if not isinstance(payload, dict):
        msg = "Table data must be a JSON object"
        raise SerializationError(msg)

    typed_payload = cast(dict[str, Any], payload)
    try:
        name = str(typed_payload["name"])
        schema_data: dict[str, Any] = typed_payload["schema"]
        next_id = int(typed_payload["next_id"])
        records_data: list[dict[str, Any]] = typed_payload["records"]
    except (KeyError, TypeError, ValueError) as exc:
        msg = f"Table data missing required field: {exc}"
        raise SerializationError(msg) from exc

    schema = deserialize_schema(schema_data)
    records = [deserialize_record(r, schema) for r in records_data]
    indexes = _deserialize_indexes(typed_payload.get("indexes", []))

    return StoredTable(name, schema, records, next_id, indexes)
