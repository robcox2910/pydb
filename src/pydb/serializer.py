"""Serialize and deserialize tables to and from JSON.

Think of the serializer as a translator. Your trading cards exist as Python
objects in memory, but to save them to a file you need to write them down in
a language that files understand -- JSON. The serializer translates back and
forth so nothing gets lost.
"""

import json
from typing import Any, cast

from pydb.errors import PyDBError
from pydb.record import Record, Value
from pydb.schema import Column, Schema
from pydb.types import DataType


class SerializationError(PyDBError):
    """Raise when data cannot be serialized or deserialized."""


def _value_to_json(value: Value) -> str | int | float | bool:
    """Convert a record value to a JSON-safe Python type.

    All Value types (str, int, float, bool) are already JSON-safe, so this
    is effectively an identity function -- but it gives us a hook if we add
    more types later.
    """
    return value


def _value_from_json(raw: str | int | float | bool, data_type: DataType) -> Value:
    """Convert a JSON value back to the correct Python type.

    JSON doesn't distinguish int from float in all cases, so we cast
    explicitly based on the column's declared type.

    Args:
        raw: The value read from JSON.
        data_type: The expected data type from the schema.

    Returns:
        The value cast to the correct Python type.

    """
    match data_type:
        case DataType.TEXT:
            return str(raw)
        case DataType.INTEGER:
            return int(raw)
        case DataType.FLOAT:
            return float(raw)
        case DataType.BOOLEAN:
            if not isinstance(raw, bool):
                return bool(raw)
            return raw


def serialize_schema(schema: Schema) -> dict[str, Any]:
    """Convert a schema to a JSON-friendly dictionary.

    Args:
        schema: The schema to serialize.

    Returns:
        A dictionary with a "columns" key containing column definitions.

    """
    return {
        "columns": [
            {"name": col.name, "data_type": col.data_type.value}
            for col in schema.columns
        ],
    }


def deserialize_schema(data: dict[str, Any]) -> Schema:
    """Reconstruct a schema from a JSON-friendly dictionary.

    Args:
        data: A dictionary produced by ``serialize_schema``.

    Returns:
        The reconstructed schema.

    Raises:
        SerializationError: If the data is missing required fields.

    """
    try:
        columns_data: list[dict[str, str]] = data["columns"]
    except KeyError:
        msg = "Schema data missing 'columns' key"
        raise SerializationError(msg) from None

    columns: list[Column] = []
    for col_data in columns_data:
        try:
            columns.append(Column(
                name=col_data["name"],
                data_type=DataType(col_data["data_type"]),
            ))
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
        SerializationError: If the data is missing required fields.

    """
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
        if not isinstance(raw_value, str | int | float | bool):
            msg = f"Unsupported value type for column {col_name!r}: {type(raw_value).__name__}"
            raise SerializationError(msg)
        values[col_name] = _value_from_json(raw_value, type_map[col_name])

    return Record(record_id=record_id, data=values)


def serialize_table_data(
    name: str,
    schema: Schema,
    records: list[Record],
    next_id: int,
) -> str:
    """Serialize a table's complete state to a JSON string.

    Args:
        name: The table name.
        schema: The table's schema.
        records: All records in the table.
        next_id: The next auto-increment ID.

    Returns:
        A JSON string representing the full table state.

    """
    payload: dict[str, Any] = {
        "name": name,
        "schema": serialize_schema(schema),
        "next_id": next_id,
        "records": [serialize_record(r) for r in records],
    }
    return json.dumps(payload, indent=2)


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
    try:
        payload: Any = json.loads(json_str)
    except json.JSONDecodeError as exc:
        msg = f"Invalid JSON: {exc}"
        raise SerializationError(msg) from exc

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

    return name, schema, records, next_id
