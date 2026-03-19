"""CSV import and export for PyDB tables.

Load data from a spreadsheet (CSV file) into a table, or export a
table's data to a CSV file. This bridges PyDB to the real world --
kids can import their own data or share it with other tools.
"""

import csv
from pathlib import Path

from pydb.errors import PyDBError
from pydb.record import Value
from pydb.table import Table
from pydb.types import DataType


class CSVError(PyDBError):
    """Raise when a CSV operation fails."""


def export_table(table: Table, file_path: str | Path) -> int:
    """Export a table's data to a CSV file.

    Args:
        table: The table to export.
        file_path: The output file path.

    Returns:
        The number of rows written.

    """
    path = Path(file_path)
    columns = table.schema.column_names
    records = table.select()

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for record in records:
            writer.writerow([record[col] for col in columns])

    return len(records)


def import_csv(table: Table, file_path: str | Path) -> int:
    """Import data from a CSV file into an existing table.

    The CSV file's first row must be a header with column names that
    match the table's schema. Values are coerced to the column's
    data type.

    Args:
        table: The table to import into.
        file_path: The input CSV file path.

    Returns:
        The number of rows imported.

    Raises:
        CSVError: If the file can't be read or values don't match.

    """
    path = Path(file_path)
    if not path.exists():
        msg = f"CSV file not found: {path}"
        raise CSVError(msg)

    type_map = {col.name: col.data_type for col in table.schema.columns}

    count = 0
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Row 2 = first data row.
            values: dict[str, Value] = {}
            for col_name, raw_value in row.items():
                if col_name is None or col_name not in type_map:
                    continue
                val = raw_value if raw_value is not None else ""
                values[col_name] = _coerce_value(val, type_map[col_name], col_name, row_num)
            table.insert(values)
            count += 1

    return count


def _coerce_value(raw: str, data_type: DataType, col_name: str, row_num: int) -> Value:
    """Convert a CSV string value to the correct Python type.

    Args:
        raw: The raw string from the CSV.
        data_type: The expected data type.
        col_name: The column name (for error messages).
        row_num: The CSV row number (for error messages).

    Returns:
        The coerced value.

    Raises:
        CSVError: If the value can't be converted.

    """
    try:
        match data_type:
            case DataType.TEXT:
                return raw
            case DataType.INTEGER:
                return int(raw)
            case DataType.FLOAT:
                return float(raw)
            case DataType.BOOLEAN:
                return raw.lower() in ("true", "1", "yes")
    except (ValueError, TypeError) as exc:
        msg = f"Row {row_num}: cannot convert {raw!r} to {data_type.value} for column {col_name!r}"
        raise CSVError(msg) from exc
    msg = f"Unsupported data type: {data_type}"  # pragma: no cover
    raise CSVError(msg)  # pragma: no cover
