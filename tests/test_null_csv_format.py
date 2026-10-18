"""Tests for NULL in CSV files and in printed results.

A CSV file has no way to write "unknown", so NULL becomes an empty field.
Reading it back, an empty field means NULL -- except in a TEXT column,
where an empty field could just as well be the empty string ``""``.
"""

from datetime import date
from pathlib import Path

import pytest

from pydb.csv_io import CSVError, export_table, import_csv
from pydb.formatter import format_results
from pydb.schema import Column, Schema
from pydb.table import Table
from pydb.types import DataType

SEVEN = 7
TWO_ROWS = 2
DAY = date(2026, 10, 18)


def _table() -> Table:
    """Return a table with nullable columns of several types."""
    return Table(
        "jobs",
        Schema(
            [
                Column("name", DataType.TEXT, not_null=True),
                Column("code", DataType.INTEGER),
                Column("ok", DataType.BOOLEAN),
                Column("day", DataType.DATE),
                Column("note", DataType.TEXT),
            ]
        ),
    )


class TestCsv:
    """NULL is an empty CSV field."""

    def test_export_writes_empty_fields(self, tmp_path: Path) -> None:
        """Each NULL is written as nothing between the commas."""
        table = _table()
        table.insert({"name": "a"})
        path = tmp_path / "out.csv"
        export_table(table, path)
        assert path.read_text(encoding="utf-8").splitlines()[1] == "a,,,,"

    def test_round_trip(self, tmp_path: Path) -> None:
        """Export then import keeps NULLs (TEXT comes back as an empty string)."""
        source = _table()
        source.insert({"name": "a"})
        source.insert({"name": "b", "code": SEVEN, "ok": False, "day": DAY, "note": "hi"})
        path = tmp_path / "jobs.csv"
        export_table(source, path)
        target = _table()
        assert import_csv(target, path) == TWO_ROWS
        first, second = (r.data for r in target.select())
        assert first == {"name": "a", "code": None, "ok": None, "day": None, "note": ""}
        assert second == {"name": "b", "code": SEVEN, "ok": False, "day": DAY, "note": "hi"}

    def test_empty_field_in_not_null_column_is_an_error(self, tmp_path: Path) -> None:
        """A NOT NULL non-TEXT column refuses an empty field."""
        table = Table("t", Schema([Column("code", DataType.INTEGER, not_null=True)]))
        path = tmp_path / "t.csv"
        path.write_text('code\n1\n""\n', encoding="utf-8")
        with pytest.raises(CSVError, match="cannot be NULL"):
            import_csv(table, path)

    def test_missing_csv_column_is_null(self, tmp_path: Path) -> None:
        """A nullable column absent from the CSV header is NULL."""
        path = tmp_path / "t.csv"
        path.write_text("name\nz\n", encoding="utf-8")
        table = _table()
        import_csv(table, path)
        assert table.get(1)["code"] is None


class TestFormatter:
    """The formatter prints NULL."""

    def test_null_is_shown(self) -> None:
        """None appears as ``NULL``, never as ``None``."""
        text = format_results([{"name": "a", "code": None}, {"name": "b", "code": SEVEN}])
        assert "NULL" in text
        assert "None" not in text
