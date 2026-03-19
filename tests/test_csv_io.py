"""Tests for CSV import and export.

CSV is the bridge between PyDB and the outside world -- spreadsheets,
other databases, and data files.
"""

from pathlib import Path

import pytest

from pydb.csv_io import CSVError, export_table, import_csv
from pydb.schema import Column, Schema
from pydb.table import Table
from pydb.types import DataType

POWER_55 = 55
POWER_52 = 52
TWO_ROWS = 2
THREE_ROWS = 3


def _make_table() -> Table:
    """Create a test table with data."""
    schema = Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )
    table = Table(name="cards", schema=schema)
    table.insert({"name": "Pikachu", "power": POWER_55})
    table.insert({"name": "Charmander", "power": POWER_52})
    return table


class TestExport:
    """Verify CSV export."""

    def test_export_creates_file(self, tmp_path: Path) -> None:
        """Export should create a CSV file."""
        table = _make_table()
        out = tmp_path / "cards.csv"
        export_table(table, out)
        assert out.exists()

    def test_export_returns_row_count(self, tmp_path: Path) -> None:
        """Export should return the number of rows written."""
        table = _make_table()
        count = export_table(table, tmp_path / "cards.csv")
        assert count == TWO_ROWS

    def test_export_content(self, tmp_path: Path) -> None:
        """Exported CSV should contain header and data rows."""
        table = _make_table()
        out = tmp_path / "cards.csv"
        export_table(table, out)
        content = out.read_text(encoding="utf-8")
        lines = content.strip().split("\n")
        assert len(lines) == THREE_ROWS  # header + 2 data rows
        assert "name,power" in lines[0]
        assert "Pikachu,55" in lines[1]


class TestImport:
    """Verify CSV import."""

    def test_import_adds_rows(self, tmp_path: Path) -> None:
        """Import should add rows to the table."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,power\nSquirtle,48\nBulbasaur,45\n", encoding="utf-8")

        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        table = Table(name="cards", schema=schema)
        count = import_csv(table, csv_file)
        assert count == TWO_ROWS
        assert table.row_count == TWO_ROWS

    def test_import_returns_count(self, tmp_path: Path) -> None:
        """Import should return the number of rows imported."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,power\nPikachu,55\n", encoding="utf-8")

        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        table = Table(name="cards", schema=schema)
        count = import_csv(table, csv_file)
        assert count == 1

    def test_import_coerces_types(self, tmp_path: Path) -> None:
        """Import should convert strings to the correct types."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,power\nPikachu,55\n", encoding="utf-8")

        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        table = Table(name="cards", schema=schema)
        import_csv(table, csv_file)
        record = table.select()[0]
        assert isinstance(record["power"], int)
        assert record["power"] == POWER_55

    def test_import_missing_file_raises(self, tmp_path: Path) -> None:
        """Importing a non-existent file should raise CSVError."""
        schema = Schema(columns=[Column(name="x", data_type=DataType.TEXT)])
        table = Table(name="t", schema=schema)
        with pytest.raises(CSVError, match="not found"):
            import_csv(table, tmp_path / "missing.csv")

    def test_import_bad_type_raises(self, tmp_path: Path) -> None:
        """Importing a value that can't be converted should raise CSVError."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("val\nnotanumber\n", encoding="utf-8")

        schema = Schema(columns=[Column(name="val", data_type=DataType.INTEGER)])
        table = Table(name="t", schema=schema)
        with pytest.raises(CSVError, match="cannot convert"):
            import_csv(table, csv_file)


class TestRoundTrip:
    """Verify export → import preserves data."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Exporting then importing should preserve the data."""
        original = _make_table()
        csv_file = tmp_path / "cards.csv"
        export_table(original, csv_file)

        schema = Schema(
            columns=[
                Column(name="name", data_type=DataType.TEXT),
                Column(name="power", data_type=DataType.INTEGER),
            ]
        )
        restored = Table(name="cards_copy", schema=schema)
        import_csv(restored, csv_file)

        assert restored.row_count == original.row_count
        orig_records = original.select()
        rest_records = restored.select()
        for orig, rest in zip(orig_records, rest_records, strict=True):
            assert orig["name"] == rest["name"]
            assert orig["power"] == rest["power"]
