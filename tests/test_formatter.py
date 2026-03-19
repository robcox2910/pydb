"""Tests for the result formatter.

The formatter turns query results into pretty ASCII tables. These tests
verify the output looks right.
"""

from pydb.formatter import format_results
from pydb.record import Value

POWER_55 = 55
POWER_52 = 52


def _rows(*dicts: dict[str, Value]) -> list[dict[str, Value]]:
    """Create a typed row list for format_results."""
    return list(dicts)


class TestFormatResults:
    """Verify table formatting."""

    def test_empty_results(self) -> None:
        """Empty results should show a message."""
        assert format_results([]) == "(empty result set)"

    def test_single_row(self) -> None:
        """A single row should produce a table with one data line."""
        result = format_results(_rows({"name": "Pikachu", "power": POWER_55}))
        assert "Pikachu" in result
        assert "55" in result
        assert "1 row returned" in result

    def test_multiple_rows(self) -> None:
        """Multiple rows should all appear."""
        result = format_results(
            _rows(
                {"name": "Pikachu", "power": POWER_55},
                {"name": "Charmander", "power": POWER_52},
            )
        )
        assert "Pikachu" in result
        assert "Charmander" in result
        assert "2 rows returned" in result

    def test_column_headers(self) -> None:
        """Column names should appear as headers."""
        result = format_results(_rows({"name": "Pikachu"}))
        assert "name" in result

    def test_custom_column_order(self) -> None:
        """Custom column order should be respected."""
        result = format_results(
            _rows({"name": "Pikachu", "power": POWER_55}),
            columns=["power", "name"],
        )
        lines = result.split("\n")
        header = lines[1]
        power_pos = header.index("power")
        name_pos = header.index("name")
        assert power_pos < name_pos

    def test_border_characters(self) -> None:
        """Output should include box-drawing border characters."""
        result = format_results(_rows({"val": 1}))
        assert "┌" in result
        assert "┘" in result
        assert "│" in result

    def test_boolean_formatting(self) -> None:
        """Booleans should display as lowercase true/false."""
        result = format_results(_rows({"active": True}))
        assert "true" in result

    def test_numbers_right_aligned(self) -> None:
        """Numeric values should be right-aligned in their column."""
        result = format_results(_rows({"power": 5}, {"power": POWER_55}))
        lines = result.split("\n")
        data_lines = [row for row in lines if row.startswith("│") and "power" not in row]
        assert " 5" in data_lines[0]
