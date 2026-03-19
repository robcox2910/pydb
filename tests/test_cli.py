"""Tests for the CLI dot commands and SQL execution.

The REPL ties everything together. These tests verify the helper
functions that handle dot commands and SQL execution without needing
an interactive terminal.
"""

from pathlib import Path

from pydb.cli import _execute_sql, _handle_dot_command
from pydb.database import Database
from pydb.schema import Column, Schema
from pydb.types import DataType

POWER_55 = 55
POWER_52 = 52


def _make_db(tmp_path: Path) -> Database:
    """Create a test database with a populated cards table."""
    db = Database(path=tmp_path)
    schema = Schema(
        columns=[
            Column(name="name", data_type=DataType.TEXT),
            Column(name="power", data_type=DataType.INTEGER),
        ]
    )
    table = db.create_table("cards", schema)
    table.insert({"name": "Pikachu", "power": POWER_55})
    table.insert({"name": "Charmander", "power": POWER_52})
    return db


class TestDotCommands:
    """Verify dot command handling."""

    def test_quit_returns_none(self, tmp_path: Path) -> None:
        """.quit should return None to signal exit."""
        db = _make_db(tmp_path)
        assert _handle_dot_command(".quit", db) is None

    def test_exit_returns_none(self, tmp_path: Path) -> None:
        """.exit should also return None."""
        db = _make_db(tmp_path)
        assert _handle_dot_command(".exit", db) is None

    def test_help_returns_text(self, tmp_path: Path) -> None:
        """.help should return help text."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".help", db)
        assert result is not None
        assert "SQL commands" in result

    def test_tables_lists_tables(self, tmp_path: Path) -> None:
        """.tables should list all table names."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".tables", db)
        assert result is not None
        assert "cards" in result

    def test_tables_empty_db(self, tmp_path: Path) -> None:
        """.tables on empty database should say no tables."""
        db = Database(path=tmp_path)
        result = _handle_dot_command(".tables", db)
        assert result == "(no tables)"

    def test_schema_shows_columns(self, tmp_path: Path) -> None:
        """.schema should show column names and types."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".schema cards", db)
        assert result is not None
        assert "name" in result
        assert "TEXT" in result
        assert "power" in result
        assert "INTEGER" in result

    def test_schema_missing_arg(self, tmp_path: Path) -> None:
        """.schema without a table name should show usage."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".schema", db)
        assert result is not None
        assert "Usage" in result

    def test_schema_unknown_table(self, tmp_path: Path) -> None:
        """.schema for a missing table should show an error."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".schema missing", db)
        assert result is not None
        assert "not found" in result

    def test_save_command(self, tmp_path: Path) -> None:
        """.save should save tables and return a confirmation message."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".save", db)
        assert result is not None
        assert "saved" in result
        assert (tmp_path / "cards.json").exists()

    def test_unknown_command(self, tmp_path: Path) -> None:
        """An unknown dot command should return an error."""
        db = _make_db(tmp_path)
        result = _handle_dot_command(".bogus", db)
        assert result is not None
        assert "Unknown command" in result


class TestSQLExecution:
    """Verify SQL execution through the CLI helper."""

    def test_select_all(self, tmp_path: Path) -> None:
        """SELECT * should return formatted results."""
        db = _make_db(tmp_path)
        result = _execute_sql("SELECT * FROM cards", db)
        assert "Pikachu" in result
        assert "Charmander" in result
        assert "2 rows returned" in result

    def test_select_with_where(self, tmp_path: Path) -> None:
        """SELECT with WHERE should filter results."""
        db = _make_db(tmp_path)
        result = _execute_sql("SELECT name FROM cards WHERE name = 'Pikachu'", db)
        assert "Pikachu" in result
        assert "1 row returned" in result

    def test_parse_error(self, tmp_path: Path) -> None:
        """Invalid SQL should return a parse error message."""
        db = _make_db(tmp_path)
        result = _execute_sql("SELEKT * FROM cards", db)
        assert "Parse error" in result

    def test_query_error(self, tmp_path: Path) -> None:
        """Querying a missing table should return a query error."""
        db = _make_db(tmp_path)
        result = _execute_sql("SELECT * FROM missing", db)
        assert "Query error" in result

    def test_empty_result(self, tmp_path: Path) -> None:
        """A query with no matches should show empty result."""
        db = _make_db(tmp_path)
        result = _execute_sql("SELECT * FROM cards WHERE name = 'MissingNo'", db)
        assert "empty result set" in result
