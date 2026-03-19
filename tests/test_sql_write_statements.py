"""Tests for parsing and executing SQL write statements.

These tests verify that CREATE TABLE, INSERT INTO, UPDATE, DELETE, and
DROP TABLE all work end-to-end through SQL text.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import Query
from pydb.sql_parser import ParseError, parse_sql
from pydb.statements import (
    CreateTableStatement,
    DeleteStatement,
    DropTableStatement,
    InsertStatement,
    UpdateStatement,
)
from pydb.types import DataType

# Named constants.
POWER_55 = 55
POWER_52 = 52
POWER_48 = 48
POWER_60 = 60
TWO_COLUMNS = 2
THREE_ROWS = 3
ALICE_SCORE = 250


class TestParseCreateTable:
    """Verify CREATE TABLE parsing."""

    def test_basic_create(self) -> None:
        """CREATE TABLE with columns should parse correctly."""
        stmt = parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.table == "cards"
        assert len(stmt.columns) == TWO_COLUMNS
        assert stmt.columns[0].name == "name"
        assert stmt.columns[0].data_type == DataType.TEXT
        assert stmt.columns[1].name == "power"
        assert stmt.columns[1].data_type == DataType.INTEGER

    def test_all_types(self) -> None:
        """All data types should be recognised."""
        stmt = parse_sql("CREATE TABLE t (a TEXT, b INTEGER, c FLOAT, d BOOLEAN)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].data_type == DataType.TEXT
        assert stmt.columns[1].data_type == DataType.INTEGER
        assert stmt.columns[2].data_type == DataType.FLOAT
        assert stmt.columns[3].data_type == DataType.BOOLEAN

    def test_short_type_names(self) -> None:
        """INT and BOOL should be accepted as type aliases."""
        stmt = parse_sql("CREATE TABLE t (a INT, b BOOL)")
        assert isinstance(stmt, CreateTableStatement)
        assert stmt.columns[0].data_type == DataType.INTEGER
        assert stmt.columns[1].data_type == DataType.BOOLEAN

    def test_unknown_type_raises(self) -> None:
        """An unknown column type should raise ParseError."""
        with pytest.raises(ParseError, match="Unknown column type"):
            parse_sql("CREATE TABLE t (a BIGINT)")


class TestParseDropTable:
    """Verify DROP TABLE parsing."""

    def test_basic_drop(self) -> None:
        """DROP TABLE should parse correctly."""
        stmt = parse_sql("DROP TABLE cards")
        assert isinstance(stmt, DropTableStatement)
        assert stmt.table == "cards"


class TestParseInsert:
    """Verify INSERT INTO parsing."""

    def test_insert_with_columns(self) -> None:
        """INSERT with column list should parse correctly."""
        stmt = parse_sql("INSERT INTO cards (name, power) VALUES ('Pikachu', 55)")
        assert isinstance(stmt, InsertStatement)
        assert stmt.table == "cards"
        assert stmt.columns == ["name", "power"]
        assert stmt.values == ["Pikachu", POWER_55]

    def test_insert_without_columns(self) -> None:
        """INSERT without column list should parse correctly."""
        stmt = parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)")
        assert isinstance(stmt, InsertStatement)
        assert stmt.columns == []
        assert stmt.values == ["Pikachu", POWER_55]

    def test_insert_boolean_value(self) -> None:
        """Boolean values should be parsed."""
        stmt = parse_sql("INSERT INTO t VALUES (TRUE, FALSE)")
        assert isinstance(stmt, InsertStatement)
        assert stmt.values == [True, False]


class TestParseUpdate:
    """Verify UPDATE parsing."""

    def test_update_without_where(self) -> None:
        """UPDATE without WHERE should affect all rows."""
        stmt = parse_sql("UPDATE cards SET power = 60")
        assert isinstance(stmt, UpdateStatement)
        assert stmt.table == "cards"
        assert stmt.assignments == {"power": POWER_60}
        assert stmt.where is None

    def test_update_with_where(self) -> None:
        """UPDATE with WHERE should have a condition."""
        stmt = parse_sql("UPDATE cards SET power = 60 WHERE name = 'Pikachu'")
        assert isinstance(stmt, UpdateStatement)
        assert stmt.where is not None

    def test_update_multiple_assignments(self) -> None:
        """UPDATE with multiple SET clauses should parse."""
        stmt = parse_sql("UPDATE cards SET power = 60, name = 'Raichu' WHERE name = 'Pikachu'")
        assert isinstance(stmt, UpdateStatement)
        assert stmt.assignments == {"power": POWER_60, "name": "Raichu"}


class TestParseDelete:
    """Verify DELETE FROM parsing."""

    def test_delete_all(self) -> None:
        """DELETE without WHERE should delete all rows."""
        stmt = parse_sql("DELETE FROM cards")
        assert isinstance(stmt, DeleteStatement)
        assert stmt.where is None

    def test_delete_with_where(self) -> None:
        """DELETE with WHERE should have a condition."""
        stmt = parse_sql("DELETE FROM cards WHERE power < 30")
        assert isinstance(stmt, DeleteStatement)
        assert stmt.where is not None


class TestSelectStillWorks:
    """Verify that SELECT still works through the new unified parser."""

    def test_select_star(self) -> None:
        """SELECT * should still produce a Query object."""
        result = parse_sql("SELECT * FROM cards")
        assert isinstance(result, Query)

    def test_select_with_where(self) -> None:
        """SELECT with WHERE should still work."""
        result = parse_sql("SELECT name FROM cards WHERE power > 50")
        assert isinstance(result, Query)
        assert result.columns == ["name"]


def _empty_db(tmp_path: Path) -> Database:
    """Create an empty test database."""
    return Database(path=tmp_path)


def _populated_db(tmp_path: Path) -> Database:
    """Create a database with a cards table via SQL."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Charmander', 52)"), db)
    execute(parse_sql("INSERT INTO cards VALUES ('Squirtle', 48)"), db)
    return db


class TestExecuteCreateTable:
    """Verify CREATE TABLE execution."""

    def test_create_table(self, tmp_path: Path) -> None:
        """CREATE TABLE should make the table available."""
        db = _empty_db(tmp_path)
        result = execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        assert "created" in str(result[0]["result"])
        assert "cards" in db.table_names()

    def test_create_duplicate_raises(self, tmp_path: Path) -> None:
        """Creating a table that already exists should raise."""
        db = _empty_db(tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT)"), db)
        with pytest.raises(QueryError, match="CREATE TABLE failed"):
            execute(parse_sql("CREATE TABLE cards (name TEXT)"), db)


class TestExecuteDropTable:
    """Verify DROP TABLE execution."""

    def test_drop_table(self, tmp_path: Path) -> None:
        """DROP TABLE should remove the table."""
        db = _populated_db(tmp_path)
        result = execute(parse_sql("DROP TABLE cards"), db)
        assert "dropped" in str(result[0]["result"])
        assert "cards" not in db.table_names()

    def test_drop_nonexistent_raises(self, tmp_path: Path) -> None:
        """Dropping a non-existent table should raise."""
        db = _empty_db(tmp_path)
        with pytest.raises(QueryError, match="DROP TABLE failed"):
            execute(parse_sql("DROP TABLE missing"), db)


class TestExecuteInsert:
    """Verify INSERT INTO execution."""

    def test_insert_with_columns(self, tmp_path: Path) -> None:
        """INSERT with column list should add a row."""
        db = _empty_db(tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        result = execute(parse_sql("INSERT INTO cards (name, power) VALUES ('Pikachu', 55)"), db)
        assert "inserted" in str(result[0]["result"])
        rows = execute(parse_sql("SELECT * FROM cards"), db)
        assert len(rows) == 1
        assert rows[0]["name"] == "Pikachu"

    def test_insert_without_columns(self, tmp_path: Path) -> None:
        """INSERT without column list should use schema order."""
        db = _empty_db(tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 55)"), db)
        rows = execute(parse_sql("SELECT * FROM cards"), db)
        assert rows[0]["name"] == "Pikachu"
        assert rows[0]["power"] == POWER_55

    def test_insert_wrong_value_count_raises(self, tmp_path: Path) -> None:
        """INSERT with wrong number of values should raise."""
        db = _empty_db(tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        with pytest.raises(QueryError, match="Expected 2 values"):
            execute(parse_sql("INSERT INTO cards VALUES ('Pikachu')"), db)

    def test_insert_wrong_type_raises(self, tmp_path: Path) -> None:
        """INSERT with wrong value type should raise."""
        db = _empty_db(tmp_path)
        execute(parse_sql("CREATE TABLE cards (name TEXT, power INTEGER)"), db)
        with pytest.raises(QueryError, match="INSERT failed"):
            execute(parse_sql("INSERT INTO cards VALUES ('Pikachu', 'banana')"), db)


class TestExecuteUpdate:
    """Verify UPDATE execution."""

    def test_update_all_rows(self, tmp_path: Path) -> None:
        """UPDATE without WHERE should update all rows."""
        db = _populated_db(tmp_path)
        result = execute(parse_sql("UPDATE cards SET power = 60"), db)
        assert "3 rows updated" in str(result[0]["result"])

    def test_update_with_where(self, tmp_path: Path) -> None:
        """UPDATE with WHERE should only update matching rows."""
        db = _populated_db(tmp_path)
        execute(parse_sql("UPDATE cards SET power = 60 WHERE name = 'Pikachu'"), db)
        rows = execute(parse_sql("SELECT * FROM cards WHERE name = 'Pikachu'"), db)
        assert rows[0]["power"] == POWER_60

    def test_update_no_matches(self, tmp_path: Path) -> None:
        """UPDATE with no matching rows should report 0 rows."""
        db = _populated_db(tmp_path)
        result = execute(parse_sql("UPDATE cards SET power = 60 WHERE name = 'MissingNo'"), db)
        assert "0 rows updated" in str(result[0]["result"])


class TestExecuteDelete:
    """Verify DELETE FROM execution."""

    def test_delete_with_where(self, tmp_path: Path) -> None:
        """DELETE with WHERE should remove matching rows."""
        db = _populated_db(tmp_path)
        result = execute(parse_sql("DELETE FROM cards WHERE name = 'Pikachu'"), db)
        assert "1 row deleted" in str(result[0]["result"])
        rows = execute(parse_sql("SELECT * FROM cards"), db)
        assert len(rows) == TWO_COLUMNS

    def test_delete_all(self, tmp_path: Path) -> None:
        """DELETE without WHERE should remove all rows."""
        db = _populated_db(tmp_path)
        result = execute(parse_sql("DELETE FROM cards"), db)
        assert "3 rows deleted" in str(result[0]["result"])
        rows = execute(parse_sql("SELECT * FROM cards"), db)
        assert rows == []


class TestEndToEnd:
    """Verify complete workflows through SQL."""

    def test_full_lifecycle(self, tmp_path: Path) -> None:
        """Create, insert, query, update, delete, drop -- all via SQL."""
        db = _empty_db(tmp_path)

        execute(parse_sql("CREATE TABLE scores (player TEXT, score INTEGER)"), db)
        execute(parse_sql("INSERT INTO scores VALUES ('Alice', 100)"), db)
        execute(parse_sql("INSERT INTO scores VALUES ('Bob', 200)"), db)
        execute(parse_sql("INSERT INTO scores VALUES ('Charlie', 150)"), db)

        rows = execute(parse_sql("SELECT * FROM scores ORDER BY score DESC"), db)
        assert len(rows) == THREE_ROWS
        assert rows[0]["player"] == "Bob"

        execute(parse_sql("UPDATE scores SET score = 250 WHERE player = 'Alice'"), db)
        rows = execute(parse_sql("SELECT * FROM scores WHERE player = 'Alice'"), db)
        assert rows[0]["score"] == ALICE_SCORE

        execute(parse_sql("DELETE FROM scores WHERE score < 200"), db)
        rows = execute(parse_sql("SELECT * FROM scores"), db)
        assert len(rows) == TWO_COLUMNS

        execute(parse_sql("DROP TABLE scores"), db)
        assert "scores" not in db.table_names()
