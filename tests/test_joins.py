"""Tests for JOIN support.

JOINs are like stapling two card binders together -- matching rows
from one table with rows from another. These tests verify that the
parser, executor, and column resolution all work correctly.
"""

from pathlib import Path

import pytest

from pydb.database import Database
from pydb.executor import QueryError, execute
from pydb.query import JoinClause, Query
from pydb.sql_parser import parse_sql

# Named constants.
THREE_ROWS = 3
TWO_ROWS = 2
ONE_ROW = 1


def _make_db(tmp_path: Path) -> Database:
    """Create a database with trainers and pokemon tables."""
    db = Database(path=tmp_path)
    execute(parse_sql("CREATE TABLE trainers (name TEXT, town TEXT)"), db)
    execute(parse_sql("INSERT INTO trainers VALUES ('Ash', 'Pallet')"), db)
    execute(parse_sql("INSERT INTO trainers VALUES ('Misty', 'Cerulean')"), db)
    execute(parse_sql("INSERT INTO trainers VALUES ('Brock', 'Pewter')"), db)

    execute(parse_sql("CREATE TABLE pokemon (name TEXT, type TEXT, trainer TEXT)"), db)
    execute(parse_sql("INSERT INTO pokemon VALUES ('Pikachu', 'Electric', 'Ash')"), db)
    execute(parse_sql("INSERT INTO pokemon VALUES ('Starmie', 'Water', 'Misty')"), db)
    execute(parse_sql("INSERT INTO pokemon VALUES ('Charmander', 'Fire', 'Ash')"), db)
    return db


class TestParseJoin:
    """Verify JOIN clause parsing."""

    def test_basic_join(self) -> None:
        """JOIN ... ON should parse correctly."""
        result = parse_sql(
            "SELECT pokemon.name, trainers.town "
            "FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name"
        )
        assert isinstance(result, Query)
        assert result.join is not None
        assert result.join.table == "trainers"
        assert result.join.left_column == "pokemon.trainer"
        assert result.join.right_column == "trainers.name"

    def test_join_with_where(self) -> None:
        """JOIN with WHERE should parse both clauses."""
        result = parse_sql(
            "SELECT * FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name "
            "WHERE pokemon.type = 'Electric'"
        )
        assert isinstance(result, Query)
        assert result.join is not None
        assert result.where is not None

    def test_join_columns_use_dot_notation(self) -> None:
        """SELECT columns in a JOIN should support dot notation."""
        result = parse_sql(
            "SELECT pokemon.name, trainers.town FROM pokemon "
            "JOIN trainers ON pokemon.trainer = trainers.name"
        )
        assert isinstance(result, Query)
        assert result.columns == ["pokemon.name", "trainers.town"]


class TestExecuteJoin:
    """Verify JOIN execution."""

    def test_basic_join(self, tmp_path: Path) -> None:
        """A JOIN should combine matching rows from both tables."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT pokemon.name, trainers.town "
                "FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name"
            ),
            db,
        )
        assert len(rows) == THREE_ROWS
        # Ash has two pokemon, Misty has one.
        towns = {str(r["pokemon.name"]): str(r["trainers.town"]) for r in rows}
        assert towns["Pikachu"] == "Pallet"
        assert towns["Starmie"] == "Cerulean"
        assert towns["Charmander"] == "Pallet"

    def test_join_no_matches(self, tmp_path: Path) -> None:
        """A JOIN with no matching rows should return empty."""
        db = Database(path=tmp_path)
        execute(parse_sql("CREATE TABLE a (id INTEGER, val TEXT)"), db)
        execute(parse_sql("CREATE TABLE b (id INTEGER, val TEXT)"), db)
        execute(parse_sql("INSERT INTO a VALUES (1, 'x')"), db)
        execute(parse_sql("INSERT INTO b VALUES (2, 'y')"), db)
        rows = execute(
            parse_sql("SELECT * FROM a JOIN b ON a.id = b.id"),
            db,
        )
        assert rows == []

    def test_join_with_where(self, tmp_path: Path) -> None:
        """WHERE should filter joined results."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT pokemon.name, trainers.town "
                "FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name "
                "WHERE pokemon.type = 'Electric'"
            ),
            db,
        )
        assert len(rows) == ONE_ROW
        assert rows[0]["pokemon.name"] == "Pikachu"

    def test_join_with_order_by(self, tmp_path: Path) -> None:
        """ORDER BY should sort joined results."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT pokemon.name, trainers.town "
                "FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name "
                "ORDER BY pokemon.name"
            ),
            db,
        )
        names = [str(r["pokemon.name"]) for r in rows]
        assert names == ["Charmander", "Pikachu", "Starmie"]

    def test_join_with_limit(self, tmp_path: Path) -> None:
        """LIMIT should cap joined results."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql(
                "SELECT pokemon.name FROM pokemon "
                "JOIN trainers ON pokemon.trainer = trainers.name "
                "ORDER BY pokemon.name LIMIT 2"
            ),
            db,
        )
        assert len(rows) == TWO_ROWS

    def test_join_select_star(self, tmp_path: Path) -> None:
        """SELECT * with a JOIN should include all columns from both tables."""
        db = _make_db(tmp_path)
        rows = execute(
            parse_sql("SELECT * FROM pokemon JOIN trainers ON pokemon.trainer = trainers.name"),
            db,
        )
        assert len(rows) == THREE_ROWS
        first_row_keys = set(rows[0].keys())
        assert "pokemon.name" in first_row_keys
        assert "trainers.town" in first_row_keys

    def test_join_missing_table_raises(self, tmp_path: Path) -> None:
        """Joining a non-existent table should raise QueryError."""
        db = _make_db(tmp_path)
        with pytest.raises(QueryError, match="JOIN failed"):
            execute(
                parse_sql("SELECT * FROM pokemon JOIN missing ON pokemon.trainer = missing.name"),
                db,
            )


class TestJoinClauseModel:
    """Verify the JoinClause dataclass."""

    def test_join_clause_fields(self) -> None:
        """JoinClause should store table, left_column, right_column."""
        jc = JoinClause(
            table="trainers", left_column="pokemon.trainer", right_column="trainers.name"
        )
        assert jc.table == "trainers"
        assert jc.left_column == "pokemon.trainer"
        assert jc.right_column == "trainers.name"
